package lsm

import (
	"OscarKV/kv"
	"OscarKV/pb"
	"OscarKV/sstable"
	"OscarKV/utils"
	"bytes"
	"container/list"
	"context"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type levelManager struct {
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	levelsRWMutex      []sync.RWMutex
	levels             []*pb.Levels
	ckptManager        *checkpointManager
	ingest             map[int]*pb.IngestBuffer
	cache              map[uint64]*tableCacheEntry
	cacheMu            sync.Mutex
	cacheList          *list.List
	tableCacheMax      int
	tableCacheTTL      int64
	nextTableID        int64
	levelOption        levelOption
	flushl0            chan *memtable
	trigerCh           chan struct{}
	levelStats         []*levelStat
	tableStats         map[uint64]tableStat
	trigerLevelStatCh  chan *levelStat
	trigerCompactionCh chan chan error
}

type levelOption struct {
	nextTableID         int64
	tableSize           int
	dataBlockSize       int
	rootDir             string
	dataDir             string
	compactionBlockSize int
	compactionTableSize int

	l0Nums        int8
	maxLevel      int8
	baseLevelSize int
	ingestNums    int
	levelSizeExp  int

	l0ForcedCompactPercentage            float32
	compactionPickL0Coefficient          float32
	compactionPickSpaceCoefficient       float32
	compactionPickMissedCoefficient      float32
	compactionPickStaleCoefficient       float32
	compactionPickCreatedCoefficient     float32
	compactionPickTableMissedCoefficient float32
}

func (opt levelOption) santanize() levelOption {
	if opt.tableSize == 0 {
		opt.tableSize = 64 << 20
	}
	if opt.dataBlockSize == 0 {
		opt.dataBlockSize = 4 << 10
	}
	if opt.compactionBlockSize == 0 {
		opt.compactionBlockSize = 1 << 10
	}
	if opt.compactionTableSize == 0 {
		opt.compactionTableSize = 8 << 20
	}
	if opt.l0Nums == 0 {
		opt.l0Nums = 8
	}
	if opt.maxLevel == 0 {
		opt.maxLevel = 7
	}
	if opt.baseLevelSize == 0 {
		opt.baseLevelSize = 1024 << 20
	}
	if opt.ingestNums == 0 {
		opt.ingestNums = 4
	}
	if opt.levelSizeExp == 0 {
		opt.levelSizeExp = 10
	}
	if opt.l0ForcedCompactPercentage == 0 {
		opt.l0ForcedCompactPercentage = 0.8
	}
	if opt.compactionPickL0Coefficient == 0 {
		opt.compactionPickL0Coefficient = 0.4
	}
	if opt.compactionPickSpaceCoefficient == 0 {
		opt.compactionPickSpaceCoefficient = 0.5
	}
	if opt.compactionPickMissedCoefficient == 0 {
		opt.compactionPickMissedCoefficient = 0.25
	}
	if opt.compactionPickStaleCoefficient == 0 {
		opt.compactionPickStaleCoefficient = 0.25
	}
	if opt.compactionPickCreatedCoefficient == 0 && opt.compactionPickTableMissedCoefficient == 0 {
		opt.compactionPickCreatedCoefficient = 0.5
		opt.compactionPickTableMissedCoefficient = 0.5
	} else {
		if opt.compactionPickCreatedCoefficient < 0 {
			opt.compactionPickCreatedCoefficient = 0
		}
		if opt.compactionPickTableMissedCoefficient < 0 {
			opt.compactionPickTableMissedCoefficient = 0
		}
		sum := opt.compactionPickCreatedCoefficient + opt.compactionPickTableMissedCoefficient
		if sum == 0 {
			opt.compactionPickCreatedCoefficient = 0.5
			opt.compactionPickTableMissedCoefficient = 0.5
		} else {
			opt.compactionPickCreatedCoefficient /= sum
			opt.compactionPickTableMissedCoefficient /= sum
		}
	}
	return opt
}

func newLevelManager(opt levelOption, ckptManager *checkpointManager) *levelManager {
	opt = opt.santanize()
	if err := os.MkdirAll(filepath.Join(opt.rootDir, opt.dataDir), 0755); err != nil {
		panic(err)
	}
	levelManager := &levelManager{
		levelOption:        opt,
		cache:              make(map[uint64]*tableCacheEntry, 128),
		cacheList:          list.New(),
		tableCacheMax:      1000,
		tableCacheTTL:      180,
		flushl0:            make(chan *memtable, 8),
		levels:             *ckptManager.levels.Load(),
		ingest:             make(map[int]*pb.IngestBuffer, 16),
		trigerCh:           make(chan struct{}, 1),
		trigerCompactionCh: make(chan chan error),
		trigerLevelStatCh:  make(chan *levelStat),
		ckptManager:        ckptManager,
	}
	levelManager.levelsRWMutex = make([]sync.RWMutex, len(levelManager.levels))
	for _, ib := range *ckptManager.ingestBuffer.Load() {
		levelManager.ingest[int(ib.Level)] = ib
	}
	levelManager.tableStats = make(map[uint64]tableStat)
	levelManager.levelStats = levelManager.initLevelStats()
	levelManager.ctx, levelManager.cancel = context.WithCancel(context.Background())
	levelManager.wg.Add(2)
	go levelManager.worker()
	go levelManager.cacheWorker()
	return levelManager
}

func (lm *levelManager) initLevelStats() []*levelStat {
	levels := lm.levels
	if levels == nil {
		return nil
	}
	stats := make([]*levelStat, len(levels))
	for i := range stats {
		stats[i] = &levelStat{level: int8(i)}
	}

	for level := range levels {
		if levels[level] == nil {
			continue
		}
		var used int
		var stale int
		for _, t := range levels[level].Tables {
			if t == nil {
				continue
			}
			used += int(t.Size)
			stale += int(t.Stale)
			if _, ok := lm.tableStats[t.Id]; !ok {
				lm.tableStats[t.Id] = tableStat{
					createdAt: t.CreatedAt,
				}
			}
		}
		stats[level].tableNums = len(levels[level].Tables)
		stats[level].usedSpace = used
		stats[level].staleSpace = stale
	}

	caps := lm.computeLevelCapacities(stats)
	for i := range stats {
		stats[i].capacity = caps[i]
	}

	return stats
}

func (lm *levelManager) computeLevelCapacities(stats []*levelStat) []int {
	caps := make([]int, len(stats))
	if len(caps) == 0 {
		return caps
	}

	caps[0] = int(lm.levelOption.l0Nums)
	if len(caps) == 1 {
		return caps
	}

	base := lm.levelOption.baseLevelSize
	if base <= 0 {
		base = 1
	}
	exp := lm.levelOption.levelSizeExp
	if exp <= 1 {
		exp = 2
	}

	last := len(caps) - 1
	lastUsed := 0
	if stats[last] != nil {
		lastUsed = stats[last].usedSpace
	}
	if lastUsed < base {
		lastUsed = base
	}
	caps[last] = lastUsed

	for i := last - 1; i >= 1; i-- {
		capacity := caps[i+1] / exp
		if capacity < base {
			capacity = base
		}
		caps[i] = capacity
	}

	return caps
}

func (lm *levelManager) flushImmutableMemTable(m *memtable) error {
	lm.flushl0 <- m
	return nil
}

func (lm *levelManager) Get(key []byte) ([]byte, error) {
	if len(lm.levels) > 0 {
		lm.levelsRWMutex[0].RLock()
		// Search L0 (newest first)
		for i := len(lm.levels[0].Tables) - 1; i >= 0; i-- {
			t := lm.levels[0].Tables[i]
			if utils.CompareKey(key, t.MinKey) >= 0 && utils.CompareKey(key, t.MaxKey) <= 0 {
				val, err := lm.getFromTable(t.Id, key)
				if err == nil {
					return val, nil
				}
			}
		}
		lm.levelsRWMutex[0].RUnlock()
	}

	// Search L1-L7
	for i := 1; i < len(lm.levels); i++ {
		lm.levelsRWMutex[i].RLock()
		level := lm.levels[i]
		levelTables := level.Tables
		idx := sort.Search(len(levelTables), func(j int) bool {
			return utils.CompareKey(levelTables[j].MaxKey, key) >= 0
		})

		if idx < len(levelTables) {
			t := levelTables[idx]
			if utils.CompareKey(key, t.MinKey) >= 0 {
				val, err := lm.getFromTable(t.Id, key)
				if err == nil {
					return val, nil
				}
			}
		}
		lm.levelsRWMutex[i].RUnlock()
	}

	return nil, ErrKeyNotFound
}

func (lm *levelManager) getFromTable(id uint64, key []byte) ([]byte, error) {
	table, err := lm.getTable(id)
	if err != nil {
		return nil, err
	}
	defer table.Decr()

	it := table.NewIterator()
	it.Incr()
	defer it.Decr()

	entry, err := it.Seek(key)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, ErrKeyNotFound
	}
	defer entry.Decr()

	// Check if user key matches
	if !bytes.Equal(entry.Key, key[:len(key)-8]) {
		return nil, ErrKeyNotFound
	}

	if entry.Meta == uint8(kv.MetaDel) {
		return nil, ErrKeyNotFound
	}

	return entry.Value, nil
}

func (lm *levelManager) worker() {
	defer lm.wg.Done()
	trigger15s := time.NewTicker(time.Second * 15)
	defer trigger15s.Stop()

	for {
		select {
		case <-lm.ctx.Done():
			close(lm.flushl0)
			for l0 := range lm.flushl0 {
				lm.persistl0(l0)
			}
			return
		case m := <-lm.flushl0:
			err := lm.persistl0(m)
			if err != nil {
				slog.Error("persistl0 failed", "err", err)
			}
		case <-trigger15s.C:
			c := make(chan error, 1)
			lm.compaction(c)
			if err := <-c; err != nil {
				slog.Error("triggerCompaction failed", "err", err)
			}
		case c := <-lm.trigerCompactionCh:
			lm.compaction(c)
		}
	}
}

func (lm *levelManager) cacheWorker() {
	defer lm.wg.Done()
	cacheTicker := time.NewTicker(time.Second * 30)
	defer cacheTicker.Stop()
	for {
		select {
		case <-lm.ctx.Done():
			lm.clearTableCache()
			return
		case <-cacheTicker.C:
			lm.cleanupTableCache()
		}
	}
}

func (lm *levelManager) persistl0(l0 *memtable) error {
	if len(lm.levels) == 0 {
		return nil
	}

	iter := l0.Iterator()
	iter.Incr()
	defer l0.Decr()
	defer iter.Decr()

	// unlimited table size becasue l0 is formed by immutable memtable
	builder := sstable.NewTableBuilder(uint32(lm.levelOption.dataBlockSize), math.MaxUint32)
	defer sstable.PutTableBuilder(builder)
	var (
		minKey []byte
		maxKey []byte
		last   *kv.Entry
	)
	defer func() {
		if last != nil {
			last.Decr()
		}
	}()

	for {
		entry, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if entry == nil {
			return io.ErrUnexpectedEOF
		}
		ikey := entry.InternalKey()
		if ikey == nil {
			entry.Decr()
			return io.ErrUnexpectedEOF
		}
		if minKey == nil {
			minKey = append([]byte(nil), ikey...)
		}
		if last != nil {
			last.Decr()
			last = nil
		}
		last = entry
		last.Incr()
		_, err = builder.AddEntry(entry)
		entry.Decr()
		if err != nil {
			return err
		}
	}
	if minKey == nil || last == nil {
		return nil
	}
	ikey := last.InternalKey()
	if ikey == nil {
		return io.ErrUnexpectedEOF
	}
	maxKey = append([]byte(nil), ikey...)

	fid := int(atomic.AddInt64(&lm.nextTableID, 1))

	n, err := builder.Flush(&sstable.Option{
		RootDir: lm.levelOption.rootDir,
		DataDir: lm.levelOption.dataDir,
		Fid:     fid,
		Flags:   os.O_CREATE | os.O_TRUNC | os.O_RDWR,
	})
	if err != nil {
		return err
	}

	lm.levelsRWMutex[0].Lock()
	lm.levels[0].Tables = append(lm.levels[0].Tables, &pb.Table{
		Id:        uint64(fid),
		MinKey:    minKey,
		MaxKey:    maxKey,
		Size:      int32(n),
		CreatedAt: time.Now().Unix(),
		Stale:     builder.StaleSize(),
	})
	lm.levelsRWMutex[0].Unlock()

	lm.ckptManager.queueAlterLevel([]levelAlter{
		{
			adds: []addLevel{
				{level: 0, pblevel: &pb.Table{Id: uint64(fid), MinKey: minKey, MaxKey: maxKey}},
			},
		},
	}, true)

	return nil

}

func (lm *levelManager) triggerCompaction() error {
	c := make(chan error)
	defer close(c)
	lm.trigerCompactionCh <- c
	err := <-c
	return err
}

func (lm *levelManager) close() error {
	lm.cancel()
	lm.wg.Wait()
	return nil
}
