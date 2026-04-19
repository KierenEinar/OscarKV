package lsm

import (
	"OscarKV/iterator"
	"OscarKV/pb"
	"OscarKV/sstable"
	"OscarKV/utils"
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type pickedResult struct {
	level int8
	score float32
}

type levelStat struct {
	level           int8
	tableNums       int
	usedSpace       int // used space bytes
	staleSpace      int // stale size bytes
	capacity        int // capacity bytes
	ingestNums      int
	ingestUsedSpace int // ingest used space bytes
	ingestCapacity  int // ingest capacity bytes

	missed uint64
	hitted uint64
}

type tableStat struct {
	missed    uint64
	hitted    uint64
	createdAt int64
}

type compactionDef struct {
	sourceLevel      int8
	targetLevel      int8
	sourceTableIxs   []int
	targetTableRange [2]int
	minKey           []byte
	maxKey           []byte
}

type compactionGroup struct {
	startIx int
	length  int
	left    []byte // closed left bound
	right   []byte // closed right bound
}

type levelInsertDef struct {
	level    int8
	table    []*pb.Table
	insertAt int
}

type levelDeleteDef struct {
	level    int8
	deleteAt int
	length   int
}

type compactionPropose struct {
	insertDefs []*levelInsertDef
	deleteDefs []*levelDeleteDef
}

func (lm *levelManager) pickLevel() []*pickedResult {

	results := make([]*pickedResult, 0, len(lm.levelStats))
	for ix := len(lm.levelStats) - 1; ix >= 1; ix-- {
		levelStat := lm.levelStats[ix]
		spaceScore := float32(levelStat.usedSpace) / (float32(levelStat.capacity) + 1e-3)
		queryTimes := levelStat.hitted
		if math.MaxUint64-levelStat.missed < queryTimes {
			queryTimes = math.MaxUint64
		} else {
			queryTimes += levelStat.missed
		}
		missedScore := float32(levelStat.missed) / (float32(queryTimes) + 1e-3)
		staleScore := float32(levelStat.staleSpace) / (float32(levelStat.capacity) + 1e-3)
		score := spaceScore*lm.levelOption.compactionPickSpaceCoefficient +
			missedScore*lm.levelOption.compactionPickMissedCoefficient +
			staleScore*lm.levelOption.compactionPickStaleCoefficient
		results = append(results, &pickedResult{
			level: levelStat.level,
			score: score,
		})
	}

	// L0 for table nums
	score := float32(lm.levelStats[0].tableNums) / float32(lm.levelOption.l0Nums)
	if score > lm.levelOption.l0ForcedCompactPercentage {
		score = math.MaxFloat32
	} else {
		score = lm.levelOption.compactionPickL0Coefficient
	}

	results = append(results, &pickedResult{
		level: 0,
		score: score,
	})

	// sort by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	return results
}

func (lm *levelManager) pickCompaction() *compactionDef {
	results := lm.pickLevel()
	if len(results) == 0 {
		return nil
	}
	source := results[0].level
	if source >= lm.levelOption.maxLevel {
		return nil
	}

	if source == 0 {
		return lm.pickL0Compaction()
	}

	return lm.pickLevelCompaction(source)
}

func (lm *levelManager) pickL0Compaction() *compactionDef {

	var baseLevel int8
	for ix := len(lm.levelStats) - 1; ix >= 1; ix-- {
		if lm.levelStats[ix].capacity == lm.levelOption.baseLevelSize && lm.levelStats[ix].usedSpace < lm.levelStats[ix].capacity {
			baseLevel = int8(ix)
		}
	}

	for ix := baseLevel; ix < lm.levelOption.maxLevel; ix++ {
		if lm.levelStats[ix].usedSpace == 0 {
			baseLevel += 1
		}
	}

	sourceOverlaps := make([]int, 0)

	var minKey []byte
	var maxKey []byte

	for ix := 0; ix < len(lm.levels[0].Tables); ix++ {

		if minKey == nil {
			minKey = lm.levels[0].Tables[ix].MinKey
		}
		if maxKey == nil {
			maxKey = lm.levels[0].Tables[ix].MaxKey
		}

		if utils.CompareKey(lm.levels[0].Tables[ix].MinKey, maxKey) > 0 ||
			utils.CompareKey(lm.levels[0].Tables[ix].MaxKey, minKey) < 0 {
			break
		}

		if utils.CompareKey(lm.levels[0].Tables[ix].MinKey, minKey) < 0 {
			minKey = lm.levels[0].Tables[ix].MinKey
		}
		if utils.CompareKey(lm.levels[0].Tables[ix].MaxKey, maxKey) > 0 {
			maxKey = lm.levels[0].Tables[ix].MaxKey
		}

		sourceOverlaps = append(sourceOverlaps, ix)
	}

	l, r := lm.pickRange(baseLevel, minKey, maxKey)

	return &compactionDef{
		sourceLevel:      0,
		targetLevel:      baseLevel,
		sourceTableIxs:   sourceOverlaps,
		targetTableRange: [2]int{l, r},
		minKey:           bytes.Clone(minKey),
		maxKey:           bytes.Clone(maxKey),
	}

}

func (lm *levelManager) pickLevelCompaction(source int8) *compactionDef {

	if source >= lm.levelOption.maxLevel {
		return nil
	}

	minScore := -float32(math.MaxFloat32 - 1)
	var selected int

	for ix, table := range lm.levels[source].Tables {
		tableStat := lm.tableStats[table.Id]
		createdScore := float32((math.MaxInt64 - tableStat.createdAt)) * lm.levelOption.compactionPickCreatedCoefficient
		missedScore := float32(tableStat.missed) / (float32(tableStat.hitted+tableStat.missed) + 1e-3) * lm.levelOption.compactionPickTableMissedCoefficient
		score := createdScore + missedScore
		if score > minScore {
			minScore = score
			selected = ix
		}
	}

	minKey := lm.levels[source].Tables[selected].MinKey
	maxKey := lm.levels[source].Tables[selected].MaxKey

	l, r := lm.pickRange(source+1, minKey, maxKey)

	return &compactionDef{
		sourceTableIxs:   []int{selected},
		targetTableRange: [2]int{l, r},
		sourceLevel:      source,
		targetLevel:      source + 1,
		minKey:           bytes.Clone(minKey),
		maxKey:           bytes.Clone(maxKey),
	}
}

// left: closed
// right: opened
func (lm *levelManager) pickRange(baseLevel int8, minKey []byte, maxKey []byte) (left int, right int) {
	l := sort.Search(len(lm.levels[baseLevel].Tables), func(ix int) bool {
		return utils.CompareKey(lm.levels[baseLevel].Tables[ix].MaxKey, minKey) >= 0
	})

	r := sort.Search(len(lm.levels[baseLevel].Tables), func(ix int) bool {
		return utils.CompareKey(lm.levels[baseLevel].Tables[ix].MinKey, maxKey) > 0
	})

	return l, r
}

func (lm *levelManager) compaction(errCh chan error) {

	compactionDef := lm.pickCompaction()
	if compactionDef == nil {
		errCh <- nil
		return
	}

	sourceTableIxs := compactionDef.sourceTableIxs
	sourceTables := make([]*pb.Table, 0, len(sourceTableIxs))
	for _, ix := range sourceTableIxs {
		sourceTables = append(sourceTables, lm.levels[compactionDef.sourceLevel].Tables[ix])
	}

	l, r := compactionDef.targetTableRange[0], compactionDef.targetTableRange[1]

	var insertDefs []*levelInsertDef
	var deleteDefs []*levelDeleteDef
	var err error

	if l == r { // insert at index
		insertDef := &levelInsertDef{
			level:    compactionDef.targetLevel,
			table:    sourceTables,
			insertAt: l,
		}
		deleteDef := &levelDeleteDef{
			level:    compactionDef.sourceLevel,
			deleteAt: compactionDef.sourceTableIxs[0],
			length:   len(compactionDef.sourceTableIxs),
		}
		insertDefs = []*levelInsertDef{insertDef}
		deleteDefs = []*levelDeleteDef{deleteDef}
	} else {

		if compactionDef.targetLevel == 0 {
			var insertDef *levelInsertDef
			insertDef, deleteDefs, err = lm.normalCompaction(compactionDef)
			if err != nil {
				errCh <- err
				return
			}
			if insertDef != nil {
				insertDefs = []*levelInsertDef{insertDef}
			}
		} else {
			insertDefs, deleteDefs, err = lm.subCompaction(compactionDef)
			if err != nil {
				errCh <- err
				return
			}
		}
	}

	result := &compactionPropose{
		insertDefs: insertDefs,
		deleteDefs: deleteDefs,
	}

	errCh <- lm.applyPropose(result)

}

func (lm *levelManager) applyPropose(propose *compactionPropose) error {
	if propose == nil {
		return nil
	}
	if lm == nil {
		return errors.New("nil levelManager")
	}

	var la levelAlter

	insertDefs := propose.insertDefs
	deleteDefs := propose.deleteDefs

	levelSet := make(map[int]struct{})
	for _, del := range deleteDefs {
		if del == nil {
			continue
		}
		level := int(del.level)
		if level < 0 || level >= len(lm.levels) {
			return errors.New("delete level out of range")
		}
		levelSet[level] = struct{}{}
	}
	for _, ins := range insertDefs {
		if ins == nil {
			continue
		}
		level := int(ins.level)
		if level < 0 || level >= len(lm.levels) {
			return errors.New("insert level out of range")
		}
		levelSet[level] = struct{}{}
	}
	levelsToLock := make([]int, 0, len(levelSet))
	for level := range levelSet {
		levelsToLock = append(levelsToLock, level)
	}
	sort.Ints(levelsToLock)
	for _, level := range levelsToLock {
		lm.levelsRWMutex[level].Lock()
	}
	unlocked := false
	unlockAll := func() {
		if unlocked {
			return
		}
		for i := len(levelsToLock) - 1; i >= 0; i-- {
			lm.levelsRWMutex[levelsToLock[i]].Unlock()
		}
		unlocked = true
	}
	defer unlockAll()

	sort.Slice(deleteDefs, func(i, j int) bool {
		if deleteDefs[i].level != deleteDefs[j].level {
			return deleteDefs[i].level < deleteDefs[j].level
		}
		return deleteDefs[i].deleteAt > deleteDefs[j].deleteAt
	})
	sort.Slice(insertDefs, func(i, j int) bool {
		if insertDefs[i].level != insertDefs[j].level {
			return insertDefs[i].level < insertDefs[j].level
		}
		return insertDefs[i].insertAt < insertDefs[j].insertAt
	})

	for _, del := range deleteDefs {
		if del == nil {
			continue
		}
		level := int(del.level)
		if lm.levels[level] == nil {
			return errors.New("delete level is nil")
		}
		deleteAt := del.deleteAt
		length := del.length
		if length < 0 {
			return errors.New("delete length < 0")
		}
		if deleteAt < 0 || deleteAt > len(lm.levels[level].Tables) {
			return errors.New("delete index out of range")
		}
		if deleteAt+length > len(lm.levels[level].Tables) {
			return errors.New("delete range out of range")
		}

		if length != 0 {
			deleted := lm.levels[level].Tables[deleteAt : deleteAt+length]

			old := lm.levels[level].Tables
			next := make([]*pb.Table, 0, len(old)-length)
			next = append(next, old[:deleteAt]...)
			next = append(next, old[deleteAt+length:]...)
			lm.levels[level].Tables = next

			if lm.levelStats != nil && level < len(lm.levelStats) && lm.levelStats[level] != nil {
				lm.levelStats[level].tableNums -= length
				for _, t := range deleted {
					if t == nil {
						continue
					}
					lm.levelStats[level].usedSpace -= int(t.Size)
					lm.levelStats[level].staleSpace -= int(t.Stale)
				}
				if lm.levelStats[level].tableNums < 0 {
					lm.levelStats[level].tableNums = 0
				}
				if lm.levelStats[level].usedSpace < 0 {
					lm.levelStats[level].usedSpace = 0
				}
				if lm.levelStats[level].staleSpace < 0 {
					lm.levelStats[level].staleSpace = 0
				}
			}

			for _, t := range deleted {
				if t == nil {
					continue
				}
				la.dels = append(la.dels, delLevel{level: level, id: t.Id})
				if lm.tableStats != nil {
					delete(lm.tableStats, t.Id)
				}
			}
		}
	}

	for _, ins := range insertDefs {
		if ins == nil {
			continue
		}
		level := int(ins.level)
		insertAt := ins.insertAt
		if insertAt < 0 {
			return errors.New("insert index < 0")
		}
		if insertAt > len(lm.levels[level].Tables) {
			return errors.New("insert index out of range")
		}

		if len(ins.table) != 0 {
			old := lm.levels[level].Tables
			next := make([]*pb.Table, 0, len(old)+len(ins.table))
			next = append(next, old[:insertAt]...)
			next = append(next, ins.table...)
			next = append(next, old[insertAt:]...)
			lm.levels[level].Tables = next

			if lm.levelStats != nil && level < len(lm.levelStats) && lm.levelStats[level] != nil {
				lm.levelStats[level].tableNums += len(ins.table)
				for _, t := range ins.table {
					if t == nil {
						continue
					}
					lm.levelStats[level].usedSpace += int(t.Size)
					lm.levelStats[level].staleSpace += int(t.Stale)
				}
			}

			if lm.tableStats == nil {
				lm.tableStats = make(map[uint64]tableStat)
			}
			for _, t := range ins.table {
				if t == nil {
					continue
				}
				ts := lm.tableStats[t.Id]
				if ts.createdAt == 0 {
					ts.createdAt = t.CreatedAt
				}
				lm.tableStats[t.Id] = ts
				la.adds = append(la.adds, addLevel{
					level:   level,
					pblevel: &pb.Table{Id: t.Id, MinKey: t.MinKey, MaxKey: t.MaxKey},
				})
			}
		}
	}

	if lm.ckptManager != nil && (len(la.adds) != 0 || len(la.dels) != 0) {
		unlockAll()
		lm.ckptManager.queueAlterLevel([]levelAlter{la}, true)
	}

	return nil
}

func (lm *levelManager) formGroups(cDef *compactionDef) []compactionGroup {

	if cDef.sourceLevel == 0 && len(cDef.sourceTableIxs) > 1 {
		panic("unsupported form L0 Groups through subcompaction")
	}

	window := 5
	maxCompute := min(runtime.NumCPU()/2, 4)
	length := cDef.targetTableRange[1] - cDef.targetTableRange[0]

	if length/window > maxCompute {
		window = length / maxCompute
	}

	groups := make([]compactionGroup, 0, maxCompute)

	for i := 0; i < length; i += window {

		lastIx := i + window - 1
		if lastIx >= length {
			lastIx = length - 1
		}

		var leftKey []byte
		if i > 0 {
			leftKey = groups[len(groups)-1].right
		}

		var rightKey []byte
		if lastIx < length-1 {
			rightKey = bytes.Clone(lm.levels[cDef.targetLevel].Tables[cDef.targetTableRange[0]+lastIx].MaxKey)
		}

		group := compactionGroup{
			startIx: cDef.targetTableRange[0] + i,
			length:  lastIx - i + 1,
			left:    leftKey,
			right:   rightKey,
		}

		groups = append(groups, group)
	}

	return groups
}

func (lm *levelManager) normalCompaction(cDef *compactionDef) (*levelInsertDef, []*levelDeleteDef, error) {
	if cDef == nil {
		return nil, nil, nil
	}
	if cDef.sourceLevel == cDef.targetLevel {
		return nil, nil, errors.New("normalCompaction sourceLevel == targetLevel")
	}

	sourceTables := make([]*pb.Table, 0, len(cDef.sourceTableIxs))
	for _, ix := range cDef.sourceTableIxs {
		sourceTables = append(sourceTables, lm.levels[cDef.sourceLevel].Tables[ix])
	}

	targetTables := make([]*pb.Table, 0, cDef.targetTableRange[1]-cDef.targetTableRange[0])
	for i := cDef.targetTableRange[0]; i < cDef.targetTableRange[1]; i++ {
		targetTables = append(targetTables, lm.levels[cDef.targetLevel].Tables[i])
	}

	iters := make([]iterator.Iterator, 0, len(sourceTables)+len(targetTables))
	tables := make([]*sstable.Table, 0, len(sourceTables)+len(targetTables))
	defer func() {
		for _, t := range tables {
			t.Decr()
		}
	}()

	for _, t := range append(sourceTables, targetTables...) {
		tbl, err := lm.getTable(t.Id)
		if err != nil {
			return nil, nil, err
		}
		tables = append(tables, tbl)
		iters = append(iters, tbl.NewIterator())
	}

	mergedIters := iterator.NewMergeIterator(iters)
	mergedIters.Incr()
	defer mergedIters.Decr()
	return lm.compactaction(mergedIters, cDef)
}

func (lm *levelManager) subCompaction(cDef *compactionDef) ([]*levelInsertDef, []*levelDeleteDef, error) {

	sourceLevel := cDef.sourceLevel

	if sourceLevel > 0 && (len(cDef.sourceTableIxs) == 0 || len(cDef.sourceTableIxs) > 1) {
		return nil, nil, errors.New("sub compaction not allow source table count not 1")
	}

	sourceTable := lm.levels[sourceLevel].Tables[cDef.sourceTableIxs[0]]
	targetGroups := lm.formGroups(cDef)

	wg := sync.WaitGroup{}
	wg.Add(len(targetGroups))
	errGroup := make(chan error, len(targetGroups))
	insertDefs := make([]*levelInsertDef, 0, len(targetGroups))
	deleteDefs := make([]*levelDeleteDef, 0, len(targetGroups))
	mutex := sync.Mutex{}
	for _, group := range targetGroups {
		go func(group compactionGroup) {
			defer wg.Done()
			table, err := lm.getTable(sourceTable.Id)
			if err != nil {
				errGroup <- err
				return
			}
			iter := table.NewIterator()
			iter.Incr()

			defer table.Decr()
			defer iter.Decr()

			targetTables := make([]*sstable.Table, 0, group.length)
			targetIters := make([]iterator.Iterator, 0, group.length)
			for i := 0; i < group.length; i++ {
				meta := lm.levels[cDef.targetLevel].Tables[group.startIx+i]
				table, err := lm.getTable(meta.Id)
				if err != nil {
					errGroup <- err
					return
				}
				targetTables = append(targetTables, table)
				targetIters = append(targetIters, table.NewIterator())
			}
			defer func() {
				for _, t := range targetTables {
					t.Decr()
				}
			}()

			subCompactIter := iterator.NewSubCompactionIterator(group.left, group.right, iter, targetIters)
			subCompactIter.Incr()
			defer subCompactIter.Decr()

			groupDef := *cDef
			groupDef.targetTableRange = [2]int{group.startIx, group.startIx + group.length}
			groupDef.sourceTableIxs = nil
			insertDef, groupDeleteDefs, err := lm.compactaction(subCompactIter, &groupDef)
			if err != nil {
				errGroup <- err
				return
			}
			mutex.Lock()
			if insertDef != nil {
				insertDefs = append(insertDefs, insertDef)
			}
			deleteDefs = append(deleteDefs, groupDeleteDefs...)
			mutex.Unlock()
			errGroup <- nil

		}(group)
	}

	wg.Wait()
	close(errGroup)

	for err := range errGroup {
		if err != nil {
			return nil, nil, err
		}
	}

	if len(cDef.sourceTableIxs) != 0 {
		deleteDefs = append(deleteDefs, &levelDeleteDef{
			level:    cDef.sourceLevel,
			deleteAt: cDef.sourceTableIxs[0],
			length:   len(cDef.sourceTableIxs),
		})
	}

	sort.Slice(insertDefs, func(i, j int) bool {
		return int8(insertDefs[i].insertAt) < int8(insertDefs[j].insertAt)
	})
	insertedBefore := 0
	baseInsertAt := cDef.targetTableRange[0]
	if baseInsertAt < 0 {
		baseInsertAt = 0
	}
	for _, ins := range insertDefs {
		ins.insertAt = baseInsertAt + insertedBefore
		insertedBefore += len(ins.table)
	}

	sort.Slice(deleteDefs, func(i, j int) bool {
		if deleteDefs[i].level != deleteDefs[j].level {
			return deleteDefs[i].level < deleteDefs[j].level
		}
		return deleteDefs[i].deleteAt < deleteDefs[j].deleteAt
	})

	return insertDefs, deleteDefs, nil
}

func (lm *levelManager) compactaction(mergedIters iterator.Iterator, cDef *compactionDef) (*levelInsertDef, []*levelDeleteDef, error) {

	bs := lm.levelOption.compactionBlockSize
	ts := lm.levelOption.compactionTableSize
	if bs <= 0 {
		bs = 1 << 10
	}
	if ts <= 0 {
		ts = 8 << 20
	}
	tb := sstable.NewTableBuilder(uint32(bs), uint32(ts))
	defer func() {
		if tb != nil {
			sstable.PutTableBuilder(tb)
		}
	}()

	newTables := make([]*pb.Table, 0, 4)
	var outMin []byte
	var outMax []byte

	flushCurrent := func() error {
		if outMin == nil || outMax == nil {
			return nil
		}
		old := tb
		fid := int(atomic.AddInt64(&lm.nextTableID, 1))
		n, err := old.Flush(&sstable.Option{
			RootDir: lm.levelOption.rootDir,
			DataDir: lm.levelOption.dataDir,
			Fid:     fid,
			Flags:   os.O_CREATE | os.O_TRUNC | os.O_RDWR,
		})
		if err != nil {
			return err
		}
		newTables = append(newTables, &pb.Table{
			Id:        uint64(fid),
			MinKey:    outMin,
			MaxKey:    outMax,
			Size:      int32(n),
			CreatedAt: time.Now().Unix(),
			Stale:     old.StaleSize(),
		})
		sstable.PutTableBuilder(old)
		tb = sstable.NewTableBuilder(uint32(bs), uint32(ts))
		outMin = nil
		outMax = nil
		return nil
	}

	var lastPrefix []byte
	for {
		e, err := mergedIters.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		ik := e.InternalKey()

		prefix := ik
		if len(prefix) >= 8 {
			prefix = prefix[:len(prefix)-8]
		}
		if lastPrefix != nil && bytes.Equal(prefix, lastPrefix) {
			e.Decr()
			continue
		}
		lastPrefix = append(lastPrefix[:0], prefix...)

		if outMin == nil {
			outMin = append([]byte(nil), ik...)
		}
		outMax = append([]byte(nil), ik...)

		done, err := tb.AddEntry(e)
		e.Decr()
		if err != nil {
			return nil, nil, err
		}
		if done {
			if err := flushCurrent(); err != nil {
				return nil, nil, err
			}
		}
	}

	if err := flushCurrent(); err != nil {
		return nil, nil, err
	}
	if len(newTables) == 0 {
		return nil, nil, nil
	}
	l := cDef.targetTableRange[0]
	r := cDef.targetTableRange[1]
	if l < 0 {
		l = 0
	}
	if r < l {
		r = l
	}
	insertDef := &levelInsertDef{
		level:    cDef.targetLevel,
		table:    newTables,
		insertAt: l,
	}
	deleteDefs := make([]*levelDeleteDef, 0, 2)
	if cDef.sourceLevel >= 0 && len(cDef.sourceTableIxs) != 0 {
		deleteDefs = append(deleteDefs, &levelDeleteDef{
			level:    cDef.sourceLevel,
			deleteAt: cDef.sourceTableIxs[0],
			length:   len(cDef.sourceTableIxs),
		})
	}
	if cDef.targetLevel >= 0 {
		deleteDefs = append(deleteDefs, &levelDeleteDef{
			level:    cDef.targetLevel,
			deleteAt: l,
			length:   r - l,
		})
	}
	return insertDef, deleteDefs, nil

}
