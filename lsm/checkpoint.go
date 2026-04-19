package lsm

import (
	"OscarKV/pb"
	"OscarKV/utils"
	"OscarKV/vfs"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

const metaDir = "meta"

var ckptFiles = []string{"000.ckpt", "001.ckpt"}

var (
	pbCkptPool = sync.Pool{
		New: func() any {
			return &pb.Checkpoint{}
		},
	}
)

type addLevel struct {
	level   int
	pblevel *pb.Table
}

type delLevel struct {
	level int
	id    uint64
}

type levelAlter struct {
	adds []addLevel
	dels []delLevel
}

type checkpointManager struct {
	ctx                     context.Context
	cancel                  context.CancelFunc
	wg                      sync.WaitGroup
	walSegmentId            int32
	levels                  atomic.Pointer[[]*pb.Levels]
	ingestBuffer            atomic.Pointer[[]*pb.IngestBuffer]
	vfs                     []vfs.Vfs
	applyAddSegmentID       chan struct{}
	asyncApplyLevelsAltered chan []levelAlter       // level: ids to alter
	applyLevelsAltered      chan mutualLevelAlter   // level: ids to alter
	asyncApplyIngestBuffer  chan applyIngestBuffer  // ingest buffer to apply
	applyIngestBuffer       chan mutualIngestBuffer // ingest buffer to apply
	opt                     checkpointOptions
	ckptId                  int
	ckptCreatedAt           uint64
}

type mutualIngestBuffer struct {
	buffer applyIngestBuffer
	done   chan struct{}
}

type ingestBuffer struct {
	id     uint64
	minKey []byte
	maxKey []byte
}

type applyIngestBuffer struct {
	l0Blocks    []ingestBuffer
	targetLevel int
}

type mutualLevelAlter struct {
	levelAlter []levelAlter
	done       chan struct{}
}

type checkpointOptions struct {
	magic         string
	rootDir       string
	preferedLevel int8
	syncOnFlush   bool
}

func (m *checkpointManager) incrSegmentID() int32 {
	defer func() {
		m.applyAddSegmentID <- struct{}{} // flush checkpoint into file system
	}()
	return atomic.AddInt32(&m.walSegmentId, 1)
}

func (m *checkpointManager) loadSegmentID() int32 {
	return atomic.LoadInt32(&m.walSegmentId)
}

func (m *checkpointManager) queueAlterLevel(la []levelAlter, async bool) {

	if async {
		m.asyncApplyLevelsAltered <- la
	} else {
		done := make(chan struct{})
		m.applyLevelsAltered <- mutualLevelAlter{
			levelAlter: la,
			done:       done,
		}
		<-done
	}
}

func (m *checkpointManager) queueApplyIngestBuffer(buffer applyIngestBuffer, async bool) {

	if async {
		m.asyncApplyIngestBuffer <- buffer
	} else {
		done := make(chan struct{})
		m.applyIngestBuffer <- mutualIngestBuffer{
			buffer: buffer,
			done:   done,
		}
		<-done
	}
}

func (m *checkpointManager) worker() {

	for {
		select {
		case <-m.ctx.Done():
			_ = m.sync()
			for _, vfs := range m.vfs {
				if vfs != nil {
					vfs.Close()
				}
			}
			m.wg.Done()
			return
		case <-m.applyAddSegmentID:
			err := m.flushSegment()
			if err != nil {
				slog.Error("flushSegment failed", "err", err)
			}
		case alter := <-m.asyncApplyLevelsAltered:
			err := m.flushLevels(alter)
			if err != nil {
				slog.Error("flushLevels failed", "err", err)
			}
		case alter := <-m.applyLevelsAltered:
			err := m.flushLevels(alter.levelAlter)
			if err != nil {
				slog.Error("flushLevels failed", "err", err)
			}
			close(alter.done)
		case ingest := <-m.asyncApplyIngestBuffer:
			err := m.flushIngestBuffer(ingest)
			if err != nil {
				slog.Error("flushIngestBuffer failed", "err", err)
			}
		case ingest := <-m.applyIngestBuffer:
			err := m.flushIngestBuffer(ingest.buffer)
			if err != nil {
				slog.Error("flushIngestBuffer failed", "err", err)
			}
			close(ingest.done)
		}
	}
}

func openCheckpoint(opt *checkpointOptions) (ckptMgr *checkpointManager, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err == nil {
			return
		}
		cancel()
		for _, vfs := range ckptMgr.vfs {
			if vfs != nil {
				vfs.Close()
			}
		}
	}()

	if err = os.MkdirAll(filepath.Join(opt.rootDir, metaDir), 0o755); err != nil {
		return nil, err
	}
	ckptMgr = &checkpointManager{
		ctx:                     ctx,
		cancel:                  cancel,
		applyAddSegmentID:       make(chan struct{}, 16),
		asyncApplyLevelsAltered: make(chan []levelAlter, 8),
		applyLevelsAltered:      make(chan mutualLevelAlter, 8),
		asyncApplyIngestBuffer:  make(chan applyIngestBuffer, 8),
		applyIngestBuffer:       make(chan mutualIngestBuffer, 8),
		opt:                     *opt,
		vfs:                     make([]vfs.Vfs, len(ckptFiles)),
	}

	for id, ckptFile := range ckptFiles {
		var (
			f      vfs.Vfs
			size   int64
			buffer []byte
		)
		path := filepath.Join(opt.rootDir, metaDir, ckptFile)
		f, err = vfs.OpenFile(path, os.O_RDWR|os.O_CREATE)
		if err != nil {
			return
		}
		storage := &pb.Checkpoint{}
		size, err = f.Size()
		if err != nil {
			return
		}
		ckptMgr.vfs[id] = f
		if size == 0 {
			continue
		}
		buffer, err = f.ReadAt(0, size)
		if err != nil {
			return
		}
		if err = proto.Unmarshal(buffer, storage); err != nil {
			return
		}
		if storage.Magic != "" && storage.Magic != opt.magic {
			err = ErrInvalidMagicNumber
			return
		}

		if ckptMgr.ckptCreatedAt < storage.CreatedAt {
			ckptMgr.ckptCreatedAt = storage.CreatedAt
			ckptMgr.ckptId = id
			ckptMgr.levels.Store(&storage.Levels)
			ckptMgr.walSegmentId = storage.WalSegmentId
			ckptMgr.ingestBuffer.Store(&storage.Buffer)
		}
	}
	if ckptMgr.levels.Load() == nil {
		preferedLevel := opt.preferedLevel
		if preferedLevel <= 0 {
			preferedLevel = 8 // L0 - L7
		}
		levels := make([]*pb.Levels, preferedLevel)
		for i := range levels {
			levels[i] = &pb.Levels{}
		}
		ckptMgr.ckptCreatedAt = uint64(time.Now().UnixNano())
		ckptMgr.ckptId = 0
		ckptMgr.walSegmentId = 1
		ckptMgr.levels.Store(&levels)
		buffer := make([]*pb.IngestBuffer, 0, 16)
		ckptMgr.ingestBuffer.Store(&buffer)
		pbckpt := &pb.Checkpoint{
			Magic:        opt.magic,
			WalSegmentId: ckptMgr.walSegmentId,
			CreatedAt:    ckptMgr.ckptCreatedAt,
			Levels:       levels,
			Buffer:       buffer,
		}
		if err = ckptMgr.flush(pbckpt); err != nil {
			return
		}
	}

	ckptMgr.wg.Add(1)
	go ckptMgr.worker()

	return ckptMgr, nil
}

func (m *checkpointManager) flushSegment() error {
	ckpt := pbCkptPool.Get().(*pb.Checkpoint)
	defer pbCkptPool.Put(ckpt)
	ckpt.Reset()
	ckpt.Magic = m.opt.magic
	ckpt.WalSegmentId = m.loadSegmentID()
	ckpt.Levels = *m.levels.Load()
	now := uint64(time.Now().UnixNano())
	if now <= m.ckptCreatedAt {
		now = m.ckptCreatedAt + 1
	}
	m.ckptCreatedAt = now
	ckpt.CreatedAt = now
	return m.flush(ckpt)
}

func (m *checkpointManager) flushLevels(alter []levelAlter) error {
	ckpt := pbCkptPool.Get().(*pb.Checkpoint)
	defer pbCkptPool.Put(ckpt)
	ckpt.Reset()
	ckpt.Magic = m.opt.magic
	ckpt.WalSegmentId = m.loadSegmentID()
	pbLevels := *m.levels.Load()
	newLevels := make([]*pb.Levels, len(pbLevels))
	for level := range newLevels {
		if pbLevels[level] == nil {
			newLevels[level] = &pb.Levels{}
			continue
		}
		newLevels[level] = &pb.Levels{Tables: append([]*pb.Table(nil), pbLevels[level].Tables...)}
	}

	delByLevel := make(map[int]map[uint64]bool)
	addByLevel := make(map[int][]*pb.Table)
	for _, la := range alter {
		for _, d := range la.dels {
			if d.level < 0 || d.level >= len(newLevels) {
				continue
			}
			mm := delByLevel[d.level]
			if mm == nil {
				mm = make(map[uint64]bool)
				delByLevel[d.level] = mm
			}
			mm[d.id] = true
		}
		for _, a := range la.adds {
			if a.level < 0 || a.level >= len(newLevels) {
				continue
			}
			if a.pblevel == nil {
				continue
			}
			addByLevel[a.level] = append(addByLevel[a.level], a.pblevel)
		}
	}

	for level, delMap := range delByLevel {
		if newLevels[level] == nil {
			newLevels[level] = &pb.Levels{}
		}
		kept := newLevels[level].Tables[:0]
		for _, l := range newLevels[level].Tables {
			if l == nil {
				continue
			}
			if !delMap[l.Id] {
				kept = append(kept, l)
			}
		}
		newLevels[level].Tables = kept
	}
	for level, adds := range addByLevel {
		if newLevels[level] == nil {
			newLevels[level] = &pb.Levels{}
		}
		newLevels[level].Tables = append(newLevels[level].Tables, adds...)
	}

	for level := range newLevels {
		if level > 0 {
			sort.Slice(newLevels[level].Tables, func(i, j int) bool {
				return utils.CompareKey(newLevels[level].Tables[i].MinKey, newLevels[level].Tables[j].MinKey) < 0
			})
		}
	}

	m.levels.Store(&newLevels)
	now := uint64(time.Now().UnixNano())
	if now <= m.ckptCreatedAt {
		now = m.ckptCreatedAt + 1
	}
	m.ckptCreatedAt = now
	ckpt.CreatedAt = now
	ckpt.Levels = newLevels
	return m.flush(ckpt)
}

func (m *checkpointManager) flushIngestBuffer(ingest applyIngestBuffer) error {

	ckpt := pbCkptPool.Get().(*pb.Checkpoint)
	defer pbCkptPool.Put(ckpt)
	ckpt.Reset()
	ckpt.Magic = m.opt.magic
	ckpt.WalSegmentId = m.loadSegmentID()
	ingestBiffer := m.ingestBuffer.Load()
	var old []*pb.IngestBuffer
	if ingestBiffer != nil {
		old = *ingestBiffer
	}
	l0Del := make(map[uint64]bool)
	for ix := range ingest.l0Blocks {
		l0Del[ingest.l0Blocks[ix].id] = true
	}

	filtered := make([]*pb.IngestBuffer, 0, len(old)+len(ingest.l0Blocks))
	for _, b := range old {
		if b == nil {
			continue
		}
		if b.Level == 0 && l0Del[b.Id] {
			continue
		}
		filtered = append(filtered, b)
	}
	for ix := range ingest.l0Blocks {
		filtered = append(filtered, &pb.IngestBuffer{
			Id:     ingest.l0Blocks[ix].id,
			Level:  int32(ingest.targetLevel),
			MinKey: append([]byte(nil), ingest.l0Blocks[ix].minKey...),
			MaxKey: append([]byte(nil), ingest.l0Blocks[ix].maxKey...),
		})
	}
	m.ingestBuffer.Store(&filtered)
	now := uint64(time.Now().UnixNano())
	if now <= m.ckptCreatedAt {
		now = m.ckptCreatedAt + 1
	}
	m.ckptCreatedAt = now
	ckpt.CreatedAt = now
	ckpt.Buffer = filtered
	return m.flush(ckpt)
}

func (m *checkpointManager) flush(ckpt *pb.Checkpoint) error {

	// slog.Info("flushSegment", "ckpt", ckpt.Levels, "len", len(ckpt.Levels))

	pbBytes, err := proto.Marshal(ckpt)
	if err != nil {
		return err
	}

	m.ckptId = (m.ckptId + 1) % len(ckptFiles)
	err = m.vfs[m.ckptId].Truncate(0, int64(len(pbBytes)))
	if err != nil {
		return err
	}
	_, err = m.vfs[m.ckptId].WriteAt(0, pbBytes)
	if err != nil {
		return err
	}

	if m.opt.syncOnFlush {
		err = m.vfs[m.ckptId].Flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *checkpointManager) sync() error {

	err := m.vfs[m.ckptId].Flush()
	if err != nil {
		return err
	}
	return nil
}

func (m *checkpointManager) Close() {
	m.cancel()
	m.wg.Wait()
}
