package lsm

import (
	"OscarKV/kv"
	"OscarKV/wal"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultImtCapacity          = 64
	defaultInstallQueueCapacity = 64
)

var (
	ErrLSMClosed = errors.New("Module: LSM, Reason: lsm has been closed")
)

type (
	Option struct {
		RootDir                string
		MemoryLimitPerMemtable int // default 64KB
		MaxLevelPerMemtable    int
		StrictMode             bool
		RandFactorPerMemtable  float64
		SlowDownDuration       time.Duration // default is 10ms
		PreferedLevel          int8
		Magic                  string
		SyncOnFlush            bool
	}

	LSM struct {
		Option
		mtMu                sync.RWMutex
		mt                  *memtable
		imt                 []*memtable
		wal                 *wal.Manager
		ckptManager         *checkpointManager
		levelManager        *levelManager
		installQueue        chan *memtable
		closed              int32
		closeSubmitSignal   chan chan struct{}
		allowReadingWriting sync.WaitGroup
	}
)

func Open(opt Option) (*LSM, error) {
	l := &LSM{
		Option:            opt,
		imt:               make([]*memtable, 0, defaultImtCapacity),
		installQueue:      make(chan *memtable, defaultInstallQueueCapacity),
		closeSubmitSignal: make(chan chan struct{}),
	}

	ckptManager, err := openCheckpoint(&checkpointOptions{
		rootDir:       opt.RootDir,
		preferedLevel: opt.PreferedLevel,
		magic:         opt.Magic,
		syncOnFlush:   opt.SyncOnFlush,
	})
	if err != nil {
		return nil, err
	}
	l.ckptManager = ckptManager
	l.wal = wal.Open(opt.RootDir)

	l.levelManager = newLevelManager(levelOption{
		rootDir: opt.RootDir,
		dataDir: "data",
	}, ckptManager)

	if err := l.recovery(); err != nil {
		return nil, err
	}

	if l.mt == nil {
		l.mt = l.newMemtable(int(l.ckptManager.incrSegmentID()))
	}

	l.allowReadingWriting.Add(1)

	go l.flushImmutableWorker()

	return l, nil
}

func (lsm *LSM) Close() error {

	if !atomic.CompareAndSwapInt32(&lsm.closed, 0, 1) {
		return ErrLSMClosed
	}

	// stop Reading and Writing
	lsm.allowReadingWriting.Done()
	lsm.allowReadingWriting.Wait()

	done := make(chan struct{})

	// close submitqueue
	lsm.closeSubmitSignal <- done

	// wait for submitted tasks to be done
	// close install queue
	<-done

	if lsm.levelManager != nil {
		_ = lsm.levelManager.close()
	}
	if lsm.ckptManager != nil {
		lsm.ckptManager.Close()
	}
	if lsm.wal != nil {
		lsm.wal.Close()
	}

	return nil
}

func (lsm *LSM) SetBatch(batch []*kv.Requests) error {

	lsm.allowReadingWriting.Add(1)
	defer lsm.allowReadingWriting.Done()

	if atomic.LoadInt32(&lsm.closed) == 1 {
		return ErrLSMClosed
	}

	entries := make([]*kv.Entry, 0)
	size := int32(0)
	for _, request := range batch {
		entries = append(entries, request.Entries...)
		size += int32(request.Size)
	}

	for {

		lsm.mtMu.RLock()
		mt := lsm.mt
		mt.Incr()
		lsm.mtMu.RUnlock()

		if mt.tryReserve(size) {
			if err := mt.writeBatch(entries); err != nil {
				mt.Decr()
				return err
			}
			mt.releaseReserved(size)
			mt.Decr()
			return nil
		}

		if err := lsm.rotate(); err != nil {
			return err
		}

		lsm.submitInstall(mt)

		mt.Decr()
	}
}

func (lsm *LSM) submitInstall(mt *memtable) {
	mt.Incr()
	lsm.installQueue <- mt
}

func (lsm *LSM) flushImmutableWorker() {

	for {
		select {
		case mt := <-lsm.installQueue:
			_ = lsm.install(mt)
			mt.Decr()
		case c := <-lsm.closeSubmitSignal:
			close(lsm.installQueue)
			for mt := range lsm.installQueue {
				_ = lsm.install(mt)
				mt.Decr()
			}
			c <- struct{}{}
			return
		}
	}
}

func (lsm *LSM) install(mt *memtable) error {
	lsm.levelManager.flushl0 <- mt
	return nil
}

func (lsm *LSM) Get(key []byte) ([]byte, error) {
	if atomic.LoadInt32(&lsm.closed) == 1 {
		return nil, ErrLSMClosed
	}
	lsm.mtMu.RLock()
	mt := lsm.mt
	mt.Incr()
	lsm.mtMu.RUnlock()
	val, err := mt.Get(key)
	mt.Decr()
	if err == nil && val != nil {
		return val, nil
	}
	// search immutable memtables (newest first)
	lsm.mtMu.RLock()
	for i := len(lsm.imt) - 1; i >= 0; i-- {
		m := lsm.imt[i]
		m.Incr()
		lsm.mtMu.RUnlock()
		v, e := m.Get(key)
		m.Decr()
		if e == nil && v != nil {
			return v, nil
		}
		lsm.mtMu.RLock()
	}
	lsm.mtMu.RUnlock()

	return lsm.levelManager.Get(key)
}
