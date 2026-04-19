package main

import (
	"OscarKV/kv"
	"OscarKV/lsm"
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type (
	DB struct {
		rootDir                  string
		commitQueueBuffer        int
		commitQueue              chan *kv.Requests
		commitClose              chan bool
		version                  uint64
		defaultCF                []byte
		ctx                      context.Context
		cancel                   context.CancelFunc
		mutex                    sync.Mutex
		closeSignal              int32 // 0->normal, 1->closing, 2->closed
		readWriteWait            sync.WaitGroup
		writeBatchCountThreshold int
		writeBatchSizeThreshold  int
		lsm                      *lsm.LSM
	}

	Option struct {
		CommitBuffer             int
		RootDir                  string
		WriteBatchCountThreshold int
		WriteBatchSizeThreshold  int
		MemoryLimitPerMemtable   int // default 64KB
		MaxImmutableMemtable     int // max number of immutable memtable
		MaxLevelPerMemtable      int
		StrictMode               bool
		RandFactorPerMemtable    float64
		SlowDownDurationMs       int64 // default is 10ms
		PreferedLevel            int8  // default is 8
	}
)

var (
	ErrDBClosed    = errors.New("Module: DB, Reason: DB has been closed")
	ErrTxnTooBig   = errors.New("Module: DB, Reason: txn write size supass memory limit")
	ErrKeyNotFound = errors.New("Module: DB, Reason: key not found in db")
	ErrKeyInvalid  = errors.New("Module: DB, Reason: key invalid")
)

const (
	running = 0
	closing = 1
	closed  = 2
)

func Open(opt *Option) (*DB, error) {
	db := &DB{
		rootDir:                  opt.RootDir,
		commitQueueBuffer:        opt.CommitBuffer,
		commitQueue:              make(chan *kv.Requests, opt.CommitBuffer),
		commitClose:              make(chan bool),
		defaultCF:                []byte("0xff_cf"),
		writeBatchCountThreshold: opt.WriteBatchCountThreshold,
		writeBatchSizeThreshold:  opt.WriteBatchSizeThreshold,
	}

	lsmInst, err := lsm.Open(lsm.Option{
		RootDir:                opt.RootDir,
		MemoryLimitPerMemtable: opt.MemoryLimitPerMemtable,
		MaxLevelPerMemtable:    opt.MaxLevelPerMemtable,
		StrictMode:             opt.StrictMode,
		RandFactorPerMemtable:  opt.RandFactorPerMemtable,
		SlowDownDuration:       time.Duration(opt.SlowDownDurationMs) * time.Millisecond,
		PreferedLevel:          opt.PreferedLevel,
	})
	if err != nil {
		return nil, err
	}
	db.lsm = lsmInst

	db.ctx, db.cancel = context.WithCancel(context.Background())
	db.readWriteWait.Add(1)
	go db.commitWorker()

	return db, nil
}

func (db *DB) Close() error {
	db.mutex.Lock()
	if db.cancel == nil {
		db.mutex.Unlock()
		return ErrDBClosed
	}
	db.cancel()
	db.cancel = nil
	db.mutex.Unlock()

	// commit
	atomic.StoreInt32(&db.closeSignal, closing)
	db.readWriteWait.Done() // Done for the commitWorker
	db.readWriteWait.Wait()
	close(db.commitQueue)
	atomic.StoreInt32(&db.closeSignal, closed)

	// wait for async task exit.
	<-db.commitClose

	// lsm write
	db.lsm.Close()

	return nil
}

func (db *DB) nextVersion() uint64 {
	return atomic.AddUint64(&db.version, 1)
}

func (db *DB) loadVersion() uint64 {
	return atomic.LoadUint64(&db.version)
}

func (db *DB) Set(key, value []byte) error {
	db.readWriteWait.Add(1)
	defer db.readWriteWait.Done()
	r := atomic.LoadInt32(&db.closeSignal)
	if r != running {
		return ErrDBClosed
	}
	version := db.nextVersion()
	entry := kv.NewInternalEntry(key, value, version,
		db.defaultCF, uint8(kv.MetaSet), 0)
	defer entry.Decr()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.readWriteWait.Add(1)
	defer db.readWriteWait.Done()
	r := atomic.LoadInt32(&db.closeSignal)
	if r != running {
		return nil, ErrDBClosed
	}

	version := uint64(math.MaxUint64)
	ikey := kv.MakeInternalKey(db.defaultCF, key, version)
	slog.Info("get key", "key", key, "version", version)
	val, err := db.lsm.Get(ikey)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, ErrKeyNotFound
	}

	return val, nil
}

func (db *DB) ApplyInternalEntries(entries []*kv.Entry) error {
	req := kv.NewRequest(entries)
	defer req.Decr()
	db.commitQueue <- req
	err := req.Wait()
	return err
}
