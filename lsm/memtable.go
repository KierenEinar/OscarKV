package lsm

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"

	// "go.uber.org/zap"
	"OscarKV/iterator"
	"OscarKV/kv"
	"OscarKV/utils"
	"OscarKV/wal"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type memtable struct {
	lsm       *LSM
	index     *utils.Skiplist
	wal       *wal.Manager
	segmentID int
	size      int32
}

func (mt *memtable) Incr() {
	mt.index.Incr()
}

func (mt *memtable) Decr() {
	if mt.index.Decr() == 0 {
		if mt.lsm != nil {
			mt.lsm.mtMu.Lock()
			defer mt.lsm.mtMu.Unlock()
			idx := -1
			for ix := range mt.lsm.imt {
				if mt.lsm.imt[ix] == mt {
					idx = ix
					break
				}
			}
			if idx >= 0 {
				mt.lsm.imt = append(mt.lsm.imt[:idx], mt.lsm.imt[idx+1:]...)
			}
		}
	}
}

func (lsm *LSM) newMemtable(segmentID int) *memtable {
	index := utils.NewSkiplist(
		uint32(lsm.MemoryLimitPerMemtable),
		uint8(lsm.MaxLevelPerMemtable),
		lsm.RandFactorPerMemtable,
		nil,
		func(a, b []byte) utils.CmpR {
			return utils.CmpR(utils.CompareKey(a, b))
		},
	)
	mt := &memtable{
		lsm:       lsm,
		size:      int32(lsm.MemoryLimitPerMemtable),
		index:     index,
		wal:       lsm.wal,
		segmentID: segmentID,
	}
	return mt
}

func (mt *memtable) tryReserve(size int32) bool {
	return mt.index.TryReserve(uint32(size))
}

func (mt *memtable) releaseReserved(size int32) {
	mt.index.ReleaseReserved(uint32(size))
}

// caller should reserved momory capacity to avoid OOM
func (mt *memtable) appendEntry(e *kv.Entry) error {
	if err := mt.index.Put(e.InternalKey(), e.InternalValue()); err != nil {
		return err
	}
	return nil
}

func (mt *memtable) Get(key []byte) ([]byte, error) {

	node, err := mt.index.FindGE(key)
	if err != nil {
		return nil, err
	}

	nKey, err := mt.index.Key(node)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(nKey[:len(nKey)-8], key[:len(key)-8]) {
		slog.Info("bb get key", "key", string(key[:len(key)-8]), "nKey", string(nKey[:len(nKey)-8]))
		return nil, ErrKeyNotFound
	}

	val, _ := mt.index.Value(node)
	meta, _, value, err := kv.SplitInternalVal(val)
	if err != nil {
		return nil, err
	}
	if meta == uint8(kv.MetaDel) {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

type memtableIterator struct {
	mt         *memtable
	ref        int32
	nodeoffset uint32
}

func (mt *memtable) Iterator() iterator.Iterator {
	return &memtableIterator{mt: mt, ref: 0}
}

func (mt *memtableIterator) Incr() {
	atomic.AddInt32(&mt.ref, 1)
	mt.mt.Incr()
}

func (it *memtableIterator) Next() (*kv.Entry, error) {
	if atomic.LoadInt32(&it.ref) <= 0 {
		return nil, io.EOF
	}

	if it.nodeoffset == 0 {
		node, err := it.mt.index.SeekToFirst()
		if err != nil {
			if err == utils.ErrKeyNotFound {
				return nil, io.EOF
			}
			return nil, err
		}
		it.nodeoffset = node
	}

	entry, next, err := it.fillEntry(it.nodeoffset)
	if err != nil {
		return nil, err
	}
	it.nodeoffset = next
	return entry, nil
}

func (it *memtableIterator) fillEntry(offset uint32) (*kv.Entry, uint32, error) {
	var (
		entry *kv.Entry
		next  uint32
		err   error
	)
	err = it.mt.index.Iterate(offset, func(key, value []byte,
		nextNodeOffset uint32) (stop bool) {
		_, cf, key, version, e := kv.SplitInternalKey(key)
		if e != nil {
			err = e
			return true
		}
		meta, ttl, value, e := kv.SplitInternalVal(value)
		if e != nil {
			err = e
			return true
		}
		next = nextNodeOffset
		entry = kv.NewInternalEntry(key, value, version, cf, meta, ttl)
		return true
	})
	if err != nil {
		return nil, 0, err
	}
	if entry == nil {
		return nil, 0, io.EOF
	}
	return entry, next, nil
}

func (it *memtableIterator) SeekToFirst() (*kv.Entry, error) {
	if atomic.LoadInt32(&it.ref) <= 0 {
		return nil, io.EOF
	}

	nodeoffset, err := it.mt.index.SeekToFirst()
	if err != nil {
		if err == utils.ErrKeyNotFound {
			return nil, io.EOF
		}
		return nil, err
	}
	entry, nextNodeOffset, err := it.fillEntry(nodeoffset)
	if err != nil {
		return nil, err
	}
	it.nodeoffset = nextNodeOffset
	return entry, nil
}

func (it *memtableIterator) Seek(key []byte) (*kv.Entry, error) {

	nodeoffset, err := it.mt.index.Seek(key)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	entry, nextNodeOffset, err := it.fillEntry(nodeoffset)
	if err != nil {
		return nil, err
	}
	it.nodeoffset = nextNodeOffset
	return entry, nil
}

func (it *memtableIterator) Decr() {
	r := atomic.AddInt32(&it.ref, -1)
	if r >= 0 {
		it.mt.Decr()
		return
	}
	if r < 0 {
		panic("memtable iterator ref underflow")
	}
}

func (mt *memtable) writeBatch(entries []*kv.Entry) (err error) {
	var offset int
	offset, err = mt.wal.AppendBatch(entries)

	defer func() {
		if err != nil {
			_ = mt.wal.Truncate(offset)
			mt.wal.Flush(true)
		}
	}()

	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err = mt.appendEntry(entry); err != nil {
			return err
		}
	}

	return nil
}

func (lsm *LSM) rotate() error {

	nextID, err := lsm.wal.RotateLocked()
	if err != nil {
		return err
	}
	mt := lsm.newMemtable(int(nextID))
	lsm.mtMu.Lock()
	lsm.imt = append(lsm.imt, lsm.mt)
	lsm.mt = mt
	lsm.mtMu.Unlock()
	return nil
}

// caller should call manually Decr.
func (lsm *LSM) openMemtable(segmentID int32) (*memtable, error) {

	iter, err := lsm.wal.Iterator(segmentID)
	if err != nil {
		return nil, err
	}
	iter.Incr()
	defer iter.Decr()

	mt := lsm.newMemtable(int(segmentID))
	for {
		entry, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if lsm.StrictMode {
				return nil, err
			}
			continue
		}
		err = mt.appendEntry(entry)
		entry.Decr()
		if err != nil {
			return nil, err
		}
	}
	mt.Incr()
	return mt, nil
}

func (lsm *LSM) recovery() error {
	lsm.mtMu.Lock()
	defer lsm.mtMu.Unlock()
	segments, err := lsm.wal.ListSegments()
	if err != nil {
		return err
	}

	segmentID := lsm.ckptManager.loadSegmentID()
	for _, segment := range segments {
		if segment.ID < segmentID {
			if err := lsm.wal.Remove(segment.ID); err != nil {
				return err
			}
			continue
		}

		mt, err := lsm.openMemtable(segment.ID)
		if err != nil {
			return err
		}

		if segment.ID == segmentID {
			lsm.mt = mt
		} else {
			lsm.imt = append(lsm.imt, mt)
		}
	}

	return nil
}
