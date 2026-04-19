package sstable

import (
	"OscarKV/iterator"
	"OscarKV/kv"
	"OscarKV/pb"
	"OscarKV/utils"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
)

const (
	checksumLen = 4
	offsetLen   = 4
)

type (
	Table struct {
		mutex  sync.RWMutex
		ss     *sstable
		index  atomic.Pointer[pb.TableIndex]
		minKey []byte
		maxKey []byte
		ref    int32
	}

	blockEntry struct {
		prefixKeyLen  uint16
		diffKeyLen    uint16
		valLen        uint16
		internalKey   []byte
		internalValue []byte
	}
)

func Open(opt *Option, loadIndex bool) (table *Table, err error) {
	var ss *sstable

	defer func() {
		if err != nil {
			if ss != nil {
				_ = ss.close()
			}
		}
	}()

	ss, err = openSSTable(opt)
	if err != nil {
		return nil, err
	}

	if err = ss.init(loadIndex); err != nil {
		return nil, err
	}

	table = &Table{
		ss: ss,
	}
	table.index.Store(ss.loadIndex())
	table.Incr()
	return
}

func (table *Table) Incr() {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	table.ref += 1
}

func (table *Table) Decr() {
	table.mutex.Lock()
	table.ref -= 1
	if table.ref == 0 {
		ss := table.ss
		table.ss = nil
		table.index.Store(nil)
		table.minKey = nil
		table.maxKey = nil
		defer func() {
			_ = ss.close()
		}()
	}
	table.mutex.Unlock()
}

func (table *Table) indexs() (*pb.TableIndex, error) {
	if idx := table.index.Load(); idx != nil {
		return idx, nil
	}

	table.mutex.Lock()
	defer table.mutex.Unlock()

	if idx := table.index.Load(); idx != nil {
		return idx, nil
	}
	if table.ss == nil {
		return nil, ErrNilSSTable
	}
	idx := table.ss.loadIndex()
	if idx != nil {
		table.index.Store(idx)
	}
	return idx, nil
}

type TableIterator struct {
	table    *Table
	ref      int32
	mutex    sync.RWMutex
	curBlock int
	bi       *blockIterator
}

func (t *Table) NewIterator() iterator.Iterator {
	return &TableIterator{table: t, curBlock: -1}
}

func (ti *TableIterator) Incr() {
	ti.mutex.Lock()
	defer ti.mutex.Unlock()
	if ti.ref == 0 && ti.table != nil {
		ti.table.Incr()
	}
	ti.ref += 1
}

func (ti *TableIterator) Decr() {
	ti.mutex.Lock()
	defer ti.mutex.Unlock()
	ti.ref -= 1
	if ti.ref == 0 {
		if ti.bi != nil {
			ti.bi.close()
			ti.bi = nil
		}
		ti.table.Decr()
		ti.table = nil
		ti.curBlock = -1
	}
	if ti.ref < 0 {
		panic("table iterator ref underflow")
	}
}

func (ti *TableIterator) SeekToFirst() (*kv.Entry, error) {
	ti.mutex.RLock()
	defer ti.mutex.RUnlock()
	if ti.table == nil {
		return nil, ErrEmptyTable
	}
	if ti.bi != nil {
		ti.bi.close()
		ti.bi = nil
	}
	ti.curBlock = -1
	e, err := ti.Next()
	return e, err
}

func (ti *TableIterator) locateIndex(key []byte) (*pb.BlockOffset, error) {

	index, err := ti.table.indexs()
	if err != nil {
		return nil, err
	}

	offset := index.GetOffset()
	if len(index.GetOffset()) < 1 {
		return nil, ErrEmptyDataBlock
	}
	if len(key) < 9 {
		return nil, ErrInvalidKeyFormat
	}

	lo := 0
	hi := len(offset) - 1
	for lo <= hi {
		mid := lo + int((hi-lo)>>1)
		midKey := offset[mid].GetMinKey()
		if utils.CompareKey(midKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if lo == 0 {
		return nil, ErrKeyNotFound
	}

	return offset[lo-1], nil
}

func (ti *TableIterator) Next() (*kv.Entry, error) {
	ti.mutex.RLock()
	defer ti.mutex.RUnlock()

	if ti.table == nil {
		return nil, ErrEmptyTable
	}

	if ti.bi != nil {
		e, err := ti.bi.next()
		if err != nil && err != io.EOF {
			return nil, err
		}
		if e != nil {
			e.Incr()
			return e, nil
		}
	}

	index, err := ti.table.indexs()
	if err != nil {
		return nil, err
	}
	offsets := index.GetOffset()
	if len(offsets) == 0 {
		return nil, io.EOF
	}

	start := ti.curBlock + 1
	if ti.bi == nil && ti.curBlock < 0 {
		start = 0
	}
	for i := start; i < len(offsets); i++ {
		bo := offsets[i]
		data, err := ti.table.ss.read(int64(bo.GetOffset()), int64(bo.GetLength()))
		if err != nil {
			return nil, err
		}
		bi, err := newBlockIterator(data)
		if err != nil {
			continue
		}
		ti.bi = bi
		ti.curBlock = i
		if err := ti.bi.seekToFirst(); err != nil {
			continue
		}
		if ti.bi.entry != nil {
			ti.bi.entry.Incr()
			return ti.bi.entry, nil
		}
	}
	return nil, io.EOF
}

func (ti *TableIterator) Seek(key []byte) (*kv.Entry, error) {
	ti.mutex.RLock()
	defer ti.mutex.RUnlock()

	if ti.table == nil {
		return nil, ErrEmptyTable
	}

	if ti.bi != nil {
		ti.bi.close()
		ti.bi = nil
	}
	ti.curBlock = -1

	index, err := ti.table.indexs()
	if err != nil {
		return nil, err
	}
	offsets := index.GetOffset()
	if len(offsets) == 0 {
		return nil, ErrEmptyDataBlock
	}
	if len(key) < 9 {
		return nil, ErrInvalidKeyFormat
	}

	lo := 0
	hi := len(offsets) - 1
	for lo <= hi {
		mid := lo + int((hi-lo)>>1)
		midKey := offsets[mid].MinKey
		if utils.CompareKey(midKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	idx := lo
	if lo > 0 {
		idx = lo - 1
	}

	if idx >= len(offsets) {
		return nil, ErrKeyNotFound
	}

	bo := offsets[idx]
	data, err := ti.table.ss.read(int64(bo.GetOffset()), int64(bo.GetLength()))
	if err != nil {
		return nil, err
	}
	bi, err := newBlockIterator(data)
	if err != nil {
		return nil, err
	}
	e, err := bi.seek(key)
	if err != nil {
		if err == ErrKeyNotFound && idx+1 < len(offsets) {
			bi.close()
			idx++
			bo = offsets[idx]
			data, err = ti.table.ss.read(int64(bo.GetOffset()), int64(bo.GetLength()))
			if err != nil {
				return nil, err
			}
			bi, err = newBlockIterator(data)
			if err != nil {
				return nil, err
			}
			if err := bi.seekToFirst(); err != nil {
				bi.close()
				return nil, err
			}
			e = bi.entry
		} else {
			bi.close()
			return nil, err
		}
	}

	ti.curBlock = idx
	ti.bi = bi
	e.Incr()
	return e, nil
}

type blockIterator struct {
	data    []byte
	off     uint32
	basekey []byte
	idx     uint32
	offsets []uint32
	entry   *kv.Entry
}

func newBlockIterator(data []byte) (*blockIterator, error) {
	if len(data) < checksumLen+offsetLen {
		return nil, ErrDataBlockCorruption
	}

	storedChecksum := binary.BigEndian.Uint32(data[len(data)-checksumLen:])
	payload := data[:len(data)-checksumLen]
	if utils.ChecksumCastagnoli(payload) != storedChecksum {
		return nil, ErrDataBlockCorruption
	}

	offsetSize := binary.BigEndian.Uint32(data[len(data)-(checksumLen+offsetLen) : len(data)-checksumLen])
	offsetBytesLen := int(offsetSize)
	if offsetBytesLen%4 != 0 {
		return nil, ErrDataBlockCorruption
	}

	offsetsEnd := len(data) - (checksumLen + offsetLen)
	offsetsStart := offsetsEnd - offsetBytesLen
	if offsetsStart < 0 || offsetsStart > offsetsEnd {
		return nil, ErrDataBlockCorruption
	}

	offsetsBytes := data[offsetsStart:offsetsEnd]
	offsets := make([]uint32, 0, offsetBytesLen/4)
	for i := 0; i < offsetBytesLen; i += 4 {
		offsets = append(offsets, binary.BigEndian.Uint32(offsetsBytes[i:i+4]))
	}
	if len(offsets) == 0 {
		return nil, ErrEmptyDataBlock
	}
	bi := &blockIterator{
		offsets: offsets,
		data:    data,
	}
	if err := bi.seekToFirst(); err != nil {
		return nil, err
	}
	return bi, nil
}

func (bi *blockIterator) seekToFirst() error {
	bi.off = 0
	bi.idx = 0
	return bi.setIdx(0)
}

func (bi *blockIterator) setIdx(index int) error {
	if index < 0 || index >= len(bi.offsets) {
		return ErrInvalidIndex
	}
	if bi.entry != nil {
		bi.entry.Decr()
		bi.entry = nil
	}

	data := bi.data
	entryOff := int(bi.offsets[index])
	if entryOff < 0 || entryOff+6 > len(data) {
		return ErrEntryCorruption
	}

	prefixKeyLen := binary.BigEndian.Uint16(data[entryOff : entryOff+2])
	diffKeyLen := binary.BigEndian.Uint16(data[entryOff+2 : entryOff+4])
	valLen := binary.BigEndian.Uint16(data[entryOff+4 : entryOff+6])

	pos := entryOff + 6
	entrySize := 6 + int(diffKeyLen) + int(valLen) + checksumLen
	if entryOff+entrySize > len(data) {
		return ErrEntryCorruption
	}

	diffKey := data[pos : pos+int(diffKeyLen)]
	pos += int(diffKeyLen)
	internalValue := data[pos : pos+int(valLen)]
	pos += int(valLen)

	storedEntryChecksum := binary.BigEndian.Uint32(data[pos : pos+checksumLen])
	if utils.ChecksumCastagnoli(data[entryOff:pos]) != storedEntryChecksum {
		return ErrEntryChecksumMismatch
	}

	internalKey := make([]byte, int(prefixKeyLen)+int(diffKeyLen))
	if prefixKeyLen != 0 {
		if bi.basekey == nil && index != 0 {
			return ErrEntryCorruption
		}
		n := int(prefixKeyLen)
		if n > len(bi.basekey) {
			return ErrEntryCorruption
		}
		copy(internalKey[:n], bi.basekey[:n])
	}
	copy(internalKey[int(prefixKeyLen):], diffKey)
	if index == 0 {
		bi.basekey = make([]byte, len(internalKey))
		copy(bi.basekey, internalKey)
	}

	var version uint64
	userKey := internalKey
	if len(internalKey) >= 8 {
		version = binary.BigEndian.Uint64(internalKey[len(internalKey)-8:])
		userKey = internalKey[:len(internalKey)-8]
	}

	var cf []byte
	var key []byte
	if len(userKey) != 0 {
		cfLen := int(userKey[0])
		if 1+cfLen <= len(userKey) {
			cf = userKey[1 : 1+cfLen]
			key = userKey[1+cfLen:]
		} else if len(userKey) > 1 {
			key = userKey[1:]
		} else {
			return ErrEntryCorruption
		}
	}
	cfCopy := make([]byte, len(cf))
	copy(cfCopy, cf)
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	var meta uint8
	var ttl int64
	var userVal []byte
	if len(internalValue) != 0 {
		meta = internalValue[0]
	}
	if len(internalValue) >= 9 {
		ttl = int64(binary.BigEndian.Uint64(internalValue[1:9]))
		userVal = internalValue[9:]
	} else if len(internalValue) > 1 {
		userVal = internalValue[1:]
	}
	valCopy := make([]byte, len(userVal))
	copy(valCopy, userVal)

	bi.entry = kv.NewInternalEntry(keyCopy, valCopy, version, cfCopy, meta, ttl)
	bi.idx = uint32(index)
	bi.off = uint32(entryOff)
	return nil
}

func (bi *blockIterator) next() (*kv.Entry, error) {
	if len(bi.offsets) == 0 {
		return nil, io.EOF
	}
	if bi.idx+1 >= uint32(len(bi.offsets)) {
		return nil, io.EOF
	}
	if err := bi.setIdx(int(bi.idx) + 1); err != nil {
		return nil, err
	}
	return bi.entry, nil
}

// key must be internalKey, cf+key+version
func (bi *blockIterator) seek(key []byte) (*kv.Entry, error) {

	if len(bi.offsets) == 0 {
		return nil, ErrEmptyDataBlock
	}
	if len(key) < 9 {
		return nil, ErrInvalidKeyFormat
	}
	cfLen := int(key[0])
	if 1+cfLen > len(key)-8 {
		return nil, ErrInvalidKeyFormat
	}

	lo := 0
	hi := len(bi.offsets) - 1

	for lo <= hi {
		mid := lo + (hi-lo)>>1
		if err := bi.setIdx(mid); err != nil {
			return nil, err
		}
		entry := bi.entry
		res := utils.CompareKey(entry.InternalKey(), key)
		if res < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if lo >= len(bi.offsets) {
		bi.entry.Decr()
		bi.entry = nil
		return nil, ErrKeyNotFound
	}

	err := bi.setIdx(lo)
	if err != nil {
		return nil, err
	}
	entry := bi.entry
	entry.Incr()
	return entry, nil
}

func (bi *blockIterator) close() {
	if bi.entry != nil {
		bi.entry.Decr()
		bi.entry = nil
	}

	bi.data = nil
	bi.idx = 0
	bi.off = 0
	bi.basekey = nil
	bi.offsets = nil
}
