package kv

import (
	"OscarKV/utils"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type Meta uint8

var (
	entryPool = sync.Pool{
		New: func() any {
			return &Entry{}
		},
	}

	requestPool = sync.Pool{
		New: func() any {
			return &Requests{
				Result: make(chan error, 1),
			}
		},
	}
)

const (
	MetaUndefined Meta = iota
	MetaSet
	MetaDel
	MetaDelRange
)

type Entry struct {
	Key     []byte
	Value   []byte
	Version uint64
	CF      []byte
	Meta    uint8
	TTL     int64
	ref     int32
	ikey    []byte
	ival    []byte
}

func (e *Entry) Pretty() string {
	return fmt.Sprintf("Entry{CF:%q Key:%q Version:%d Meta:%d TTL:%d Value:%q}", e.CF, e.Key, e.Version, e.Meta, e.TTL, e.Value)
}
func (e *Entry) reset() {
	utils.PutBytes(e.Key)
	utils.PutBytes(e.Value)
	utils.PutBytes(e.CF)
	utils.PutBytes(e.ikey)
	utils.PutBytes(e.ival)
	e.Key = nil
	e.Value = nil
	e.ikey = nil
	e.ival = nil
	e.Version = 0
	e.CF = nil
	e.Meta = uint8(MetaUndefined)
	e.TTL = 0
	e.ref = 0
}

func (e *Entry) Incr() {
	atomic.AddInt32(&e.ref, 1)
}

func (e *Entry) Decr() {
	r := atomic.AddInt32(&e.ref, -1)
	if r == 0 {
		e.reset()
		entryPool.Put(e)
	}
}

type EntryHeader struct {
	Meta uint8
	TTL  int64
}

type InternalKey struct {
	CFLen   uint8
	CF      []byte
	Key     []byte
	Version uint64
}

type ValueStruct struct {
	EntryHeader
	Value []byte
}

type (
	Requests struct {
		Entries []*Entry
		Size    int
		Count   int
		ref     int32
		Result  chan error // be careful when using this channel
	}
)

func estimateSize(e *Entry) int {
	// header, internal_key, value, crc32
	// header -> meta: 1byte, ttl: varint, klen: varint, vlen: varint
	// internal_key -> 1byte + len(CF) + len(key) + 8byte
	// value -> len(value)
	// crc32 -> 4byte
	//
	headerLen := 1 + binary.MaxVarintLen64 + binary.MaxVarintLen64 + binary.MaxVarintLen64
	internalKeyLen := 1 + len(e.CF) + len(e.Key) + 8
	valueLen := len(e.Value)
	return headerLen + internalKeyLen + valueLen + 4
}

func Encode(e *Entry) []byte {
	// header, internal_key, value, crc32
	// buf layout:
	// | Meta (1) | TTL (varint) | KLen (varint) | VLen (varint) | CF Len (1) | CF | Key | Version (8) | Value | CRC32 (4) |
	maxSize := estimateSize(e)
	buf := utils.GetBytes(maxSize)

	// 1. Header: Meta (1) + TTL (varint)
	buf[0] = e.Meta
	offset := 1
	offset += binary.PutVarint(buf[offset:], e.TTL)

	// 2. KLen (varint) + VLen (varint)
	offset += binary.PutUvarint(buf[offset:], uint64(len(e.Key)))
	offset += binary.PutUvarint(buf[offset:], uint64(len(e.Value)))

	// 3. CF Len (1) + CF
	buf[offset] = uint8(len(e.CF))
	offset++
	copy(buf[offset:], e.CF)
	offset += len(e.CF)

	// 4. Key
	copy(buf[offset:], e.Key)
	offset += len(e.Key)

	// 5. Version (8)
	binary.BigEndian.PutUint64(buf[offset:], e.Version)
	offset += 8

	// 6. Value
	copy(buf[offset:], e.Value)
	offset += len(e.Value)

	// 7. CRC32 (4)
	crc := utils.ChecksumCastagnoli(buf[:offset])
	binary.BigEndian.PutUint32(buf[offset:], crc)
	offset += 4

	return buf[:offset]
}

func Decode(buf []byte) (int, *Entry, error) {
	e := entryPool.Get().(*Entry)
	e.reset()
	offset := 0
	// 1. Header: Meta (1) + TTL (varint)
	if len(buf) < 1 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	e.Meta = buf[0]
	ttl, n := binary.Varint(buf[1:])
	if n <= 0 {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	e.TTL = ttl
	offset = 1 + n

	// 2. KLen (varint) + VLen (varint)
	kLen, n := binary.Uvarint(buf[offset:])
	if n <= 0 {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	offset += n
	vLen, n := binary.Uvarint(buf[offset:])
	if n <= 0 {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	offset += n

	// 3. CF Len (1) + CF
	if offset >= len(buf) {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	cfLen := int(buf[offset])
	offset++
	if offset+cfLen > len(buf) {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	e.CF = make([]byte, cfLen)
	e.CF = utils.GetBytes(cfLen)
	copy(e.CF, buf[offset:offset+cfLen])
	offset += cfLen

	// 4. Key
	if offset+int(kLen) > len(buf) {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	e.Key = utils.GetBytes(int(kLen))
	copy(e.Key, buf[offset:offset+int(kLen)])
	offset += int(kLen)

	// 5. Version (8)
	if offset+8 > len(buf) {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	e.Version = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	// 6. Value
	if offset+int(vLen) > len(buf) {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	e.Value = utils.GetBytes(int(vLen))
	copy(e.Value, buf[offset:offset+int(vLen)])
	offset += int(vLen)

	// 7. CRC32 Check
	if offset+4 > len(buf) {
		e.reset()
		entryPool.Put(e)
		return 0, nil, io.ErrUnexpectedEOF
	}
	actualCrc := binary.BigEndian.Uint32(buf[offset : offset+4])
	expectedCrc := utils.ChecksumCastagnoli(buf[:offset])
	if actualCrc != expectedCrc {
		e.reset()
		entryPool.Put(e)
		return 0, nil, nil
	}

	e.Incr()
	return offset + 4, e, nil
}

func NewInternalEntry(key, value []byte, version uint64, cf []byte,
	meta uint8, ttl int64) *Entry {
	e := entryPool.Get().(*Entry)
	e.reset()
	e.Incr()
	e.Key = key
	e.Value = value
	e.Version = version
	e.CF = cf
	e.Meta = meta
	e.TTL = ttl
	return e
}

func (e *Entry) InternalKey() []byte {
	if e.ikey != nil {
		return e.ikey
	}
	if len(e.CF) > 255 {
		return nil
	}
	b := utils.GetBytes(1 + len(e.CF) + len(e.Key) + 8)
	offset := 0
	b[offset] = byte(len(e.CF))
	offset++
	copy(b[offset:], e.CF)
	offset += len(e.CF)
	copy(b[offset:], e.Key)
	offset += len(e.Key)
	binary.BigEndian.PutUint64(b[offset:], e.Version)
	e.ikey = b
	return b
}

func (e *Entry) InternalValue() []byte {
	if e.ival != nil {
		return e.ival
	}
	b := utils.GetBytes(1 + 8 + len(e.Value))
	b[0] = e.Meta
	binary.BigEndian.PutUint64(b[1:9], uint64(e.TTL))
	copy(b[9:], e.Value)
	e.ival = b
	return b
}

func NewRequest(entries []*Entry) *Requests {
	r := requestPool.Get().(*Requests)
	r.reset()
	r.Incr()
	r.applyEntries(entries)
	return r
}

func (r *Requests) reset() {
	for idx := range r.Entries {
		r.Entries[idx].Decr()
	}
	r.Entries = r.Entries[:0]
	r.Size = 0
	r.Count = 0
	// Do not reset ref here, it's managed by Incr/Decr
	if r.Result != nil {
		select {
		case <-r.Result:
		default:
		}
	}
}

func (r *Requests) Incr() {
	atomic.AddInt32(&r.ref, 1)
}

func (r *Requests) Decr() {
	ref := atomic.AddInt32(&r.ref, -1)
	if ref < 0 {
		panic("reference number invalid")
	}
	if ref == 0 {
		r.reset()
		requestPool.Put(r)
	}
}

func (r *Requests) applyEntries(entries []*Entry) {
	r.reset()
	r.Entries = append(r.Entries, entries...)
	r.Count = len(r.Entries)
	for idx, entry := range entries {
		r.Size += estimateSize(entry)
		entries[idx].Incr()
	}
}

func (r *Requests) Wait() error {
	err := <-r.Result
	return err
}
