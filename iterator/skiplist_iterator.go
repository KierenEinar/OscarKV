package iterator

// import (
// 	"OscarKV/kv"
// 	"OscarKV/utils"
// 	"bytes"
// 	"encoding/binary"
// 	"io"
// )

// type SkiplistIterator struct {
// 	sl     *utils.Skiplist
// 	closed bool
// 	ref    int32
// }

// func NewSkiplistIterator(sl *utils.Skiplist) *SkiplistIterator {
// 	return &SkiplistIterator{sl: sl, ref: 0}
// }

// func (it *SkiplistIterator) Next() (*kv.Entry, error) {
// 	if it.closed {
// 		return nil, io.EOF
// 	}
// 	if it.ref <= 0 {
// 		return nil, io.EOF
// 	}
// 	if it.sl == nil {
// 		return nil, io.EOF
// 	}

// 	var (
// 		k   []byte
// 		v   []byte
// 		err error
// 	)
// 	if it.seekGE {
// 		n, err := it.sl.FindGE(it.curr)
// 		it.seekGE = false
// 		if err != nil {
// 			if err == utils.ErrKeyNotFound {
// 				return nil, io.EOF
// 			}
// 			return nil, err
// 		}
// 		k, err = it.sl.Key(n)
// 		if err != nil {
// 			return nil, err
// 		}
// 		if it.prefix != nil && !bytes.HasPrefix(k, it.prefix) {
// 			return nil, io.EOF
// 		}
// 		v, err = it.sl.Value(n)
// 		if err != nil {
// 			return nil, err
// 		}
// 	} else {
// 		n, err := it.sl.FindGT(it.curr)
// 		if err != nil {
// 			if err == utils.ErrKeyNotFound {
// 				return nil, io.EOF
// 			}
// 			return nil, err
// 		}
// 		k, err = it.sl.Key(n)
// 		if err != nil {
// 			return nil, err
// 		}
// 		if it.prefix != nil && !bytes.HasPrefix(k, it.prefix) {
// 			return nil, io.EOF
// 		}
// 		v, err = it.sl.Value(n)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	if err != nil {
// 		return nil, err
// 	}

// 	ik, err := decodeInternalKey(k)
// 	if err != nil {
// 		return nil, err
// 	}
// 	vs, err := decodeInternalValue(v)
// 	if err != nil {
// 		return nil, err
// 	}

// 	it.curr = append(it.curr[:0], k...)
// 	e := kv.NewInternalEntry(ik.Key, vs.Value, ik.Version, ik.CF, vs.Meta, vs.TTL)
// 	return e, nil
// }

// func (it *SkiplistIterator) SeekToLast() error {
// 	if it.sl == nil {
// 		return nil
// 	}
// 	if it.prefix != nil {
// 		it.curr = append(it.curr[:0], it.prefix...)
// 		it.curr = append(it.curr, 0xFF)
// 		it.seekGE = false
// 		return nil
// 	}
// 	n, err := it.sl.FindLast()
// 	if err != nil {
// 		if err == utils.ErrKeyNotFound {
// 			return nil
// 		}
// 		return err
// 	}
// 	k, err := it.sl.Key(n)
// 	if err != nil {
// 		return err
// 	}
// 	it.curr = append(it.curr[:0], k...)
// 	it.seekGE = false
// 	return nil
// }

// func (it *SkiplistIterator) SeekToFirst() error {
// 	if it.prefix != nil {
// 		it.curr = append(it.curr[:0], it.prefix...)
// 	} else {
// 		it.curr = nil
// 	}
// 	it.seekGE = false
// 	return nil
// }

// func (it *SkiplistIterator) Seek(key []byte, mode SeekMode) error {

// 	var (
// 		keyFn func() []byte
// 		valFn func() []byte
// 		err   error
// 	)

// 	switch mode {
// 	case GTE, EQ:
// 		keyFn, valFn, err = it.sl.Seek(key, true)
// 	default:
// 		keyFn, valFn, err = it.sl.Seek(key, false)
// 	}
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (it *SkiplistIterator) Incr() {
// 	if it.ref == 0 && it.sl != nil {
// 		it.sl.Incr()
// 	}
// 	it.ref++
// }

// func (it *SkiplistIterator) Decr() {
// 	it.ref--
// 	if it.ref > 0 {
// 		return
// 	}
// 	if it.ref < 0 {
// 		panic("skiplist iterator ref underflow")
// 	}
// 	it.closed = true
// 	if it.sl != nil {
// 		it.sl.Decr()
// 		it.sl = nil
// 	}
// }

// func decodeInternalKey(b []byte) (kv.InternalKey, error) {
// 	if len(b) < 1+8 {
// 		return kv.InternalKey{}, io.ErrUnexpectedEOF
// 	}
// 	cfLen := int(b[0])
// 	if 1+cfLen > len(b)-8 {
// 		return kv.InternalKey{}, io.ErrUnexpectedEOF
// 	}
// 	cf := append([]byte(nil), b[1:1+cfLen]...)
// 	key := append([]byte(nil), b[1+cfLen:len(b)-8]...)
// 	ver := binary.BigEndian.Uint64(b[len(b)-8:])
// 	return kv.InternalKey{CFLen: uint8(cfLen), CF: cf, Key: key, Version: ver}, nil
// }

// func decodeInternalValue(b []byte) (kv.ValueStruct, error) {
// 	if len(b) < 9 {
// 		return kv.ValueStruct{}, io.ErrUnexpectedEOF
// 	}
// 	meta := b[0]
// 	ttl := int64(binary.BigEndian.Uint64(b[1:9]))
// 	val := append([]byte(nil), b[9:]...)
// 	return kv.ValueStruct{EntryHeader: kv.EntryHeader{Meta: meta, TTL: ttl}, Value: val}, nil
// }
