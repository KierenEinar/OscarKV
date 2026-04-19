package kv

import (
	"encoding/binary"
	"io"
)

func SplitInternalKey(b []byte) (cfLen uint8, cf []byte, key []byte, version uint64, err error) {
	if len(b) < 1+8 {
		return 0, nil, nil, 0, io.ErrUnexpectedEOF
	}
	cfLen = b[0]
	if int(1+cfLen) > len(b)-8 {
		return 0, nil, nil, 0, io.ErrUnexpectedEOF
	}
	cf = b[1 : 1+cfLen]
	key = b[1+cfLen : len(b)-8]
	version = binary.BigEndian.Uint64(b[len(b)-8:])
	return cfLen, cf, key, version, nil
}

func SplitInternalVal(b []byte) (meta byte, ttl int64, value []byte, err error) {
	if len(b) < 1+8 {
		return 0, 0, nil, io.ErrUnexpectedEOF
	}
	meta = b[0]
	ttl = int64(binary.BigEndian.Uint64(b[1:9]))
	value = b[9:]
	return meta, ttl, value, nil
}

func MakeInternalKey(cf []byte, key []byte, version uint64) []byte {
	if len(cf) > 255 {
		return nil
	}
	b := make([]byte, 0, 1+len(cf)+len(key)+8)
	b = append(b, byte(len(cf)))
	b = append(b, cf...)
	b = append(b, key...)
	var ver [8]byte
	binary.BigEndian.PutUint64(ver[:], version)
	b = append(b, ver[:]...)
	return b
}
