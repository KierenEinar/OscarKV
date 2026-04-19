package utils

import (
	"hash"
	"hash/crc32"
	"sync"
)

var crc32Pool = sync.Pool{
	New: func() any {
		return crc32.NewIEEE()
	},
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var crc32cPool = sync.Pool{
	New: func() any {
		return crc32.New(crc32cTable)
	},
}

func GetCRC32() hash.Hash32 {
	h := crc32Pool.Get().(hash.Hash32)
	h.Reset()
	return h
}

func GetCRC32C() hash.Hash32 {
	h := crc32cPool.Get().(hash.Hash32)
	h.Reset()
	return h
}

func PutCRC32(h hash.Hash32) {
	if h == nil {
		return
	}
	h.Reset()
	crc32Pool.Put(h)
}

func PutCRC32C(h hash.Hash32) {
	if h == nil {
		return
	}
	h.Reset()
	crc32cPool.Put(h)
}

func ChecksumCastagnoli(parts ...[]byte) uint32 {
	h := GetCRC32C()
	for _, p := range parts {
		_, _ = h.Write(p)
	}
	sum := h.Sum32()
	PutCRC32C(h)
	return sum
}
