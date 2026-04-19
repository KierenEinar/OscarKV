package utils

import (
	"bytes"
	"encoding/binary"
)

func CompareKey(entryKey, key []byte) int {
	if len(entryKey) < 8 || len(key) < 8 {
		return bytes.Compare(entryKey, key)
	}
	entryUserKey := entryKey[:len(entryKey)-8]
	userKey := key[:len(key)-8]
	res := bytes.Compare(entryUserKey, userKey)
	if res != 0 {
		return res
	}

	entryVersion := binary.BigEndian.Uint64(entryKey[len(entryKey)-8:])
	userKeyVersion := binary.BigEndian.Uint64(key[len(key)-8:])
	// version sorted by descending order
	if entryVersion < userKeyVersion {
		return 1
	} else if entryVersion > userKeyVersion {
		return -1
	}
	return 0
}
