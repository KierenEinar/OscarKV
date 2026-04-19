package utils

import "errors"

// skiplist module errors
var (
	ErrDataEmpty   = errors.New("Module: skiplist, Reason: empty data")
	ErrDummyNode   = errors.New("Module: skiplist, Reason: node is dummy, this is occured by bug")
	ErrOutOfBound  = errors.New("Module: invalid node, Reason: key or value out of bound")
	ErrOutOfMemory = errors.New("Module: skiplist, Reason: out of memory")
)

// wal module errors
var (
	ErrDropBlocked      = errors.New("Module: wal, Reason: removal blocked")
	ErrCorruption       = errors.New("Module: wal, Reason: chunk corrupted")
	ErrSeekNotSupported = errors.New("Module: wal, Reason: seek not supported")
)

// common errors
var (
	ErrKeyNotFound = errors.New("key not found")
)
