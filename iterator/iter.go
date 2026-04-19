package iterator

import (
	"OscarKV/kv"
)

type Iterator interface {
	// returns io.EOF when the iterator is empty.
	SeekToFirst() (*kv.Entry, error)
	// seek returns the entry with key greater than or equal to the given key.
	// it returns ErrKeyNotFound when the key is not found.
	Seek(key []byte) (*kv.Entry, error)
	// returns io.EOF when the iterator is empty.
	Next() (*kv.Entry, error)
	Incr()
	Decr()
}
