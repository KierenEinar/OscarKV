package utils

import (
	"bytes"
	"testing"
)

func TestSkiplist_PutAndFind(t *testing.T) {
	cmp := func(a, b []byte) CmpR {
		res := bytes.Compare(a, b)
		if res < 0 {
			return LessThan
		} else if res > 0 {
			return GreaterThan
		}
		return Equal
	}

	sl := NewSkiplist(1024*1024, 16, 0.25, nil, cmp)

	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
	}
	values := [][]byte{
		[]byte("red"),
		[]byte("yellow"),
		[]byte("reddish"),
		[]byte("brown"),
	}

	for i := range keys {
		err := sl.Put(keys[i], values[i])
		if err != nil {
			t.Fatalf("Put failed for key %s: %v", keys[i], err)
		}
	}

	// Test FindGT
	for i := range keys {
		node, err := sl.FindGT(keys[i])
		if err != nil {
			// FindGT(key) returns the node with key > given key, so it should find cherry if given banana
			if i < len(keys)-1 {
				t.Fatalf("FindGT failed for key %s: %v", keys[i], err)
			} else if err != ErrKeyNotFound {
				t.Fatalf("Expected ErrKeyNotFound for last key, got %v", err)
			}
			continue
		}
		
		foundKey, _ := sl.Key(node)
		if !bytes.Equal(foundKey, keys[i+1]) {
			t.Errorf("Expected key %s, got %s", keys[i+1], foundKey)
		}
	}
}

func TestSkiplist_Update(t *testing.T) {
	cmp := func(a, b []byte) CmpR {
		res := bytes.Compare(a, b)
		if res < 0 {
			return LessThan
		} else if res > 0 {
			return GreaterThan
		}
		return Equal
	}

	sl := NewSkiplist(1024, 16, 0.25, nil, cmp)

	key := []byte("test")
	val1 := []byte("value1")
	val2 := []byte("value2_longer")

	sl.Put(key, val1)
	sl.Put(key, val2)

	// We don't have a Get method, but we can use findGT to find the node
	// findPrev returns the previous node, so searching for "tes" should return the node with "test"
	prevOffsets := sl.findPrev([]byte("tes"), false)
	nextOffset := sl.decodeNode(prevOffsets[0]).next[0]
	if nextOffset == dummyOffset {
		t.Fatal("Node not found")
	}
	node := sl.decodeNode(nextOffset)
	v, _ := sl.Value(node)
	if !bytes.Equal(v, val2) {
		t.Errorf("Expected %s, got %s", val2, v)
	}
}

func TestSkiplist_OutOfMemory(t *testing.T) {
	cmp := func(a, b []byte) CmpR {
		res := bytes.Compare(a, b)
		if res < 0 {
			return LessThan
		} else if res > 0 {
			return GreaterThan
		}
		return Equal
	}

	sl := NewSkiplist(10, 16, 0.25, nil, cmp)
	err := sl.Put([]byte("very_long_key"), []byte("value"))
	if err != ErrOutOfMemory {
		t.Errorf("Expected ErrOutOfMemory, got %v", err)
	}
}

func TestSkiplist_RefCounting(t *testing.T) {
	closed := false
	onClose := func() {
		closed = true
	}
	cmp := func(a, b []byte) CmpR { return Equal }

	sl := NewSkiplist(1024, 16, 0.25, onClose, cmp)
	sl.Incr()
	sl.Incr()
	sl.Decr()
	if closed {
		t.Error("Expected not closed after first Decr")
	}
	sl.Decr()
	if !closed {
		t.Error("Expected closed after second Decr")
	}
}
