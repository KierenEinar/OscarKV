package sstable

import (
	"OscarKV/kv"
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBlockIteratorSeek_ReturnsExactMatch(t *testing.T) {
	builder := NewTableBuilder(1<<20, 64<<20)
	defer PutTableBuilder(builder)

	cf := []byte("cf")

	type rec struct {
		key     string
		version uint64
		value   string
	}
	records := []rec{
		{key: "k0000001", version: 1, value: "v1"},
		{key: "k0000003", version: 1, value: "v3"},
		{key: "k0000005", version: 1, value: "v5"},
	}

	for _, r := range records {
		e := kv.NewInternalEntry([]byte(r.key), []byte(r.value), r.version, cf, uint8(kv.MetaSet), 0)
		if _, err := builder.AddEntry(e); err != nil {
			t.Fatalf("AddEntry: %v", err)
		}
		e.Decr()
	}
	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(builder.blocks) != 1 {
		t.Fatalf("expected 1 data block, got %d", len(builder.blocks))
	}

	b := builder.blocks[0]
	data := builder.buffer.Bytes()
	start := int(b.offset)
	end := start + int(b.length)
	if start < 0 || end > len(data) || start >= end {
		t.Fatalf("invalid block range: [%d,%d) of %d", start, end, len(data))
	}

	bi, err := newBlockIterator(data[start:end])
	if err != nil {
		t.Fatalf("newBlockIterator: %v", err)
	}
	defer bi.close()

	var ver [8]byte
	binary.BigEndian.PutUint64(ver[:], records[1].version)
	searchKey := make([]byte, 0, 1+len(cf)+len(records[1].key)+8)
	searchKey = append(searchKey, byte(len(cf)))
	searchKey = append(searchKey, cf...)
	searchKey = append(searchKey, records[1].key...)
	searchKey = append(searchKey, ver[:]...)

	e, err := bi.seek(searchKey)
	if err != nil {
		t.Fatalf("seek: %v", err)
	}
	if e == nil {
		t.Fatalf("seek returned nil entry")
	}
	defer e.Decr()

	if string(e.CF) != string(cf) {
		t.Fatalf("cf mismatch: want %q got %q", string(cf), string(e.CF))
	}
	if string(e.Key) != records[1].key {
		t.Fatalf("key mismatch: want %q got %q", records[1].key, string(e.Key))
	}
	if string(e.Value) != records[1].value {
		t.Fatalf("value mismatch: want %q got %q", records[1].value, string(e.Value))
	}
	if e.Version != records[1].version {
		t.Fatalf("version mismatch: want %d got %d", records[1].version, e.Version)
	}
}

func TestBlockIteratorSeek_MissingKey_ReturnsNotFound(t *testing.T) {
	builder := NewTableBuilder(1<<20, 64<<20)
	defer PutTableBuilder(builder)

	cf := []byte("cf")

	e1 := kv.NewInternalEntry([]byte("k0000001"), []byte("v1"), 1, cf, uint8(kv.MetaSet), 0)
	e2 := kv.NewInternalEntry([]byte("k0000005"), []byte("v5"), 1, cf, uint8(kv.MetaSet), 0)
	if _, err := builder.AddEntry(e1); err != nil {
		t.Fatalf("AddEntry e1: %v", err)
	}
	if _, err := builder.AddEntry(e2); err != nil {
		t.Fatalf("AddEntry e2: %v", err)
	}
	e1.Decr()
	e2.Decr()

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(builder.blocks) != 1 {
		t.Fatalf("expected 1 data block, got %d", len(builder.blocks))
	}

	b := builder.blocks[0]
	data := builder.buffer.Bytes()
	start := int(b.offset)
	end := start + int(b.length)

	bi, err := newBlockIterator(data[start:end])
	if err != nil {
		t.Fatalf("newBlockIterator: %v", err)
	}
	defer bi.close()

	var ver [8]byte
	binary.BigEndian.PutUint64(ver[:], 1)
	searchKey := make([]byte, 0, 1+len(cf)+len("k0000003")+8)
	searchKey = append(searchKey, byte(len(cf)))
	searchKey = append(searchKey, cf...)
	searchKey = append(searchKey, "k0000003"...)
	searchKey = append(searchKey, ver[:]...)

	e, err := bi.seek(searchKey)
	if err != nil && err != ErrKeyNotFound {
		t.Fatalf("seek: %v", err)
	}
	if e != nil {
		if bytes.Equal(e.Key, []byte("k0000003")) {
			t.Fatalf("expected ErrKeyNotFound or different user key, got same user key %v", e)
		}
		e.Decr()
	}
}
