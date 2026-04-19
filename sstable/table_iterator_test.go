package sstable

import (
	"OscarKV/kv"
	"OscarKV/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

type expect struct {
	cf      string
	key     string
	value   string
	version uint64
}

func TestTableIteratorNext_ReadsAllEntries(t *testing.T) {
	expected := make([]expect, 0, 200)
	for i := 0; i < 200; i++ {
		expected = append(expected, expect{
			cf:      fmt.Sprintf("cf%d", i%3),
			key:     fmt.Sprintf("k%03d", i),
			value:   fmt.Sprintf("v%03d", i),
			version: uint64(i + 1),
		})
	}
	sortExpected(expected)

	table := writeAndOpenTable(t, expected, 256)
	defer table.Decr()

	it := table.NewIterator()
	it.Incr()
	defer it.Decr()

	var prevInternalKey []byte
	for i := 0; i < len(expected); i++ {
		e, err := it.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if e == nil {
			t.Fatalf("Next[%d]: got nil entry", i)
		}

		ikey := e.InternalKey()
		if ikey == nil {
			t.Fatalf("Next[%d]: nil internal key", i)
		}
		if prevInternalKey != nil && utils.CompareKey(prevInternalKey, ikey) > 0 {
			t.Fatalf("Next[%d]: internal key not monotonic", i)
		}
		prevInternalKey = append(prevInternalKey[:0], ikey...)

		if string(e.CF) != expected[i].cf {
			t.Fatalf("entry[%d] cf mismatch: want %q got %q", i, expected[i].cf, string(e.CF))
		}
		if string(e.Key) != expected[i].key {
			t.Fatalf("entry[%d] key mismatch: want %q got %q", i, expected[i].key, string(e.Key))
		}
		if string(e.Value) != expected[i].value {
			t.Fatalf("entry[%d] value mismatch: want %q got %q", i, expected[i].value, string(e.Value))
		}
		e.Decr()
	}

	e, err := it.Next()
	if err != io.EOF {
		t.Fatalf("Next after exhausted: want io.EOF, got %v", err)
	}
	if e != nil {
		e.Decr()
		t.Fatalf("expected nil after exhausted, got %v", e)
	}
}

func TestTableIteratorSeek_ZeroVersionKey(t *testing.T) {
	expected := make([]expect, 0, 100)
	for i := 0; i < 100; i++ {
		expected = append(expected, expect{
			cf:      "cf",
			key:     fmt.Sprintf("k%09d", i),
			value:   fmt.Sprintf("v%03d", i),
			version: uint64(i + 1),
		})
	}
	sortExpected(expected)

	table := writeAndOpenTable(t, expected, 256)
	defer table.Decr()

	it := table.NewIterator()
	it.Incr()
	defer it.Decr()

	var zeroVer [8]byte
	for i := 0; i < len(expected); i++ {
		searchKey := makeInternalKey([]byte(expected[i].cf), []byte(expected[i].key), zeroVer[:])

		e, err := it.Seek(searchKey)
		if err != nil && err != ErrKeyNotFound {
			t.Fatalf("Seek[%d]: %v", i, err)
		}
		if e != nil {
			if bytes.Equal(e.Key, []byte(expected[i].key)) {
				t.Fatalf("Seek[%d] expected ErrKeyNotFound or different user key, got same user key %v", i, e)
			}
			e.Decr()
		}
	}
}

func TestTableIteratorSeek_VersionedKey(t *testing.T) {
	expected := make([]expect, 0, 100)
	for i := 0; i < 100; i++ {
		expected = append(expected, expect{
			cf:      "cf",
			key:     fmt.Sprintf("k%09d", i),
			value:   fmt.Sprintf("v%03d", i),
			version: uint64(i + 1),
		})
	}
	sortExpected(expected)

	table := writeAndOpenTable(t, expected, 256)
	defer table.Decr()

	it := table.NewIterator()
	it.Incr()
	defer it.Decr()

	for i := 0; i < 100; i++ {
		var ver [8]byte
		binary.BigEndian.PutUint64(ver[:], expected[i].version)
		searchKey := makeInternalKey([]byte(expected[i].cf), []byte(expected[i].key), ver[:])

		e, err := it.Seek(searchKey)
		if err != nil {
			t.Fatalf("Seek[%d]: %v", i, err)
		}
		if e == nil {
			t.Fatalf("Seek[%d]: got nil entry", i)
		}
		if string(e.CF) != expected[i].cf {
			t.Fatalf("Seek[%d] cf mismatch: want %q got %q", i, expected[i].cf, string(e.CF))
		}
		if string(e.Key) != expected[i].key {
			t.Fatalf("Seek[%d] key mismatch: want %q got %q", i, expected[i].key, string(e.Key))
		}
		if string(e.Value) != expected[i].value {
			t.Fatalf("Seek[%d] value mismatch: want %q got %q", i, expected[i].value, string(e.Value))
		}
		e.Decr()
	}
}

func newInternalEntry(key, value []byte, version uint64, cf []byte) *kv.Entry {
	return kv.NewInternalEntry(key, value, version, cf, uint8(kv.MetaSet), 0)
}

func writeAndOpenTable(t *testing.T, entries []expect, maxBlockSize uint32) *Table {
	t.Helper()

	rootDir := t.TempDir()
	dataDir := "data"
	if err := os.MkdirAll(filepath.Join(rootDir, dataDir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	writeOpt := &Option{
		RootDir:       rootDir,
		DataDir:       dataDir,
		Fid:           1,
		EstimatedSize: 0,
		Flags:         os.O_RDWR | os.O_CREATE | os.O_TRUNC,
	}

	builder := NewTableBuilder(maxBlockSize, 64<<20)
	for _, e := range entries {
		en := newInternalEntry([]byte(e.key), []byte(e.value), e.version, []byte(e.cf))
		if _, err := builder.AddEntry(en); err != nil {
			en.Decr()
			PutTableBuilder(builder)
			t.Fatalf("AddEntry: %v", err)
		}
		en.Decr()
	}
	if _, err := builder.Flush(writeOpt); err != nil {
		PutTableBuilder(builder)
		t.Fatalf("Flush: %v", err)
	}
	PutTableBuilder(builder)

	readOpt := &Option{
		RootDir: rootDir,
		DataDir: dataDir,
		Fid:     1,
		Flags:   os.O_RDONLY,
	}
	table, err := Open(readOpt, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return table
}

func encodeCF(cf []byte) []byte {
	b := make([]byte, 1+len(cf))
	b[0] = byte(len(cf))
	copy(b[1:], cf)
	return b
}

func makeInternalKey(cf []byte, userKey []byte, versionBytes []byte) []byte {
	b := make([]byte, 0, 1+len(cf)+len(userKey)+8)
	b = append(b, byte(len(cf)))
	b = append(b, cf...)
	b = append(b, userKey...)
	b = append(b, versionBytes...)
	return b
}

func sortExpected(entries []expect) {
	sort.Slice(entries, func(i, j int) bool {
		var vi [8]byte
		var vj [8]byte
		binary.BigEndian.PutUint64(vi[:], entries[i].version)
		binary.BigEndian.PutUint64(vj[:], entries[j].version)
		ki := makeInternalKey([]byte(entries[i].cf), []byte(entries[i].key), vi[:])
		kj := makeInternalKey([]byte(entries[j].cf), []byte(entries[j].key), vj[:])
		return utils.CompareKey(ki, kj) < 0
	})
}
