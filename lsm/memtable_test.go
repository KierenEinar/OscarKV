package lsm

import (
	"io"
	"testing"

	"OscarKV/kv"
	"OscarKV/utils"
)

func TestMemtable_AppendAndGet(t *testing.T) {
	// Build a memtable with a fresh skiplist; wal not required for this test
	sl := utils.NewSkiplist(
		/*memoryLimit*/ 1<<20, // 1MiB
		/*maxLevel*/ 16,
		/*randFactor*/ 0.5,
		/*onClose*/ nil,
		/*cmp*/ func(a, b []byte) utils.CmpR {
			return utils.CmpR(utils.CompareKey(a, b))
		},
	)
	mt := &memtable{
		index: sl,
	}

	cf := []byte("default")
	key := []byte("user:1")
	val := []byte("Alice")
	version := uint64(123)

	e := kv.NewInternalEntry(key, val, version, cf, uint8(kv.MetaSet), 0)
	defer e.Decr()

	if err := mt.appendEntry(e); err != nil {
		t.Fatalf("appendEntry failed: %v", err)
	}

	// Search with same or newer version (larger version number) should find it
	getKey := kv.MakeInternalKey(cf, key, uint64(123))
	got, err := mt.Get(getKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil {
		t.Fatal("Get returned nil value")
	}

	if string(got) != string(val) {
		t.Fatalf("value mismatch: want %q, got %q", val, got)
	}
}

func TestMemtable_Iterator(t *testing.T) {
	sl := utils.NewSkiplist(
		1<<20,
		16,
		0.5,
		nil,
		func(a, b []byte) utils.CmpR { return utils.CmpR(utils.CompareKey(a, b)) },
	)
	mt := &memtable{index: sl}

	cf := []byte("default")
	e1 := kv.NewInternalEntry([]byte("a"), []byte("va1"), 1000, cf, uint8(kv.MetaSet), 0)
	e2 := kv.NewInternalEntry([]byte("a"), []byte("va2"), 2000, cf, uint8(kv.MetaSet), 0)
	e3 := kv.NewInternalEntry([]byte("b"), []byte("vb1"), 1000, cf, uint8(kv.MetaSet), 0)
	defer e1.Decr()
	defer e2.Decr()
	defer e3.Decr()
	if err := mt.appendEntry(e1); err != nil {
		t.Fatalf("append e1: %v", err)
	}
	if err := mt.appendEntry(e2); err != nil {
		t.Fatalf("append e2: %v", err)
	}
	if err := mt.appendEntry(e3); err != nil {
		t.Fatalf("append e3: %v", err)
	}

	it := mt.Iterator()
	it.Incr()
	defer it.Decr()

	var got []struct {
		key     string
		version uint64
		value   string
		meta    uint8
		ttl     int64
	}
	for {
		e, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		t.Logf("Iter Entry: %s", e.Pretty())
		got = append(got, struct {
			key     string
			version uint64
			value   string
			meta    uint8
			ttl     int64
		}{
			key:     string(e.Key),
			version: e.Version,
			value:   string(e.Value),
			meta:    e.Meta,
			ttl:     e.TTL,
		})
		e.Decr()
	}

	want := []struct {
		key     string
		version uint64
		value   string
		meta    uint8
		ttl     int64
	}{
		{key: "a", version: 2000, value: "va2", meta: uint8(kv.MetaSet), ttl: 0},
		{key: "a", version: 1000, value: "va1", meta: uint8(kv.MetaSet), ttl: 0},
		{key: "b", version: 1000, value: "vb1", meta: uint8(kv.MetaSet), ttl: 0},
	}
	if len(got) != len(want) {
		t.Fatalf("iterator count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].key != want[i].key || got[i].version != want[i].version || got[i].value != want[i].value || got[i].meta != want[i].meta || got[i].ttl != want[i].ttl {
			t.Errorf("entry[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}
