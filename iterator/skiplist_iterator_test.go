package iterator

// import (
// 	"OscarKV/kv"
// 	"OscarKV/utils"
// 	"bytes"
// 	"io"
// 	"sort"
// 	"testing"
// )

// func TestSkiplistIterator_Next_OrderAndDecode(t *testing.T) {
// 	sl := utils.NewSkiplist(1<<20, 12, 0.5, nil, func(a, b []byte) utils.CmpR {
// 		return utils.CmpR(bytes.Compare(a, b))
// 	})

// 	type exp struct {
// 		cf      string
// 		key     string
// 		value   string
// 		meta    uint8
// 		ttl     int64
// 		version uint64
// 		ikey    []byte
// 	}
// 	var expected []exp

// 	add := func(cf, key, value string, version uint64, meta uint8, ttl int64) {
// 		e := kv.NewInternalEntry([]byte(key), []byte(value), version, []byte(cf), meta, ttl)
// 		defer e.Decr()
// 		if err := sl.Put(e.InternalKey(), e.InternalValue()); err != nil {
// 			t.Fatalf("Put: %v", err)
// 		}
// 		expected = append(expected, exp{
// 			cf:      cf,
// 			key:     key,
// 			value:   value,
// 			meta:    meta,
// 			ttl:     ttl,
// 			version: version,
// 			ikey:    append([]byte(nil), e.InternalKey()...),
// 		})
// 	}

// 	add("cf", "a", "va1", 1, uint8(kv.MetaSet), 0)
// 	add("cf", "a", "va2", 2, uint8(kv.MetaSet), 1)
// 	add("cf", "b", "vb1", 1, uint8(kv.MetaSet), 0)
// 	add("cf2", "a", "v2a", 1, uint8(kv.MetaDel), 0)

// 	sort.Slice(expected, func(i, j int) bool {
// 		return bytes.Compare(expected[i].ikey, expected[j].ikey) < 0
// 	})

// 	it := NewSkiplistIterator(sl)
// 	it.Incr()
// 	defer it.Decr()

// 	for i := 0; i < len(expected); i++ {
// 		e, err := it.Next()
// 		if err != nil {
// 			t.Fatalf("Next[%d]: %v", i, err)
// 		}
// 		if e == nil {
// 			t.Fatalf("Next[%d]: nil entry", i)
// 		}
// 		if string(e.CF) != expected[i].cf {
// 			t.Fatalf("cf[%d] mismatch: want %q got %q", i, expected[i].cf, string(e.CF))
// 		}
// 		if string(e.Key) != expected[i].key {
// 			t.Fatalf("key[%d] mismatch: want %q got %q", i, expected[i].key, string(e.Key))
// 		}
// 		if string(e.Value) != expected[i].value {
// 			t.Fatalf("value[%d] mismatch: want %q got %q", i, expected[i].value, string(e.Value))
// 		}
// 		if e.Version != expected[i].version {
// 			t.Fatalf("version[%d] mismatch: want %d got %d", i, expected[i].version, e.Version)
// 		}
// 		if e.Meta != expected[i].meta {
// 			t.Fatalf("meta[%d] mismatch: want %d got %d", i, expected[i].meta, e.Meta)
// 		}
// 		if e.TTL != expected[i].ttl {
// 			t.Fatalf("ttl[%d] mismatch: want %d got %d", i, expected[i].ttl, e.TTL)
// 		}
// 		e.Decr()
// 	}

// 	e, err := it.Next()
// 	if err != io.EOF {
// 		if e != nil {
// 			e.Decr()
// 		}
// 		t.Fatalf("expected io.EOF, got entry=%v err=%v", e, err)
// 	}
// }

// func TestSkiplistIterator_Prefix(t *testing.T) {
// 	sl := utils.NewSkiplist(1<<20, 12, 0.5, nil, func(a, b []byte) utils.CmpR {
// 		return utils.CmpR(bytes.Compare(a, b))
// 	})

// 	put := func(cf, key, value string, ver uint64) {
// 		e := kv.NewInternalEntry([]byte(key), []byte(value), ver, []byte(cf), uint8(kv.MetaSet), 0)
// 		defer e.Decr()
// 		if err := sl.Put(e.InternalKey(), e.InternalValue()); err != nil {
// 			t.Fatalf("Put: %v", err)
// 		}
// 	}

// 	put("cf", "a", "va1", 1)
// 	put("cf", "a", "va2", 2)
// 	put("cf", "b", "vb1", 1)
// 	put("cf2", "a", "v2a", 1)
// 	put("a", "z", "vz", 1)

// 	prefix := make([]byte, 0, 1+len("cf")+len("a"))
// 	prefix = append(prefix, byte(len("cf")))
// 	prefix = append(prefix, "cf"...)
// 	prefix = append(prefix, "a"...)

// 	it := NewSkiplistIteratorByPrefix(sl, prefix)
// 	it.Incr()
// 	defer it.Decr()

// 	var got []string
// 	for {
// 		e, err := it.Next()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			t.Fatalf("Next: %v", err)
// 		}
// 		got = append(got, string(e.Value))
// 		e.Decr()
// 	}
// 	sort.Strings(got)
// 	if len(got) != 2 || got[0] != "va1" || got[1] != "va2" {
// 		t.Fatalf("unexpected got: %v", got)
// 	}
// }

// func TestSkiplistIterator_Decr(t *testing.T) {
// 	sl := utils.NewSkiplist(1<<20, 12, 0.5, nil, func(a, b []byte) utils.CmpR {
// 		return utils.CmpR(bytes.Compare(a, b))
// 	})
// 	it := NewSkiplistIterator(sl)
// 	it.Incr()
// 	it.Decr()
// 	if _, err := it.Next(); err != io.EOF {
// 		t.Fatalf("expected io.EOF after Decr, got %v", err)
// 	}
// }
