package lsm

import (
	"OscarKV/kv"
	"OscarKV/sstable"
	"container/list"
	"os"
	"path/filepath"
	"testing"
)

func writeTable(t *testing.T, rootDir string, dataDir string, fid int, key string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(rootDir, dataDir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	builder := sstable.NewTableBuilder(256, 64<<20)
	defer sstable.PutTableBuilder(builder)
	e := kv.NewInternalEntry([]byte(key), []byte("v"), 1, []byte("cf"), uint8(kv.MetaSet), 0)
	if _, err := builder.AddEntry(e); err != nil {
		e.Decr()
		t.Fatalf("AddEntry: %v", err)
	}
	e.Decr()
	if _, err := builder.Flush(&sstable.Option{
		RootDir: rootDir,
		DataDir: dataDir,
		Fid:     fid,
		Flags:   os.O_CREATE | os.O_TRUNC | os.O_RDWR,
	}); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestTableCache_LRUEvictsLeastRecentlyUsed(t *testing.T) {
	rootDir := t.TempDir()
	dataDir := "data"
	writeTable(t, rootDir, dataDir, 1, "a")
	writeTable(t, rootDir, dataDir, 2, "b")
	writeTable(t, rootDir, dataDir, 3, "c")

	lm := &levelManager{
		levelOption: levelOption{
			rootDir: rootDir,
			dataDir: dataDir,
		},
		cache:         make(map[uint64]*tableCacheEntry),
		cacheList:     list.New(),
		tableCacheMax: 2,
		tableCacheTTL: 180,
	}

	t1, err := lm.getTable(1)
	if err != nil {
		t.Fatalf("getTable(1): %v", err)
	}
	t1.Decr()
	t2, err := lm.getTable(2)
	if err != nil {
		t.Fatalf("getTable(2): %v", err)
	}
	t2.Decr()

	t1b, err := lm.getTable(1)
	if err != nil {
		t.Fatalf("getTable(1) again: %v", err)
	}
	t1b.Decr()

	t3, err := lm.getTable(3)
	if err != nil {
		t.Fatalf("getTable(3): %v", err)
	}
	t3.Decr()

	if _, ok := lm.cache[2]; ok {
		t.Fatalf("expected id=2 evicted")
	}
	if _, ok := lm.cache[1]; !ok {
		t.Fatalf("expected id=1 still cached")
	}
	if _, ok := lm.cache[3]; !ok {
		t.Fatalf("expected id=3 cached")
	}
}

func TestTableCache_TTLEvictsIdleEntries(t *testing.T) {
	rootDir := t.TempDir()
	dataDir := "data"
	writeTable(t, rootDir, dataDir, 1, "a")
	writeTable(t, rootDir, dataDir, 2, "b")

	lm := &levelManager{
		levelOption: levelOption{
			rootDir: rootDir,
			dataDir: dataDir,
		},
		cache:         make(map[uint64]*tableCacheEntry),
		cacheList:     list.New(),
		tableCacheMax: 1000,
		tableCacheTTL: 180,
	}

	t1, err := lm.getTable(1)
	if err != nil {
		t.Fatalf("getTable(1): %v", err)
	}
	t1.Decr()
	t2, err := lm.getTable(2)
	if err != nil {
		t.Fatalf("getTable(2): %v", err)
	}
	t2.Decr()

	now := int64(10_000)
	lm.cache[1].lastAccess = now - 181
	lm.cache[2].lastAccess = now
	lm.cleanupTableCacheAt(now)

	if _, ok := lm.cache[1]; ok {
		t.Fatalf("expected id=1 expired and evicted")
	}
	if _, ok := lm.cache[2]; !ok {
		t.Fatalf("expected id=2 kept")
	}
}
