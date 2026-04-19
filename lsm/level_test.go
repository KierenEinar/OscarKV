package lsm

import (
	"OscarKV/kv"
	"OscarKV/pb"
	"OscarKV/sstable"
	"OscarKV/utils"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLevelManager_PersistL0_WritesSSTableAndUpdatesLevels(t *testing.T) {
	rootDir := t.TempDir()
	dataDir := "data"
	if err := os.MkdirAll(filepath.Join(rootDir, dataDir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	ckptMgr, err := openCheckpoint(&checkpointOptions{
		rootDir:       rootDir,
		preferedLevel: 2,
		magic:         "magic",
		syncOnFlush:   true,
	})
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	defer ckptMgr.Close()

	lm := &levelManager{
		levelOption: levelOption{
			dataBlockSize: 256,
			rootDir:       rootDir,
			dataDir:       dataDir,
		},
		levels:      *ckptMgr.levels.Load(),
		ckptManager: ckptMgr,
	}
	lm.levelsRWMutex = make([]sync.RWMutex, len(lm.levels))
	if lm.levels[0] == nil {
		lm.levels[0] = &pb.Levels{}
	}

	lsm := &LSM{
		Option: Option{
			MemoryLimitPerMemtable: 1 << 20,
			MaxLevelPerMemtable:    12,
			RandFactorPerMemtable:  0.5,
		},
	}
	mt := lsm.newMemtable(1)
	mt.Incr()

	e1 := kv.NewInternalEntry([]byte("a"), []byte("va"), 1, []byte("cf"), uint8(kv.MetaSet), 0)
	e2 := kv.NewInternalEntry([]byte("b"), []byte("vb"), 1, []byte("cf"), uint8(kv.MetaSet), 0)
	e3 := kv.NewInternalEntry([]byte("c"), []byte("vc"), 1, []byte("cf"), uint8(kv.MetaSet), 0)
	wantMin := append([]byte(nil), e1.InternalKey()...)
	wantMax := append([]byte(nil), e3.InternalKey()...)
	if utils.CompareKey(wantMin, wantMax) > 0 {
		wantMin, wantMax = wantMax, wantMin
	}

	entries := []*kv.Entry{e1, e2, e3}
	for _, e := range entries {
		if err := mt.appendEntry(e); err != nil {
			t.Fatalf("appendEntry: %v", err)
		}
		e.Decr()
	}

	if err := lm.persistl0(mt); err != nil {
		t.Fatalf("persistl0: %v", err)
	}

	if len(lm.levels[0].Tables) != 1 {
		t.Fatalf("expected 1 level0 table, got %d", len(lm.levels[0].Tables))
	}
	lvl := lm.levels[0].Tables[0]
	if !bytes.Equal(lvl.MinKey, wantMin) {
		t.Fatalf("minKey mismatch: want %x got %x", wantMin, lvl.MinKey)
	}
	if !bytes.Equal(lvl.MaxKey, wantMax) {
		t.Fatalf("maxKey mismatch: want %x got %x", wantMax, lvl.MaxKey)
	}

	table, err := sstable.Open(&sstable.Option{
		RootDir: rootDir,
		DataDir: dataDir,
		Fid:     int(lvl.Id),
		Flags:   os.O_RDONLY,
	}, true)
	if err != nil {
		t.Fatalf("sstable.Open: %v", err)
	}
	defer table.Decr()

	it := table.NewIterator()
	it.Incr()
	defer it.Decr()

	var gotKeys []string
	for {
		e, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		gotKeys = append(gotKeys, string(e.Key))
		e.Decr()
	}
	if len(gotKeys) != 3 {
		t.Fatalf("expected 3 entries, got %d (%v)", len(gotKeys), gotKeys)
	}
	if gotKeys[0] != "a" || gotKeys[1] != "b" || gotKeys[2] != "c" {
		t.Fatalf("unexpected keys: %v", gotKeys)
	}
}
