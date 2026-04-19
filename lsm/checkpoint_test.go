package lsm

import (
	"OscarKV/pb"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestOpenCheckpoint_InitializesEmpty(t *testing.T) {
	rootDir := t.TempDir()
	m, err := openCheckpoint(&checkpointOptions{
		magic:         "magic",
		rootDir:       rootDir,
		preferedLevel: 3,
		syncOnFlush:   true,
	})
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	m.Close()

	for _, name := range ckptFiles {
		if _, err := os.Stat(filepath.Join(rootDir, metaDir, name)); err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
	}

	levelsPtr := m.levels.Load()
	if levelsPtr == nil {
		t.Fatalf("levels is nil")
	}
	if len(*levelsPtr) != 3 {
		t.Fatalf("levels len mismatch: want %d got %d", 3, len(*levelsPtr))
	}
	for i := range *levelsPtr {
		if (*levelsPtr)[i] == nil {
			t.Fatalf("levels[%d] is nil", i)
		}
	}
}

func TestOpenCheckpoint_MagicMismatch(t *testing.T) {
	rootDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(rootDir, metaDir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	bad := &pb.Checkpoint{
		Magic:     "bad",
		CreatedAt: 1,
	}
	b, err := proto.Marshal(bad)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, metaDir, ckptFiles[0]), b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err = openCheckpoint(&checkpointOptions{
		magic:         "magic",
		rootDir:       rootDir,
		preferedLevel: 1,
	})
	if err != ErrInvalidMagicNumber {
		t.Fatalf("expected ErrInvalidMagicNumber, got %v", err)
	}
}

func TestCheckpointManager_FlushSegment_RoundTrip(t *testing.T) {
	rootDir := t.TempDir()
	m, err := openCheckpoint(&checkpointOptions{
		magic:         "magic",
		rootDir:       rootDir,
		preferedLevel: 2,
		syncOnFlush:   true,
	})
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	defer m.Close()

	m.queueAlterLevel([]levelAlter{
		{
			adds: []addLevel{
				{level: 1, pblevel: &pb.Table{Id: 1, MinKey: []byte("b"), MaxKey: []byte("c")}},
			},
		},
	}, false)

	t.Logf("levels: %v, len: %d", *m.levels.Load(), len(*m.levels.Load()))

	var found *pb.Checkpoint
	for _, name := range ckptFiles {
		buf, err := os.ReadFile(filepath.Join(rootDir, metaDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if len(buf) == 0 {
			continue
		}
		got := &pb.Checkpoint{}
		if err := proto.Unmarshal(buf, got); err != nil {
			continue
		}
		if got.Magic == "magic" && got.CreatedAt != 0 {
			if found == nil || got.CreatedAt > found.CreatedAt {
				found = got
			}
		}

		t.Logf("read %s: %+v", name, got)
	}
	if found == nil {
		t.Fatalf("expected to find flushed checkpoint")
	}
	if found.WalSegmentId != 1 {
		t.Fatalf("walSegmentId mismatch: want %d got %d", 1, found.WalSegmentId)
	}
	if len(found.Levels) != 2 {
		t.Fatalf("levels len mismatch: want %d got %d", 2, len(found.Levels))
	}
	if len(found.Levels[1].Tables) != 1 || found.Levels[1].Tables[0].Id != 1 {
		t.Fatalf("levels content mismatch: %+v", found.Levels)
	}
}

func TestCheckpointManager_FlushLevels_Sorts(t *testing.T) {
	rootDir := t.TempDir()
	m, err := openCheckpoint(&checkpointOptions{
		magic:         "magic",
		rootDir:       rootDir,
		preferedLevel: 2,
		syncOnFlush:   true,
	})
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	defer m.Close()

	levels := make([]*pb.Levels, 2)
	levels[0] = &pb.Levels{}
	levels[1] = &pb.Levels{
		Tables: []*pb.Table{
			{Id: 2, MinKey: []byte("d"), MaxKey: []byte("e")},
			{Id: 1, MinKey: []byte("b"), MaxKey: []byte("c")},
		},
	}
	m.levels.Store(&levels)

	alter := []levelAlter{
		{
			adds: []addLevel{
				{level: 1, pblevel: &pb.Table{Id: 3, MinKey: []byte("a"), MaxKey: []byte("a")}},
			},
			dels: []delLevel{
				{level: 1, id: 2},
			},
		},
	}

	if err := m.flushLevels(alter); err != nil {
		t.Fatalf("flushLevels: %v", err)
	}

	loaded := m.levels.Load()
	if loaded == nil || len(*loaded) != 2 {
		t.Fatalf("loaded levels invalid")
	}
	l1 := (*loaded)[1].Tables
	if len(l1) != 2 {
		t.Fatalf("level1 len mismatch: want %d got %d", 2, len(l1))
	}
	if string(l1[0].MinKey) != "a" || string(l1[1].MinKey) != "b" {
		t.Fatalf("level1 not sorted or wrong: %+v", l1)
	}
}

func TestCheckpointManager_FlushLevels_AddAndDelete(t *testing.T) {
	rootDir := t.TempDir()
	m, err := openCheckpoint(&checkpointOptions{
		magic:         "magic",
		rootDir:       rootDir,
		preferedLevel: 2,
		syncOnFlush:   true,
	})
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	defer m.Close()

	levels := make([]*pb.Levels, 2)
	levels[0] = &pb.Levels{}
	levels[1] = &pb.Levels{
		Tables: []*pb.Table{
			{Id: 1, MinKey: []byte("b"), MaxKey: []byte("b")},
			{Id: 2, MinKey: []byte("d"), MaxKey: []byte("d")},
		},
	}
	m.levels.Store(&levels)

	alter := []levelAlter{
		{
			adds: []addLevel{
				{level: 1, pblevel: &pb.Table{Id: 3, MinKey: []byte("c"), MaxKey: []byte("c")}},
				{level: 1, pblevel: &pb.Table{Id: 4, MinKey: []byte("a"), MaxKey: []byte("a")}},
			},
			dels: []delLevel{
				{level: 1, id: 1},
			},
		},
	}
	if err := m.flushLevels(alter); err != nil {
		t.Fatalf("flushLevels: %v", err)
	}

	loaded := m.levels.Load()
	if loaded == nil || len(*loaded) != 2 {
		t.Fatalf("loaded levels invalid")
	}
	got := (*loaded)[1].Tables
	if len(got) != 3 {
		t.Fatalf("level1 len mismatch: want %d got %d", 3, len(got))
	}
	if got[0].Id != 4 || string(got[0].MinKey) != "a" {
		t.Fatalf("level1[0] mismatch: %+v", got[0])
	}
	if got[1].Id != 3 || string(got[1].MinKey) != "c" {
		t.Fatalf("level1[1] mismatch: %+v", got[1])
	}
	if got[2].Id != 2 || string(got[2].MinKey) != "d" {
		t.Fatalf("level1[2] mismatch: %+v", got[2])
	}

	var found *pb.Checkpoint
	for _, name := range ckptFiles {
		buf, err := os.ReadFile(filepath.Join(rootDir, metaDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if len(buf) == 0 {
			continue
		}
		gotCkpt := &pb.Checkpoint{}
		if err := proto.Unmarshal(buf, gotCkpt); err != nil {
			continue
		}
		if gotCkpt.Magic == "magic" && gotCkpt.CreatedAt != 0 {
			if found == nil || gotCkpt.CreatedAt > found.CreatedAt {
				found = gotCkpt
			}
		}
	}
	if found == nil {
		t.Fatalf("expected flushed checkpoint on disk")
	}
	if len(found.Levels) != 2 || len(found.Levels[1].Tables) != 3 {
		t.Fatalf("disk levels mismatch: %+v", found.Levels)
	}
	if found.Levels[1].Tables[0].Id != 4 || found.Levels[1].Tables[1].Id != 3 || found.Levels[1].Tables[2].Id != 2 {
		t.Fatalf("disk level1 mismatch: %+v", found.Levels[1].Tables)
	}
}

func TestCheckpointManager_FlushIngestBuffer_AddAndDeleteL0(t *testing.T) {
	rootDir := t.TempDir()
	m, err := openCheckpoint(&checkpointOptions{
		magic:         "magic",
		rootDir:       rootDir,
		preferedLevel: 2,
		syncOnFlush:   true,
	})
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	defer m.Close()

	initBuf := []*pb.IngestBuffer{
		{Id: 10, Level: 0, MinKey: []byte("a"), MaxKey: []byte("b")},
		{Id: 11, Level: 0, MinKey: []byte("c"), MaxKey: []byte("d")},
		{Id: 20, Level: 1, MinKey: []byte("x"), MaxKey: []byte("y")},
	}
	m.ingestBuffer.Store(&initBuf)

	err = m.flushIngestBuffer(applyIngestBuffer{
		l0Blocks: []ingestBuffer{
			{id: 11, minKey: []byte("c2"), maxKey: []byte("d2")},
			{id: 12, minKey: []byte("e"), maxKey: []byte("f")},
		},
		targetLevel: 0,
	})
	if err != nil {
		t.Fatalf("flushIngestBuffer: %v", err)
	}

	loaded := m.ingestBuffer.Load()
	if loaded == nil {
		t.Fatalf("ingestBuffer is nil")
	}
	got := *loaded
	if len(got) != 4 {
		t.Fatalf("buffer len mismatch: want %d got %d", 4, len(got))
	}

	find := func(id uint64, level int32) *pb.IngestBuffer {
		for _, b := range got {
			if b != nil && b.Id == id && b.Level == level {
				return b
			}
		}
		return nil
	}
	if find(10, 0) == nil {
		t.Fatalf("expected to keep id=10 level=0")
	}
	if b := find(11, 0); b == nil {
		t.Fatalf("expected to re-add id=11 level=0")
	} else if string(b.MinKey) != "c2" || string(b.MaxKey) != "d2" {
		t.Fatalf("id=11 keys mismatch: %+v", b)
	}
	if find(12, 0) == nil {
		t.Fatalf("expected to add id=12 level=0")
	}
	if find(20, 1) == nil {
		t.Fatalf("expected to keep id=20 level=1")
	}

	var found *pb.Checkpoint
	for _, name := range ckptFiles {
		buf, err := os.ReadFile(filepath.Join(rootDir, metaDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if len(buf) == 0 {
			continue
		}
		gotCkpt := &pb.Checkpoint{}
		if err := proto.Unmarshal(buf, gotCkpt); err != nil {
			continue
		}
		if gotCkpt.Magic == "magic" && gotCkpt.CreatedAt != 0 {
			if found == nil || gotCkpt.CreatedAt > found.CreatedAt {
				found = gotCkpt
			}
		}
	}
	if found == nil {
		t.Fatalf("expected flushed checkpoint on disk")
	}
	if len(found.Buffer) != 4 {
		t.Fatalf("disk buffer len mismatch: want %d got %d", 4, len(found.Buffer))
	}
}
