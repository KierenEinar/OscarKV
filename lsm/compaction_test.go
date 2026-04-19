package lsm

import (
	"OscarKV/iterator"
	"OscarKV/kv"
	"OscarKV/pb"
	"OscarKV/sstable"
	"OscarKV/utils"
	"bytes"
	"container/list"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestPickL0Compaction_SelectsOverlaps(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:        8,
			maxLevel:      7,
			baseLevelSize: 100,
		},
		levels: make([]*pb.Levels, 7),
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}

	lm.levelStats = make([]*levelStat, 7)
	for i := range lm.levelStats {
		lm.levelStats[i] = &levelStat{level: int8(i)}
	}
	lm.levelStats[3].capacity = 100
	lm.levelStats[3].usedSpace = 50
	lm.levelStats[4].usedSpace = 1
	lm.levelStats[5].usedSpace = 1
	lm.levelStats[6].usedSpace = 1

	lm.levels[0].Tables = []*pb.Table{
		{Id: 1, MinKey: []byte("b"), MaxKey: []byte("d")},
		{Id: 2, MinKey: []byte("c"), MaxKey: []byte("e")},
		{Id: 3, MinKey: []byte("x"), MaxKey: []byte("y")},
	}

	lm.levels[3].Tables = []*pb.Table{
		{Id: 10, MinKey: []byte("a"), MaxKey: []byte("a")},
		{Id: 11, MinKey: []byte("c"), MaxKey: []byte("c")},
		{Id: 12, MinKey: []byte("f"), MaxKey: []byte("f")},
		{Id: 13, MinKey: []byte("z"), MaxKey: []byte("z")},
	}

	def := lm.pickL0Compaction()
	if def == nil {
		t.Fatalf("expected non-nil compaction def")
	}
	if def.sourceLevel != 0 {
		t.Fatalf("sourceLevel mismatch: want %d got %d", 0, def.sourceLevel)
	}
	if def.targetLevel != 3 {
		t.Fatalf("targetLevel mismatch: want %d got %d", 3, def.targetLevel)
	}

	if !reflect.DeepEqual(def.sourceTableIxs, []int{0, 1}) {
		t.Fatalf("sourceTableIxs mismatch: want %v got %v", []int{0, 1}, def.sourceTableIxs)
	}
	if def.targetTableRange != [2]int{1, 2} {
		t.Fatalf("targetTableRange mismatch: want %v got %v", [2]int{1, 2}, def.targetTableRange)
	}
}

func TestPickCompaction_RoutesToL0(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:        8,
			maxLevel:      7,
			baseLevelSize: 100,
		},
		levels: make([]*pb.Levels, 7),
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}

	lm.levelStats = make([]*levelStat, 7)
	for i := range lm.levelStats {
		lm.levelStats[i] = &levelStat{level: int8(i), capacity: 1, usedSpace: 0}
	}

	lm.levelStats[0].tableNums = 100
	lm.levelOption.l0ForcedCompactPercentage = 0.8

	lm.levelStats[3].capacity = 100
	lm.levelStats[3].usedSpace = 50
	lm.levelStats[4].usedSpace = 1
	lm.levelStats[5].usedSpace = 1
	lm.levelStats[6].usedSpace = 1

	lm.levels[0].Tables = []*pb.Table{
		{Id: 1, MinKey: []byte("b"), MaxKey: []byte("d")},
		{Id: 2, MinKey: []byte("c"), MaxKey: []byte("e")},
		{Id: 3, MinKey: []byte("x"), MaxKey: []byte("y")},
	}

	lm.levels[3].Tables = []*pb.Table{
		{Id: 10, MinKey: []byte("a"), MaxKey: []byte("a")},
		{Id: 11, MinKey: []byte("c"), MaxKey: []byte("c")},
		{Id: 12, MinKey: []byte("f"), MaxKey: []byte("f")},
		{Id: 13, MinKey: []byte("z"), MaxKey: []byte("z")},
	}

	def := lm.pickCompaction()
	if def == nil {
		t.Fatalf("expected non-nil compaction def")
	}
	if def.sourceLevel != 0 {
		t.Fatalf("expected sourceLevel 0, got %d", def.sourceLevel)
	}
	if def.targetLevel != 3 {
		t.Fatalf("expected targetLevel 3, got %d", def.targetLevel)
	}
}

func TestPickCompaction_RoutesToNonL0(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:                               8,
			maxLevel:                             7,
			baseLevelSize:                        100,
			compactionPickCreatedCoefficient:     0.5,
			compactionPickTableMissedCoefficient: 0.5,
		},
		levels:     make([]*pb.Levels, 7),
		tableStats: map[uint64]tableStat{100: {createdAt: 1, missed: 10, hitted: 0}},
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}
	lm.levels[1].Tables = []*pb.Table{
		{Id: 100, MinKey: []byte("b"), MaxKey: []byte("d")},
	}
	lm.levels[2].Tables = []*pb.Table{
		{Id: 200, MinKey: []byte("c"), MaxKey: []byte("c")},
		{Id: 201, MinKey: []byte("z"), MaxKey: []byte("z")},
	}

	lm.levelStats = make([]*levelStat, 7)
	for i := range lm.levelStats {
		lm.levelStats[i] = &levelStat{level: int8(i), capacity: 100, usedSpace: 0}
	}
	lm.levelStats[1].usedSpace = 100
	lm.levelOption.compactionPickSpaceCoefficient = 1
	lm.levelOption.compactionPickMissedCoefficient = 0
	lm.levelOption.compactionPickStaleCoefficient = 0
	lm.levelOption.compactionPickL0Coefficient = 0
	lm.levelOption.l0ForcedCompactPercentage = 2

	def := lm.pickCompaction()
	if def == nil {
		t.Fatalf("expected non-nil compaction def")
	}
	if def.sourceLevel != 1 {
		t.Fatalf("expected sourceLevel 1, got %d", def.sourceLevel)
	}
	if def.targetLevel != 2 {
		t.Fatalf("expected targetLevel 2, got %d", def.targetLevel)
	}
	if !reflect.DeepEqual(def.sourceTableIxs, []int{0}) {
		t.Fatalf("expected sourceTableIxs [0], got %v", def.sourceTableIxs)
	}
	if def.targetTableRange != [2]int{0, 1} {
		t.Fatalf("expected targetTableRange [0 1], got %v", def.targetTableRange)
	}
}

func TestPickCompaction_ReturnsNil_WhenSourceGTEMaxLevel(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:                         8,
			maxLevel:                       7,
			compactionPickSpaceCoefficient: 1,
			l0ForcedCompactPercentage:      2,
		},
		levelStats: []*levelStat{
			{level: 0, capacity: 1, usedSpace: 0},
			{level: 7, capacity: 100, usedSpace: 100},
		},
	}
	if def := lm.pickCompaction(); def != nil {
		t.Fatalf("expected nil, got %+v", def)
	}
}

func TestPickCompaction_RoutesToL0_WhenCoefficientDominates(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:                         8,
			maxLevel:                       7,
			baseLevelSize:                  100,
			compactionPickL0Coefficient:    10,
			l0ForcedCompactPercentage:      2,
			compactionPickSpaceCoefficient: 1,
		},
		levels: make([]*pb.Levels, 7),
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}

	lm.levelStats = make([]*levelStat, 7)
	for i := range lm.levelStats {
		lm.levelStats[i] = &levelStat{level: int8(i), capacity: 100, usedSpace: 0}
	}
	lm.levelStats[1].usedSpace = 100
	lm.levelStats[3].capacity = 100
	lm.levelStats[3].usedSpace = 50
	lm.levelStats[4].usedSpace = 1
	lm.levelStats[5].usedSpace = 1
	lm.levelStats[6].usedSpace = 1

	lm.levels[0].Tables = []*pb.Table{
		{Id: 1, MinKey: []byte("b"), MaxKey: []byte("d")},
		{Id: 2, MinKey: []byte("c"), MaxKey: []byte("e")},
	}
	lm.levels[3].Tables = []*pb.Table{
		{Id: 11, MinKey: []byte("c"), MaxKey: []byte("c")},
	}

	def := lm.pickCompaction()
	if def == nil {
		t.Fatalf("expected non-nil compaction def")
	}
	if def.sourceLevel != 0 {
		t.Fatalf("expected sourceLevel 0, got %d", def.sourceLevel)
	}
}

func TestPickCompaction_ClonesMinMaxKey(t *testing.T) {
	min := []byte("b")
	max := []byte("d")
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:                               8,
			maxLevel:                             7,
			compactionPickCreatedCoefficient:     0.5,
			compactionPickTableMissedCoefficient: 0.5,
			compactionPickSpaceCoefficient:       1,
			l0ForcedCompactPercentage:            2,
		},
		levels:     make([]*pb.Levels, 3),
		tableStats: map[uint64]tableStat{100: {createdAt: 1, missed: 1, hitted: 0}},
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}
	lm.levels[1].Tables = []*pb.Table{
		{Id: 100, MinKey: min, MaxKey: max},
	}
	lm.levels[2].Tables = []*pb.Table{
		{Id: 200, MinKey: []byte("c"), MaxKey: []byte("c")},
	}

	lm.levelStats = []*levelStat{
		{level: 0, capacity: 1, usedSpace: 0},
		{level: 1, capacity: 100, usedSpace: 100},
		{level: 2, capacity: 100, usedSpace: 0},
	}

	def := lm.pickCompaction()
	if def == nil {
		t.Fatalf("expected non-nil compaction def")
	}
	min[0] = 'z'
	max[0] = 'a'
	if bytes.Equal(def.minKey, min) || bytes.Equal(def.maxKey, max) {
		t.Fatalf("expected cloned keys, got min=%q max=%q", string(def.minKey), string(def.maxKey))
	}
}

func TestCompaction_InsertAtIndex_MovesTables(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			rootDir:                     t.TempDir(),
			dataDir:                     "data",
			l0Nums:                      8,
			maxLevel:                    2,
			baseLevelSize:               100,
			l0ForcedCompactPercentage:   0.1,
			compactionPickL0Coefficient: 1,
		},
		levels: make([]*pb.Levels, 2),
	}
	lm.levelsRWMutex = make([]sync.RWMutex, len(lm.levels))
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}
	lm.levels[0].Tables = []*pb.Table{
		{Id: 1, MinKey: []byte("a"), MaxKey: []byte("b")},
		{Id: 2, MinKey: []byte("b"), MaxKey: []byte("c")},
	}
	lm.levels[1].Tables = nil

	lm.levelStats = []*levelStat{
		{level: 0, tableNums: 2},
		{level: 1, capacity: 100, usedSpace: 1},
	}

	errCh := make(chan error, 1)
	lm.compaction(errCh)
	err := <-errCh
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(lm.levels[0].Tables) != 0 {
		t.Fatalf("expected level0 empty, got %d", len(lm.levels[0].Tables))
	}
	if len(lm.levels[1].Tables) != 2 {
		t.Fatalf("expected level1 has 2 tables, got %d", len(lm.levels[1].Tables))
	}
	if lm.levels[1].Tables[0].Id != 1 || lm.levels[1].Tables[1].Id != 2 {
		t.Fatalf("unexpected table ids: %d %d", lm.levels[1].Tables[0].Id, lm.levels[1].Tables[1].Id)
	}
}

func TestCompaction_MergeRange_PropagatesNormalCompactionError(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			rootDir:                   t.TempDir(),
			dataDir:                   "data",
			l0Nums:                    8,
			maxLevel:                  2,
			baseLevelSize:             100,
			l0ForcedCompactPercentage: 0.1,
		},
		levels: make([]*pb.Levels, 2),
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}
	lm.levels[0].Tables = []*pb.Table{
		{Id: 1, MinKey: []byte("a"), MaxKey: []byte("c")},
	}
	lm.levels[1].Tables = []*pb.Table{
		{Id: 10, MinKey: []byte("b"), MaxKey: []byte("d")},
	}

	lm.levelStats = []*levelStat{
		{level: 0, tableNums: 1},
		{level: 1, capacity: 100, usedSpace: 1},
	}

	beforeL0 := append([]*pb.Table(nil), lm.levels[0].Tables...)
	beforeL1 := append([]*pb.Table(nil), lm.levels[1].Tables...)

	errCh := make(chan error, 1)
	lm.compaction(errCh)
	err := <-errCh
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if !reflect.DeepEqual(lm.levels[0].Tables, beforeL0) {
		t.Fatalf("expected level0 unchanged")
	}
	if !reflect.DeepEqual(lm.levels[1].Tables, beforeL1) {
		t.Fatalf("expected level1 unchanged")
	}
}

func TestPickCompaction_PanicsWithoutLevelStats(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic")
		}
	}()
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:   8,
			maxLevel: 7,
		},
	}
	_ = lm.pickCompaction()
}

func writeTestTable(t *testing.T, rootDir string, dataDir string, fid int, start int, end int, version uint64, valueSize int) *pb.Table {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(rootDir, dataDir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	builder := sstable.NewTableBuilder(4<<10, 64<<20)
	defer sstable.PutTableBuilder(builder)

	val := make([]byte, valueSize)
	for i := range val {
		val[i] = 'v'
	}

	var minKey []byte
	var maxKey []byte
	for i := start; i <= end; i++ {
		k := fmt.Sprintf("k%05d", i)
		e := kv.NewInternalEntry([]byte(k), val, version, []byte("cf"), uint8(kv.MetaSet), 0)
		ik := e.InternalKey()
		if minKey == nil {
			minKey = append([]byte(nil), ik...)
		}
		maxKey = append([]byte(nil), ik...)
		_, err := builder.AddEntry(e)
		e.Decr()
		if err != nil {
			t.Fatalf("AddEntry: %v", err)
		}
	}
	n, err := builder.Flush(&sstable.Option{
		RootDir: rootDir,
		DataDir: dataDir,
		Fid:     fid,
		Flags:   os.O_CREATE | os.O_TRUNC | os.O_RDWR,
	})
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return &pb.Table{
		Id:        uint64(fid),
		MinKey:    minKey,
		MaxKey:    maxKey,
		Size:      int32(n),
		CreatedAt: 1,
		Stale:     builder.StaleSize(),
	}
}

func readAllInternalKeys(t *testing.T, rootDir string, dataDir string, fids []uint64) [][]byte {
	t.Helper()
	var out [][]byte
	var prev []byte
	for _, id := range fids {
		table, err := sstable.Open(&sstable.Option{
			RootDir: rootDir,
			DataDir: dataDir,
			Fid:     int(id),
			Flags:   os.O_RDONLY,
		}, true)
		if err != nil {
			t.Fatalf("Open fid=%d: %v", id, err)
		}
		it := table.NewIterator()
		it.Incr()
		for {
			e, err := it.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next fid=%d: %v", id, err)
			}
			ik := e.InternalKey()
			out = append(out, append([]byte(nil), ik...))
			if prev != nil && utils.CompareKey(prev, ik) > 0 {
				t.Fatalf("output not monotonic")
			}
			prev = append(prev[:0], ik...)
			e.Decr()
		}
		it.Decr()
		table.Decr()
	}
	return out
}

func readMergedInternalKeys(t *testing.T, rootDir string, dataDir string, fids []uint64) [][]byte {
	t.Helper()
	tables := make([]*sstable.Table, 0, len(fids))
	iters := make([]iterator.Iterator, 0, len(fids))
	for _, id := range fids {
		table, err := sstable.Open(&sstable.Option{
			RootDir: rootDir,
			DataDir: dataDir,
			Fid:     int(id),
			Flags:   os.O_RDONLY,
		}, true)
		if err != nil {
			t.Fatalf("Open fid=%d: %v", id, err)
		}
		tables = append(tables, table)
		iters = append(iters, table.NewIterator())
	}
	defer func() {
		for _, table := range tables {
			table.Decr()
		}
	}()

	merged := iterator.NewMergeIterator(iters)
	merged.Incr()
	defer merged.Decr()

	var count int
	for {
		e, err := merged.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("merged Next: %v", err)
		}
		count++
		e.Decr()
	}
	// Use 3000 as a check for specific tests if needed, but here we just show the logic from the instruction
	if count != 3000 && len(fids) == 3 {
		t.Errorf("Direct merged count mismatch: got %d want 3000", count)
	}

	e, err := merged.SeekToFirst() // Reset for the actual test loop
	var out [][]byte
	var prev []byte
	for {
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("merged Next: %v", err)
		}
		ik := e.InternalKey()
		out = append(out, append([]byte(nil), ik...))
		if prev != nil && utils.CompareKey(prev, ik) > 0 {
			t.Fatalf("merged output not monotonic")
		}
		prev = append(prev[:0], ik...)
		e.Decr()
		e, err = merged.Next()
	}
	return out
}

func TestCompaction_MergeRange_RealSSTables(t *testing.T) {
	rootDir := t.TempDir()
	dataDir := "data"

	source := writeTestTable(t, rootDir, dataDir, 1, 500, 1499, 2, 64)
	t10 := writeTestTable(t, rootDir, dataDir, 10, 0, 999, 1, 64)
	t11 := writeTestTable(t, rootDir, dataDir, 11, 1000, 1999, 1, 64)
	t12 := writeTestTable(t, rootDir, dataDir, 12, 3000, 3999, 1, 64)

	lm := &levelManager{
		levelOption: levelOption{
			rootDir:                              rootDir,
			dataDir:                              dataDir,
			l0Nums:                               8,
			maxLevel:                             3,
			compactionPickSpaceCoefficient:       1,
			compactionPickMissedCoefficient:      0,
			compactionPickStaleCoefficient:       0,
			compactionPickL0Coefficient:          0,
			l0ForcedCompactPercentage:            2,
			compactionPickCreatedCoefficient:     0.5,
			compactionPickTableMissedCoefficient: 0.5,
			compactionBlockSize:                  4 << 10,
			compactionTableSize:                  4 << 10,
		},
		levels:        make([]*pb.Levels, 3),
		cache:         make(map[uint64]*tableCacheEntry, 128),
		cacheList:     list.New(),
		tableCacheMax: 1000,
		tableCacheTTL: 180,
		nextTableID:   12,
		tableStats: map[uint64]tableStat{
			1:  {createdAt: 1, missed: 10, hitted: 0},
			10: {createdAt: 2, missed: 0, hitted: 0},
			11: {createdAt: 2, missed: 0, hitted: 0},
			12: {createdAt: 2, missed: 0, hitted: 0},
		},
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}

	lm.levelsRWMutex = make([]sync.RWMutex, len(lm.levels))

	lm.levels[1].Tables = []*pb.Table{source}
	lm.levels[2].Tables = []*pb.Table{t10, t11, t12}

	lm.levelStats = []*levelStat{
		{level: 0, tableNums: 0, capacity: 1, usedSpace: 0},
		{level: 1, capacity: 100, usedSpace: 100},
		{level: 2, capacity: 100, usedSpace: 1},
	}

	expTables := []uint64{source.Id, t10.Id, t11.Id}
	expMerged := readMergedInternalKeys(t, rootDir, dataDir, expTables)
	exp := make([][]byte, 0, len(expMerged))
	var lastPrefix []byte
	for _, ik := range expMerged {
		prefix := ik
		if len(prefix) >= 8 {
			prefix = prefix[:len(prefix)-8]
		}
		if lastPrefix != nil && bytes.Equal(prefix, lastPrefix) {
			continue
		}
		lastPrefix = append(lastPrefix[:0], prefix...)
		exp = append(exp, ik)
	}

	errCh := make(chan error, 1)
	lm.compaction(errCh)
	if err := <-errCh; err != nil {
		t.Fatalf("compaction error: %v", err)
	}

	if len(lm.levels[1].Tables) != 0 {
		t.Fatalf("expected source level emptied, got %d", len(lm.levels[1].Tables))
	}

	var newFids []uint64
	var found12 bool
	var found10 bool
	var found11 bool
	for _, t := range lm.levels[2].Tables {
		if t.Id == 12 {
			found12 = true
			continue
		}
		if t.Id == 10 {
			found10 = true
		}
		if t.Id == 11 {
			found11 = true
		}
		if t.Id > 12 {
			newFids = append(newFids, t.Id)
		}
	}
	if !found12 {
		t.Fatalf("expected table 12 kept")
	}
	if found10 || found11 {
		t.Fatalf("expected overlapped target tables removed (10/11)")
	}
	if len(newFids) == 0 {
		t.Fatalf("expected new output tables")
	}

	got := readAllInternalKeys(t, rootDir, dataDir, newFids)
	if len(got) != len(exp) {
		i, j := 0, 0
		for i < len(got) && j < len(exp) {
			c := utils.CompareKey(got[i], exp[j])
			if c == 0 {
				i++
				j++
				continue
			}
			if c < 0 {
				i++
				continue
			}
			_, cf, uk, ver, _ := kv.SplitInternalKey(exp[j])
			t.Fatalf("merged entry count mismatch: got %d want %d; missing userKey=%q cf=%q ver=%d at exp[%d]", len(got), len(exp), string(uk), string(cf), ver, j)
		}
		if j < len(exp) {
			_, cf, uk, ver, _ := kv.SplitInternalKey(exp[j])
			t.Fatalf("merged entry count mismatch: got %d want %d; missing userKey=%q cf=%q ver=%d at exp[%d]", len(got), len(exp), string(uk), string(cf), ver, j)
		}
		t.Fatalf("merged entry count mismatch: got %d want %d", len(got), len(exp))
	}
	for i := range exp {
		if !bytes.Equal(got[i], exp[i]) {
			t.Fatalf("merged internal key mismatch at %d", i)
		}
	}
}

func TestSubCompaction_RealSSTables(t *testing.T) {
	rootDir := t.TempDir()
	dataDir := "data"

	source := writeTestTable(t, rootDir, dataDir, 1, 0, 4999, 2, 64)
	target := make([]*pb.Table, 0, 10)
	for i := 0; i < 10; i++ {
		fid := 10 + i
		start := i * 500
		end := start + 499
		target = append(target, writeTestTable(t, rootDir, dataDir, fid, start, end, 1, 64))
	}

	lm := &levelManager{
		levelOption: levelOption{
			rootDir:                              rootDir,
			dataDir:                              dataDir,
			maxLevel:                             3,
			compactionBlockSize:                  4 << 10,
			compactionTableSize:                  4 << 10,
			compactionPickCreatedCoefficient:     0.5,
			compactionPickTableMissedCoefficient: 0.5,
		},
		levels:        make([]*pb.Levels, 3),
		cache:         make(map[uint64]*tableCacheEntry, 128),
		cacheList:     list.New(),
		tableCacheMax: 1000,
		tableCacheTTL: 180,
		nextTableID:   19,
		tableStats:    map[uint64]tableStat{1: {createdAt: 1, missed: 10, hitted: 0}},
		levelStats:    []*levelStat{{level: 0}, {level: 1}, {level: 2}},
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}
	lm.levelsRWMutex = make([]sync.RWMutex, len(lm.levels))
	lm.levels[1].Tables = []*pb.Table{source}
	lm.levels[2].Tables = target

	cDef := &compactionDef{
		sourceLevel:      1,
		targetLevel:      2,
		sourceTableIxs:   []int{0},
		targetTableRange: [2]int{0, 10},
		minKey:           bytes.Clone(target[0].MinKey),
		maxKey:           bytes.Clone(target[9].MaxKey),
	}

	insertDefs, deleteDefs, err := lm.subCompaction(cDef)
	if err != nil {
		t.Fatalf("subCompaction: %v", err)
	}
	if len(insertDefs) != 2 {
		t.Fatalf("expected 2 insertDefs (two groups), got %d", len(insertDefs))
	}

	if err := lm.applyPropose(&compactionPropose{insertDefs: insertDefs, deleteDefs: deleteDefs}); err != nil {
		t.Fatalf("applyPropose: %v", err)
	}

	if len(lm.levels[1].Tables) != 0 {
		t.Fatalf("expected source level emptied, got %d", len(lm.levels[1].Tables))
	}

	var outFids []uint64
	for _, tbl := range lm.levels[2].Tables {
		if tbl.Id >= 10 && tbl.Id <= 19 {
			t.Fatalf("expected target original tables deleted, found id=%d", tbl.Id)
		}
		outFids = append(outFids, tbl.Id)
	}

	expFids := make([]uint64, 0, 11)
	expFids = append(expFids, source.Id)
	for _, t := range target {
		expFids = append(expFids, t.Id)
	}
	expMerged := readMergedInternalKeys(t, rootDir, dataDir, expFids)
	exp := make([][]byte, 0, len(expMerged))
	var lastPrefix []byte
	for _, ik := range expMerged {
		prefix := ik
		if len(prefix) >= 8 {
			prefix = prefix[:len(prefix)-8]
		}
		if lastPrefix != nil && bytes.Equal(prefix, lastPrefix) {
			continue
		}
		lastPrefix = append(lastPrefix[:0], prefix...)
		exp = append(exp, ik)
	}
	got := readAllInternalKeys(t, rootDir, dataDir, outFids)
	if len(got) != len(exp) {
		min := len(got)
		if len(exp) < min {
			min = len(exp)
		}
		mismatchAt := -1
		for i := 0; i < min; i++ {
			if !bytes.Equal(got[i], exp[i]) {
				mismatchAt = i
				break
			}
		}
		if mismatchAt >= 0 {
			t.Fatalf("merged entry count mismatch: got %d want %d; first mismatch at %d", len(got), len(exp), mismatchAt)
		}
		t.Fatalf("merged entry count mismatch: got %d want %d; got_last=%q exp_last=%q", len(got), len(exp), string(got[len(got)-1]), string(exp[len(exp)-1]))
	}
	for i := range exp {
		if !bytes.Equal(got[i], exp[i]) {
			t.Fatalf("merged internal key mismatch at %d", i)
		}
	}
}

func TestPickRange_OverlapBoundariesInclusive(t *testing.T) {
	lm := &levelManager{
		levels: make([]*pb.Levels, 2),
	}
	lm.levels[0] = &pb.Levels{}
	lm.levels[1] = &pb.Levels{
		Tables: []*pb.Table{
			{Id: 1, MinKey: []byte("a"), MaxKey: []byte("a")},
			{Id: 2, MinKey: []byte("b"), MaxKey: []byte("c")},
			{Id: 3, MinKey: []byte("d"), MaxKey: []byte("d")},
		},
	}

	l, r := lm.pickRange(1, []byte("c"), []byte("d"))
	if l != 1 || r != 3 {
		t.Fatalf("expected [1,3), got [%d,%d)", l, r)
	}
}

func TestPickRange_NoOverlap(t *testing.T) {
	lm := &levelManager{
		levels: make([]*pb.Levels, 2),
	}
	lm.levels[0] = &pb.Levels{}
	lm.levels[1] = &pb.Levels{
		Tables: []*pb.Table{
			{Id: 1, MinKey: []byte("a"), MaxKey: []byte("a")},
			{Id: 2, MinKey: []byte("b"), MaxKey: []byte("c")},
			{Id: 3, MinKey: []byte("d"), MaxKey: []byte("d")},
		},
	}

	l, r := lm.pickRange(1, []byte("x"), []byte("y"))
	if l != 3 || r != 3 {
		t.Fatalf("expected [3,3), got [%d,%d)", l, r)
	}
}

func TestPickRange_FullOverlap(t *testing.T) {
	lm := &levelManager{
		levels: make([]*pb.Levels, 2),
	}
	lm.levels[0] = &pb.Levels{}
	lm.levels[1] = &pb.Levels{
		Tables: []*pb.Table{
			{Id: 1, MinKey: []byte("a"), MaxKey: []byte("a")},
			{Id: 2, MinKey: []byte("b"), MaxKey: []byte("c")},
			{Id: 3, MinKey: []byte("d"), MaxKey: []byte("d")},
		},
	}

	l, r := lm.pickRange(1, []byte("a"), []byte("z"))
	if l != 0 || r != 3 {
		t.Fatalf("expected [0,3), got [%d,%d)", l, r)
	}
}

func TestPickL0Compaction_SourceOverlapTouchingRanges(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			l0Nums:        8,
			maxLevel:      7,
			baseLevelSize: 100,
		},
		levels: make([]*pb.Levels, 7),
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}

	lm.levelStats = make([]*levelStat, 7)
	for i := range lm.levelStats {
		lm.levelStats[i] = &levelStat{level: int8(i)}
	}
	lm.levelStats[1].capacity = 100
	lm.levelStats[1].usedSpace = 50
	lm.levelStats[2].usedSpace = 1
	lm.levelStats[3].usedSpace = 1
	lm.levelStats[4].usedSpace = 1
	lm.levelStats[5].usedSpace = 1
	lm.levelStats[6].usedSpace = 1

	lm.levels[0].Tables = []*pb.Table{
		{Id: 1, MinKey: []byte("a"), MaxKey: []byte("b")},
		{Id: 2, MinKey: []byte("b"), MaxKey: []byte("c")},
		{Id: 3, MinKey: []byte("e"), MaxKey: []byte("f")},
	}
	lm.levels[1].Tables = []*pb.Table{
		{Id: 10, MinKey: []byte("a"), MaxKey: []byte("a")},
		{Id: 11, MinKey: []byte("b"), MaxKey: []byte("b")},
		{Id: 12, MinKey: []byte("d"), MaxKey: []byte("d")},
	}

	def := lm.pickL0Compaction()
	if def == nil {
		t.Fatalf("expected non-nil compaction def")
	}
	if !reflect.DeepEqual(def.sourceTableIxs, []int{0, 1}) {
		t.Fatalf("expected sourceTableIxs [0 1], got %v", def.sourceTableIxs)
	}
	if def.targetLevel != 1 {
		t.Fatalf("expected targetLevel 1, got %d", def.targetLevel)
	}
	if def.targetTableRange != [2]int{0, 2} {
		t.Fatalf("expected targetTableRange [0 2], got %v", def.targetTableRange)
	}
	if string(def.minKey) != "a" || string(def.maxKey) != "c" {
		t.Fatalf("expected minKey=a maxKey=c, got min=%q max=%q", string(def.minKey), string(def.maxKey))
	}
	origMin := lm.levels[0].Tables[0].MinKey
	origMax := lm.levels[0].Tables[1].MaxKey
	origMin[0] = 'z'
	origMax[0] = 'a'
	if bytes.Equal(def.minKey, origMin) || bytes.Equal(def.maxKey, origMax) {
		t.Fatalf("expected cloned keys, got min=%q max=%q", string(def.minKey), string(def.maxKey))
	}
}

func TestPickLevelCompaction_TargetRangeMultipleOverlaps(t *testing.T) {
	lm := &levelManager{
		levelOption: levelOption{
			maxLevel:                             7,
			compactionPickCreatedCoefficient:     0.5,
			compactionPickTableMissedCoefficient: 0.5,
		},
		levels:     make([]*pb.Levels, 3),
		tableStats: map[uint64]tableStat{100: {createdAt: 1, missed: 10, hitted: 0}},
	}
	for i := range lm.levels {
		lm.levels[i] = &pb.Levels{}
	}
	lm.levels[1].Tables = []*pb.Table{
		{Id: 100, MinKey: []byte("b"), MaxKey: []byte("d")},
	}
	lm.levels[2].Tables = []*pb.Table{
		{Id: 200, MinKey: []byte("a"), MaxKey: []byte("a")},
		{Id: 201, MinKey: []byte("c"), MaxKey: []byte("c")},
		{Id: 202, MinKey: []byte("d"), MaxKey: []byte("e")},
		{Id: 203, MinKey: []byte("f"), MaxKey: []byte("f")},
	}

	def := lm.pickLevelCompaction(1)
	if def == nil {
		t.Fatalf("expected non-nil compaction def")
	}
	if def.sourceLevel != 1 || def.targetLevel != 2 {
		t.Fatalf("unexpected levels: %+v", def)
	}
	if !reflect.DeepEqual(def.sourceTableIxs, []int{0}) {
		t.Fatalf("expected sourceTableIxs [0], got %v", def.sourceTableIxs)
	}
	if def.targetTableRange != [2]int{1, 3} {
		t.Fatalf("expected targetTableRange [1 3], got %v", def.targetTableRange)
	}
}
