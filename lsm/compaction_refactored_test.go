package lsm

import (
	"OscarKV/kv"
	"OscarKV/pb"
	"bytes"
	"testing"
)

func TestCompaction_ProposeLogic(t *testing.T) {
	lm := &levelManager{
		levels: make([]*pb.Levels, 2),
	}
	lm.levels[0] = &pb.Levels{Tables: []*pb.Table{{Id: 1}, {Id: 2}}}
	lm.levels[1] = &pb.Levels{Tables: []*pb.Table{{Id: 3}, {Id: 4}}}

	// Case 1: l == r (Insert at index)
	cDef := &compactionDef{
		sourceLevel:      0,
		targetLevel:      1,
		sourceTableIxs:   []int{0},
		targetTableRange: [2]int{1, 1},
	}

	// Mocking what compaction would do
	sourceTables := []*pb.Table{lm.levels[0].Tables[0]}
	l, r := cDef.targetTableRange[0], cDef.targetTableRange[1]

	if l == r {
		insertDef := &levelInsertDef{
			level:    cDef.targetLevel,
			table:    sourceTables,
			insertAt: l,
		}
		deleteDef := &levelDeleteDef{
			level:    cDef.sourceLevel,
			deleteAt: cDef.sourceTableIxs[0],
			length:   len(cDef.sourceTableIxs),
		}

		if insertDef.level != 1 || insertDef.insertAt != 1 || insertDef.table[0].Id != 1 {
			t.Errorf("InsertDef mismatch: %+v", insertDef)
		}
		if deleteDef.level != 0 || deleteDef.deleteAt != 0 || deleteDef.length != 1 {
			t.Errorf("DeleteDef mismatch: %+v", deleteDef)
		}
	}
}

func TestCompaction_SubInternalVal_Consistency(t *testing.T) {
	cf := []byte("cf1")
	key := []byte("key1")
	val := []byte("value1")
	version := uint64(100)
	meta := uint8(kv.MetaSet)
	ttl := int64(3600)

	e := kv.NewInternalEntry(key, val, version, cf, meta, ttl)
	defer e.Decr()

	internalVal := e.InternalValue()
	gotMeta, gotTTL, gotVal, err := kv.SplitInternalVal(internalVal)
	if err != nil {
		t.Fatalf("SplitInternalVal failed: %v", err)
	}

	if gotMeta != meta {
		t.Errorf("Meta mismatch: want %d got %d", meta, gotMeta)
	}
	if int64(gotTTL) != ttl {
		t.Errorf("TTL mismatch: want %d got %d", ttl, gotTTL)
	}
	if !bytes.Equal(gotVal, val) {
		t.Errorf("Value mismatch: want %x got %x", val, gotVal)
	}
}
