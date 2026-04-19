package lsm

import "testing"

func TestLevelOptionSantanize_Defaults(t *testing.T) {
	var opt levelOption
	opt = opt.santanize()
	if opt.tableSize == 0 {
		t.Fatalf("tableSize default not set")
	}
	if opt.dataBlockSize == 0 {
		t.Fatalf("dataBlockSize default not set")
	}
	if opt.l0Nums == 0 {
		t.Fatalf("l0Nums default not set")
	}
	if opt.maxLevel == 0 {
		t.Fatalf("maxLevel default not set")
	}
	if opt.baseLevelSize == 0 {
		t.Fatalf("baseLevelSize default not set")
	}
	if opt.ingestNums == 0 {
		t.Fatalf("ingestNums default not set")
	}
	if opt.levelSizeExp == 0 {
		t.Fatalf("levelSizeExp default not set")
	}
	if opt.l0ForcedCompactPercentage == 0 {
		t.Fatalf("l0ForcedCompactPercentage default not set")
	}
	if opt.compactionPickL0Coefficient == 0 {
		t.Fatalf("compactionPickL0Coefficient default not set")
	}
	if opt.compactionPickSpaceCoefficient == 0 {
		t.Fatalf("compactionPickSpaceCoefficient default not set")
	}
	if opt.compactionPickMissedCoefficient == 0 {
		t.Fatalf("compactionPickMissedCoefficient default not set")
	}
	if opt.compactionPickStaleCoefficient == 0 {
		t.Fatalf("compactionPickStaleCoefficient default not set")
	}
	if opt.compactionPickCreatedCoefficient+opt.compactionPickTableMissedCoefficient != 1 {
		t.Fatalf("expected created+tableMissed == 1, got %f", opt.compactionPickCreatedCoefficient+opt.compactionPickTableMissedCoefficient)
	}
}

func TestLevelOptionSantanize_NormalizesCompactionPickCoefficients(t *testing.T) {
	opt := levelOption{
		compactionPickCreatedCoefficient:     2,
		compactionPickTableMissedCoefficient: 1,
	}
	opt = opt.santanize()
	sum := opt.compactionPickCreatedCoefficient + opt.compactionPickTableMissedCoefficient
	if sum != 1 {
		t.Fatalf("expected sum == 1, got %f", sum)
	}
	if opt.compactionPickCreatedCoefficient <= opt.compactionPickTableMissedCoefficient {
		t.Fatalf("expected created > tableMissed after normalization, got created=%f missed=%f", opt.compactionPickCreatedCoefficient, opt.compactionPickTableMissedCoefficient)
	}
}
