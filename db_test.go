package oscarkv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"OscarKV/wal"
	"OscarKV/kv"
)

func TestDB_SetGet(t *testing.T) {
	dir := t.TempDir()
	opt := &Option{
		CommitBuffer:             8,
		RootDir:                  dir,
		WriteBatchCountThreshold: 16,
		WriteBatchSizeThreshold:  1 << 16,
		MemoryLimitPerMemtable:   1 << 20,
		MaxImmutableMemtable:     0,
		MaxLevelPerMemtable:      16,
		StrictMode:               false,
		RandFactorPerMemtable:    0.5,
		SlowDownDurationMs:       0,
	}

	db, err := Open(opt)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("hello")
	val := []byte("world")

	if err := db.Set(key, val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil {
		t.Fatalf("Get returned nil")
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch: want %q, got %q", val, got)
	}

	t.Logf("Get key %q, got value %q", key, got)

	db.Close()

}

func TestDB_RotationAndRecovery(t *testing.T) {
	dir := t.TempDir()
	// Pre-populate >3 WAL segments
	m := wal.Open(dir)
	for seg := 0; seg < 4; seg++ {
		entries := []*kv.Entry{
			kv.NewInternalEntry([]byte("a"), []byte("va"), uint64(1000+seg), []byte("0xff_cf"), uint8(kv.MetaSet), 0),
			kv.NewInternalEntry([]byte("b"), []byte("vb"), uint64(2000+seg), []byte("0xff_cf"), uint8(kv.MetaSet), 0),
		}
		if _, err := m.AppendBatch(entries); err != nil {
			t.Fatalf("preload AppendBatch failed: %v", err)
		}
		if err := m.Flush(true); err != nil {
			t.Fatalf("preload Flush failed: %v", err)
		}
		if _, err := m.RotateLocked(); err != nil {
			t.Fatalf("preload RotateLocked failed: %v", err)
		}
		for _, e := range entries { e.Decr() }
	}
	m.Close()

	opt := &Option{
		CommitBuffer:             8,
		RootDir:                  dir,
		WriteBatchCountThreshold: 4,
		WriteBatchSizeThreshold:  256,
		MemoryLimitPerMemtable:   512,
		MaxImmutableMemtable:     0,
		MaxLevelPerMemtable:      8,
		StrictMode:               false,
		RandFactorPerMemtable:    0.5,
		SlowDownDurationMs:       0,
	}
	db, err := Open(opt)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	keys := [][]byte{
		[]byte("k1"), []byte("k2"), []byte("k3"),
		[]byte("k4"), []byte("k5"), []byte("k6"),
		[]byte("k7"), []byte("k8"), []byte("k9"),
		[]byte("k10"),
	}
	for i, k := range keys {
		val := []byte("val_" + string('A'+byte(i)))
		if err := db.Set(k, val); err != nil {
			t.Fatalf("Set %q failed: %v", k, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	logs, _ := filepath.Glob(filepath.Join(dir, "wal", "*.log"))
	if len(logs) < 3 {
		t.Fatalf("expected at least 3 wal segments, got %d", len(logs))
	}

	db2, err := Open(opt)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	gotA, err := db2.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after recovery failed for a: %v", err)
	}
	if !bytes.Equal(gotA, []byte("va")) {
		t.Fatalf("after recovery value mismatch for a: want %q, got %q", "va", gotA)
	}
	gotB, err := db2.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get after recovery failed for b: %v", err)
	}
	if !bytes.Equal(gotB, []byte("vb")) {
		t.Fatalf("after recovery value mismatch for b: want %q, got %q", "vb", gotB)
	}
}

func TestDB_LargeWriteTriggersRotate(t *testing.T) {
	dir := t.TempDir()
	t.Logf("wal dir: %s", filepath.Join(dir, "wal"))
	opt := &Option{
		CommitBuffer:             32,
		RootDir:                  dir,
		WriteBatchCountThreshold: 1,
		WriteBatchSizeThreshold:  1 << 20,
		MemoryLimitPerMemtable:   512,
		MaxImmutableMemtable:     0,
		MaxLevelPerMemtable:      8,
		StrictMode:               false,
		RandFactorPerMemtable:    0.5,
		SlowDownDurationMs:       0,
	}

	db, err := Open(opt)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	var rotated bool
	var firstKey, firstVal []byte
	var lastKey, lastVal []byte
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		v := []byte(fmt.Sprintf("v%06d", i))
		if i == 0 {
			firstKey, firstVal = k, v
		}
		lastKey, lastVal = k, v

		if err := db.Set(k, v); err != nil {
			t.Fatalf("Set failed at %d: %v", i, err)
		}
		if i%20 == 0 {
			logs, _ := filepath.Glob(filepath.Join(dir, "wal", "*.log"))
			if len(logs) >= 2 {
				for _, p := range logs {
					t.Logf("wal segment: %s", p)
				}
				rotated = true
				break
			}
		}
	}
	if !rotated {
		logs, _ := filepath.Glob(filepath.Join(dir, "wal", "*.log"))
		t.Fatalf("expected wal rotate to create >=2 segments, got %d", len(logs))
	}

	got, err := db.Get(firstKey)
	if err != nil {
		t.Fatalf("Get firstKey failed: %v", err)
	}
	if !bytes.Equal(got, firstVal) {
		t.Fatalf("firstKey value mismatch: want %q, got %q", firstVal, got)
	}

	got, err = db.Get(lastKey)
	if err != nil {
		t.Fatalf("Get lastKey failed: %v", err)
	}
	if !bytes.Equal(got, lastVal) {
		t.Fatalf("lastKey value mismatch: want %q, got %q", lastVal, got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	logs, _ := filepath.Glob(filepath.Join(dir, "wal", "*.log"))
	if len(logs) < 2 {
		t.Fatalf("expected wal segments >=2 after close, got %d", len(logs))
	}

	b, err := os.ReadFile(logs[0])
	if err != nil {
		t.Fatalf("read wal segment failed: %v", err)
	}
	n := len(b)
	if n > 256 {
		n = 256
	}
	t.Logf("wal dump: %s (first %d/%d bytes)", logs[0], n, len(b))
	for off := 0; off < n; off += 16 {
		end := off + 16
		if end > n {
			end = n
		}
		var hexPart [16 * 3]byte
		var asciiPart [16]byte
		for i := 0; i < 16; i++ {
			hexPart[i*3+0] = ' '
			hexPart[i*3+1] = ' '
			hexPart[i*3+2] = ' '
			asciiPart[i] = ' '
		}
		for i := off; i < end; i++ {
			v := b[i]
			j := i - off
			hex := "0123456789abcdef"
			hexPart[j*3+0] = hex[v>>4]
			hexPart[j*3+1] = hex[v&0x0f]
			hexPart[j*3+2] = ' '
			if v >= 32 && v <= 126 {
				asciiPart[j] = v
			} else {
				asciiPart[j] = '.'
			}
		}
		t.Logf("%08x  %s |%s|", off, string(hexPart[:]), string(asciiPart[:]))
	}
}
