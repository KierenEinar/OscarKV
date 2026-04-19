package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"OscarKV/kv"
	"OscarKV/utils"
)

func TestMakeRecord(t *testing.T) {
	entries := []*kv.Entry{
		{Key: []byte("hello"), Value: []byte("v1"), Meta: uint8(kv.MetaSet)},
		{Key: []byte("world"), Value: []byte("v2"), Meta: uint8(kv.MetaSet)},
	}

	recordBuf := makeChunk(entries)
	record := recordBuf.Bytes()

	// Verify total length
	if len(record) < 12 { // 4B totalLen + 4B entryNum + 4B crc32 + payloads
		t.Fatalf("record too short: %d", len(record))
	}
	totalLen := binary.BigEndian.Uint32(record[0:4])
	if totalLen != uint32(len(record)) {
		t.Errorf("total length mismatch: expected %d, got %d", len(record), totalLen)
	}

	// Verify entryNum
	entryNum := binary.BigEndian.Uint32(record[4:8])
	if entryNum != uint32(len(entries)) {
		t.Errorf("entry num mismatch: expected %d, got %d", len(entries), entryNum)
	}

	// Verify entries
	offset := 8
	for i, e := range entries {
		n, decoded, err := kv.Decode(record[offset : len(record)-4])
		if err != nil || decoded == nil {
			t.Fatalf("Decode failed for entry %d: %v", i, err)
		}
		offset += n
		if string(decoded.Key) != string(e.Key) {
			t.Errorf("key %d mismatch: expected %s, got %s", i, e.Key, decoded.Key)
		}
		if string(decoded.Value) != string(e.Value) {
			t.Errorf("value %d mismatch: expected %s, got %s", i, e.Value, decoded.Value)
		}
		decoded.Decr()
	}

	// Verify CRC32
	crcOffset := len(record) - 4
	if offset != crcOffset {
		t.Fatalf("offset mismatch before CRC: expected %d, got %d", crcOffset, offset)
	}
	actualCrc := binary.BigEndian.Uint32(record[crcOffset:])
	expectedCrc := utils.ChecksumCastagnoli(record[8:crcOffset])
	if actualCrc != expectedCrc {
		t.Errorf("CRC mismatch: expected %x, got %x", expectedCrc, actualCrc)
	}
}

func TestLogIterator_NextDelegatesToChunkReader(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_logiter_next")
	defer os.RemoveAll(tmpDir)

	m := Open(tmpDir)
	defer m.Close()

	entries := []*kv.Entry{
		{Key: []byte("k1"), Value: []byte("v1"), Meta: uint8(kv.MetaSet)},
		{Key: []byte("k2"), Value: []byte("v2"), Meta: uint8(kv.MetaSet)},
	}
	if _, err := m.AppendBatch(entries); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}
	if err := m.Flush(true); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	itIface, err := m.Iterator(m.Active.ID)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	it := itIface.(*LogIterator)
	it.Incr()
	defer it.Decr()

	e, err := it.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if e == nil || string(e.Key) != "k1" || string(e.Value) != "v1" {
		t.Fatalf("unexpected first entry: %#v", e)
	}
	e.Decr()

	e, err = it.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if e == nil || string(e.Key) != "k2" || string(e.Value) != "v2" {
		t.Fatalf("unexpected second entry: %#v", e)
	}
	e.Decr()

	_, err = it.Next()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestIterator_ReadBatches(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_iter_read")
	defer os.RemoveAll(tmpDir)

	m := Open(tmpDir)
	defer m.Close()

	b1 := []*kv.Entry{
		{Key: []byte("a1"), Value: []byte("va1"), Meta: uint8(kv.MetaSet)},
		{Key: []byte("a2"), Value: []byte("va2"), Meta: uint8(kv.MetaSet)},
	}
	b2 := []*kv.Entry{
		{Key: []byte("b1"), Value: []byte("vb1"), Meta: uint8(kv.MetaSet)},
	}
	if _, err := m.AppendBatch(b1); err != nil {
		t.Fatalf("AppendBatch b1 failed: %v", err)
	}
	if err := m.Flush(true); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if _, err := m.AppendBatch(b2); err != nil {
		t.Fatalf("AppendBatch b2 failed: %v", err)
	}
	if err := m.Flush(true); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	itIface, err := m.Iterator(m.Active.ID)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	it := itIface.(*LogIterator)
	it.Incr()
	defer it.Decr()

	var gotKeys []string
	var gotVals []string
	for {
		e, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil && (errors.Is(err, utils.ErrCorruption) || err.Error() == utils.ErrCorruption.Error()) {
			continue
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if e == nil {
			break
		}
		gotKeys = append(gotKeys, string(e.Key))
		gotVals = append(gotVals, string(e.Value))
		e.Decr()
	}
	wantKeys := []string{"a1", "a2", "b1"}
	wantVals := []string{"va1", "va2", "vb1"}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("iterator returned %d kvs, want %d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], wantKeys[i])
		}
		if gotVals[i] != wantVals[i] {
			t.Errorf("value[%d] = %q, want %q", i, gotVals[i], wantVals[i])
		}
	}
}

func TestIterator_SkipCorruptedRecord(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_iter_corrupt")
	defer os.RemoveAll(tmpDir)

	m := Open(tmpDir)
	defer m.Close()

	ok := []*kv.Entry{{Key: []byte("ok1"), Value: []byte("vok1"), Meta: uint8(kv.MetaSet)}}
	if _, err := m.AppendBatch(ok); err != nil {
		t.Fatalf("AppendBatch ok failed: %v", err)
	}
	if err := m.Flush(true); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Manually append a corrupted chunk (bad CRC)
	rec := makeChunk([]*kv.Entry{{Key: []byte("bad1"), Value: []byte("vbad1"), Meta: uint8(kv.MetaSet)}}).Bytes()
	// Flip last byte (part of CRC)
	rec[len(rec)-1] ^= 0xFF
	f, err := os.OpenFile(m.active.Name(), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open for append failed: %v", err)
	}
	if _, err := f.Write(rec); err != nil {
		t.Fatalf("write corrupt record failed: %v", err)
	}
	f.Close()

	// Append another good record
	ok2 := []*kv.Entry{{Key: []byte("ok2"), Value: []byte("vok2"), Meta: uint8(kv.MetaSet)}}
	if _, err := m.AppendBatch(ok2); err != nil {
		t.Fatalf("AppendBatch ok2 failed: %v", err)
	}
	if err := m.Flush(true); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	itIface, err := m.Iterator(m.Active.ID)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	it := itIface.(*LogIterator)
	it.Incr()
	defer it.Decr()

	var gotKeys []string
	var gotVals []string
	for {
		e, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil && (errors.Is(err, utils.ErrCorruption) || err.Error() == utils.ErrCorruption.Error()) {
			continue
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if e == nil {
			break
		}
		gotKeys = append(gotKeys, string(e.Key))
		gotVals = append(gotVals, string(e.Value))
		e.Decr()
	}
	// Corrupted record should be skipped
	wantKeys := []string{"ok1", "ok2"}
	wantVals := []string{"vok1", "vok2"}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("iterator returned %d kvs, want %d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Errorf("key[%d] = %q, want %q", i, gotKeys[i], wantKeys[i])
		}
		if gotVals[i] != wantVals[i] {
			t.Errorf("value[%d] = %q, want %q", i, gotVals[i], wantVals[i])
		}
	}
}

func TestIterator_OpenMissingSegment(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_iter_missing")
	defer os.RemoveAll(tmpDir)

	m := Open(tmpDir)
	defer m.Close()

	_, err := m.Iterator(999)
	if err == nil {
		t.Fatal("expected error for missing segment file")
	}
}

func TestIterator_CloseDoesNotBreakWriter(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_iter_close")
	defer os.RemoveAll(tmpDir)

	m := Open(tmpDir)
	defer m.Close()

	if _, err := m.AppendBatch([]*kv.Entry{{Key: []byte("k1"), Value: []byte("v1"), Meta: uint8(kv.MetaSet)}}); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}
	if err := m.Flush(true); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	itIface, err := m.Iterator(m.Active.ID)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	it := itIface.(*LogIterator)
	if it.chunkReader == nil || it.chunkReader.buffer != it.buffer {
		t.Fatalf("chunkReader not initialized correctly")
	}

	f := it.file
	it.Incr()
	it.Decr()
	if _, err := f.Stat(); err == nil {
		t.Fatalf("expected Stat error after Close")
	}

	// Closing iterator should not impact writer append
	if _, err := m.AppendBatch([]*kv.Entry{{Key: []byte("k2"), Value: []byte("v2"), Meta: uint8(kv.MetaSet)}}); err != nil {
		t.Fatalf("AppendBatch after iterator close failed: %v", err)
	}
	if err := m.Flush(true); err != nil {
		t.Fatalf("Flush after iterator close failed: %v", err)
	}
}

func TestOpen(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_test")
	defer os.RemoveAll(tmpDir)

	// Test 1: Open creates directory and initial file
	m := Open(tmpDir)
	if m == nil {
		t.Fatal("Open returned nil")
	}
	m.active.Close()

	walPath := filepath.Join(tmpDir, "wal")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Errorf("wal directory not created")
	}

	// Test 2: Find max log
	log1 := filepath.Join(walPath, fmt.Sprintf(logPattern, 1))
	log5 := filepath.Join(walPath, fmt.Sprintf(logPattern, 5))
	os.WriteFile(log1, []byte("log1"), 0644)
	os.WriteFile(log5, []byte("log5"), 0644)

	m2 := Open(tmpDir)
	if !strings.Contains(m2.active.Name(), "00000005.log") {
		t.Errorf("expected active log 5, got %s", m2.active.Name())
	}
	m2.active.Close()
}

func TestAppendBatch(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_append_test")
	defer os.RemoveAll(tmpDir)

	m := Open(tmpDir)
	defer m.active.Close()

	entries := []*kv.Entry{
		{Key: []byte("k1"), Value: []byte("v1"), Meta: uint8(kv.MetaSet)},
		{Key: []byte("k2"), Value: []byte("v2"), Meta: uint8(kv.MetaSet)},
	}
	if _, err := m.AppendBatch(entries); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	// Read back and verify
	m.writer.Flush()
	data, err := os.ReadFile(m.active.Name())
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("log file is empty")
	}
}

func TestManager_Close(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal_close_test")
	defer os.RemoveAll(tmpDir)

	m := Open(tmpDir)
	// Try to append some data before close
	_, _ = m.AppendBatch([]*kv.Entry{{Key: []byte("k"), Value: []byte("v"), Meta: uint8(kv.MetaSet)}})

	// Close should flush and close the file
	m.Close()

	// Verify the file is closed (Write to a closed file should error)
	_, err := m.active.Write([]byte("more data"))
	if err == nil {
		t.Error("expected error writing to a closed file")
	}

}
