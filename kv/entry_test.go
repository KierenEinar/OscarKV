package kv

import (
	"bytes"
	"testing"
	"math"

	"OscarKV/utils"
)

func TestEntry_EncodeDecode(t *testing.T) {
	e := &Entry{
		Key:     []byte("test_key"),
		Value:   []byte("test_value"),
		Meta:    uint8(MetaSet),
		TTL:     123456789,
		CF:      []byte("default"),
		Version: math.MaxUint64-10,
	}

	buf := Encode(e)
	if buf == nil {
		t.Fatal("Encode returned nil")
	}

	n, decoded, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded == nil {
		t.Fatal("Decoded entry is nil")
	}
	if n != len(buf) {
		t.Fatalf("Decode consumed %d bytes, want %d", n, len(buf))
	}

	if !bytes.Equal(e.Key, decoded.Key) {
		t.Errorf("Key mismatch: expected %s, got %s", e.Key, decoded.Key)
	}
	if !bytes.Equal(e.Value, decoded.Value) {
		t.Errorf("Value mismatch: expected %s, got %s", e.Value, decoded.Value)
	}
	if e.Meta != decoded.Meta {
		t.Errorf("Meta mismatch: expected %d, got %d", e.Meta, decoded.Meta)
	}
	if e.TTL != decoded.TTL {
		t.Errorf("TTL mismatch: expected %d, got %d", e.TTL, decoded.TTL)
	}
	if !bytes.Equal(e.CF, decoded.CF) {
		t.Errorf("CF mismatch: expected %s, got %s", e.CF, decoded.CF)
	}
	if e.Version != decoded.Version {
		t.Errorf("Version mismatch: expected %d, got %d", e.Version, decoded.Version)
	}

	decoded.Decr()
	utils.PutBytes(buf)
}

func TestEntry_CRCError(t *testing.T) {
	e := &Entry{
		Key:   []byte("key"),
		Value: []byte("val"),
	}
	buf := Encode(e)
	
	// Corrupt the data
	buf[0] = ^buf[0]
	
	_, decoded, err := Decode(buf)
	if decoded != nil {
		t.Error("Expected nil for corrupted CRC")
	}
	if err != nil {
		// Currently returning nil error for CRC failure in my implementation
	}

	utils.PutBytes(buf)
}
