package kv

import (
	"bytes"
	"testing"
)

func TestSplitInternalKey_RoundTrip(t *testing.T) {
	origCF := []byte("cf")
	origKey := []byte("k")
	origVer := uint64(123)
	ik := MakeInternalKey(origCF, origKey, origVer)
	if ik == nil {
		t.Fatalf("MakeInternalKey returned nil")
	}
	cfLen, cf, key, ver, err := SplitInternalKey(ik)
	if err != nil {
		t.Fatalf("SplitInternalKey: %v", err)
	}
	if cfLen != uint8(len(origCF)) {
		t.Fatalf("cfLen mismatch: want %d got %d", len(origCF), cfLen)
	}
	if !bytes.Equal(cf, origCF) {
		t.Fatalf("cf mismatch: want %q got %q", string(origCF), string(cf))
	}
	if !bytes.Equal(key, origKey) {
		t.Fatalf("key mismatch: want %q got %q", string(origKey), string(key))
	}
	if ver != origVer {
		t.Fatalf("ver mismatch: want %d got %d", origVer, ver)
	}
}

func TestSplitInternalKey_Invalid(t *testing.T) {
	_, _, _, _, err := SplitInternalKey([]byte{1, 2, 3})
	if err == nil {
		t.Fatalf("expected error")
	}
}

