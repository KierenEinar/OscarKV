//go:build linux
package vfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestVfs_MmapMode_WriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t_mmap.dat")

	f, err := Open(path, Option{Mode: FileModeMmap, Flag: os.O_RDWR | os.O_CREATE | os.O_TRUNC, MmapSize: 16 << 10})
	if err != nil {
		t.Fatalf("Open mmap failed: %v", err)
	}

	if _, err := f.WriteAt(0, []byte("hello")); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := f.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	f, err = Open(path, Option{Mode: FileModeMmap, Flag: os.O_RDONLY, MmapSize: 0})
	if err != nil {
		t.Fatalf("Open mmap read failed: %v", err)
	}
	defer f.Close()

	got, err := f.ReadAt(0, 5)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("ReadAt got %q want %q", got, "hello")
	}
}

func TestVfs_MmapMode_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t_mmap_trunc.dat")

	f, err := Open(path, Option{Mode: FileModeMmap, Flag: os.O_RDWR | os.O_CREATE | os.O_TRUNC, MmapSize: 16 << 10})
	if err != nil {
		t.Fatalf("Open mmap failed: %v", err)
	}
	defer f.Close()

	if _, err := f.WriteAt(0, []byte("0123456789")); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := f.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if err := f.Truncate(0, 4); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
	if err := f.Flush(); err != nil {
		t.Fatalf("Flush after truncate failed: %v", err)
	}

	got, err := f.ReadAt(0, 16)
	if err != nil && err != os.ErrInvalid {
	}
	if !bytes.Equal(got, []byte("0123")) {
		t.Fatalf("after truncate got %q want %q", got, "0123")
	}
}

