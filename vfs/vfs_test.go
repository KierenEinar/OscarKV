package vfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestVfs_DefaultMode_SequentialWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dat")

	f, err := Open(path, Option{Mode: FileModeDefault, Flag: os.O_RDWR | os.O_CREATE | os.O_TRUNC})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	f, err = Open(path, Option{Mode: FileModeDefault, Flag: os.O_RDONLY})
	if err != nil {
		t.Fatalf("Open read failed: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 5)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 5 {
		t.Fatalf("Read n=%d want 5", n)
	}
	if !bytes.Equal(buf, []byte("hello")) {
		t.Fatalf("Read got %q want %q", buf, "hello")
	}
}

func TestVfs_DefaultMode_ReadAtWriteAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dat")

	f, err := Open(path, Option{Mode: FileModeDefault, Flag: os.O_RDWR | os.O_CREATE | os.O_TRUNC})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	if _, err := f.WriteAt(5, []byte("xyz")); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := f.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	got, err := f.ReadAt(0, 8)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("ReadAt len=%d want 8", len(got))
	}
	if !bytes.Equal(got[5:], []byte("xyz")) {
		t.Fatalf("ReadAt suffix got %q want %q", got[5:], "xyz")
	}
}

func TestVfs_DefaultMode_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dat")

	f, err := Open(path, Option{Mode: FileModeDefault, Flag: os.O_RDWR | os.O_CREATE | os.O_TRUNC})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatalf("Write failed: %v", err)
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
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(got, []byte("0123")) {
		t.Fatalf("after truncate got %q want %q", got, "0123")
	}
}
