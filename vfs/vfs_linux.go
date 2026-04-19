//go:build linux
package vfs

import (
	"io"
)

type Vfs interface {
	// sequential read and write
	io.Writer
	io.Reader
	io.Closer
	Flush() error

	// random read and write
	ReadAt(off int64, size int64) ([]byte, error)
	WriteAt(off int64, p []byte) (int, error)

	// truncate
	Truncate(off int64, size int64) error
}

type FileMode int8

const (
	FileModeDefault FileMode = 0
	FileModeMmap FileMode = 1
)

type Option struct {
	Mode FileMode
	Flag int
	MmapSize int64 // mmap size in bytes
}


func Open(path string, opt Option) (Vfs, error) {
	var impl Vfs
	var err error
	switch opt.Mode {
	case FileModeMmap:
		impl, err = OpenMmapSized(path, opt.Flag, opt.MmapSize)
	default:
		impl, err = OpenFile(path, opt.Flag)
	}
	if err != nil {
		return nil, err
	}
	return impl, nil
}

