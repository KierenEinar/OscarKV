//go:build windows

package vfs

import (
	"errors"
	"io"
)

type Vfs interface {
	io.Writer
	io.Reader
	io.Closer
	Flush() error

	ReadAt(off int64, size int64) ([]byte, error)
	WriteAt(off int64, p []byte) (int, error)

	Truncate(off int64, size int64) error
}

type FileMode int8

const (
	FileModeDefault FileMode = 0
)

type Option struct {
	Mode     FileMode
	Flag     int
	MmapSize int64
}

func Open(path string, opt Option) (Vfs, error) {
	switch opt.Mode {
	case FileModeDefault:
		return OpenFile(path, opt.Flag)
	default:
		return nil, errors.New("vfs: unknown file mode")
	}
}
