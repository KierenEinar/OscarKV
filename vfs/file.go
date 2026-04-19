package vfs

import (
	"bufio"
	"io"
	"os"
)

type File struct {
	f *os.File
	r *bufio.Reader
	w *bufio.Writer
}

func OpenFile(path string, flag int) (*File, error) {
	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	return &File{
		f: f,
		r: bufio.NewReaderSize(f, 16<<10),
		w: bufio.NewWriterSize(f, 16<<10),
	}, nil
}

func (file *File) Read(p []byte) (int, error) {
	if file.w.Buffered() != 0 {
		if err := file.Flush(); err != nil {
			return 0, err
		}
	}
	return file.r.Read(p)
}

func (file *File) Write(p []byte) (int, error) {
	n, err := file.w.Write(p)
	file.r.Reset(file.f)
	return n, err
}

func (file *File) Close() error {
	_ = file.Flush()
	return file.f.Close()
}

func (file *File) Flush() error {
	if err := file.w.Flush(); err != nil {
		return err
	}
	file.r.Reset(file.f)
	return file.f.Sync()
}

func (file *File) ReadAt(off int64, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}
	if file.w.Buffered() != 0 {
		if err := file.Flush(); err != nil {
			return nil, err
		}
	}
	buf := make([]byte, size)
	n, err := file.f.ReadAt(buf, off)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return buf[:n], err
}

func (file *File) WriteAt(off int64, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := file.w.Flush(); err != nil {
		return 0, err
	}
	n, err := file.f.WriteAt(p, off)
	file.r.Reset(file.f)
	return n, err
}

func (file *File) Truncate(off int64, size int64) error {
	if err := file.Flush(); err != nil {
		return err
	}
	newSize := off
	if size > 0 {
		newSize = off + size
	}
	if err := file.f.Truncate(newSize); err != nil {
		return err
	}
	file.r.Reset(file.f)
	file.w.Reset(file.f)
	return nil
}

func (file *File) Size() (int64, error) {
	if file.w.Buffered() != 0 {
		if err := file.w.Flush(); err != nil {
			return 0, err
		}
		file.r.Reset(file.f)
	}
	info, err := file.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
