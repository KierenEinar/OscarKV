//go:build linux
package vfs

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

type Mmap struct {
	fd int32
	f *os.File
	data []byte
	size int64
	offset int64
	writable bool
	prot int
}

func OpenMmapSized(path string, flag int, mapSize int64) (*Mmap, error) {
	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	writable := flag&(os.O_WRONLY|os.O_RDWR) != 0
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		if writable {
			size = mapSize
			if err := f.Truncate(size); err != nil {
				_ = f.Close()
				return nil, err
			}
		} else {
			return &Mmap{fd: int32(f.Fd()), f: f, data: nil, size: 0, writable: false, prot: syscall.PROT_READ}, nil
		}
	}
	fd := int32(f.Fd())
	prot := syscall.PROT_READ
	if writable {
		prot |= syscall.PROT_WRITE
	}
	data, err := syscall.Mmap(int(fd), 0, int(size), prot, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Mmap{fd: fd, f: f, data: data, size: size, writable: writable, prot: prot}, nil
}

func (m *Mmap) remap(newSize int64) error {
	if !m.writable {
		return os.ErrPermission
	}
	if newSize <= 0 {
		newSize = 1
	}
	if err := m.f.Truncate(newSize); err != nil {
		return err
	}
	if m.data != nil {
		_ = syscall.Munmap(m.data)
		m.data = nil
	}
	data, err := syscall.Mmap(int(m.fd), 0, int(newSize), m.prot, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	m.data = data
	m.size = newSize
	if m.offset > newSize {
		m.offset = newSize
	}
	return nil
}

func (m *Mmap) ensureSize(min int64) error {
	if !m.writable {
		return os.ErrPermission
	}
	if min <= m.size {
		return nil
	}
	newSize := m.size
	if newSize <= 0 {
		newSize = 1
	}
	for newSize < min {
		newSize *= 2
	}
	return m.remap(newSize)
}

func (m *Mmap) Read(p []byte) (int, error) {
	if m.offset >= m.size {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.offset:])
	m.offset += int64(n)
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (m *Mmap) Write(p []byte) (int, error) {
	if !m.writable {
		return 0, os.ErrPermission
	}
	end := m.offset + int64(len(p))
	if err := m.ensureSize(end); err != nil {
		return 0, err
	}
	n := copy(m.data[m.offset:end], p)
	m.offset += int64(n)
	return n, nil
}

func (m *Mmap) Close() error {
	if m.data != nil {
		_ = m.Flush()
		_ = syscall.Munmap(m.data)
		m.data = nil
	}
	if m.f != nil {
		return m.f.Close()
	}
	return nil
}

func (m *Mmap) Flush() error {
	if m.data == nil || len(m.data) == 0 {
		if m.f != nil {
			return m.f.Sync()
		}
		return nil
	}
	const msSync = 0x0010
	_, _, e := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&m.data[0])), uintptr(len(m.data)), uintptr(msSync))
	if e != 0 {
		return e
	}
	return m.f.Sync()
}

func (m *Mmap) ReadAt(off int64, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}
	if m.size == 0 || m.data == nil {
		return nil, io.EOF
	}
	if off >= m.size {
		return nil, io.EOF
	}
	end := off + size
	if end > m.size {
		end = m.size
	}
	return m.data[off:end], nil
}

func (m *Mmap) Truncate(off int64, size int64) error {
	if !m.writable {
		return os.ErrPermission
	}
	newSize := off
	if size > 0 {
		newSize = off + size
	}
	return m.remap(newSize)
}

func (m *Mmap) WriteAt(off int64, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !m.writable {
		return 0, os.ErrPermission
	}
	end := off + int64(len(p))
	if err := m.ensureSize(end); err != nil {
		return 0, err
	}
	n := copy(m.data[off:end], p)
	return n, nil
}

func (m *Mmap) Size() (int64, error) {
	return m.size, nil 
}