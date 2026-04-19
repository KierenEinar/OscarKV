//go:build darwin

package sstable

import (
	"OscarKV/vfs"
	"fmt"
	"os"
	"path/filepath"
)

const (
	defaultMmapSize = 4 << 20
)

func openSSTable(opt *Option) (*sstable, error) {

	path := filepath.Join(opt.RootDir, opt.DataDir, fmt.Sprintf("%016d.sst", opt.Fid))
	var mmapsize int64
	info, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if info != nil {
		mmapsize = info.Size()
	}

	if mmapsize == 0 {
		if opt.EstimatedSize > 0 {
			mmapsize = int64(opt.EstimatedSize)
		} else {
			mmapsize = defaultMmapSize
		}
	}

	handle, err := vfs.Open(path, vfs.Option{
		Mode:     vfs.FileModeMmap,
		Flag:     opt.Flags,
		MmapSize: mmapsize,
	})
	if err != nil {
		return nil, err
	}
	t := &sstable{
		fileHandle: handle,
		fid:        opt.Fid,
	}

	return t, nil
}
