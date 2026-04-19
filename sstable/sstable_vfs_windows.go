//go:build windows

package sstable

import (
	"OscarKV/vfs"
	"fmt"
	"path/filepath"
)

func openSSTable(opt *Option) (*sstable, error) {

	path := filepath.Join(opt.RootDir, opt.DataDir, fmt.Sprintf("%016d.sst", opt.Fid))

	handle, err := vfs.Open(path, vfs.Option{
		Mode: vfs.FileModeDefault,
		Flag: opt.Flags,
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
