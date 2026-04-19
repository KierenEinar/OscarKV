package sstable

import (
	"OscarKV/pb"
	"OscarKV/utils"
	"OscarKV/vfs"
	"encoding/binary"

	"google.golang.org/protobuf/proto"
)

type (
	Option struct {
		RootDir       string
		DataDir       string
		Fid           int
		EstimatedSize int // default is 4k
		Flags         int
	}

	sstable struct {
		fileHandle vfs.Vfs
		index      *pb.TableIndex
		idxOffset  uint32
		idxLen     uint32
		fid        int
	}
)

const (
	footerSizeBytes     = 8
	footerIndexLenBytes = 4
	footerChecksumBytes = 4
)

// tableLayout just represents the sstable structure encoded by tableBuilder.
type tableLayout struct {

	// datablock
	// repeated entry: prefixKeyLen: 2bytes, diffKeyLen: 2bytes, diffKey: nbytes, valuelen: 2bytes, val: nbytes, checksum: 4bytes
	entries []struct {
		prefixKeyLen int16
		diffKeyLen   int16
		valuelen     int16
		diffKey      []byte
		value        []byte
	}

	// indexBlock
	// repeated entry: key: nbytes, offset: 4bytes, len: 4bytes
	// bloomFilter: nbytes
	// maxVersion: 8bytes
	// keyCount: 4bytes
	// staleDataSize: 4bytes
	// valueSize: 4bytes
	pb.TableIndex
	// footer
	// indexBlockLen: 4bytes
	// checksum: 4bytes
	footer struct {
		indexBlockLen int32
		checksum      uint32
	}
}

func (ss *sstable) close() error {
	return ss.fileHandle.Close()
}

func (ss *sstable) read(off, size int64) ([]byte, error) {
	return ss.fileHandle.ReadAt(off, size)
}

func (ss *sstable) write(buf []byte) (int, error) {
	return ss.fileHandle.Write(buf)
}

func (ss *sstable) init(loadIndex bool) error {
	size, err := ss.fileHandle.Size()
	if err != nil {
		return err
	}
	if size < footerSizeBytes {
		return ErrSSTableTooSmall
	}

	// decode footer and verify checksum
	footer, err := ss.fileHandle.ReadAt(size-footerSizeBytes, footerSizeBytes)
	if err != nil || int64(len(footer)) < footerSizeBytes {
		if err != nil {
			return err
		}
		return ErrSSTableCorruption
	}
	idxLen := binary.BigEndian.Uint32(footer[0:footerIndexLenBytes])
	checksum := binary.BigEndian.Uint32(footer[footerIndexLenBytes:footerSizeBytes])

	if int64(idxLen)+footerSizeBytes > size {
		return ErrSSTableCorruption
	}

	contentSize := size - footerSizeBytes
	content, err := ss.fileHandle.ReadAt(0, contentSize)
	if err != nil {
		return err
	}
	if int64(len(content)) != contentSize {
		return ErrSSTableCorruption
	}
	if utils.ChecksumCastagnoli(content) != checksum {
		return ErrSSTableChecksumMismatch
	}

	// decode index, write idxOffset, idxLen, fid
	idxOff := size - footerSizeBytes - int64(idxLen)
	if idxOff < 0 {
		return ErrSSTableCorruption
	}
	idxBytes, err := ss.fileHandle.ReadAt(idxOff, int64(idxLen))
	if err != nil || len(idxBytes) != int(idxLen) {
		if err != nil {
			return err
		}
		return ErrSSTableCorruption
	}

	if loadIndex {
		var idx pb.TableIndex
		if err := proto.Unmarshal(idxBytes, &idx); err != nil {
			return err
		}
		ss.index = &idx
	}
	ss.idxOffset = uint32(idxOff)
	ss.idxLen = idxLen
	return nil
}

func (ss *sstable) loadIndex() *pb.TableIndex {

	if ss.index != nil {
		return ss.index
	}

	idxBytes, err := ss.fileHandle.ReadAt(int64(ss.idxOffset), int64(ss.idxLen))
	if err != nil || len(idxBytes) != int(ss.idxLen) {
		return nil
	}
	var idx pb.TableIndex
	if err := proto.Unmarshal(idxBytes, &idx); err != nil {
		return nil
	}
	ss.index = &idx
	return ss.index
}
