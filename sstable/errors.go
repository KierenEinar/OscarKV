package sstable

import "errors"

var (
	ErrInvalidIndex = errors.New("sstable: invalid index")
	ErrKeyNotFound = errors.New("sstable: key not found")
	ErrInvalidKeyFormat = errors.New("sstable: key format invalid")
	ErrNilSSTable     = errors.New("sstable: nil")
	ErrNilEntry       = errors.New("sstable: nil entry")
	ErrNilOption      = errors.New("sstable: nil option")
	ErrEmptyTable     = errors.New("sstable: empty table")
	ErrNilBuffer      = errors.New("sstable: nil buffer")
	ErrNilDataBlock   = errors.New("sstable: nil data block")
	ErrEmptyDataBlock = errors.New("sstable: empty data block")

	ErrSSTableTooSmall         = errors.New("sstable: too small")
	ErrSSTableCorruption       = errors.New("sstable: corruption")
	ErrSSTableChecksumMismatch = errors.New("sstable: checksum mismatch")

	ErrDataBlockTooSmall         = errors.New("sstable: data block too small")
	ErrDataBlockCorruption       = errors.New("sstable: data block corruption")
	ErrDataBlockChecksumMismatch = errors.New("sstable: data block checksum mismatch")

	ErrEntryCorruption       = errors.New("sstable: entry corruption")
	ErrEntryChecksumMismatch = errors.New("sstable: entry checksum mismatch")
)
