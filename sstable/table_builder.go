package sstable

import (
	"OscarKV/kv"
	"OscarKV/pb"
	"OscarKV/utils"
	"bytes"
	"encoding/binary"
	"sync"

	"google.golang.org/protobuf/proto"
)

type TableBuilder struct {
	buffer            *bytes.Buffer
	maxBlockSize      uint32
	maxTableSize      uint32
	dataBlock         *blockWriter
	blocks            []block
	indexBlock        *pb.TableIndex
	indexBlockLen     uint32
	tableChecksum     uint32
	keyCount          uint32
	estimateStaleSize uint32
	maxVersion        uint64
	valSize           uint32
	prevKey           []byte // cf + key
	staleSize         uint32
}

type blockWriter struct {
	baseKey     []byte
	offsets     []uint32 // a container storing all the starting positions of entries.
	offset      uint32
	startOffset uint32
	buffer      *bytes.Buffer
	blockID     uint32
	keyCount    uint32
	entryHeader [6]byte
	crcBuf      [4]byte
}

type block struct {
	minKey []byte
	offset uint32
	length uint32
}

var (
	tableBuilderPool = sync.Pool{
		New: func() any {
			return &TableBuilder{
				buffer:     bytes.NewBuffer(nil),
				indexBlock: &pb.TableIndex{},
			}
		},
	}
	blockBuilderPool = sync.Pool{
		New: func() any {
			return &blockWriter{
				buffer: bytes.NewBuffer(nil),
			}
		},
	}
)

func (tb *TableBuilder) reset() {
	if tb.buffer == nil {
		tb.buffer = bytes.NewBuffer(nil)
	} else {
		tb.buffer.Reset()
	}
	if tb.dataBlock != nil {
		putBlockBuilder(tb.dataBlock)
		tb.dataBlock = nil
	}
	for i := range tb.blocks {
		if tb.blocks[i].minKey != nil {
			utils.PutBytes(tb.blocks[i].minKey)
			tb.blocks[i].minKey = nil
		}
	}
	tb.blocks = tb.blocks[:0]
	if tb.indexBlock == nil {
		tb.indexBlock = &pb.TableIndex{}
	} else {
		tb.indexBlock.Offset = tb.indexBlock.Offset[:0]
		tb.indexBlock.BloomFilter = nil
		tb.indexBlock.MaxVersion = 0
		tb.indexBlock.KeyCount = 0
		tb.indexBlock.StaleDataSize = 0
		tb.indexBlock.ValueSize = 0
	}
	tb.indexBlockLen = 0
	tb.tableChecksum = 0
	tb.keyCount = 0
	tb.estimateStaleSize = 0
	tb.maxVersion = 0
	tb.valSize = 0
	if tb.prevKey != nil {
		utils.PutBytes(tb.prevKey)
		tb.prevKey = nil
	}
}

func (bb *blockWriter) reset() {
	bb.baseKey = bb.baseKey[:0]
	bb.offsets = bb.offsets[:0]
	bb.offset = 0
	bb.keyCount = 0
	bb.startOffset = 0
	bb.buffer.Reset()
}

func getBlockBuilder() *blockWriter {
	bb := blockBuilderPool.Get().(*blockWriter)
	bb.reset()
	return bb
}

func putBlockBuilder(bb *blockWriter) {
	bb.reset()
	blockBuilderPool.Put(bb)
}

func NewTableBuilder(maxBlockSize, maxTableSize uint32) *TableBuilder {
	tb := tableBuilderPool.Get().(*TableBuilder)
	tb.reset()
	tb.maxBlockSize = maxBlockSize
	tb.maxTableSize = maxTableSize
	return tb
}

func PutTableBuilder(tb *TableBuilder) {
	if tb == nil {
		return
	}
	tb.reset()
	tableBuilderPool.Put(tb)
}

func (tb *TableBuilder) Flush(opt *Option) (int, error) {
	if opt == nil {
		return 0, ErrNilOption
	}
	if err := tb.Finish(); err != nil {
		return 0, err
	}
	if opt.EstimatedSize == 0 {
		opt.EstimatedSize = len(tb.buffer.Bytes())
	}
	ss, err := openSSTable(opt)
	if err != nil {
		return 0, err
	}
	data := tb.buffer.Bytes()
	n := 0
	if n, err = ss.fileHandle.Write(data); err != nil {
		_ = ss.close()
		return 0, err
	}
	if err := ss.fileHandle.Flush(); err != nil {
		_ = ss.close()
		return 0, err
	}
	err = ss.close()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (tb *TableBuilder) AddEntry(entry *kv.Entry) (bool, error) {
	if entry == nil {
		return false, ErrNilEntry
	}

	if tb.dataBlock == nil {
		tb.dataBlock = getBlockBuilder()
	}

	internalKey := entry.InternalKey()
	internalValue := entry.InternalValue()
	var prefixLen uint16
	if tb.dataBlock.keyCount != 0 {
		prefixLen = tb.dataBlock.getPrefixKeyLen(internalKey)
	}
	if tb.dataBlock.trySealed(internalKey, internalValue, prefixLen, tb.maxBlockSize) {
		if err := tb.finishDataBlock(); err != nil {
			return false, err
		}
		prefixLen = 0
	}

	bufSize := tb.dataBlock.addEntry(internalKey, internalValue, prefixLen)

	if tb.prevKey != nil && len(internalKey) >= 8 && bytes.Equal(tb.prevKey, internalKey[:len(internalKey)-8]) {
		tb.estimateStaleSize += bufSize
	}

	tableDone := tb.estimateStaleSize >= tb.maxTableSize

	if tb.prevKey != nil {
		utils.PutBytes(tb.prevKey)
		tb.prevKey = nil
	}

	if len(internalKey) >= 8 {
		tb.prevKey = utils.GetBytes(len(internalKey) - 8)
		copy(tb.prevKey, internalKey[:len(internalKey)-8])
	}

	if entry.Version > tb.maxVersion {
		tb.maxVersion = entry.Version
	}
	tb.keyCount++
	tb.valSize += uint32(len(internalValue))
	return tableDone, nil
}

func (tb *TableBuilder) Finish() error {

	if (tb.buffer == nil || len(tb.buffer.Bytes()) == 0) && (tb.dataBlock == nil || tb.dataBlock.keyCount == 0) {
		return ErrEmptyTable
	}

	if tb.dataBlock != nil && tb.dataBlock.keyCount != 0 {
		if err := tb.finishDataBlock(); err != nil {
			return err
		}
	}
	if tb.dataBlock != nil {
		putBlockBuilder(tb.dataBlock)
		tb.dataBlock = nil
	}
	if tb.prevKey != nil {
		utils.PutBytes(tb.prevKey)
		tb.prevKey = nil
	}

	if err := tb.writeIndexBlock(); err != nil {
		return err
	}

	if err := tb.writeFooter(); err != nil {
		return err
	}

	return nil
}

func (tb *TableBuilder) StaleSize() uint32 {
	return tb.staleSize
}

func (tb *TableBuilder) writeIndexBlock() error {
	// Binary Format for Index Block (protobuf-encoded bytes):
	// +------------------------------------------------------------------------------------------+
	// | TableIndex (protobuf bytes, var length)                                                  |
	// +------------------------------------------------------------------------------------------+
	//
	// TableIndex message layout (see pb/table.proto):
	// +------------------------------------------------------------------------------------------+
	// | repeated BlockOffset offset = 1;                                                         |
	// | bytes   bloomFilter   = 2;                                                               |
	// | int64   maxVersion    = 3;                                                               |
	// | uint32  keyCount      = 4;                                                               |
	// | uint32  staleDataSize = 5;                                                               |
	// | uint32  valueSize     = 6;                                                               |
	// +------------------------------------------------------------------------------------------+
	//
	// BlockOffset message layout:
	// +---------------------------------------------------------------+
	// | bytes minKey = 1; | uint32 offset = 2; | uint32 length = 3;    |
	// +---------------------------------------------------------------+
	if tb.buffer == nil {
		tb.buffer = bytes.NewBuffer(nil)
	}
	if tb.indexBlock == nil {
		tb.indexBlock = &pb.TableIndex{}
	}
	tb.indexBlock.Offset = tb.indexBlock.Offset[:0]
	for _, b := range tb.blocks {
		minKey := make([]byte, len(b.minKey))
		copy(minKey, b.minKey)
		tb.indexBlock.Offset = append(tb.indexBlock.Offset, &pb.BlockOffset{
			MinKey: minKey,
			Offset: b.offset,
			Length: b.length,
		})
	}
	tb.indexBlock.MaxVersion = int64(tb.maxVersion)
	tb.indexBlock.KeyCount = tb.keyCount
	tb.indexBlock.StaleDataSize = tb.estimateStaleSize
	tb.indexBlock.ValueSize = tb.valSize
	tb.staleSize = tb.estimateStaleSize
	indexBytes, err := proto.Marshal(tb.indexBlock)
	if err != nil {
		return err
	}
	tb.indexBlockLen = uint32(len(indexBytes))
	_, _ = tb.buffer.Write(indexBytes)
	return nil
}

func (tb *TableBuilder) writeFooter() error {
	// Binary Format for Footer (BigEndian, fixed 8 bytes):
	// +----------------------+----------------------+
	// | indexBlockLen (4B)   | tableChecksum (4B)   |
	// +----------------------+----------------------+
	//
	// - indexBlockLen: byte length of the protobuf-encoded Index Block
	// - tableChecksum: CRC32C(Castagnoli) over all bytes BEFORE the footer
	//   (i.e., Data Blocks concatenation + Index Block bytes)
	if tb.buffer == nil {
		return ErrNilBuffer
	}
	tableChecksum := utils.ChecksumCastagnoli(tb.buffer.Bytes())
	var footer [8]byte
	binary.BigEndian.PutUint32(footer[0:4], tb.indexBlockLen)
	binary.BigEndian.PutUint32(footer[4:8], tableChecksum)
	_, _ = tb.buffer.Write(footer[:])
	return nil
}

func (tb *TableBuilder) finishDataBlock() error {
	if tb.buffer == nil {
		return ErrNilBuffer
	}
	if tb.dataBlock == nil {
		return ErrNilDataBlock
	}
	if tb.dataBlock.offset == 0 {
		return ErrEmptyDataBlock
	}

	// Binary Format for a Data Block (BigEndian):
	// +----------------------------------+------------------------------------+
	// | ... (Entries, var length) ...    | Entry Offsets List (4B * N)         |
	// +----------------------------------+------------------------------------+
	// | Entry Offsets List Length (4B)   | Block Checksum (4B)                 |
	// +----------------------------------+------------------------------------+
	//
	// Entry Offsets List:
	// - N uint32 offsets, each points to the starting position of one entry within this data block.
	//
	// Entry layout (repeated, var length):
	// +--------------------+------------------+------------------+-------------------+-------------------+----------------+
	// | prefixKeyLen (2B)  | diffKeyLen (2B)  | valueLen (2B)    | diffKey (N bytes) | value (M bytes)   | checksum (4B)  |
	// +--------------------+------------------+------------------+-------------------+-------------------+----------------+
	//
	// Block Checksum:
	// - CRC32C(Castagnoli) over all bytes of the data block BEFORE the checksum
	//   (i.e., entries + offsets list + offsets list length).
	offsets := make([]byte, 0, 4*len(tb.dataBlock.offsets))
	for _, offset := range tb.dataBlock.offsets {
		offsets = binary.BigEndian.AppendUint32(offsets, offset)
	}
	var offsetLen [4]byte
	binary.BigEndian.PutUint32(offsetLen[:], uint32(4*len(tb.dataBlock.offsets)))
	_, _ = tb.dataBlock.buffer.Write(offsets)
	_, _ = tb.dataBlock.buffer.Write(offsetLen[:])

	crc32 := utils.ChecksumCastagnoli(tb.dataBlock.Bytes())
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32)
	_, _ = tb.dataBlock.buffer.Write(crc[:])
	tb.dataBlock.offset = uint32(tb.dataBlock.buffer.Len())

	_, _ = tb.buffer.Write(tb.dataBlock.Bytes())
	minKey := utils.GetBytes(len(tb.dataBlock.baseKey))
	copy(minKey, tb.dataBlock.baseKey)
	tb.blocks = append(tb.blocks, block{
		minKey: minKey,
		offset: tb.dataBlock.startOffset,
		length: tb.dataBlock.offset,
	})
	nextStart := tb.dataBlock.startOffset + tb.dataBlock.offset
	nextBlockID := tb.dataBlock.blockID + 1
	tb.dataBlock.reset()
	tb.dataBlock.blockID = nextBlockID
	tb.dataBlock.startOffset = nextStart
	return nil
}

func (bb *blockWriter) getPrefixKeyLen(internalKey []byte) uint16 {
	max := len(bb.baseKey)
	if len(internalKey) < max {
		max = len(internalKey)
	}
	var prefixLen uint16
	for i := 0; i < max; i++ {
		if bb.baseKey[i] != internalKey[i] {
			break
		}
		prefixLen++
	}
	return prefixLen
}

func (bb *blockWriter) trySealed(internalKey, internalValue []byte, prefixLen uint16, blockSize uint32) bool {
	if blockSize == 0 || bb.keyCount == 0 {
		return false
	}
	diffKeyLen := uint32(len(internalKey)) - uint32(prefixLen)
	valueLen := uint32(len(internalValue))
	estimated := uint32(len(bb.entryHeader)) + diffKeyLen + valueLen + uint32(len(bb.crcBuf))
	return bb.offset+estimated > blockSize
}

func (bb *blockWriter) addEntry(internalKey, internalValue []byte, prefixLen uint16) uint32 {

	if bb.keyCount == 0 {
		bb.baseKey = make([]byte, len(internalKey))
		copy(bb.baseKey, internalKey)
		prefixLen = 0
	}

	// Binary Format for a Block Entry (BigEndian):
	// +--------------------+------------------+------------------+-------------------+-------------------+----------------+
	// | prefixKeyLen (2B)  | diffKeyLen (2B)  | valueLen (2B)    | diffKey (N bytes) | value (M bytes)   | checksum (4B)  |
	// +--------------------+------------------+------------------+-------------------+-------------------+----------------+
	//
	// - prefixKeyLen: common prefix length between baseKey and internalKey
	// - diffKey: internalKey[prefixKeyLen:]
	// - checksum: CRC32C(Castagnoli) over (header(6B) + diffKey + value)

	diffKey := internalKey[int(prefixLen):]
	diffLen := uint16(len(diffKey))
	valueLen := uint16(len(internalValue))

	bb.offsets = append(bb.offsets, bb.offset)
	binary.BigEndian.PutUint16(bb.entryHeader[0:2], prefixLen)
	binary.BigEndian.PutUint16(bb.entryHeader[2:4], diffLen)
	binary.BigEndian.PutUint16(bb.entryHeader[4:6], valueLen)
	checksum := utils.ChecksumCastagnoli(bb.entryHeader[:], diffKey, internalValue)
	_, _ = bb.buffer.Write(bb.entryHeader[:])
	_, _ = bb.buffer.Write(diffKey)
	_, _ = bb.buffer.Write(internalValue)

	binary.BigEndian.PutUint32(bb.crcBuf[:], checksum)
	_, _ = bb.buffer.Write(bb.crcBuf[:])

	bufSize := uint32(len(bb.entryHeader) + len(diffKey) + len(internalValue) + len(bb.crcBuf))

	bb.offset += bufSize
	bb.keyCount += 1

	return bufSize
}

func (bb *blockWriter) Bytes() []byte {
	return bb.buffer.Bytes()
}
