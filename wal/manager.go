package wal

import (
	"OscarKV/iterator"
	"OscarKV/kv"
	"OscarKV/utils"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Manager struct {
	rootDir string
	active  *os.File
	mutex   sync.Mutex
	writer  *bufio.Writer

	Active      Segment
	checkPoints map[int]*Segment
	offset      int
}

type Segment struct {
	ID   int32
	Path string
}

type LogIterator struct {
	buffer      *bufio.Reader
	segmentID   int32
	curRec      []byte
	curIdx      int
	left        int
	file        *os.File
	chunkReader *chunkReader
	ref         int32
}

type chunkReader struct {
	buffer      *bufio.Reader
	payload     []byte
	entryLeft   uint32
	offset      int
	chunkID     int
	chunkLength uint32
}

var (
	bufferPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(nil)
		},
	}
	walDir     = "wal"
	logPattern = "%08d.log"
)

const (
	// chunk layout
	// len: 4bytes, count: 4bytes, payload: nbytes, crc32: 4bytes
	chunkLenBytes        = 4
	chunkEntryCountBytes = 4
	chunkCRC32Bytes      = 4
	chunkEntryLenBytes   = 4
)

func Open(rootDir string) *Manager {
	dir := filepath.Join(rootDir, walDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(err)
	}

	ext := filepath.Ext(logPattern)
	matches, _ := filepath.Glob(filepath.Join(dir, "*"+ext))
	var maxID int
	all := make(map[int]*Segment)
	for _, match := range matches {
		name := filepath.Base(match)
		var id int
		if _, err := fmt.Sscanf(name, logPattern, &id); err == nil {
			all[id] = &Segment{ID: int32(id), Path: match}
			if id > maxID {
				maxID = id
			}
		}
	}

	// If no log files exist, start with 1. Otherwise, open the latest one for appending.
	if maxID == 0 {
		maxID = 1
		fileName := fmt.Sprintf(logPattern, maxID)
		all[maxID] = &Segment{ID: int32(maxID), Path: filepath.Join(dir, fileName)}
	}

	fileName := fmt.Sprintf(logPattern, maxID)
	filePath := filepath.Join(dir, fileName)

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		panic(err)
	}

	checkPoints := make(map[int]*Segment)
	for id, seg := range all {
		if id == maxID {
			continue
		}
		checkPoints[id] = seg
	}

	m := &Manager{
		rootDir:     rootDir,
		active:      f,
		writer:      bufio.NewWriter(f),
		Active:      Segment{ID: int32(maxID), Path: filePath},
		checkPoints: checkPoints,
		offset:      int(offset),
	}

	return m
}

// caller needs to flush manually
func (manager *Manager) AppendBatch(entries []*kv.Entry) (int, error) {
	chunkBuffer := makeChunk(entries)
	defer func() {
		chunkBuffer.Reset()
		bufferPool.Put(chunkBuffer)
	}()

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	n, err := manager.writer.Write(chunkBuffer.Bytes())
	if err != nil {
		return 0, err
	}
	offset := manager.offset
	manager.offset += n
	return offset, nil
}

func makeChunk(entries []*kv.Entry) *bytes.Buffer {
	chunkBuffer := bufferPool.Get().(*bytes.Buffer)
	chunkBuffer.Reset()

	var lenBuf [chunkLenBytes]byte
	chunkBuffer.Write(lenBuf[:])

	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(entries)))
	chunkBuffer.Write(lenBuf[:])

	for _, entry := range entries {
		enc := kv.Encode(entry)
		chunkBuffer.Write(enc)
	}

	res := chunkBuffer.Bytes()
	crc := utils.ChecksumCastagnoli(res[8:])
	binary.BigEndian.PutUint32(lenBuf[:], crc)
	chunkBuffer.Write(lenBuf[:])

	finalRes := chunkBuffer.Bytes()
	binary.BigEndian.PutUint32(finalRes[0:4], uint32(len(finalRes)))

	return chunkBuffer
}

func (manager *Manager) RotateLocked() (int32, error) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	prev := manager.Active
	manager.Active.ID += 1
	dir := filepath.Join(manager.rootDir, walDir)
	fileName := fmt.Sprintf(logPattern, manager.Active.ID)
	filePath := filepath.Join(dir, fileName)

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}

	_ = manager.flush(true)
	manager.writer.Reset(f)
	if manager.active != nil {
		_ = manager.active.Close()
	}
	manager.active = f
	manager.Active.Path = filePath
	manager.offset = 0
	if manager.checkPoints == nil {
		manager.checkPoints = make(map[int]*Segment)
	}
	// Previous active becomes a checkpoint.
	manager.checkPoints[int(prev.ID)] = &Segment{ID: prev.ID, Path: prev.Path}
	return manager.Active.ID, nil
}

func (manager *Manager) Flush(sync bool) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return manager.flush(sync)
}

func (manager *Manager) Truncate(offset int) error {
	// No-op stub to satisfy lsm references; real implementation would truncate active log.
	return nil
}

func (manager *Manager) flush(sync bool) error {
	if err := manager.writer.Flush(); err != nil {
		return err
	}
	if sync {
		return manager.active.Sync()
	}
	return nil
}

func (manager *Manager) Close() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if manager.writer != nil {
		_ = manager.flush(true)
	}
	if manager.active != nil {
		_ = manager.active.Close()
	}
}

func (manager *Manager) Remove(segmentID int32) error {
	manager.mutex.Lock()
	if manager.Active.ID >= segmentID {
		manager.mutex.Unlock()
		return utils.ErrDropBlocked
	}
	manager.mutex.Unlock()
	dir := filepath.Join(manager.rootDir, walDir)
	path := filepath.Join(dir, fmt.Sprintf(logPattern, int(segmentID)))
	err := os.Remove(path)
	if err != nil {
		return err
	}
	manager.mutex.Lock()
	delete(manager.checkPoints, int(segmentID))
	manager.mutex.Unlock()
	return nil
}

func (manager *Manager) ListSegments() ([]Segment, error) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	ids := make([]int, 0, len(manager.checkPoints)+1)
	for id := range manager.checkPoints {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	out := make([]Segment, 0, len(ids))
	for _, id := range ids {
		seg := manager.checkPoints[id]
		if seg == nil {
			continue
		}
		out = append(out, *seg)
	}

	// Include active segment as the last element (sorted insert by ID).
	out = append(out, manager.Active)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (manager *Manager) Iterator(segmentID int32) (iterator.Iterator, error) {
	dir := filepath.Join(manager.rootDir, walDir)
	fileName := fmt.Sprintf(logPattern, int(segmentID))
	filePath := filepath.Join(dir, fileName)
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	it := &LogIterator{
		buffer:    bufio.NewReader(f),
		segmentID: segmentID,
		file:      f,
		ref:       0,
	}

	it.chunkReader = &chunkReader{
		buffer: it.buffer,
	}

	return it, nil
}

func (iter *LogIterator) Next() (*kv.Entry, error) {
	if iter.ref <= 0 {
		return nil, io.EOF
	}

	if iter.chunkReader == nil {
		return iter.SeekToFirst()
	}

	return iter.chunkReader.Next()
}

func (iter *LogIterator) SeekToFirst() (*kv.Entry, error) {
	if iter.file == nil {
		return nil, io.EOF
	}
	if _, err := iter.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	iter.buffer = bufio.NewReader(iter.file)
	iter.chunkReader = &chunkReader{buffer: iter.buffer}
	return iter.Next()
}

func (iter *LogIterator) Seek(key []byte) (*kv.Entry, error) {
	return nil, utils.ErrSeekNotSupported
}

func (iter *LogIterator) Incr() {
	if iter.ref < 0 {
		panic("log iterator ref underflow")
	}
	iter.ref++
}

func (iter *LogIterator) Decr() {
	iter.ref--
	if iter.ref > 0 {
		return
	}
	if iter.ref < 0 {
		panic("log iterator ref underflow")
	}
	if iter.file != nil {
		_ = iter.file.Close()
		iter.file = nil
	}
}

func (chunk *chunkReader) readBytes(lenBuf []byte) error {
	_, err := io.ReadFull(chunk.buffer, lenBuf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return io.EOF
	}
	return err
}

func (chunk *chunkReader) Next() (*kv.Entry, error) {
	var lenBuf [chunkLenBytes]byte
	var err error
	var crcRead uint32
	var crcCalc uint32
	var n int
	var entry *kv.Entry

	for {
		if chunk.entryLeft > 0 {
			goto next
		}

		chunk.chunkID += 1
		chunk.offset = 0

		err = chunk.readBytes(lenBuf[:]) // length
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		chunk.chunkLength = binary.BigEndian.Uint32(lenBuf[:])
		if chunk.chunkLength < 12 {
			return nil, utils.ErrCorruption
		}

		err = chunk.readBytes(lenBuf[:]) // nums
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		chunk.entryLeft = binary.BigEndian.Uint32(lenBuf[:])
		chunk.payload = make([]byte, int(chunk.chunkLength)-12)
		err = chunk.readBytes(chunk.payload)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}

		err = chunk.readBytes(lenBuf[:]) // crc
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		crcRead = binary.BigEndian.Uint32(lenBuf[:])
		crcCalc = utils.ChecksumCastagnoli(chunk.payload)
		if crcRead != crcCalc {
			chunk.entryLeft = 0
			chunk.payload = nil
			return nil, utils.ErrCorruption
		}

	next:
		n, entry, err = kv.Decode(chunk.payload)
		if err != nil {
			return nil, err
		}
		if n <= 0 || entry == nil {
			return nil, utils.ErrCorruption
		}
		chunk.payload = chunk.payload[n:]
		chunk.entryLeft -= 1
		chunk.offset += n
		return entry, nil
	}
}
