package utils

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// dummy
	dummyOffset = ^uint32(0)
	dummyHead   = 0
	// offset layout
	keyOffset    = 0
	valOffset    = 1
	kvLenOffset  = 2
	heightOffset = 3

	// kvlen bits
	kvLenBits = 16
	kvShift   = (1 << kvLenBits) - 1
)

type CmpR int8

const (
	LessThan    CmpR = -1
	Equal       CmpR = 0
	GreaterThan CmpR = 1
)

type node struct {
	offset    uint32
	keyOffset uint32
	valOffset uint32
	keyLen    uint16
	valLen    uint16
	height    uint8
	next      []uint32
}

type Skiplist struct {
	// data is a container which holds the key and value.
	data     []byte
	capacity uint32
	size     uint32
	reserved uint32
	// nodeData
	// 0 -> keyOffset
	// 1 -> valOffset
	// 2 -> keyLen(top 16bits) | valLen (bottom 16bits)
	// 3 -> height
	// 4 -> next node offset
	nodeData     []uint32
	maxLevel     uint8
	currentLevel uint8
	randFactor   float64
	rnd          *rand.Rand

	ref int32
	rw  sync.RWMutex

	// callback
	onClose func()
	cmp     func(a, b []byte) CmpR
}

func NewSkiplist(memoryLimit uint32,
	maxLevel uint8,
	randFactor float64,
	onClose func(),
	cmp func(a, b []byte) CmpR) *Skiplist {

	sl := &Skiplist{
		data:       make([]byte, memoryLimit),
		nodeData:   make([]uint32, 0, 4096),
		maxLevel:   maxLevel,
		randFactor: randFactor,
		onClose:    onClose,
		cmp:        cmp,
		rnd:        rand.New(rand.NewSource(time.Now().UnixNano())),
		capacity:   memoryLimit,
	}

	// make a dummy head
	sl.nodeData = append(sl.nodeData,
		dummyOffset,
		dummyOffset,
		0,
		uint32(maxLevel),
	)

	for i := uint8(0); i < maxLevel; i++ {
		sl.nodeData = append(sl.nodeData, dummyOffset)
	}

	return sl
}

func (sl *Skiplist) Incr() {
	atomic.AddInt32(&sl.ref, 1)
}

func (sl *Skiplist) Decr() int32 {
	r := atomic.AddInt32(&sl.ref, -1)
	if r < 0 { // Underflow check
		panic("a counter reference bug has raised a panic")
	}
	if r == 0 {
		sl.rw.Lock()
		if sl.onClose != nil {
			sl.onClose()
		}
		sl.rw.Unlock()
	}

	return r
}

func (sl *Skiplist) decodeNode(nodeOffset uint32) node {
	kOffset := sl.nodeData[nodeOffset+keyOffset]
	vOffset := sl.nodeData[nodeOffset+valOffset]
	kvlen := sl.nodeData[nodeOffset+kvLenOffset]
	height := uint8(sl.nodeData[nodeOffset+heightOffset])
	klen := uint16(kvlen >> kvLenBits & kvShift)
	vlen := uint16(kvlen & kvShift)
	next := sl.nodeData[nodeOffset+heightOffset+1 : nodeOffset+heightOffset+1+uint32(height)]
	n := node{
		offset:    nodeOffset,
		keyOffset: kOffset,
		valOffset: vOffset,
		keyLen:    klen,
		valLen:    vlen,
		height:    height,
		next:      next,
	}
	return n
}

func (sl *Skiplist) randHeight() uint8 {
	h := uint8(1)
	for h < sl.maxLevel && sl.rnd.Float64() < sl.randFactor {
		h++
	}
	return h
}

// findPrev returns the previous nodes at each level.
// If allowEqual is true, it returns the node with key <= target.
// If allowEqual is false, it returns the node with key < target.
func (sl *Skiplist) findPrev(key []byte, allowEqual bool) []uint32 {
	prevList := make([]uint32, sl.maxLevel)
	currOffset := uint32(dummyHead)

	for i := int(sl.maxLevel) - 1; i >= 0; i-- {
		for {
			currNode := sl.decodeNode(currOffset)
			nextOffset := currNode.next[i]
			if nextOffset == dummyOffset {
				break
			}
			nextNode := sl.decodeNode(nextOffset)
			nodeKey := sl.data[nextNode.keyOffset : nextNode.keyOffset+uint32(nextNode.keyLen)]
			cmpRes := sl.cmp(nodeKey, key)
			if cmpRes < 0 || (allowEqual && cmpRes == 0) {
				currOffset = nextOffset
			} else {
				break
			}
		}
		prevList[i] = currOffset
	}
	return prevList
}

func (sl *Skiplist) Key(n node) ([]byte, error) {
	if n.keyOffset == dummyOffset {
		return nil, ErrDummyNode
	}
	if n.keyOffset+uint32(n.keyLen) <= uint32(len(sl.data)) {
		return sl.data[n.keyOffset : n.keyOffset+uint32(n.keyLen)], nil
	}
	return nil, ErrOutOfBound
}

func (sl *Skiplist) Value(n node) ([]byte, error) {
	if n.valOffset == dummyOffset {
		return nil, ErrDummyNode
	}
	if n.valOffset+uint32(n.valLen) <= uint32(len(sl.data)) {
		return sl.data[n.valOffset : n.valOffset+uint32(n.valLen)], nil
	}
	return nil, ErrOutOfBound
}

func (sl *Skiplist) FindGT(key []byte) (node, error) {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	prevOffsets := sl.findPrev(key, true)
	curr := sl.decodeNode(prevOffsets[0])
	nextOffset := curr.next[0]
	if nextOffset == dummyOffset {
		return node{}, ErrKeyNotFound
	}

	return sl.decodeNode(nextOffset), nil
}

func (sl *Skiplist) FindGE(key []byte) (node, error) {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	prevOffsets := sl.findPrev(key, false)
	curr := sl.decodeNode(prevOffsets[0])
	nextOffset := curr.next[0]
	if nextOffset == dummyOffset {
		return node{}, ErrKeyNotFound
	}

	return sl.decodeNode(nextOffset), nil
}

func (sl *Skiplist) FindLast() (node, error) {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	currOffset := uint32(dummyHead)
	for i := int(sl.maxLevel) - 1; i >= 0; i-- {
		for {
			curr := sl.decodeNode(currOffset)
			nextOffset := curr.next[i]
			if nextOffset == dummyOffset {
				break
			}
			currOffset = nextOffset
		}
	}
	if currOffset == uint32(dummyHead) {
		return node{}, ErrKeyNotFound
	}
	return sl.decodeNode(currOffset), nil
}

func (sl *Skiplist) Put(key, val []byte) (err error) {
	sl.rw.Lock()
	defer sl.rw.Unlock()

	prevOffsets := sl.findPrev(key, false)
	// Check if key already exists
	nextOffset := sl.decodeNode(prevOffsets[0]).next[0]
	if nextOffset != dummyOffset {
		nextNode := sl.decodeNode(nextOffset)
		nodeKey := sl.data[nextNode.keyOffset : nextNode.keyOffset+uint32(nextNode.keyLen)]
		if sl.cmp(nodeKey, key) == 0 {
			// Update value if length is the same, or append new value
			if uint32(len(val)) <= uint32(nextNode.valLen) {
				copy(sl.data[nextNode.valOffset:], val)
				sl.nodeData[nextOffset+kvLenOffset] = (uint32(nextNode.keyLen) << kvLenBits) | uint32(len(val))
				return nil
			}
			// For simplicity, always append new value if it doesn't fit
			if sl.size+uint32(len(val)) > sl.capacity {
				return ErrOutOfMemory
			}
			newValOffset := sl.size
			copy(sl.data[newValOffset:], val)
			sl.size += uint32(len(val))
			sl.nodeData[nextOffset+valOffset] = newValOffset
			sl.nodeData[nextOffset+kvLenOffset] = (uint32(nextNode.keyLen) << kvLenBits) | uint32(len(val))
			return nil
		}
	}

	height := sl.randHeight()
	if height > sl.currentLevel {
		sl.currentLevel = height
	}

	if sl.size+uint32(len(key)+len(val)) > sl.capacity {
		return ErrOutOfMemory
	}

	kOffset := sl.size
	copy(sl.data[kOffset:], key)
	sl.size += uint32(len(key))

	vOffset := sl.size
	copy(sl.data[vOffset:], val)
	sl.size += uint32(len(val))

	newNodeOffset := uint32(len(sl.nodeData))
	sl.nodeData = append(sl.nodeData, kOffset, vOffset, (uint32(len(key))<<kvLenBits)|uint32(len(val)), uint32(height))

	for i := uint8(0); i < height; i++ {
		prevNode := sl.decodeNode(prevOffsets[i])
		sl.nodeData = append(sl.nodeData, prevNode.next[i])
		sl.nodeData[prevOffsets[i]+heightOffset+1+uint32(i)] = newNodeOffset
	}

	return nil
}

func (sl *Skiplist) TryReserve(size uint32) bool {
	sl.reserved += size
	return sl.reserved+sl.size <= sl.capacity
}

func (sl *Skiplist) ReleaseReserved(size uint32) {
	if sl.reserved < size {
		panic("reserved size is negative")
	}
	sl.reserved -= size
}

func (sl *Skiplist) SeekToFirst() (uint32, error) {
	sl.rw.RLock()
	defer sl.rw.RUnlock()
	currOffset := uint32(dummyHead)
	cur := sl.decodeNode(currOffset)
	next := cur.next[0]
	if next == dummyOffset {
		return 0, ErrKeyNotFound
	}
	return next, nil
}

func (sl *Skiplist) Iterate(nodeoffset uint32, callback func(key, value []byte,
	nextNodeOffset uint32) (stop bool)) error {
	sl.rw.RLock()
	for nodeoffset != 0 && nodeoffset != dummyOffset {
		sl.rw.RUnlock()
		node := sl.decodeNode(nodeoffset)
		k, err := sl.Key(node)
		if err != nil {
			return err
		}
		v, err := sl.Value(node)
		if err != nil {
			return err
		}
		nodeoffset = node.next[0]
		if callback(k, v, nodeoffset) {
			return nil
		}
		sl.rw.RLock()
	}
	sl.rw.RUnlock()
	return nil
}

// Seek seeks to the first node with a key greater than or equal to the given key.
func (sl *Skiplist) Seek(key []byte) (nodeOffset uint32, err error) {
	sl.rw.RLock()
	prevOffsets := sl.findPrev(key, false)
	curr := sl.decodeNode(prevOffsets[0])
	sl.rw.RUnlock()
	nextOffset := curr.next[0]
	if nextOffset == dummyOffset {
		return 0, ErrKeyNotFound
	}
	return nextOffset, nil
}
