package iterator

import (
	"OscarKV/kv"
	"OscarKV/utils"
	"container/heap"
	"io"
)

type MergeIterator struct {
	iterators []Iterator
	minHeap   *MinHeap
	ref       int32
}

type entryWrap struct {
	Entry *kv.Entry
	ix    int
}

type MinHeap struct {
	entries []*entryWrap
}

func (h MinHeap) Len() int {
	return len(h.entries)
}

func (h MinHeap) Less(i, j int) bool {
	return utils.CompareKey(h.entries[i].Entry.InternalKey(), h.entries[j].Entry.InternalKey()) < 0
}

func (h MinHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *MinHeap) Push(x interface{}) {
	h.entries = append(h.entries, x.(*entryWrap))
}

func (h *MinHeap) Pop() interface{} {
	n := len(h.entries)
	e := h.entries[n-1]
	h.entries[n-1] = nil
	h.entries = h.entries[:n-1]
	return e
}

func NewMergeIterator(iterators []Iterator) *MergeIterator {
	return &MergeIterator{iterators: iterators, ref: 0}
}

func (it *MergeIterator) SeekToFirst() (*kv.Entry, error) {
	minHeap := &MinHeap{}
	for ix, iter := range it.iterators {
		if iter == nil {
			continue
		}
		e, err := iter.SeekToFirst()
		if err != nil {
			if err == io.EOF {
				continue
			}
			return nil, err
		}
		if e == nil {
			continue
		}
		heap.Push(minHeap, &entryWrap{Entry: e, ix: ix})
	}
	it.minHeap = minHeap
	if minHeap.Len() == 0 {
		return nil, io.EOF
	}
	return it.Next()
}

func (it *MergeIterator) Seek(key []byte) (*kv.Entry, error) {
	minHeap := &MinHeap{}
	for ix, iter := range it.iterators {
		if iter == nil {
			continue
		}
		e, err := iter.Seek(key)
		if err != nil {
			if err == io.EOF {
				continue
			}
			return nil, err
		}
		if e == nil {
			continue
		}
		heap.Push(minHeap, &entryWrap{Entry: e, ix: ix})
	}
	it.minHeap = minHeap
	if minHeap.Len() == 0 {
		return nil, io.EOF
	}
	return it.Next()
}

func (it *MergeIterator) Next() (*kv.Entry, error) {
	if it.ref <= 0 {
		return nil, io.EOF
	}

	if it.minHeap == nil {
		return it.SeekToFirst()
	}

	if it.minHeap.Len() > 0 {
		e := heap.Pop(it.minHeap).(*entryWrap)
		next, err := it.iterators[e.ix].Next()
		if err != nil && err != io.EOF {
			return nil, err
		}
		if next != nil {
			heap.Push(it.minHeap, &entryWrap{
				Entry: next,
				ix:    e.ix,
			})
		}
		return e.Entry, nil
	}

	return nil, io.EOF
}

func (it *MergeIterator) Incr() {
	if it.ref == 0 {
		for _, iter := range it.iterators {
			if iter != nil {
				iter.Incr()
			}
		}
	}
	it.ref++
}

func (it *MergeIterator) Decr() {
	it.ref--
	if it.ref > 0 {
		return
	}
	if it.ref < 0 {
		panic("merge iterator ref underflow")
	}
	for _, iter := range it.iterators {
		if iter != nil {
			iter.Decr()
		}
	}
	it.iterators = nil
	it.minHeap = nil
}

type SubCompactionIterator struct {
	startKey []byte
	endKey   []byte
	source   Iterator
	targets  []Iterator
	minHeap  *MinHeap
}

// NewSubCompactionIterator creates a new subCompactionIterator.
// caller should move the cursor to the first key before using it.
func NewSubCompactionIterator(startKey, endKey []byte, source Iterator,
	targets []Iterator) *SubCompactionIterator {
	return &SubCompactionIterator{
		startKey: startKey,
		endKey:   endKey,
		source:   source,
		targets:  targets,
	}
}

func (it *SubCompactionIterator) Incr() {
	it.source.Incr()
	for _, iter := range it.targets {
		if iter != nil {
			iter.Incr()
		}
	}
}

func (it *SubCompactionIterator) Decr() {
	it.source.Decr()
	for _, iter := range it.targets {
		if iter != nil {
			iter.Decr()
		}
	}
}

func (it *SubCompactionIterator) SeekToFirst() (*kv.Entry, error) {
	minHeap := &MinHeap{}

	if it.source != nil {
		var e *kv.Entry
		var err error
		if it.startKey == nil {
			e, err = it.source.SeekToFirst()
		} else {
			e, err = it.source.Seek(it.startKey)
			for err == nil && e != nil && utils.CompareKey(e.InternalKey(), it.startKey) == 0 {
				e.Decr()
				e, err = it.source.Next()
			}
		}

		if err == nil && e != nil {
			if it.endKey == nil || utils.CompareKey(e.InternalKey(), it.endKey) <= 0 {
				heap.Push(minHeap, &entryWrap{Entry: e, ix: 0})
			} else {
				e.Decr()
			}
		} else if err != nil && err != io.EOF {
			return nil, err
		}
	}

	for ix, iter := range it.targets {
		if iter == nil {
			continue
		}
		var e *kv.Entry
		var err error
		if it.startKey == nil {
			e, err = iter.SeekToFirst()
		} else {
			e, err = iter.Seek(it.startKey)
			for err == nil && e != nil && utils.CompareKey(e.InternalKey(), it.startKey) == 0 {
				e.Decr()
				e, err = iter.Next()
			}
		}

		if err == nil && e != nil {
			if it.endKey == nil || utils.CompareKey(e.InternalKey(), it.endKey) <= 0 {
				heap.Push(minHeap, &entryWrap{Entry: e, ix: ix + 1})
			} else {
				e.Decr()
			}
		} else if err != nil && err != io.EOF {
			continue
		}
	}
	it.minHeap = minHeap
	if minHeap.Len() == 0 {
		return nil, io.EOF
	}
	return it.Next()
}

func (it *SubCompactionIterator) Seek(key []byte) (*kv.Entry, error) {
	minHeap := &MinHeap{}

	if it.source != nil {
		e, err := it.source.Seek(key)
		if err == nil && e != nil {
			if (it.startKey == nil || utils.CompareKey(e.InternalKey(), it.startKey) > 0) &&
				(it.endKey == nil || utils.CompareKey(e.InternalKey(), it.endKey) <= 0) {
				heap.Push(minHeap, &entryWrap{Entry: e, ix: 0})
			} else {
				e.Decr()
			}
		} else if err != nil && err != io.EOF {
			return nil, err
		}
	}

	for ix, iter := range it.targets {
		if iter == nil {
			continue
		}
		e, err := iter.Seek(key)
		if err == nil && e != nil {
			if (it.startKey == nil || utils.CompareKey(e.InternalKey(), it.startKey) > 0) &&
				(it.endKey == nil || utils.CompareKey(e.InternalKey(), it.endKey) <= 0) {
				heap.Push(minHeap, &entryWrap{Entry: e, ix: ix + 1})
			} else {
				e.Decr()
			}
		} else if err != nil && err != io.EOF {
			continue
		}
	}
	it.minHeap = minHeap
	if minHeap.Len() == 0 {
		return nil, io.EOF
	}
	return it.Next()
}

func (it *SubCompactionIterator) Next() (*kv.Entry, error) {

	if it.minHeap == nil {
		return it.SeekToFirst()
	}

	if it.minHeap.Len() > 0 {
		e := heap.Pop(it.minHeap).(*entryWrap)
		var (
			next *kv.Entry
			err  error
		)
		if e.ix == 0 {
			next, err = it.source.Next()
		} else {
			next, err = it.targets[e.ix-1].Next()
		}

		if next != nil && it.endKey != nil && utils.CompareKey(next.InternalKey(), it.endKey) > 0 {
			// fmt.Printf("Skipping key %q because it is > endKey %q\n", string(next.InternalKey()), string(it.endKey))
			next.Decr()
			next = nil
		}

		if err != nil && err != io.EOF {
			return nil, err
		}
		if next != nil {
			heap.Push(it.minHeap, &entryWrap{
				Entry: next,
				ix:    e.ix,
			})
		}
		return e.Entry, nil
	}

	return nil, io.EOF
}
