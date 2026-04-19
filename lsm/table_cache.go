package lsm

import (
	"OscarKV/sstable"
	"container/list"
	"errors"
	"math"
	"os"
	"time"
)

type tableCacheEntry struct {
	id         uint64
	table      *sstable.Table
	lastAccess int64
	elem       *list.Element
}

func (lm *levelManager) getTable(id uint64) (*sstable.Table, error) {
	now := time.Now().Unix()

	lm.cacheMu.Lock()
	if lm.cache == nil {
		lm.cache = make(map[uint64]*tableCacheEntry, 128)
	}
	if lm.cacheList == nil {
		lm.cacheList = list.New()
	}
	if e, ok := lm.cache[id]; ok && e != nil && e.table != nil {
		e.lastAccess = now
		lm.cacheList.MoveToFront(e.elem)
		e.table.Incr()
		lm.cacheMu.Unlock()
		return e.table, nil
	}
	lm.cacheMu.Unlock()

	if id > uint64(math.MaxInt) {
		return nil, errors.New("table id out of int range")
	}
	tbl, err := sstable.Open(&sstable.Option{
		RootDir: lm.levelOption.rootDir,
		DataDir: lm.levelOption.dataDir,
		Fid:     int(id),
		Flags:   os.O_RDONLY,
	}, true)
	if err != nil {
		return nil, err
	}

	lm.cacheMu.Lock()
	if lm.cacheList == nil {
		lm.cacheList = list.New()
	}
	if e, ok := lm.cache[id]; ok && e != nil && e.table != nil {
		e.lastAccess = now
		lm.cacheList.MoveToFront(e.elem)
		e.table.Incr()
		lm.cacheMu.Unlock()
		tbl.Decr()
		return e.table, nil
	}
	e := &tableCacheEntry{id: id, table: tbl, lastAccess: now}
	e.elem = lm.cacheList.PushFront(e)
	lm.cache[id] = e
	tbl.Incr()
	lm.tableCacheEvictLocked(now)
	lm.cacheMu.Unlock()

	return tbl, nil
}

func (lm *levelManager) tableCacheEvictLocked(now int64) {
	if lm.cacheList == nil {
		return
	}
	for {
		back := lm.cacheList.Back()
		if back == nil {
			return
		}
		e, _ := back.Value.(*tableCacheEntry)
		if e == nil {
			lm.cacheList.Remove(back)
			continue
		}
		expired := now-e.lastAccess >= lm.tableCacheTTL
		over := lm.tableCacheMax > 0 && len(lm.cache) > lm.tableCacheMax
		if !expired && !over {
			return
		}
		lm.cacheList.Remove(back)
		delete(lm.cache, e.id)
		if e.table != nil {
			e.table.Decr()
		}
	}
}

func (lm *levelManager) cleanupTableCache() {
	lm.cleanupTableCacheAt(time.Now().Unix())
}

func (lm *levelManager) cleanupTableCacheAt(now int64) {
	lm.cacheMu.Lock()
	lm.tableCacheEvictLocked(now)
	lm.cacheMu.Unlock()
}

func (lm *levelManager) clearTableCache() {
	lm.cacheMu.Lock()
	if lm.cache != nil {
		for _, e := range lm.cache {
			if e != nil && e.table != nil {
				e.table.Decr()
			}
		}
	}
	lm.cache = nil
	lm.cacheList = nil
	lm.cacheMu.Unlock()
}
