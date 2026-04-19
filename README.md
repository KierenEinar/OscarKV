# OscarKV

```text
  OOO   SSS   CCC   AAA  RRRR  K  K  V   V
 O   O S   S C   C A   A R   R K K   V   V
 O   O  SSS  C     AAAAA RRRR  KK     V V
 O   O     S C   C A   A R  R  K K     V
  OOO  SSSS   CCC  A   A R   R K  K    V
```

I am delighted to announce that OscarKV is now available! However, it is not well tested yet, so this is still a beta version. 

Why named it by OscarKV? 
I named it after my lovely cat, who has been by my side for seven years!

OscarKV is a Go-native key-value storage engine built around an LSM-Tree. It focuses on a readable, testable implementation of the classic storage pipeline (WAL → Memtable → SSTables → Compaction).


## Status
- Embedded single-node engine
- API: `Open`, `Set`, `Get`, `Close`
- Go toolchain: `go 1.25.0`

## Features (Current)
- Write path
  - WAL with segmented files and rotation
  - Memtable backed by a skiplist
  - Batched writes through a commit worker
- Read path
  - Point lookup through Memtable → Immutable Memtables → SSTables (levels)
  - Table cache with LRU + TTL eviction
  - Iterator contract: `SeekToFirst/Next` return `io.EOF` when exhausted; `Seek(k)` returns first entry `>= k`
- SSTable format
  - Block-based layout with prefix-compressed keys inside blocks
  - CRC32C (Castagnoli) checksums for data integrity
  - Cross-platform VFS layer (mmap-backed implementations per OS)
- LSM management
  - Checkpoint metadata persisted via protobuf (`meta/*.ckpt`)
  - Compaction with sub-compaction (range partitioning) and deterministic lock ordering when applying results
  - “Keep latest per key” behavior during compaction (dedupe by `cf + userKey`)

## Architecture
High-level data flow:

```
Client
  |
  v
DB (commit worker)
  |
  +--> WAL (append batch, rotate segments)
  |
  v
Memtable (skiplist)  ----rotate---->  Immutable Memtables  ----flush---->  L0 SSTables
     |                                                         |
     +-------------------- read -------------------------------+
                                                               v
                                                        LevelManager
                                                               |
                                                               v
                                                Compaction (sub-compaction)
                                                               |
                                                               v
                                                     Lower levels SSTables
```

### Key Model
OscarKV stores and sorts **internal keys**:

```
[ cfLen (1B) ][ cf bytes ][ userKey bytes ][ version (8B, big-endian) ]
```

Compaction can treat `cfLen+cf+userKey` as the “prefix” and keep only the latest version per prefix.

## Repository Layout
- `db.go`, `db_write.go`: user-facing DB wrapper and commit worker
- `lsm/`: LSM tree, checkpoints, levels, compaction
- `sstable/`: SSTable format, builder, iterators
- `wal/`: WAL segments, append/rotate, iterator
- `iterator/`: merge iterator and sub-compaction iterator
- `utils/`: skiplist, pools, key comparison helpers
- `vfs/`: cross-platform file & mmap abstractions
- `pb/`: protobuf definitions for checkpoints and table metadata

## Quick Start
Run all tests:

```bash
go test ./...
```

Minimal usage (simplified from tests):

```go
package main

import (
  "fmt"
)

func main() {
  opt := &Option{
    RootDir:                  "./workdir",
    CommitBuffer:             8,
    WriteBatchCountThreshold: 16,
    WriteBatchSizeThreshold:  1 << 10, // 1k
    MemoryLimitPerMemtable:   8 << 20, // 8MB
    MaxImmutableMemtable:     16,
    MaxLevelPerMemtable:      32,
    StrictMode:               true,
    RandFactorPerMemtable:    0.25,
    SlowDownDurationMs:       0,
    PreferedLevel:            8,
  }

  db, err := Open(opt)
  if err != nil {
    panic(err)
  }
  defer db.Close()

  if err := db.Set([]byte("hello"), []byte("world")); err != nil {
    panic(err)
  }
  v, err := db.Get([]byte("hello"))
  if err != nil {
    panic(err)
  }
  fmt.Printf("value=%s\n", string(v))
}
```

## Roadmap
- Storage Engine
  - Value Log (VLog): separate large values from LSM to reduce write amplification and improve compaction efficiency
  - Hot-Key Path: fast path for frequently accessed keys (adaptive caching and read-optimization hooks)
  - Ingest-Aware Compaction: support L0/Ln ingest pipelines (bulk load) and compaction scheduling around ingest buffers
- Transactions + MVCC: snapshot reads, conflict detection, and write intents on top of internal keys/versioning  
- Distributed & Consistency
  - Raft Replication: turn OscarKV into a distributed KV by adding a Raft-based replication layer without changing the storage core
- Observability & Operations
  - Monitoring Dashboard: metrics endpoint + dashboard panels for WAL/LSM/table-cache/compaction stats
  - Debug/Forensics Tools: offline inspection for checkpoints, tables, and VLog segments
