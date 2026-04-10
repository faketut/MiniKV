

# MiniKV

> A lightweight, crash-safe, LSM-tree based key-value storage engine implemented in Go.

---

## Overview

MiniKV-Go is a simple key-value storage engine designed to demonstrate core storage system concepts such as **Write-Ahead Logging (WAL)**, **LSM Tree**, **SSTable**, and **Compaction**.

The project focuses on:

* High write throughput using sequential disk I/O
* Crash-safe persistence
* Clear and modular system design

This project is built for learning and interview preparation purposes.

---

## Features

* Key-Value API: `Put`, `Get`, `Delete`
* In-memory MemTable (SkipList / ordered map)
* Write-Ahead Logging (WAL) for crash recovery
* SSTable-based persistent storage
* LSM Tree architecture with multi-level storage
* Background compaction
* Concurrent-safe access (RWMutex)
* Unit tests and basic benchmarks

---

## Architecture

```
          ┌──────────┐
          │  Client  │
          └────┬─────┘
               │
          ┌────▼─────┐
          │  KV API  │
          └────┬─────┘
   Put / Get / Delete
               │
        ┌──────▼──────┐
        │  MemTable   │  (in-memory, ordered)
        └──────┬──────┘
               │
        ┌──────▼──────┐
        │     WAL     │  (append-only log)
        └─────────────┘
               │
   MemTable full → Immutable
               │
        ┌──────▼──────┐
        │  SSTable    │  (on disk)
        │  Level 0    │
        └──────┬──────┘
               │
        ┌──────▼──────┐
        │ Compaction  │
        └──────┬──────┘
               │
        ┌──────▼──────┐
        │ SSTable L1+ │
        └─────────────┘
```

---

## Data Flow

### Write Path (Put / Delete)

1. Append operation to WAL
2. Insert key-value into MemTable
3. When MemTable reaches size threshold:

   * Convert to Immutable MemTable
   * Flush to disk as SSTable (Level 0)
4. Trigger background compaction if needed

### Read Path (Get)

1. Check MemTable
2. Check Immutable MemTable
3. Search SSTables from newest to oldest
4. Return value or not found

---

## Directory Structure

```
mini-kv-go/
├── cmd/
│   └── cli.go           # Simple CLI for testing
├── kv/
│   ├── kv.go            # Public KV API
│   ├── memtable.go      # In-memory table
│   ├── wal.go           # Write-Ahead Log
│   ├── sstable.go       # SSTable implementation
│   ├── lsm.go           # LSM Tree coordinator
│   └── compaction.go    # Compaction logic
├── tests/
│   └── kv_test.go
├── go.mod
└── README.md
```

---

## Getting Started

### Build

```bash
go build ./...
```

### Run Demo

```bash
go run cmd/cli.go
```

Example:

```bash
> put k1 v1
> get k1
v1
> delete k1
> get k1
not found
```

---

## Crash Recovery

* All write operations are first appended to WAL
* On startup:

  * Replay WAL to rebuild MemTable
  * Ensure no committed data is lost after crash

---

## Compaction Strategy

* Level-based compaction
* Merge overlapping SSTables
* Remove obsolete keys and tombstones
* Reduce read amplification and disk usage

---

## Testing

```bash
go test ./...
```

### Benchmark (optional)

```bash
go test -bench=. ./tests
```

---

## Limitations

* Single-node only (no replication)
* No TTL or transactions
* No compression
* No distributed consensus (e.g., Raft)

---

## Future Work

* Bloom filter for faster reads
* Configurable compaction strategy
* Snapshot support
* Distributed replication (Raft)
* gRPC interface
