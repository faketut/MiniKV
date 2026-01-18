package kv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// LSMTree coordinates the LSM tree structure
type LSMTree struct {
	mu                sync.RWMutex
	dataDir           string
	memtable          *MemTable
	immutableMemtable *MemTable
	sstableMetas      []*SSTableMetadata // sorted by level, then by ID
	nextSSTableID     uint64
	compactor         *CompactionManager
	memtableMaxSize   int64
}

// NewLSMTree creates a new LSM tree
func NewLSMTree(dataDir string, memtableMaxSize int64) (*LSMTree, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	lsm := &LSMTree{
		dataDir:         dataDir,
		memtable:        NewMemTable(memtableMaxSize),
		sstableMetas:    make([]*SSTableMetadata, 0),
		nextSSTableID:   1,
		memtableMaxSize: memtableMaxSize,
	}

	lsm.compactor = NewCompactionManager(dataDir, lsm)

	// Load existing SSTables
	if err := lsm.loadSSTables(); err != nil {
		return nil, err
	}

	return lsm, nil
}

// loadSSTables scans the data directory for existing SSTables
func (lsm *LSMTree) loadSSTables() error {
	maxLevel := 10
	for level := 0; level < maxLevel; level++ {
		levelDir := filepath.Join(lsm.dataDir, fmt.Sprintf("level-%d", level))
		files, err := filepath.Glob(filepath.Join(levelDir, "sstable-*.dat"))
		if err != nil {
			continue
		}

		for _, file := range files {
			// Extract ID from filename
			var id uint64
			var levelNum int
			_, err := fmt.Sscanf(filepath.Base(file), "sstable-%d-%d.dat", &levelNum, &id)
			if err != nil {
				continue
			}

			// Read metadata
			fileInfo, err := os.Stat(file)
			if err != nil {
				continue
			}

			// Load index to get key range
			indexPath := file[:len(file)-4] + ".idx"
			index, err := loadIndex(indexPath)
			if err != nil {
				continue
			}

			var minKey, maxKey []byte
			if len(index) > 0 {
				keys := make([]string, 0, len(index))
				for k := range index {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				if len(keys) > 0 {
					minKey = []byte(keys[0])
					maxKey = []byte(keys[len(keys)-1])
				}
			}

			meta := &SSTableMetadata{
				Level:      level,
				ID:         id,
				MinKey:     minKey,
				MaxKey:     maxKey,
				EntryCount: len(index),
				FileSize:   fileInfo.Size(),
			}

			lsm.sstableMetas = append(lsm.sstableMetas, meta)

			if id >= lsm.nextSSTableID {
				lsm.nextSSTableID = id + 1
			}
		}
	}

	return nil
}

// FlushMemtable flushes the memtable to disk as an SSTable
func (lsm *LSMTree) FlushMemtable() error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	if lsm.immutableMemtable == nil {
		return nil // Nothing to flush
	}

	entries := lsm.immutableMemtable.GetAllEntries()
	if len(entries) == 0 {
		lsm.immutableMemtable = nil
		return nil
	}

	// Write to Level 0
	id := lsm.GetNextSSTableID()
	meta, err := WriteSSTable(lsm.dataDir, 0, id, entries)
	if err != nil {
		return err
	}

	lsm.sstableMetas = append(lsm.sstableMetas, meta)
	lsm.immutableMemtable = nil

	// Trigger compaction if needed
	go func() {
		if err := lsm.compactor.CompactAll(); err != nil {
			// Log error in production
			fmt.Printf("Compaction error: %v\n", err)
		}
	}()

	return nil
}

// Get searches for a key across memtable, immutable memtable, and SSTables
func (lsm *LSMTree) Get(key []byte) ([]byte, bool, error) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	// Check memtable
	if value, found := lsm.memtable.Get(key); found {
		return value, true, nil
	}

	// Check immutable memtable
	if lsm.immutableMemtable != nil {
		if value, found := lsm.immutableMemtable.Get(key); found {
			return value, true, nil
		}
	}

	// Check SSTables from newest to oldest (Level 0 first, then higher levels)
	// Sort by level (ascending) and ID (descending) to get newest first
	sortedMetas := make([]*SSTableMetadata, len(lsm.sstableMetas))
	copy(sortedMetas, lsm.sstableMetas)
	
	// Simple sort: level 0 first, then by ID descending within same level
	sort.Slice(sortedMetas, func(i, j int) bool {
		if sortedMetas[i].Level != sortedMetas[j].Level {
			return sortedMetas[i].Level < sortedMetas[j].Level
		}
		return sortedMetas[i].ID > sortedMetas[j].ID
	})

	for _, meta := range sortedMetas {
		// Check if key is in range
		if len(meta.MinKey) > 0 && bytes.Compare(key, meta.MinKey) < 0 {
			continue
		}
		if len(meta.MaxKey) > 0 && bytes.Compare(key, meta.MaxKey) > 0 {
			continue
		}

		filePath := filepath.Join(lsm.dataDir, fmt.Sprintf("level-%d", meta.Level),
			fmt.Sprintf("sstable-%d-%d.dat", meta.Level, meta.ID))
		
		value, found, err := ReadSSTable(filePath, key)
		if err != nil {
			return nil, false, err
		}
		if found {
			return value, true, nil
		}
	}

	return nil, false, nil
}

// CheckAndFlush checks if memtable should be flushed and flushes it
func (lsm *LSMTree) CheckAndFlush() error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	if lsm.memtable.ShouldFlush() && lsm.immutableMemtable == nil {
		lsm.immutableMemtable = lsm.memtable
		lsm.memtable = NewMemTable(lsm.memtableMaxSize)
		
		// Flush in background
		go func() {
			if err := lsm.FlushMemtable(); err != nil {
				fmt.Printf("Flush error: %v\n", err)
			}
		}()
	}

	return nil
}

// GetNextSSTableID returns the next SSTable ID
func (lsm *LSMTree) GetNextSSTableID() uint64 {
	return atomic.AddUint64(&lsm.nextSSTableID, 1) - 1
}

