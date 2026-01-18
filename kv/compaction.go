package kv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// CompactionManager handles background compaction
type CompactionManager struct {
	dataDir string
	lsm     *LSMTree
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(dataDir string, lsm *LSMTree) *CompactionManager {
	return &CompactionManager{
		dataDir: dataDir,
		lsm:     lsm,
	}
}

// CompactLevel performs compaction for a specific level
func (cm *CompactionManager) CompactLevel(level int) error {
	levelDir := filepath.Join(cm.dataDir, fmt.Sprintf("level-%d", level))
	
	// Get all SSTables in this level
	files, err := filepath.Glob(filepath.Join(levelDir, "sstable-*.dat"))
	if err != nil {
		return err
	}

	if len(files) < 2 {
		return nil // Need at least 2 files to compact
	}

	// Read all entries from all SSTables
	var allEntries []*Entry
	for _, file := range files {
		entries, err := ReadAllSSTableEntries(file)
		if err != nil {
			return err
		}
		allEntries = append(allEntries, entries...)
	}

	// Sort by key
	sort.Slice(allEntries, func(i, j int) bool {
		return bytes.Compare(allEntries[i].Key, allEntries[j].Key) < 0
	})

	// Deduplicate (keep latest)
	deduped := deduplicateEntries(allEntries)

	// Remove tombstones
	var liveEntries []*Entry
	for _, entry := range deduped {
		if !entry.Tombstone {
			liveEntries = append(liveEntries, entry)
		}
	}

	if len(liveEntries) == 0 {
		// All entries are tombstones, just delete the files
		for _, file := range files {
			os.Remove(file)
			os.Remove(file[:len(file)-4] + ".idx")
		}
		return nil
	}

	// Write to next level
	nextLevel := level + 1
	nextID := cm.lsm.GetNextSSTableID()
	_, err = WriteSSTable(cm.dataDir, nextLevel, nextID, liveEntries)
	if err != nil {
		return err
	}

	// Delete old files
	for _, file := range files {
		os.Remove(file)
		os.Remove(file[:len(file)-4] + ".idx")
	}

	return nil
}

// ShouldCompact checks if a level should be compacted
func (cm *CompactionManager) ShouldCompact(level int) bool {
	levelDir := filepath.Join(cm.dataDir, fmt.Sprintf("level-%d", level))
	files, err := filepath.Glob(filepath.Join(levelDir, "sstable-*.dat"))
	if err != nil {
		return false
	}

	// Simple strategy: compact if level 0 has more than 2 files
	// or if level > 0 has more than 10 files
	if level == 0 {
		return len(files) >= 2
	}
	return len(files) >= 10
}

// CompactAll performs compaction on all levels that need it
func (cm *CompactionManager) CompactAll() error {
	for level := 0; level < 10; level++ {
		if cm.ShouldCompact(level) {
			if err := cm.CompactLevel(level); err != nil {
				return err
			}
		}
	}
	return nil
}

