package kv

import (
	"bytes"
	"math/rand"
	"sync"
)

// Entry represents a key-value entry in the memtable
type Entry struct {
	Key   []byte
	Value []byte
	Tombstone bool // true if this is a delete marker
}

// SkipListNode represents a node in the skip list
type SkipListNode struct {
	Entry  *Entry
	Next   []*SkipListNode
}

// MemTable is an in-memory ordered key-value store using SkipList
type MemTable struct {
	mu       sync.RWMutex
	head     *SkipListNode
	maxLevel int
	size     int64 // approximate size in bytes
	maxSize  int64 // threshold to trigger flush
}

const (
	maxLevel = 16
	p        = 0.5 // probability for skip list level
)

// NewMemTable creates a new MemTable
func NewMemTable(maxSize int64) *MemTable {
	head := &SkipListNode{
		Entry: nil,
		Next:  make([]*SkipListNode, maxLevel),
	}
	return &MemTable{
		head:     head,
		maxLevel: maxLevel,
		size:     0,
		maxSize:  maxSize,
	}
}

// randomLevel generates a random level for skip list insertion
func randomLevel() int {
	level := 1
	for rand.Float32() < p && level < maxLevel {
		level++
	}
	return level
}

// Put inserts or updates a key-value pair
func (mt *MemTable) Put(key, value []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Calculate size increase
	keySize := int64(len(key))
	valueSize := int64(len(value))
	entrySize := keySize + valueSize + 8 // +8 for overhead

	// Check if key exists
	prev := make([]*SkipListNode, maxLevel)
	current := mt.head
	for i := mt.maxLevel - 1; i >= 0; i-- {
		for current.Next[i] != nil && bytes.Compare(current.Next[i].Entry.Key, key) < 0 {
			current = current.Next[i]
		}
		prev[i] = current
	}

	// If key exists, update it
	if current.Next[0] != nil && bytes.Equal(current.Next[0].Entry.Key, key) {
		oldValueSize := int64(len(current.Next[0].Entry.Value))
		current.Next[0].Entry.Value = value
		current.Next[0].Entry.Tombstone = false
		mt.size += valueSize - oldValueSize
		return
	}

	// Insert new node
	level := randomLevel()
	newNode := &SkipListNode{
		Entry: &Entry{
			Key:       make([]byte, len(key)),
			Value:     make([]byte, len(value)),
			Tombstone: false,
		},
		Next: make([]*SkipListNode, level),
	}
	copy(newNode.Entry.Key, key)
	copy(newNode.Entry.Value, value)

	for i := 0; i < level; i++ {
		newNode.Next[i] = prev[i].Next[i]
		prev[i].Next[i] = newNode
	}

	mt.size += entrySize
}

// Get retrieves a value by key
func (mt *MemTable) Get(key []byte) ([]byte, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	current := mt.head
	for i := mt.maxLevel - 1; i >= 0; i-- {
		for current.Next[i] != nil && bytes.Compare(current.Next[i].Entry.Key, key) < 0 {
			current = current.Next[i]
		}
	}

	if current.Next[0] != nil && bytes.Equal(current.Next[0].Entry.Key, key) {
		entry := current.Next[0].Entry
		if entry.Tombstone {
			return nil, false
		}
		value := make([]byte, len(entry.Value))
		copy(value, entry.Value)
		return value, true
	}

	return nil, false
}

// Delete marks a key as deleted
func (mt *MemTable) Delete(key []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	prev := make([]*SkipListNode, maxLevel)
	current := mt.head
	for i := mt.maxLevel - 1; i >= 0; i-- {
		for current.Next[i] != nil && bytes.Compare(current.Next[i].Entry.Key, key) < 0 {
			current = current.Next[i]
		}
		prev[i] = current
	}

	if current.Next[0] != nil && bytes.Equal(current.Next[0].Entry.Key, key) {
		// Mark as tombstone
		oldValueSize := int64(len(current.Next[0].Entry.Value))
		current.Next[0].Entry.Tombstone = true
		current.Next[0].Entry.Value = nil
		mt.size -= oldValueSize
	}
}

// Size returns the approximate size in bytes
func (mt *MemTable) Size() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// ShouldFlush checks if memtable should be flushed
func (mt *MemTable) ShouldFlush() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size >= mt.maxSize
}

// GetAllEntries returns all entries for flushing
func (mt *MemTable) GetAllEntries() []*Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	var entries []*Entry
	current := mt.head.Next[0]
	for current != nil {
		entry := &Entry{
			Key:       make([]byte, len(current.Entry.Key)),
			Value:     make([]byte, len(current.Entry.Value)),
			Tombstone: current.Entry.Tombstone,
		}
		copy(entry.Key, current.Entry.Key)
		copy(entry.Value, current.Entry.Value)
		entries = append(entries, entry)
		current = current.Next[0]
	}
	return entries
}

