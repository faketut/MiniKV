package kv

import (
	"sync"
)

// KV is the main key-value store interface
type KV struct {
	mu     sync.RWMutex
	lsm    *LSMTree
	wal    *WAL
	dataDir string
}

// Config holds configuration for the KV store
type Config struct {
	DataDir         string
	MemtableMaxSize int64 // in bytes
}

// DefaultConfig returns a default configuration
func DefaultConfig(dataDir string) *Config {
	return &Config{
		DataDir:         dataDir,
		MemtableMaxSize: 10 * 1024 * 1024, // 10MB default
	}
}

// NewKV creates a new KV store instance
func NewKV(config *Config) (*KV, error) {
	if config == nil {
		config = DefaultConfig("./data")
	}

	// Create LSM tree
	lsm, err := NewLSMTree(config.DataDir, config.MemtableMaxSize)
	if err != nil {
		return nil, err
	}

	// Create WAL
	wal, err := NewWAL(config.DataDir)
	if err != nil {
		return nil, err
	}

	kv := &KV{
		lsm:     lsm,
		wal:     wal,
		dataDir: config.DataDir,
	}

	// Replay WAL on startup
	if err := wal.Replay(lsm.memtable); err != nil {
		return nil, err
	}

	return kv, nil
}

// Put stores a key-value pair
func (kv *KV) Put(key, value []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Write to WAL first
	if err := kv.wal.Append(OpPut, key, value); err != nil {
		return err
	}

	// Write to memtable
	kv.lsm.memtable.Put(key, value)

	// Check if memtable should be flushed
	if err := kv.lsm.CheckAndFlush(); err != nil {
		return err
	}

	return nil
}

// Get retrieves a value by key
func (kv *KV) Get(key []byte) ([]byte, bool, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.lsm.Get(key)
}

// Delete removes a key (marks as tombstone)
func (kv *KV) Delete(key []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Write delete to WAL
	if err := kv.wal.Append(OpDelete, key, nil); err != nil {
		return err
	}

	// Mark as deleted in memtable
	kv.lsm.memtable.Delete(key)

	// Check if memtable should be flushed
	if err := kv.lsm.CheckAndFlush(); err != nil {
		return err
	}

	return nil
}

// Close closes the KV store and flushes pending writes
func (kv *KV) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Flush memtable if needed
	if kv.lsm.immutableMemtable != nil {
		if err := kv.lsm.FlushMemtable(); err != nil {
			return err
		}
	}

	// Close WAL
	return kv.wal.Close()
}

