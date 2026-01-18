package kv

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL represents a Write-Ahead Log
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	filePath string
}

// WALEntry represents a single entry in the WAL
type WALEntry struct {
	Op      byte   // 0 = Put, 1 = Delete
	KeyLen  uint32
	Key     []byte
	ValueLen uint32
	Value   []byte
}

const (
	OpPut    = 0
	OpDelete = 1
)

// NewWAL creates a new WAL file
func NewWAL(dataDir string) (*WAL, error) {
	walPath := filepath.Join(dataDir, "wal.log")
	
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:     file,
		writer:   bufio.NewWriter(file),
		filePath: walPath,
	}, nil
}

// Append writes an entry to the WAL
func (w *WAL) Append(op byte, key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Format: Op(1) + KeyLen(4) + Key + ValueLen(4) + Value
	if err := binary.Write(w.writer, binary.LittleEndian, op); err != nil {
		return err
	}

	keyLen := uint32(len(key))
	if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}

	valueLen := uint32(len(value))
	if err := binary.Write(w.writer, binary.LittleEndian, valueLen); err != nil {
		return err
	}
	if _, err := w.writer.Write(value); err != nil {
		return err
	}

	return w.writer.Flush()
}

// Replay reads all entries from WAL and applies them to memtable
func (w *WAL) Replay(memtable *MemTable) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensure any buffered data is flushed to disk before reading
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	// Open a separate read-only handle for replay without closing WAL's write handle
	file, err := os.Open(w.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL file, nothing to replay
		}
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		entry, err := readWALEntry(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if entry.Op == OpPut {
			memtable.Put(entry.Key, entry.Value)
		} else if entry.Op == OpDelete {
			memtable.Delete(entry.Key)
		}
	}

	return nil
}

// readWALEntry reads a single entry from the reader
func readWALEntry(reader *bufio.Reader) (*WALEntry, error) {
	var op byte
	if err := binary.Read(reader, binary.LittleEndian, &op); err != nil {
		return nil, err
	}

	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, err
	}

	var valueLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return nil, err
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, err
	}

	return &WALEntry{
		Op:       op,
		KeyLen:   keyLen,
		Key:      key,
		ValueLen: valueLen,
		Value:    value,
	}, nil
}

// Clear removes the WAL file (called after successful flush)
func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.writer.Flush()
	w.file.Close()

	if err := os.Remove(w.filePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Recreate WAL file
	file, err := os.OpenFile(w.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

