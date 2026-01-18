package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// SSTable represents a Sorted String Table on disk
type SSTable struct {
	filePath string
	level    int
	id       uint64
}

// SSTableMetadata stores metadata about an SSTable
type SSTableMetadata struct {
	Level      int
	ID         uint64
	MinKey     []byte
	MaxKey     []byte
	EntryCount int
	FileSize   int64
}

// WriteSSTable writes entries to disk as an SSTable
func WriteSSTable(dataDir string, level int, id uint64, entries []*Entry) (*SSTableMetadata, error) {
	// Create level directory if needed
	levelDir := filepath.Join(dataDir, fmt.Sprintf("level-%d", level))
	if err := os.MkdirAll(levelDir, 0755); err != nil {
		return nil, err
	}

	// Sort entries by key
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	// Remove duplicates (keep latest)
	deduped := deduplicateEntries(entries)

	// Write SSTable file
	fileName := fmt.Sprintf("sstable-%d-%d.dat", level, id)
	filePath := filepath.Join(levelDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Write index: offsets for each key
	index := make(map[string]int64)
	var offset int64 = 0

	// Write entries
	for _, entry := range deduped {
		// Skip tombstones when writing to disk (they're handled during compaction)
		if entry.Tombstone {
			continue
		}

		keyStr := string(entry.Key)
		index[keyStr] = offset

		// Write entry: KeyLen(4) + Key + ValueLen(4) + Value
		keyLen := uint32(len(entry.Key))
		if err := binary.Write(writer, binary.LittleEndian, keyLen); err != nil {
			return nil, err
		}
		if _, err := writer.Write(entry.Key); err != nil {
			return nil, err
		}

		valueLen := uint32(len(entry.Value))
		if err := binary.Write(writer, binary.LittleEndian, valueLen); err != nil {
			return nil, err
		}
		if _, err := writer.Write(entry.Value); err != nil {
			return nil, err
		}

		offset += int64(4 + len(entry.Key) + 4 + len(entry.Value))
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Determine min/max keys
	var minKey, maxKey []byte
	if len(deduped) > 0 {
		minKey = deduped[0].Key
		maxKey = deduped[len(deduped)-1].Key
	}

	// Write index file
	indexPath := filepath.Join(levelDir, fmt.Sprintf("sstable-%d-%d.idx", level, id))
	if err := writeIndex(indexPath, index); err != nil {
		return nil, err
	}

	return &SSTableMetadata{
		Level:      level,
		ID:         id,
		MinKey:     minKey,
		MaxKey:     maxKey,
		EntryCount: len(deduped),
		FileSize:   fileInfo.Size(),
	}, nil
}

// deduplicateEntries removes duplicate keys, keeping the latest value
func deduplicateEntries(entries []*Entry) []*Entry {
	if len(entries) == 0 {
		return entries
	}

	seen := make(map[string]*Entry)
	for _, entry := range entries {
		keyStr := string(entry.Key)
		seen[keyStr] = entry
	}

	result := make([]*Entry, 0, len(seen))
	for _, entry := range seen {
		result = append(result, entry)
	}

	// Sort again after deduplication
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].Key, result[j].Key) < 0
	})

	return result
}

// writeIndex writes the index to disk
func writeIndex(indexPath string, index map[string]int64) error {
	file, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Write number of entries
	count := uint32(len(index))
	if err := binary.Write(writer, binary.LittleEndian, count); err != nil {
		return err
	}

	// Write each index entry: KeyLen(4) + Key + Offset(8)
	for key, offset := range index {
		keyBytes := []byte(key)
		keyLen := uint32(len(keyBytes))
		if err := binary.Write(writer, binary.LittleEndian, keyLen); err != nil {
			return err
		}
		if _, err := writer.Write(keyBytes); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.LittleEndian, offset); err != nil {
			return err
		}
	}

	return writer.Flush()
}

// ReadSSTable reads a value from an SSTable
func ReadSSTable(filePath string, key []byte) ([]byte, bool, error) {
	// Load index
	indexPath := filePath[:len(filePath)-4] + ".idx"
	index, err := loadIndex(indexPath)
	if err != nil {
		return nil, false, err
	}

	keyStr := string(key)
	offset, exists := index[keyStr]
	if !exists {
		return nil, false, nil
	}

	// Read value at offset
	file, err := os.Open(filePath)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, false, err
	}

	reader := bufio.NewReader(file)

	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return nil, false, err
	}

	readKey := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, readKey); err != nil {
		return nil, false, err
	}

	if !bytes.Equal(readKey, key) {
		return nil, false, nil
	}

	var valueLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return nil, false, err
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, false, err
	}

	return value, true, nil
}

// loadIndex loads the index from disk
func loadIndex(indexPath string) (map[string]int64, error) {
	file, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	index := make(map[string]int64, count)
	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, err
		}

		var offset int64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}

		index[string(keyBytes)] = offset
	}

	return index, nil
}

// ReadAllSSTableEntries reads all entries from an SSTable
func ReadAllSSTableEntries(filePath string) ([]*Entry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entries []*Entry

	for {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
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

		entries = append(entries, &Entry{
			Key:       key,
			Value:     value,
			Tombstone: false,
		})
	}

	return entries, nil
}

