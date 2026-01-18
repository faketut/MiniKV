package tests

import (
	"os"
	"path/filepath"
	"testing"

	"minikv/kv"
)

func TestPutAndGet(t *testing.T) {
	dataDir := filepath.Join(os.TempDir(), "minikv_test_put_get")
	defer os.RemoveAll(dataDir)

	config := kv.DefaultConfig(dataDir)
	db, err := kv.NewKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer db.Close()

	// Test Put
	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	value, found, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Key not found")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", string(value))
	}
}

func TestDelete(t *testing.T) {
	dataDir := filepath.Join(os.TempDir(), "minikv_test_delete")
	defer os.RemoveAll(dataDir)

	config := kv.DefaultConfig(dataDir)
	db, err := kv.NewKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer db.Close()

	// Put a key
	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete the key
	err = db.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's deleted
	_, found, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Fatal("Key should be deleted but was found")
	}
}

func TestUpdate(t *testing.T) {
	dataDir := filepath.Join(os.TempDir(), "minikv_test_update")
	defer os.RemoveAll(dataDir)

	config := kv.DefaultConfig(dataDir)
	db, err := kv.NewKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer db.Close()

	// Put initial value
	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Update value
	err = db.Put([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify update
	value, found, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Key not found")
	}
	if string(value) != "value2" {
		t.Fatalf("Expected 'value2', got '%s'", string(value))
	}
}

func TestMultipleKeys(t *testing.T) {
	dataDir := filepath.Join(os.TempDir(), "minikv_test_multiple")
	defer os.RemoveAll(dataDir)

	config := kv.DefaultConfig(dataDir)
	db, err := kv.NewKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer db.Close()

	// Put multiple keys
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		err = db.Put([]byte(key), []byte(values[i]))
		if err != nil {
			t.Fatalf("Put failed for %s: %v", key, err)
		}
	}

	// Get all keys
	for i, key := range keys {
		value, found, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %s not found", key)
		}
		if string(value) != values[i] {
			t.Fatalf("Expected '%s' for %s, got '%s'", values[i], key, string(value))
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	dataDir := filepath.Join(os.TempDir(), "minikv_test_recovery")
	defer os.RemoveAll(dataDir)

	// Create first instance and write data
	config := kv.DefaultConfig(dataDir)
	db1, err := kv.NewKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}

	err = db1.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = db1.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	db1.Close()

	// Create second instance (simulating recovery)
	db2, err := kv.NewKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer db2.Close()

	// Verify data is recovered
	value, found, err := db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Key1 not found after recovery")
	}
	if string(value) != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", string(value))
	}

	value, found, err = db2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Key2 not found after recovery")
	}
	if string(value) != "value2" {
		t.Fatalf("Expected 'value2', got '%s'", string(value))
	}
}

func BenchmarkPut(b *testing.B) {
	dataDir := filepath.Join(os.TempDir(), "minikv_bench_put")
	defer os.RemoveAll(dataDir)

	config := kv.DefaultConfig(dataDir)
	db, err := kv.NewKV(config)
	if err != nil {
		b.Fatalf("Failed to create KV store: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		value := []byte("value")
		err := db.Put(key, value)
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	dataDir := filepath.Join(os.TempDir(), "minikv_bench_get")
	defer os.RemoveAll(dataDir)

	config := kv.DefaultConfig(dataDir)
	db, err := kv.NewKV(config)
	if err != nil {
		b.Fatalf("Failed to create KV store: %v", err)
	}
	defer db.Close()

	// Pre-populate
	key := []byte("testkey")
	value := []byte("testvalue")
	err = db.Put(key, value)
	if err != nil {
		b.Fatalf("Put failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := db.Get(key)
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

