package preprocessing

import (
	"container/list"
	"fmt"
	"sync"
)

// LSMNode represents a node in the LSM tree
type LSMNode struct {
	Key   string
	Value interface{}
}

// LSMTree implements a Log-Structured Merge Tree
type LSMTree struct {
	memoryTable   map[string]interface{} // MemTable
	sortedFiles   []*list.List           // SSTables
	maxMemorySize int
	currentSize   int
	mutex         sync.RWMutex
}

// NewLSMTree creates a new LSM tree with specified memory size limit
func NewLSMTree(maxMemorySize int) *LSMTree {
	return &LSMTree{
		memoryTable:   make(map[string]interface{}),
		sortedFiles:   make([]*list.List, 0),
		maxMemorySize: maxMemorySize,
		currentSize:   0,
	}
}

// Put adds or updates a key-value pair in the LSM tree
func (lsm *LSMTree) Put(key string, value interface{}) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	lsm.memoryTable[key] = value
	lsm.currentSize++

	if lsm.currentSize >= lsm.maxMemorySize {
		lsm.flushMemoryTable()
	}

	return nil
}

// Get retrieves a value by key from the LSM tree
func (lsm *LSMTree) Get(key string) (interface{}, error) {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	if value, exists := lsm.memoryTable[key]; exists {
		return value, nil
	}

	for i := len(lsm.sortedFiles) - 1; i >= 0; i-- {
		file := lsm.sortedFiles[i]
		for e := file.Front(); e != nil; e = e.Next() {
			node := e.Value.(LSMNode)
			if node.Key == key {
				return node.Value, nil
			}
		}
	}

	return nil, fmt.Errorf("key '%s' not found", key)
}

// Delete marks a key for deletion in the LSM tree
func (lsm *LSMTree) Delete(key string) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	lsm.memoryTable[key] = nil
	lsm.currentSize++

	if lsm.currentSize >= lsm.maxMemorySize {
		lsm.flushMemoryTable()
	}

	return nil
}

// flushMemoryTable moves the in-memory table to a sorted file
func (lsm *LSMTree) flushMemoryTable() {
	sortedFile := list.New()

	for k, v := range lsm.memoryTable {
		sortedFile.PushBack(LSMNode{Key: k, Value: v})
	}

	lsm.sortedFiles = append(lsm.sortedFiles, sortedFile)

	lsm.memoryTable = make(map[string]interface{})
	lsm.currentSize = 0
}

// Compact merges sorted files to optimize storage
func (lsm *LSMTree) Compact() {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if len(lsm.sortedFiles) > 1 {
		consolidated := make(map[string]interface{})

		for _, file := range lsm.sortedFiles {
			for e := file.Front(); e != nil; e = e.Next() {
				node := e.Value.(LSMNode)
				if node.Value != nil {
					consolidated[node.Key] = node.Value
				} else {
					if _, exists := lsm.memoryTable[node.Key]; !exists {
						delete(consolidated, node.Key)
					}
				}
			}
		}

		compactedFile := list.New()
		for k, v := range consolidated {
			compactedFile.PushBack(LSMNode{Key: k, Value: v})
		}

		lsm.sortedFiles = []*list.List{compactedFile}
	}
}

// Size returns the number of key-value pairs in the LSM tree
func (lsm *LSMTree) Size() int {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	count := len(lsm.memoryTable)
	for _, file := range lsm.sortedFiles {
		count += file.Len()
	}
	return count
}

// Keys returns all keys in the LSM tree
func (lsm *LSMTree) Keys() []string {
	lsm.mutex.RLock()
	defer lsm.mutex.RUnlock()

	keys := make([]string, 0)

	for k := range lsm.memoryTable {
		keys = append(keys, k)
	}

	for _, file := range lsm.sortedFiles {
		for e := file.Front(); e != nil; e = e.Next() {
			node := e.Value.(LSMNode)
			found := false
			for _, existingKey := range keys {
				if existingKey == node.Key {
					found = true
					break
				}
			}
			if !found {
				keys = append(keys, node.Key)
			}
		}
	}

	return keys
}

// BatchPut adds multiple key-value pairs efficiently
func (lsm *LSMTree) BatchPut(pairs map[string]interface{}) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	for key, value := range pairs {
		lsm.memoryTable[key] = value
		lsm.currentSize++
	}

	if lsm.currentSize >= lsm.maxMemorySize {
		lsm.flushMemoryTable()
	}

	return nil
}