package sstable

import (
	memtable "stinky-db/db/MemTable"
	"strings"
	"sync"
)

const (
	sparseIdxSize = 2
)

type Data struct {
	Key   string `json:"key"`
	Value string `json:"Value"`
}

type Table struct {
	Data           []Data                 `json:"data"`
	SparseIndex    map[string]SparseIndex `json:"sparse_index"`
	FileIndex      FileIndex              `json:"file_index"`
	FilePath       string
	MemSparseIndex map[string]int
	mu             *sync.Mutex
}

type SparseIndex struct {
	Len   int `json:"len"`
	Start int `json:"start"`
}

type FileIndex struct {
	Version    int `json:"version"`
	DataStart  int `json:"data_start"`
	DataLen    int `json:"data_len"`
	IndexStart int `json:"index_start"`
	IndexLen   int `json:"index_len"`
	PartSize   int `json:"part_size"`
}

// write order should be
// Data
// Sparse Index
// File Index -- should be fixed size in the file so that we can always index for this

func (t *Table) writeToFile(filePath string) error {

	return nil
}

func constructTableFromFile(filePath string) (Table, error) {

	return Table{}, nil
}

func newTable(filePath string) Table {
	return Table{
		FilePath: filePath,
		mu:       &sync.Mutex{},
	}
}

func GenerateFromTree(mem *memtable.RBTree, filePath string) Table {
	table := newTable(filePath)
	orderedNodes := mem.Nodes()
	data := []Data{}
	memSparseIndex := map[string]int{}
	for i, node := range orderedNodes {
		kv := Data{Key: node.Key, Value: node.Value}
		data = append(data, kv)
		if i%sparseIdxSize == 0 {
			memSparseIndex[kv.Key] = i
		}
	}

	table.Data = data
	table.MemSparseIndex = memSparseIndex
	return table
}

func (t *Table) Get(key string) string {
	if t.MemSparseIndex == nil {
		return ""
	}

	if idx, ok := t.MemSparseIndex[key]; ok {
		return t.Data[idx].Value
	}

	prevKeyIdx := 0
	finalKeyIdx := 0
	for memKey := range t.MemSparseIndex {
		higherOrBigger := strings.Compare(memKey, key)
		if higherOrBigger == 1 {
			finalKeyIdx = t.MemSparseIndex[memKey]
		} else {
			prevKeyIdx = t.MemSparseIndex[memKey]
		}
	}

	for idx := prevKeyIdx; idx < finalKeyIdx; idx += 1 {
		if t.Data[idx].Key == key {
			return t.Data[idx].Value
		}
	}

	return ""
}
