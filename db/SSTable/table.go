package sstable

import (
	"encoding/json"
	"fmt"
	"os"
	memtable "stinky-db/db/MemTable"
	"strings"
	"sync"
	"time"
)

const (
	sparseIdxSize = 4
)

type Data struct {
	Key     string    `json:"key"`
	Value   string    `json:"Value"`
	Written time.Time `json:"Written"`
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
	//Version    int `json:"version"`
	DataStart  int `json:"data_start"`
	DataLen    int `json:"data_len"`
	IndexStart int `json:"index_start"`
	IndexLen   int `json:"index_len"`
}

// write order should be
// Data
// Sparse Index
// File Index -- should be fixed size in the file so that we can always index for this

func (t *Table) writeToFile() error {
	fileSparseIndex := map[string]SparseIndex{}
	writeData := []byte{}
	for i, keyVal := range t.Data {
		bytes, err := json.Marshal(keyVal)
		if err != nil {
			return err
		}
		if i%sparseIdxSize == 0 {
			sparseIdx := SparseIndex{
				Len:   len(bytes),
				Start: len(writeData),
			}

			fileSparseIndex[keyVal.Key] = sparseIdx
		}
		fmt.Println(string(bytes))
		writeData = append(writeData, bytes...)
	}

	fileSparseBytes, err := json.Marshal(fileSparseIndex)
	if err != nil {
		return err
	}

	fileIdx := FileIndex{
		DataStart:  0,
		DataLen:    len(writeData),
		IndexStart: len(writeData),
		IndexLen:   len(fileSparseBytes),
	}

	fileIdxBytes, err := json.Marshal(fileIdx)
	if err != nil {
		return err
	}

	file, err := os.Create(t.FilePath)
	defer file.Close()
	if err != nil {
		return err
	}

	_, err = file.Write(writeData)
	if err != nil {

		return err
	}

	_, err = file.Write(fileSparseBytes)
	if err != nil {
		return err
	}

	_, err = file.Write(fileIdxBytes)
	if err != nil {
		return err
	}

	return nil
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
		kv := Data{Key: node.Key, Value: node.Value, Written: time.Now()}
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
		//todo
		return ""
	}

	return t.getFromMemorySSTable(key)
}

func (t *Table) getFromMemorySSTable(key string) string {
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
	if finalKeyIdx == 0 {
		finalKeyIdx = len(t.Data) - 1
	}

	for idx := prevKeyIdx; idx <= finalKeyIdx; idx += 1 {
		if t.Data[idx].Key == key {
			return t.Data[idx].Value
		}
	}

	return ""
}
