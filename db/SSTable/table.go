package sstable

import (
	"encoding/json"
	"os"
	memtable "stinky-db/db/MemTable"
	"strings"
	"sync"
	"time"
)

const (
	sparseIdxSize = 999
)

type Data struct {
	Key     string    `json:"key"`
	Value   string    `json:"value"`
	Written time.Time `json:"written"`
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
		writeData = append(writeData, bytes...)
	}
	t.SparseIndex = fileSparseIndex

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
	t.FileIndex = fileIdx

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
		return nil
	}

	_, err = file.Write(fileIdxBytes)
	if err != nil {
		return err
	}

	t.MemSparseIndex = nil

	return nil
}

func (t *Table) writeData(data []byte, file *os.File) error {
	_, err := file.Write(data)
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

func (t *Table) Get(key string) (string, error) {
	if t.MemSparseIndex == nil {
		return t.readFromDisc(key)
	}

	return t.getFromMemorySSTable(key)
}

func (t *Table) readFromDisc(key string) (string, error) {
	file, err := os.Open(t.FilePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	if index, ok := t.SparseIndex[key]; ok {
		data := Data{}
		bytes := make([]byte, index.Len)
		_, err = file.ReadAt(bytes, int64(index.Start))

		err = json.Unmarshal(bytes, &data)
		if err != nil {
			return "", err
		}

		return data.Value, nil
	}

	prevKeyIdx := 0
	finalKeyIdx := 0
	for idxKey, val := range t.SparseIndex {
		higherOrBigger := strings.Compare(idxKey, key)
		if higherOrBigger == 1 {
			finalKeyIdx = val.Start
			break
		} else {
			prevKeyIdx = val.Start + val.Len
		}
	}
	if finalKeyIdx == 0 {
		finalKeyIdx = t.FileIndex.DataLen - 1
	}

	bytesToParse := make([]byte, finalKeyIdx-prevKeyIdx+1)
	_, err = file.ReadAt(bytesToParse, int64(prevKeyIdx))
	if err != nil {
		return "", err
	}

	lenToParse := len(bytesToParse)
	numOfLBraces := 0
	numOfRBraces := 0
	objRead := []byte{}
	for idx := 0; idx < lenToParse; idx += 1 {
		if bytesToParse[idx] == '{' {
			numOfLBraces += 1
		}

		if bytesToParse[idx] == '}' {
			numOfRBraces += 1
		}
		objRead = append(objRead, bytesToParse[idx])

		if numOfRBraces == numOfLBraces {
			data := Data{}
			err := json.Unmarshal(objRead, &data)
			if err != nil {
				return "", err
			}

			if data.Key == key {
				return data.Value, nil
			} else {
				numOfLBraces = 0
				numOfRBraces = 0
				objRead = []byte{}
			}
		}
	}

	return "", nil
}

func (t *Table) getFromMemorySSTable(key string) (string, error) {
	if idx, ok := t.MemSparseIndex[key]; ok {
		return t.Data[idx].Value, nil
	}

	prevKeyIdx := 0
	finalKeyIdx := 0
	for memKey := range t.MemSparseIndex {
		higherOrBigger := strings.Compare(memKey, key)
		if higherOrBigger == 1 {
			finalKeyIdx = t.MemSparseIndex[memKey]
			break
		} else {
			prevKeyIdx = t.MemSparseIndex[memKey]
		}
	}
	if finalKeyIdx == 0 {
		finalKeyIdx = len(t.Data) - 1
	}

	for idx := prevKeyIdx; idx <= finalKeyIdx; idx += 1 {
		if t.Data[idx].Key == key {
			return t.Data[idx].Value, nil
		}
	}

	return "", nil
}
