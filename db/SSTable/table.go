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
	sparseIdxSize = 4
)

var fileIdxSeparator = []byte{"$"[0], "$"[0]}

type Data struct {
	Key     string    `json:"key"`
	Value   string    `json:"value"`
	Written time.Time `json:"written"`
	Delete  bool      `json:"delete"`
}

type Table struct {
	Data           []Data                 `json:"data"`
	SparseIndex    map[string]SparseIndex `json:"sparse_index"`
	FileIndex      FileIndex              `json:"file_index"`
	FilePath       string
	Size           int
	MemSparseIndex map[string]int
	mu             *sync.Mutex
}

type MinMax struct {
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

type SparseIndex struct {
	Len   int `json:"len"`
	Start int `json:"start"`
}

type FileIndex struct {
	//Version    int `json:"version"`
	DataStart  int    `json:"data_start"`
	DataLen    int    `json:"data_len"`
	IndexStart int    `json:"index_start"`
	IndexLen   int    `json:"index_len"`
	MinMax     MinMax `json:"min_max"`
}

func (t *Table) WriteToFile() error {
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
		MinMax: MinMax{
			StartKey: t.Data[0].Key,
			EndKey:   t.Data[len(t.Data)-1].Key,
		},
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

	_, err = file.Write(fileIdxSeparator)
	if err != nil {
		return err
	}

	_, err = file.Write(fileIdxBytes)
	if err != nil {
		return err
	}

	t.MemSparseIndex = nil
	t.Data = nil

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
	table.Size = mem.GetSize()
	return table
}

func (t *Table) GetAllElements() ([]Data, error) {
	var data []Data
	file, err := os.Open(t.FilePath)
	if err != nil {
		return data, err
	}

	bytesToRead := make([]byte, t.FileIndex.DataLen)
	_, err = file.ReadAt(bytesToRead, int64(t.FileIndex.DataStart))
	if err != nil {
		return data, err
	}

	numOfLBraces := 0
	numOfRBraces := 0
	currReading := []byte{}
	for idx := 0; idx < len(bytesToRead); idx += 1 {
		if bytesToRead[idx] == '{' {
			numOfLBraces += 1
		}

		if bytesToRead[idx] == '}' {
			numOfRBraces += 1
		}

		currReading = append(currReading, bytesToRead[idx])

		if numOfRBraces == numOfLBraces {
			numOfLBraces = 0
			numOfRBraces = 0

			keyVal := Data{}
			err := json.Unmarshal(currReading, &keyVal)
			if err != nil {
				return data, err
			}

			data = append(data, keyVal)
			currReading = []byte{}
		}
	}

	return data, nil
}

func (t *Table) GetNumOfElements() (int, error) {
	var count int
	file, err := os.Open(t.FilePath)
	if err != nil {
		return count, err
	}

	bytesToRead := make([]byte, t.FileIndex.DataLen)
	_, err = file.ReadAt(bytesToRead, int64(t.FileIndex.DataStart))
	if err != nil {
		return count, err
	}

	numOfLBraces := 0
	numOfRBraces := 0
	for idx := 0; idx < len(bytesToRead); idx += 1 {
		if bytesToRead[idx] == '{' {
			numOfLBraces += 1
		}

		if bytesToRead[idx] == '}' {
			numOfRBraces += 1
		}

		if numOfRBraces == numOfLBraces {
			numOfLBraces = 0
			numOfRBraces = 0
			count += 1
		}
	}

	return count, nil
}

func GenerateFromDisk(filepath string) (Table, error) {
	table := newTable(filepath)

	file, err := os.Open(filepath)
	if err != nil {
		return table, err
	}
	defer file.Close()

	fileStats, err := file.Stat()
	if err != nil {
		return table, err
	}

	fileSize := fileStats.Size()
	bytesToReadForIndex := make([]byte, 150) // fileindex is always smaller than 150 bytes
	_, err = file.ReadAt(bytesToReadForIndex, fileSize-150)
	if err != nil {
		return table, err
	}

	indexBytes := []byte{}
	allowedToRead := false
	for index := 0; index < len(bytesToReadForIndex); index += 1 {
		currRune := bytesToReadForIndex[index]
		nextRune := "0"[0]
		if index+1 != len(bytesToReadForIndex) {
			nextRune = bytesToReadForIndex[index+1]
		}
		if currRune == '$' && nextRune == '$' {
			allowedToRead = true
		}

		if !allowedToRead || currRune == '$' {
			continue
		} else {
			indexBytes = append(indexBytes, currRune)
		}
	}

	fileIndex := FileIndex{}
	err = json.Unmarshal(indexBytes, &fileIndex)
	if err != nil {
		return table, err
	}

	sparseIndexBytes := make([]byte, fileIndex.IndexLen)
	_, err = file.ReadAt(sparseIndexBytes, int64(fileIndex.IndexStart))
	if err != nil {
		return table, err
	}

	sparseIdx := map[string]SparseIndex{}
	err = json.Unmarshal(sparseIndexBytes, &sparseIdx)
	if err != nil {
		return table, err
	}

	table.FileIndex = fileIndex
	table.SparseIndex = sparseIdx

	tableSize, err := table.GetNumOfElements()
	if err != nil {
		return table, err
	}

	table.Size = tableSize

	return table, nil
}

func (t *Table) Get(key string) (string, error) {
	if t.MemSparseIndex == nil {
		return t.readFromDisk(key)
	}

	return t.getFromMemorySSTable(key)
}

func (t *Table) readFromDisk(key string) (string, error) {
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

	startKeyIdx := 0
	finalKeyIdx := 0
	for idxKey, val := range t.SparseIndex {
		smallerOrBigger := strings.Compare(idxKey, key)
		if smallerOrBigger == 1 {
			finalKeyIdx = val.Start
		} else {
			startKeyIdx = val.Start + val.Len
		}
	}
	if finalKeyIdx == 0 {
		finalKeyIdx = t.FileIndex.DataLen - 1
	}

	bytesToParse := make([]byte, finalKeyIdx-startKeyIdx+1)
	_, err = file.ReadAt(bytesToParse, int64(startKeyIdx))
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

	startKeyIdx := 0
	finalKeyIdx := 0
	for memKey := range t.MemSparseIndex {
		smallerOrBigger := strings.Compare(memKey, key)
		if smallerOrBigger == 1 {
			finalKeyIdx = t.MemSparseIndex[memKey]
		} else {
			startKeyIdx = t.MemSparseIndex[memKey]
		}
	}
	if finalKeyIdx == 0 {
		finalKeyIdx = len(t.Data) - 1
	}

	for idx := startKeyIdx; idx <= finalKeyIdx; idx += 1 {
		if t.Data[idx].Key == key {
			return t.Data[idx].Value, nil
		}
	}

	return "", nil
}
