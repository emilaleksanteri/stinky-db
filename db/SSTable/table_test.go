package sstable

import (
	"os"
	"reflect"
	memtable "stinky-db/db/MemTable"
	"testing"
	"time"
)

func TestWriteTableToFile(t *testing.T) {
	tree := memtable.NewRBTree(0)
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")

	_, err := GenerateFromTree(tree, "./myfile")
	if err != nil {
		t.Errorf("could not write data: %s", err.Error())
	}

	err = os.Remove("./myfile")
	if err != nil {
		t.Errorf("could not delete file: %s", err.Error())
	}
}

func TestReadFromDisk(t *testing.T) {
	defer os.Remove("./myfile")

	tree := memtable.NewRBTree(0)
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")

	table, err := GenerateFromTree(tree, "./myfile")
	if err != nil {
		t.Errorf("could not write data: %s", err.Error())
	}

	toGet := map[string]string{
		"5": "e",
		"6": "f",
		"7": "g",
		"3": "c",
		"4": "d",
		"1": "x",
		"2": "b",
	}

	for key, val := range toGet {
		gotten, err := table.Get(key)
		if err != nil {
			t.Errorf("got an error getting data: %s", err.Error())
		}

		if gotten != val {
			t.Errorf("expected to get %s, got %s", val, gotten)
		}
	}
}

func TestRestoreTableFromDisk(t *testing.T) {
	filepath := "./my_test_file"
	wantedFileIndx := FileIndex{
		DataStart:  0,
		DataLen:    476,
		IndexStart: 476,
		IndexLen:   53,
		MinMax: MinMax{
			StartKey: "1",
			EndKey:   "7",
		},
	}

	wantedSparseIndex := map[string]SparseIndex{
		"1": {Len: 68, Start: 0},
		"5": {Len: 68, Start: 272},
	}

	table, err := GenerateFromDisk(filepath)
	if err != nil {
		t.Errorf("could not generate table from disk: %s", err.Error())
	}

	if table.FileIndex != wantedFileIndx {
		t.Errorf("wanted file index %+v, got %+v", wantedFileIndx, table.FileIndex)
	}

	if !reflect.DeepEqual(table.SparseIndex, wantedSparseIndex) {
		t.Errorf("wanted sparse index %+v, got %+v", wantedSparseIndex, table.SparseIndex)
	}
}

func TestGetAllElements(t *testing.T) {
	defer os.Remove("./myfile")

	tree := memtable.NewRBTree(0)
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")

	table, err := GenerateFromTree(tree, "./myfile")
	if err != nil {
		t.Errorf("could not write data: %s", err.Error())
	}

	elements, err := table.GetAllElements()
	if err != nil {
		t.Errorf("could not get all elements: %v\n", err)
	}

	expectedPairs := []Data{
		{Key: "1", Value: "x"},
		{Key: "2", Value: "b"},
		{Key: "3", Value: "c"},
		{Key: "4", Value: "d"},
		{Key: "5", Value: "e"},
		{Key: "6", Value: "f"},
		{Key: "7", Value: "g"},
	}

	if len(elements) != len(expectedPairs) {
		t.Errorf("expected num of elements: %d, got %d", len(expectedPairs), len(elements))
	}

	for i, val := range expectedPairs {
		if elements[i].Value != val.Value && elements[i].Key != val.Key {
			t.Errorf("expected at i %d, key: %s and val: %s but got key: %s and val: %s\n", i, val.Key, val.Value, elements[i].Key, elements[i].Value)
		}
	}
}

func TestGenerateFromData(t *testing.T) {
	location, err := time.LoadLocation("")
	if err != nil {
		t.Errorf("invalid location data: %s\n", err.Error())
		return
	}

	repeatDate := time.Date(2024, time.January, 1, 1, 1, 1, 1, location)
	febDate := time.Date(2024, time.February, 1, 1, 1, 1, 1, location)
	marDate := time.Date(2024, time.March, 1, 1, 1, 1, 1, location)
	aprDate := time.Date(2024, time.April, 1, 1, 1, 1, 1, location)

	dataToInsert := []Data{
		{Key: "1", Value: "xxx", Written: aprDate},
		{Key: "5", Value: "e", Written: repeatDate},
		{Key: "1", Value: "x", Written: repeatDate},
		{Key: "2", Value: "b", Written: repeatDate},
		{Key: "3", Value: "c", Written: repeatDate},
		{Key: "4", Value: "d", Written: repeatDate},
		{Key: "1", Value: "yyy", Written: marDate},
		{Key: "6", Value: "f", Written: repeatDate},
		{Key: "7", Value: "g", Written: repeatDate},
		{Key: "1", Value: "xyz", Written: febDate},
	}

	expectedData := []Data{
		{Key: "1", Value: "xxx", Written: aprDate},
		{Key: "2", Value: "b", Written: repeatDate},
		{Key: "3", Value: "c", Written: repeatDate},
		{Key: "4", Value: "d", Written: repeatDate},
		{Key: "5", Value: "e", Written: repeatDate},
		{Key: "6", Value: "f", Written: repeatDate},
		{Key: "7", Value: "g", Written: repeatDate},
	}

	table := GenerateFromData(dataToInsert, "")
	if len(table.Data) != len(expectedData) {
		t.Errorf("data is not same len as expected data, got %d, expected %d", len(expectedData), len(table.Data))
		return
	}

	for i, keyval := range table.Data {
		if !reflect.DeepEqual(keyval, expectedData[i]) {
			t.Errorf("table keyval %+v did not match wanted keyval %+v\n", keyval, expectedData[i])
		}
	}
}
