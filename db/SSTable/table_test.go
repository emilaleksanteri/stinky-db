package sstable

import (
	"os"
	memtable "stinky-db/db/MemTable"
	"testing"
)

func TestTableWrite(t *testing.T) {}

func TestTableFromTree(t *testing.T) {
	tree := memtable.NewRBTree()
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")

	table := GenerateFromTree(tree, "./myfile")
	if len(table.Data) != tree.Size {
		t.Errorf("expected to have table size %d, got %d", tree.Size, len(table.Data))
	}
}

func TestGetKeyFromMemSSTable(t *testing.T) {
	tree := memtable.NewRBTree()
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")

	table := GenerateFromTree(tree, "./myfile")

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
			t.Errorf("got an error reading data: %s", err.Error())
		}

		if gotten != val {
			t.Errorf("expected to get %s, got %s", val, gotten)
		}
	}
}

func TestWriteTableToFile(t *testing.T) {
	tree := memtable.NewRBTree()
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")

	table := GenerateFromTree(tree, "./myfile")
	err := table.writeToFile()
	if err != nil {
		t.Errorf("could not write data: %s", err.Error())
	}

	err = os.Remove("./myfile")
	if err != nil {
		t.Errorf("could not delete file: %s", err.Error())
	}
}

func TestReadFromTableFile(t *testing.T) {
	defer os.Remove("./myfile")

	tree := memtable.NewRBTree()
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")

	table := GenerateFromTree(tree, "./myfile")
	err := table.writeToFile()
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
