package lsmtree

import (
	"fmt"
	"os"
	"slices"
	memtable "stinky-db/db/MemTable"
	"testing"
)

func clearDataAndCompactionDir(t *testing.T) {
	files, err := os.ReadDir(test_data_dir)
	if err != nil {
		t.Fatalf("clear dir files error: %+v\n", err)
	}

	for _, file := range files {
		err = os.Remove(test_data_dir + "/" + file.Name())
		if err != nil {
			t.Fatalf("could not delete file: %+v\n", err)
		}
	}

	files, err = os.ReadDir(test_compaction_dir)
	if err != nil {
		t.Fatalf("clear dir files error: %+v\n", err)
	}

	for _, file := range files {
		err = os.Remove(test_compaction_dir + "/" + file.Name())
		if err != nil {
			t.Fatalf("could not delete file: %+v\n", err)
		}
	}
}

func TestCanInsertToLevel0(t *testing.T) {
	defer clearDataAndCompactionDir(t)

	lsm, err := NewTree(test_data_dir, test_compaction_dir)
	if err != nil {
		t.Errorf("could not make a lsm tree: %+v\n", err)
	}

	mem := memtable.NewRBTree(0)
	mem.Insert("a", "val")
	mem.Insert("b", "val2")
	mem.Insert("c", "val3")

	err = lsm.InsertMemtable(mem)
	if err != nil {
		t.Fatalf("could not insert memtable into lsm: %+v\n", err)
	}
}

func TestGenerateTreeFromWrittenFiles(t *testing.T) {
	lsm, err := NewTree(test_data_gen_dir, test_compaction_dir)
	if err != nil {
		t.Fatalf("could not generate tree from gen dir: %+v\n", err)
	}

	if len(lsm.Level_0) != 1 {
		t.Fatalf("level 0 should only have a single SSTable, got %d\n", len(lsm.Level_0))
	}
}

func TestCompactLevel0(t *testing.T) {
	defer clearDataAndCompactionDir(t)

	lsm, err := NewTree(test_data_dir, test_compaction_dir)
	if err != nil {
		t.Fatalf("could not make an lsm tree: %+v\n", err)
	}

	memTables := []*memtable.RBTree{}
	for i := 0; i < 4; i += 1 {
		mem := memtable.NewRBTree(0)
		mem.Insert("a", fmt.Sprintf("val_%d", i))
		mem.Insert("b", fmt.Sprintf("val2_%d", i))
		mem.Insert("c", fmt.Sprintf("val3_%d", i))
		memTables = append(memTables, mem)
	}

	slices.Reverse(memTables)

	for _, mem := range memTables {
		err = lsm.InsertMemtable(mem)
		if err != nil {
			t.Fatalf("could not insert mem: %+v\n", err)
		}
	}

	mem := memtable.NewRBTree(0)
	mem.Insert("d", "val")
	mem.Insert("e", "val2")
	mem.Insert("f", "val3")

	err = lsm.InsertMemtable(mem)
	if err != nil {
		t.Fatalf("could not insert a mem when lsm full: %+v\n", err)
	}

	if len(lsm.Level_0) != 1 {
		t.Fatalf("layer 0 should only have a single item after compaction, got %d\n", len(lsm.Level_0))
	}

	if len(lsm.Layers) != 1 {
		t.Fatalf("rest of lsm layers should only contain 1 item, got %d\n", len(lsm.Layers))
	}

	nodeInLayer1 := lsm.Layers["1"][0].Table
	data, err := nodeInLayer1.GetAllElements()
	if err != nil {
		t.Fatalf("could not get data for node in 1: %+v\n", err)
	}

	expectedValues := []string{
		"val_3",
		"val2_3",
		"val3_3",
	}

	if len(data) != len(expectedValues) {
		t.Fatalf("data should be len expected %d, got %d\n", len(expectedValues), len(data))
	}

	for i, keyval := range data {
		if keyval.Value != expectedValues[i] {
			t.Fatalf("wanted %s, got %s\n", expectedValues[i], keyval.Value)
		}
	}
}
