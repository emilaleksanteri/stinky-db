package lsmtree

import (
	"os"
	memtable "stinky-db/db/MemTable"
	"testing"
)

func clearDataDir(t *testing.T) {
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
}

func TestCanInsertToLevel0(t *testing.T) {
	defer clearDataDir(t)

	lsm, err := NewTree(test_data_dir)
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
	lsm, err := NewTree(test_data_gen_dir)
	if err != nil {
		t.Fatalf("could not generate tree from gen dir: %+v\n", err)
	}

	if len(lsm.Level_0) != 1 {
		t.Fatalf("level 0 should only have a single SSTable, got %d\n", len(lsm.Level_0))
	}
}
