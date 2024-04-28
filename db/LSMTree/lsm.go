package lsmtree

import (
	"fmt"
	"os"
	"slices"
	memtable "stinky-db/db/MemTable"
	sstable "stinky-db/db/SSTable"
	"strings"
)

type LSMTreeNode struct {
	Table       *sstable.Table
	BloomFilter any // for now
}

type LSMTree struct {
	Level_0       []LSMTreeNode
	Layers        []LSMTreeNode
	DataDir       string
	CompactionDir string
}

var (
	layer_prefix        = "layer_"
	compaction_ratio    = 10 // each new layer has x10 more sstables
	compaction_dir      = "./compaction"
	test_compaction_dir = "./test-compaction"
	data_dir            = "./data"
	test_data_dir       = "./test-data"
	test_data_gen_dir   = "./test-data-gen"
	lvl_0_max_len       = 4
)

func NewNode(ss *sstable.Table) LSMTreeNode {
	return LSMTreeNode{
		Table: ss,
	}
}

func NewTree(dataDir, compactionDir string) (LSMTree, error) {
	var lsmtree LSMTree

	files, err := os.ReadDir(dataDir)
	if err != nil {
		return lsmtree, nil
	}

	sortedFileNames := []string{}
	for _, file := range files {
		sortedFileNames = append(sortedFileNames, file.Name())
	}
	slices.Sort(sortedFileNames)

	tables := []LSMTreeNode{}
	layer0 := []LSMTreeNode{}
	for _, fileName := range sortedFileNames {
		ss, err := sstable.GenerateFromDisk(dataDir + "/" + fileName)
		if err != nil {
			return lsmtree, err
		}

		node := LSMTreeNode{Table: &ss}

		if strings.Contains(fileName, layer_prefix+"0") {
			layer0 = append(layer0, node)
		} else {
			tables = append(tables, node)
		}
	}

	lsmtree.DataDir = dataDir
	lsmtree.Layers = tables
	lsmtree.Level_0 = layer0
	lsmtree.CompactionDir = compactionDir

	return lsmtree, nil
}

func (lsm *LSMTree) getLayer0NameNum() int {
	if len(lsm.Level_0) == 0 {
		return 1
	}

	return len(lsm.Level_0) + 1
}

func (lsm *LSMTree) InsertMemtable(mem *memtable.RBTree) error {
	if len(lsm.Level_0) != lvl_0_max_len {
		ss, err := sstable.GenerateFromTree(mem, fmt.Sprintf("%s/%s0_%d", lsm.DataDir, layer_prefix, lsm.getLayer0NameNum()))
		if err != nil {
			return err
		}
		lsm.Level_0 = append(lsm.Level_0, NewNode(&ss))
		return nil
	}
	err := lsm.compact()
	if err != nil {
		return err
	}

	newLvl0 := []LSMTreeNode{}
	lsm.Level_0 = newLvl0

	ss, err := sstable.GenerateFromTree(mem, fmt.Sprintf("%s/%s0_%d", lsm.DataDir, layer_prefix, lsm.getLayer0NameNum()))
	if err != nil {
		return err
	}

	lsm.Level_0 = append(lsm.Level_0, NewNode(&ss))

	return nil
}

func (lsm *LSMTree) compactLayer0() (*sstable.Table, error) {
	mergedData := []sstable.Data{}
	for _, ss := range lsm.Level_0 {
		err := ss.Table.ReadIntoMem()
		if err != nil {
			return nil, err
		}

		mergedData = slices.Concat(mergedData, ss.Table.Data)
	}

	mergedSS := sstable.GenerateFromData(mergedData, lsm.CompactionDir+"/layer_0")

	return &mergedSS, nil
}

func (lsm *LSMTree) compact() error {
	compacted0, err := lsm.compactLayer0()
	if err != nil {
		return err
	}

	files, err := os.ReadDir(lsm.DataDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.Contains(file.Name(), layer_prefix+"0") {
			err = os.Remove(lsm.DataDir + "/" + file.Name())
			if err != nil {
				return err
			}
		}
	}

	if len(lsm.Layers) == 0 {
		compacted0.FilePath = fmt.Sprintf("%s/%s1_1", lsm.DataDir, layer_prefix)
		err = compacted0.WriteToFile()
		if err != nil {
			return err
		}
		lsm.Layers = append(lsm.Layers, NewNode(compacted0))
		return nil
	}

	return nil
}
