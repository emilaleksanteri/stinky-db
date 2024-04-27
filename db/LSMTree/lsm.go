package lsmtree

import (
	"os"
	"slices"
	sstable "stinky-db/db/SSTable"
)

type LSMTreeNode struct {
	Table       *sstable.Table
	BloomFilter any // for now
}

type LSMTree struct {
	Level_0 [4]LSMTreeNode
	Layers  []LSMTreeNode
	DataDir string
}

var (
	layer_prefix     = "layer_"
	compaction_ratio = 10 // each new layer has x10 more sstables
	compaction_dir   = "./compaction"
)

func NewTree(dataDir string) (LSMTree, error) {
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
	for _, fileName := range sortedFileNames {
		ss, err := sstable.GenerateFromDisk(dataDir + "/" + fileName)
		if err != nil {
			return lsmtree, err
		}

		node := LSMTreeNode{Table: &ss}
		tables = append(tables, node)
	}

	lsmtree.DataDir = dataDir
	lsmtree.Layers = tables

	return lsmtree, nil
}
