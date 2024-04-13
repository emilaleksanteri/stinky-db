package lsmtree

import (
	"os"
	"slices"
	memtable "stinky-db/db/MemTable"
	sstable "stinky-db/db/SSTable"
)

type LSMTreeNode struct {
	Table       *sstable.Table
	BloomFilter any // for now
}

type LSMTree struct {
	Level_0 *LSMTreeNode
	Layers  []LSMTreeNode
	DataDir string
}

var (
	layer_prefix     = "layer_"
	compaction_ratio = 2
	compaction_dir   = "./compaction"
)

// Leveled compaction approach, so each layer has a one lsmtree that grows bigger untill max comapactions reached
// To organize files, we can name each file has e.g., 1_layer, 2_layer etc for easy reconstruction
// if we get a new table to layer 0, write the existing to table to disc, compaction start,
// while compacting, swap layer 0 with new sstable from memtable

// methods needed:
// make new
// make sstable from memtable, add it to the layers
// compaction
// get
// reconstruct from disk

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

func (t *LSMTree) FlushMemtable(memtable *memtable.MemTable) error {
	table := memtable.SwapTree() // safely get old memtable while new one is put up to use
	if t.Level_0 != nil {
		err := t.Level_0.Table.WriteToFile()
		if err != nil {
			return err
		}
		// during compation the current layer 0 gets written and once compacted, deleted so that a new file
		// can take its place for next flush
		// compaction here plz
	}

	ss := sstable.GenerateFromTree(table, t.DataDir+"/"+layer_prefix+"0")
	layer0Node := LSMTreeNode{Table: &ss}
	t.Level_0 = &layer0Node

	return nil
}

func (t *LSMTree) Compact(toMerge, toMergeInto *sstable.Table) error {

	return nil
}
