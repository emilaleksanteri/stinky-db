package sstable

type Table struct {
	Data        []byte
	SparseIndex map[string]SparseIndex
	FileIndex   FileIndex
	FilePath    string
}

type SparseIndex struct {
	Len   int
	Start int
}

type FileIndex struct {
	Version    int
	DataStart  int
	DataLen    int
	IndexStart int
	IndexLen   int
	PartSize   int
}
