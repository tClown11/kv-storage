package index

import "github.com/tClown11/kv-storage/structure"

type Indexer interface {
	Put(key []byte, pos *structure.LogRecordPos) *structure.LogRecordPos
	Get(key []byte) *structure.LogRecordPos
	Delete(key []byte) (*structure.LogRecordPos, bool)
}

type IndexType = int8

const (
	// Btree 索引
	BTree IndexType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+ 树索引
	BPTree
)

// IndexOpts 用于初始化不同类型的 Index
type IndexOpts struct {
	Type    IndexType
	DirPath string
	Sync    bool
	Size    int
}

// NewIndexer 根据类型初始化索引
func NewIndexer(opts *IndexOpts) Indexer {
	switch opts.Type {
	case BTree:
		return NewBtree(opts.Size)
	// case ART:
	// 	return NewART()
	// case BPTree:
	// 	return NewBPlusTree(opts.DirPath, opts.Sync)
	default:
		panic("unsupported index type")
	}
}
