package index

import "github.com/tClown11/kv-storage/structure"

type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *structure.LogRecordPos) *structure.LogRecordPos

	// Get 获取对应 key 的索引位置信息
	Get(key []byte) *structure.LogRecordPos

	// Delete 根据 key 删除对应索引位置的信息
	Delete(key []byte) (*structure.LogRecordPos, bool)

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator

	// Size 索引中的数据量
	Size() int

	// Close 关闭索引
	Close() error
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

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于(或小于) 等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *structure.LogRecordPos

	// Close 关闭迭代器，释放相应的资源
	Close()
}
