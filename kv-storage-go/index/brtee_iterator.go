package index

import (
	"bytes"
	"sort"

	"github.com/google/btree"
	"github.com/tClown11/kv-storage/structure"
)

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int      // 当前遍历的下标位置
	reverse   bool     // 是否是方向遍历
	values    []*BItem // key+位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*BItem, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*BItem)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于(或小于) 等于的目标 key，根据从这个 key 开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的 key 数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *btreeIterator) Value() *structure.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器，释放相应的资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
