package index

import (
	"bytes"
	"sync"

	"github.com/google/btree"

	"github.com/tClown11/kv-storage/data"
)

// BTree 索引，主要封装 google 的 btree 的实现
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree(degree int) *BTree {
	return &BTree{
		tree: btree.New(degree),
		lock: &sync.RWMutex{},
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &BItem{key: key, pos: pos}

	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*BItem).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &BItem{key: key}
	bt.lock.RLock()
	defer bt.lock.RUnlock()

	res := bt.tree.Get(it)
	if res == nil {
		return nil
	}
	return res.(*BItem).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &BItem{key: key}

	bt.lock.Lock()
	defer bt.lock.Unlock()

	oldItem := bt.tree.Delete(it)
	// 无效删除
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*BItem).pos, true
}

// BItem btree 中的单个数据对象
type BItem struct {
	key []byte
	pos *data.LogRecordPos
}

func (bi *BItem) Less(item btree.Item) bool {
	return bytes.Compare(bi.key, item.(*BItem).key) == -1
}
