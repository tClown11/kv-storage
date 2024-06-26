package index

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/tClown11/kv-storage/structure"
)

const defaultDegree = 32

// Btree 索引，主要封装 google 的 btree 的实现
type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree(degree int) *Btree {
	if degree == 0 {
		degree = defaultDegree
	}

	return &Btree{
		tree: btree.New(degree),
		lock: &sync.RWMutex{},
	}
}

func (bt *Btree) Put(key []byte, pos *structure.LogRecordPos) *structure.LogRecordPos {
	it := &BItem{key: key, pos: pos}

	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*BItem).pos
}

func (bt *Btree) Get(key []byte) *structure.LogRecordPos {
	it := &BItem{key: key}
	bt.lock.RLock()
	defer bt.lock.RUnlock()

	res := bt.tree.Get(it)
	if res == nil {
		return nil
	}
	return res.(*BItem).pos
}

func (bt *Btree) Delete(key []byte) (*structure.LogRecordPos, bool) {
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

func (bt *Btree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

func (bt *Btree) Close() error {
	return nil
}

func (bt *Btree) Size() int {
	return bt.tree.Len()
}

// BItem btree 中的单个数据对象
type BItem struct {
	key []byte
	pos *structure.LogRecordPos
}

func (bi *BItem) Less(item btree.Item) bool {
	return bytes.Compare(bi.key, item.(*BItem).key) == -1
}
