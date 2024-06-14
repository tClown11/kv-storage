package db

import (
	"bytes"

	"github.com/tClown11/kv-storage/index"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

// Rewind 重新回到迭代器的起点
func (iter *Iterator) Rewind() {
	iter.indexIter.Rewind()
	iter.skipToNext()
}

// Seek 根据传入的 key 查找第一个大于( 或小于 ) 等于的目标 key，根据从这个 key 开始遍历
func (iter *Iterator) Seek(key []byte) {
	iter.indexIter.Seek(key)
	iter.skipToNext()
}

// Next 跳转到下一个 key
func (iter *Iterator) Next() {
	iter.indexIter.Next()
	iter.skipToNext()
}

// Valid 判断迭代器是否有效( 即迭代器是否能继续迭代 )
func (iter *Iterator) Valid() bool {
	return iter.indexIter.Valid()
}

// Key 获取当前索引位的 key 数据
func (iter *Iterator) Key() []byte {
	return iter.indexIter.Key()
}

// Value 获取当前索引位置指向的 value 数据
func (iter *Iterator) Value() ([]byte, error) {
	logRecordPos := iter.indexIter.Value()
	iter.db.mu.Lock()
	defer iter.db.mu.Unlock()
	return iter.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器，释放相应资源
func (iter *Iterator) Close() {
	iter.indexIter.Close()
}

func (iter *Iterator) skipToNext() {
	prefixLen := len(iter.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; iter.indexIter.Valid(); iter.indexIter.Next() {
		key := iter.indexIter.Key()
		if prefixLen <= len(key) && bytes.Equal(iter.options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
