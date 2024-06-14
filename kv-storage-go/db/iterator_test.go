package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tClown11/kv-storage/utils"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "test_iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		for _, of := range db.olderFiles {
			if of != nil {
				_ = of.Close()
			}
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_Iterator_One_Value(t *testing.T) {
	tests := []struct {
		opts     IteratorOptions
		dbOpts   Options
		dirPath  string
		keyLen   int
		valueLen int
	}{
		{
			opts:     DefaultIteratorOptions,
			dirPath:  "test_iterator-1",
			keyLen:   10,
			valueLen: 10,
			dbOpts:   DefaultOptions,
		},
	}

	for i := range tests {
		dir, _ := os.MkdirTemp("", tests[i].dirPath)
		tests[i].dbOpts.DirPath = dir
		db, err := Open(tests[i].dbOpts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
		assert.NotNil(t, db.mu)

		value := utils.GetTestValue(tests[i].valueLen)
		err = db.Put(utils.GetTestKey(tests[i].keyLen), value)
		assert.Nil(t, err)

		iterator := db.NewIterator(tests[i].opts)
		defer iterator.Close()
		assert.NotNil(t, iterator)
		assert.Equal(t, true, iterator.Valid())
		assert.Equal(t, utils.GetTestKey(tests[i].keyLen), iterator.Key())
		val, err := iterator.Value()
		assert.Nil(t, err)
		assert.Equal(t, value, val)
	}
}

func TestDB_Iterator_Mutil_Values(t *testing.T) {
	tests := []struct {
		opts     IteratorOptions
		dbOpts   Options
		dirPath  string
		keyData  []byte
		valueLen int
	}{
		{
			opts:     DefaultIteratorOptions,
			dirPath:  "test_iterator-2",
			keyData:  []byte("annde"),
			valueLen: 10,
			dbOpts:   DefaultOptions,
		},
		{
			opts:     DefaultIteratorOptions,
			dirPath:  "test_iterator-2",
			keyData:  []byte("cnedc"),
			valueLen: 10,
			dbOpts:   DefaultOptions,
		},
		{
			opts:     DefaultIteratorOptions,
			dirPath:  "test_iterator-2",
			keyData:  []byte("deeue"),
			valueLen: 10,
			dbOpts:   DefaultOptions,
		},
		{
			opts:     DefaultIteratorOptions,
			dirPath:  "test_iterator-2",
			keyData:  []byte("bende"),
			valueLen: 10,
			dbOpts:   DefaultOptions,
		},
	}
	var opts = DefaultOptions
	dir, _ := os.MkdirTemp("", "test_iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, db.mu)

	for i := range tests {
		err = db.Put(tests[i].keyData, utils.GetTestValue(tests[i].valueLen))
		assert.Nil(t, err)
	}

	// 正向迭代
	iter_1 := db.NewIterator(DefaultIteratorOptions)
	for iter_1.Rewind(); iter_1.Valid(); iter_1.Next() {
		assert.NotNil(t, iter_1.Key())
	}

	iter_1.Rewind()

	for iter_1.Seek([]byte("c")); iter_1.Valid(); iter_1.Next() {
		assert.NotNil(t, iter_1.Key())
	}
	iter_1.Close()

	// 反向迭代
	iterOpts := DefaultIteratorOptions
	iterOpts.Reverse = true
	iter_2 := db.NewIterator(iterOpts)
	for iter_2.Rewind(); iter_2.Valid(); iter_2.Next() {
		assert.NotNil(t, iter_2.Key())
	}

	iter_2.Rewind()

	for iter_2.Seek([]byte("c")); iter_2.Valid(); iter_2.Next() {
		assert.NotNil(t, iter_2.Key())
	}
	iter_2.Close()

	// 指定 prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("cne")
	iter_3 := db.NewIterator(iterOpts2)
	for iter_3.Rewind(); iter_3.Valid(); iter_3.Next() {
		assert.NotNil(t, iter_3.Key())
	}
	iter_3.Close()
}
