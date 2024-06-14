package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tClown11/kv-storage/utils"
)

func TestDB_Close(t *testing.T) {
	tests := []struct {
		opts     Options
		dirPath  string
		keyLen   int
		valueLen int
	}{
		{
			opts:     DefaultOptions,
			dirPath:  "storage-kv-close",
			keyLen:   11,
			valueLen: 20,
		},
	}

	for i := range tests {
		dir, _ := os.MkdirTemp("", tests[i].dirPath)
		tests[i].opts.DirPath = dir
		db, err := Open(tests[i].opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)

		err = db.Put(utils.GetTestKey(tests[i].keyLen), utils.GetTestValue(tests[i].valueLen))
		assert.Nil(t, err)
	}
}

func TestDB_Sync(t *testing.T) {
	tests := []struct {
		opts     Options
		dirPath  string
		keyLen   int
		valueLen int
	}{
		{
			opts:     DefaultOptions,
			dirPath:  "storage-kv-sync",
			keyLen:   11,
			valueLen: 20,
		},
	}

	for i := range tests {
		dir, _ := os.MkdirTemp("", tests[i].dirPath)
		tests[i].opts.DirPath = dir
		db, err := Open(tests[i].opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)

		err = db.Put(utils.GetTestKey(tests[i].keyLen), utils.GetTestValue(tests[i].valueLen))
		assert.Nil(t, err)

		err = db.Sync()
		assert.Nil(t, err)
	}
}
