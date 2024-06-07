package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFileName = "test.data"

var testIOManager, _ = NewFileIO(testFileName)

func clearTestVal(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestFileIO_Write(t *testing.T) {
	tests := []struct {
		testData []byte
		len      int
		err      error
	}{
		{
			testData: []byte(""),
			len:      0,
			err:      nil,
		},
		{
			testData: []byte("bitcask kv"),
			len:      10,
			err:      nil,
		},
		{
			testData: []byte("this is my frist data"),
			len:      21,
			err:      nil,
		},
	}

	for i := range tests {
		n, err := testIOManager.Write(tests[i].testData)
		if !assert.Equal(t, tests[i].len, n) || !assert.Equal(t, tests[i].err, err) {
			t.Errorf("write data(%+v) in file fail", tests[i].testData)
		}
	}
}

func TestFileIO_Read(t *testing.T) {
	tests := []struct {
		testData []byte
		len      int
		offer    int64
		err      error
	}{
		{
			testData: []byte("bitcask kv"),
			len:      10,
			offer:    0,
			err:      nil,
		},
		{
			testData: []byte("this is my frist data"),
			len:      21,
			offer:    10,
			err:      nil,
		},
	}

	for i := range tests {
		buf := make([]byte, tests[i].len)
		n, err := testIOManager.Read(buf, tests[i].offer)
		if !assert.Equal(t, tests[i].len, n) || !assert.Equal(t, tests[i].err, err) || !assert.Equal(t, tests[i].testData, buf) {
			t.Errorf("read data(%+v) from file fail", tests[i].testData)
		}
	}
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("./", "test2.data")

	fio, err := NewFileIO(path)
	defer clearTestVal(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)

	err = testIOManager.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "test3.data")
	fio, err := NewFileIO(path)
	defer clearTestVal(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)

	err = testIOManager.Close()
	assert.Nil(t, err)

	// 关闭全局的 testIOManager
	clearTestVal(testFileName)
}
