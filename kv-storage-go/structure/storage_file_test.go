package structure

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tClown11/kv-storage/fio"
)

var (
	dirPathTest = destroyTest()
)

func destroyTest() string {
	var dirPathTest = "./test/"
	err := os.Mkdir(dirPathTest, 0755)
	if err != nil {
		panic(err)
	}
	return dirPathTest
}

func TestOpenStorageFile(t *testing.T) {

	tests := []struct {
		dirPath string
		fileID  int
	}{
		{
			dirPath: dirPathTest,
			fileID:  0,
		},
		{
			dirPath: dirPathTest,
			fileID:  111,
		},
		{
			dirPath: dirPathTest,
			fileID:  111,
		},
	}

	for i := range tests {
		fd, err := OpenStorageFile(tests[i].dirPath, uint32(tests[i].fileID), fio.StandardFIO)
		assert.Nil(t, err)
		assert.NotNil(t, fd)
		defer fd.Close()
	}
}

func TestStorageFile_Write(t *testing.T) {
	tests := []struct {
		dirPath   string
		fileID    int
		writeData []byte
	}{
		{
			dirPath:   dirPathTest,
			fileID:    0,
			writeData: []byte("aaa"),
		},
		{
			dirPath:   dirPathTest,
			fileID:    111,
			writeData: []byte("data is none"),
		},
		{
			dirPath:   dirPathTest,
			fileID:    0,
			writeData: []byte("why are you so far"),
		},
	}

	for i := range tests {
		fd, err := OpenStorageFile(tests[i].dirPath, uint32(tests[i].fileID), fio.StandardFIO)
		assert.Nil(t, err)
		assert.NotNil(t, fd)
		defer fd.Close()

		err = fd.Write(tests[i].writeData)
		assert.Nil(t, err)
	}
}

func TestStorageFile_Close(t *testing.T) {
	tests := []struct {
		fileID int
	}{
		{
			fileID: 1,
		},
		{
			fileID: 111,
		},
	}

	for i := range tests {
		fd, err := OpenStorageFile(dirPathTest, uint32(tests[i].fileID), fio.StandardFIO)
		assert.Nil(t, err)
		assert.NotNil(t, fd)

		err = fd.Write([]byte("\nThis is test close file\n"))
		assert.Nil(t, err)

		err = fd.Close()
		assert.Nil(t, err)
	}
}

func TestStorageFile_Sync(t *testing.T) {
	tests := []struct {
		fileID int
	}{
		{
			fileID: 231,
		},
		{
			fileID: 456,
		},
	}

	for i := range tests {
		fd, err := OpenStorageFile(dirPathTest, uint32(tests[i].fileID), fio.StandardFIO)
		assert.Nil(t, err)
		assert.NotNil(t, fd)

		err = fd.Write([]byte("\nThis is test sync file\n"))
		assert.Nil(t, err)

		err = fd.Sync()
		assert.Nil(t, err)
	}
}

func TestStorageFile_ReadLogRecord(t *testing.T) {
	tests := []struct {
		logRecordList []*LogRecord
		fileID        int
	}{
		{
			logRecordList: []*LogRecord{
				{
					Key:   []byte("one"),
					Value: []byte("bitcask kv go"),
					Type:  LogRecordNormal,
				},
			},
			fileID: 10,
		},
		{
			logRecordList: []*LogRecord{
				{
					Key:   []byte("two"),
					Value: []byte("use lsm tree designer"),
					Type:  LogRecordDeleted,
				},
				{
					Key:   []byte("three"),
					Value: []byte("this is three command"),
					Type:  LogRecordNormal,
				},
			},
			fileID: 10,
		},
		{
			logRecordList: []*LogRecord{
				{
					Key:   []byte("four"),
					Value: []byte("storage kv is storage system"),
					Type:  LogRecordDeleted,
				},
			},
			fileID: 10,
		},
	}

	var offset, size int64

	for i := range tests {
		fd, err := OpenStorageFile(dirPathTest, uint32(tests[i].fileID), fio.StandardFIO)
		assert.Nil(t, err)
		assert.NotNil(t, fd)
		defer fd.Close()

		for j := range tests[i].logRecordList {
			logRecordBytes, logSize := tests[i].logRecordList[j].EncodeLogRecord()
			err = fd.Write(logRecordBytes)
			assert.Nil(t, err)

			size = logSize

			var (
				readLogRecord *LogRecord
				readSize      int64
				err           error
			)

			readLogRecord, readSize, err = fd.ReadLogRecord(offset)

			assert.Nil(t, err)
			assert.Equal(t, size, readSize)
			assert.Equal(t, tests[i].logRecordList[j], readLogRecord)
			t.Log(readSize)
			offset += logSize
		}
	}
}

// 测试完成之后销毁 DB 数据目录
func TestDestroyDir(t *testing.T) {
	err := os.RemoveAll(dirPathTest)
	assert.Nil(t, err)
}
