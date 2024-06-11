package data

import (
	"fmt"
	"path/filepath"

	"github.com/tClown11/kv-storage/fio"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

type DataFile struct {
	FileID    uint32        // 文件编号(id)
	WriteOff  int64         // 文件写入偏移量( 当前文件写入到了哪个位置 )
	IoManager fio.IOManager // io 读写管理
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func OpenDataFile(dirPath string, fileID uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileID)
	return newDataFile(fileName, fileID, ioType)
}

func GetDataFileName(dirPath string, fileID uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileID:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}
