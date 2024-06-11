package fio

import "os"

const DataFilePerm = 0644

type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

// FileIO 标准系统文件 IO
type FileIO struct {
	fd *os.File
}

func NewFileIO(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

// Read 从文件的给定位置读取对应的数据
func (fio *FileIO) Read(buf []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(buf, offset)
}

// Write 写入字节数组到文件中
func (fio *FileIO) Write(data []byte) (int, error) {
	return fio.fd.Write(data)
}

// Sync 持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
