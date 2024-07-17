package structure

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/tClown11/kv-storage/errs"
	"github.com/tClown11/kv-storage/fio"
)

const (
	StorageFileNameSuffix = ".data"
	HintFileName          = "hint-index"
	MergeFinishedfileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

type StorageFile struct {
	FileID    uint32        // 文件编号(id)
	WriteOff  int64         // 文件写入偏移量( 当前文件写入到了哪个位置 )
	IoManager fio.IOManager // io 读写管理
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*StorageFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newStorageFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*StorageFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedfileName)
	return newStorageFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*StorageFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newStorageFile(fileName, 0, fio.StandardFIO)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+StorageFileNameSuffix)
}

// WriteHintRecord 写入索引信息到 hint 文件中
func (sf *StorageFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := record.EncodeLogRecord()
	return sf.Write(encRecord)
}

func (sf *StorageFile) Write(buf []byte) error {
	n, err := sf.IoManager.Write(buf)
	if err != nil {
		return err
	}
	sf.WriteOff += int64(n)
	return nil
}

func (sf *StorageFile) Sync() error {
	return sf.IoManager.Sync()
}

func (sf *StorageFile) Close() error {
	return sf.IoManager.Close()
}

// ReadLogRecord 根据 offset 从数据文件中读取 LogRecord
func (sf *StorageFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	var err error
	fileSize, err := sf.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 header 长度已经超过了文件的长度，则只需要读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取 Header 信息
	headerBuf := make([]byte, headerBytes)
	err = sf.fillBufWithOffset(headerBuf, offset)
	if err != nil {
		return nil, 0, err
	}

	header := &logRecordHeader{}
	headerSize := header.DecodeLogRecordHeader(headerBuf)
	// 下面的条件表示读取到了文件末尾，直接返回 EOF 错误
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}

	// 开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		kvBuf := make([]byte, keySize+valueSize)
		if err := sf.fillBufWithOffset(kvBuf, offset+headerSize); err != nil {
			return nil, 0, err
		}

		// 解出 key value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性
	crc := logRecord.EncodeCRCFromBytes(headerBuf[crcLength:headerSize])
	if crc != header.crc {
		return nil, 0, errs.ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (sf *StorageFile) fillBufWithOffset(buf []byte, offset int64) error {
	_, err := sf.IoManager.Read(buf, offset)
	return err
}

func OpenStorageFile(dirPath string, fileID uint32, ioType fio.FileIOType) (*StorageFile, error) {
	fileName := GetStorageFileName(dirPath, fileID)
	return newStorageFile(fileName, fileID, ioType)
}

func GetStorageFileName(dirPath string, fileID uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+StorageFileNameSuffix)
}

func newStorageFile(fileName string, fileId uint32, ioType fio.FileIOType) (*StorageFile, error) {
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &StorageFile{
		FileID:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}
