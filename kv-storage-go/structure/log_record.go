// Package structure 是管理存储数据的包
package structure

import (
	"encoding/binary"
	"hash/crc32"
)

// crc type keySize valueSize
// 4 +  1  +  5   +   5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

const crcLength = crc32.Size

// LogRecordPos 数据内存索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储在哪个文件中
	Offset int64  // 偏移，表示将数据存储到了数据文件的具体位置偏移量
	Size   uint32 // 标识数据在磁盘上的大小
}

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// LogRecord 写入到数据文件的日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func (logRe *LogRecord) EncodeLogRecord() ([]byte, int64) {
	// 初始化一个 header 部分的字节数据
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = byte(logRe.Type)
	var index = 5

	// 5 字节后，存储的事 key 和 value 的长度信息
	// 使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRe.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRe.Value)))

	var size = index + len(logRe.Key) + len(logRe.Value)
	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝出来
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], logRe.Key)
	copy(encBytes[index+len(logRe.Key):], logRe.Value)

	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// EncodeCRCFromBytes 根据 buf 中的 key/value 信息，更新 crc 的值，并返回
func (logRe *LogRecord) EncodeCRCFromBytes(buf []byte) uint32 {
	if len(logRe.Key) == 0 && len(logRe.Value) == 0 {
		return 0
	}

	crc := crc32.ChecksumIEEE(buf[:])
	crc = crc32.Update(crc, crc32.IEEETable, logRe.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRe.Value)

	return crc
}

// EncodeKeyWithSeq 根据事务 ID 和 key，编码新的 key
func EncodeKeyWithSeq(key []byte, seqID uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqID)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n+1:], key)

	return encKey
}

// ParseKeyAndSeqFromLogRecordKey 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func ParseKeyAndSeqFromLogRecordKey(key []byte) ([]byte, uint64) {
	if len(key) == 0 {
		return nil, 0
	}

	seqNo, n := binary.Uvarint(key)
	readKey := key[n:]
	return readKey, seqNo
}
