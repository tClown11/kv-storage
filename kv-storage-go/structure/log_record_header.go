package structure

import "encoding/binary"

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 长度
	valueSize  uint32        // value 的长度
}

// 对字节数组中的 header 信息进行解码
func (header *logRecordHeader) DecodeLogRecordHeader(buf []byte) int64 {
	if len(buf) <= 4 {
		return 0
	}

	header.crc = binary.LittleEndian.Uint32(buf[:4])
	header.recordType = LogRecordType(buf[4])

	var index = 5

	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return int64(index)
}
