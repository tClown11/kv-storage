package structure

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnocdeLogRecord(t *testing.T) {
	tests := []struct {
		testData *LogRecord
	}{
		{
			// 正常情况
			testData: &LogRecord{
				Key:   []byte("one"),
				Value: []byte("storage-kv"),
				Type:  LogRecordNormal,
			},
		},
		{
			// value 为空的情况
			testData: &LogRecord{
				Key:  []byte("one"),
				Type: LogRecordNormal,
			},
		},
		{
			// Deleted 情况
			testData: &LogRecord{
				Key:   []byte("one"),
				Value: []byte("storage-kv-2"),
				Type:  LogRecordDeleted,
			},
		},
	}

	for i := range tests {
		res, n := tests[i].testData.EncodeLogRecord()
		assert.NotNil(t, res)
		assert.Greater(t, n, int64(5))
		t.Logf("res: %+v \n", res)
	}
}

// crc: 2589323248  buf: [224 11 175 187 0 6 0]
// crc: 223330275
//     /Users/tanjie/Clown/go_code/kv-storage/kv-storage-go/structure/log_record_test.go:126:
//         	Error Trace:	/Users/tanjie/Clown/go_code/kv-storage/kv-storage-go/structure/log_record_test.go:126
//         	Error:      	Not equal:
//         	            	expected: 0xbbaf0be0
//         	            	actual  : 0xd4fbfe3
//         	Test:       	TestEncodeCRCFromBytes
// crc: 3707763229  buf: [169 84 109 124 1 6 24]
// crc: 1994958735

func TestDecodeLogRecordHeader(t *testing.T) {
	tests := []struct {
		testData   []byte
		testCrc    uint32
		keySize    uint32
		valueSize  uint32
		headerType LogRecordType
	}{
		{
			testData:   []byte{197, 186, 137, 81, 0, 6, 20},
			testCrc:    1367980741,
			keySize:    3,
			valueSize:  10,
			headerType: LogRecordNormal,
		},
		{
			testData:   []byte{224, 11, 175, 187, 0, 6, 0},
			testCrc:    3148811232,
			keySize:    3,
			valueSize:  0,
			headerType: LogRecordNormal,
		},
		{
			testData:   []byte{169, 84, 109, 124, 1, 6, 24},
			testCrc:    2087539881,
			keySize:    3,
			valueSize:  12,
			headerType: LogRecordDeleted,
		},
	}

	for i := range tests {
		header := &logRecordHeader{}
		size := header.DecodeLogRecordHeader(tests[i].testData)
		assert.Equal(t, int64(7), size)
		assert.Equal(t, tests[i].testCrc, header.crc)
		assert.Equal(t, tests[i].headerType, header.recordType)
		assert.Equal(t, tests[i].keySize, header.keySize)
		assert.Equal(t, tests[i].valueSize, header.valueSize)
	}
}

func TestEncodeCRCFromBytes(t *testing.T) {
	tests := []struct {
		testData  *LogRecord
		headerBuf []byte
		targetCrc uint32
	}{
		{
			testData: &LogRecord{
				Key:   []byte("test-1"),
				Value: []byte("this is test 1"),
				Type:  LogRecordNormal,
			},
			headerBuf: []byte{197, 186, 137, 81, 0, 6, 20},
			targetCrc: 3667967157,
		},
		{
			testData: &LogRecord{
				Key:  []byte("one"),
				Type: LogRecordNormal,
			},
			headerBuf: []byte{224, 11, 175, 187, 0, 6, 0},
			targetCrc: 3148811232,
		},
		{
			testData: &LogRecord{
				Key:   []byte("one"),
				Value: []byte("storage-kv-2"),
				Type:  LogRecordDeleted,
			},
			headerBuf: []byte{169, 84, 109, 124, 1, 6, 24},
			targetCrc: 2087539881,
		},
	}

	for i := range tests {
		crc := tests[i].testData.EncodeCRCFromBytes(tests[i].headerBuf[crcLength:])
		assert.Equal(t, tests[i].targetCrc, crc)
	}
}
