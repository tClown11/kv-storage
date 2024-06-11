package index

import "github.com/tClown11/kv-storage/data"

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) (*data.LogRecordPos, bool)
}
