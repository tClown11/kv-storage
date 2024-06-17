package db

import (
	"sync"
	"sync/atomic"

	"github.com/tClown11/kv-storage/errs"
	"github.com/tClown11/kv-storage/structure"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

type Writebatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*structure.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *Writebatch {
	return &Writebatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*structure.LogRecord),
	}
}

// Put 批量写入数据
func (wb *Writebatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存 LogRecord
	logRecord := &structure.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(logRecord.Key)] = logRecord
	return nil
}

// Delete 批量删除数据
func (wb *Writebatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在则直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存 LogRecord
	logRecord := &structure.LogRecord{Key: key, Type: structure.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Commit  提交事务，将暂存的数据写到数据文件中，并更新内存索引
func (wb *Writebatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return errs.ErrExceedMaxBatchNum
	}

	// 加锁保证事务提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 开始写数据到数据文件中
	positions := make(map[string]*structure.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecords, err := wb.db.appendLogRecord(&structure.LogRecord{
			Key:   record.Key,
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecords
	}

	// 写一条表示事务完成的数据
	finishedRecord := &structure.LogRecord{
		Type: structure.LogRecordTxnFinished,
	}
	finishedRecord.Key = finishedRecord.EncodeKeyWithSeq(txnFinKey, seqNo)
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *structure.LogRecordPos
		if record.Type == structure.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == structure.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*structure.LogRecord)

	return nil
}
