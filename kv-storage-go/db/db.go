package db

import (
	"errors"
	"os"
	"sync"

	"github.com/gofrs/flock"
	"github.com/tClown11/kv-storage/data"
	"github.com/tClown11/kv-storage/errs"
	"github.com/tClown11/kv-storage/fio"
	"github.com/tClown11/kv-storage/index"
)

// DB bitcask 存储引擎
type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIDs         []int                        // 文件 id ，只用在加载索引的时候
	activeFile      *data.StorageFile            // 当前活跃数据文件，可以用于写入
	olderFiles      map[uint32]*data.StorageFile // 旧数据文件，只用于读
	index           index.Indexer                // 内存索引
	seqNo           uint64                       // 事务序列号，全局递增
	isMerging       bool                         // 是否正在 merge
	seqNoFileExists bool                         // 存储事务序列号的文件是否存在
	isInitial       bool                         // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock                 // 文件锁保证多进程之间的互斥
	bytesWrite      uint                         // 累计写了多少个字节
	reclaimSize     int64                        // 表示有多少数据是无效的
}

func newDB(options Options) *DB {
	return &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.StorageFile),
		index: index.NewIndexer(&index.IndexOpts{
			Type: options.IndexType,
		}),
	}
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := newDB(options)

	// 加载数据文件
	if err := db.loadStorageFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromStorageFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return errors.New("")
	}

	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃的数据文件中
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, errs.ErrKeyIsEmpty
	}

	// 从内存数据结构中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)

	// 如果 key 不在内存索引中，说明 key 不存在
	if logRecordPos == nil {
		return nil, errs.ErrKeyNotFound
	}
	return db.getValueByPosition(logRecordPos)
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前是否存在活跃的数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	enRecord, size := logRecord.EncodeLogRecord()
	// 如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的日志记录文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化当前的活跃数据文件，保证已有的数据持久到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileID] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(enRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	// 根据用户配置决定是否持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 清空累积值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构建内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

// 设置当前活跃文件
func (db *DB) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileID + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenStorageFile(db.options.DirPath, initialFileID, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 根据索引信息获取对应的 value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件 id 找到对应的数据文件
	var storageFile *data.StorageFile
	if db.activeFile.FileID == logRecordPos.Fid {
		storageFile = db.activeFile
	} else {
		storageFile = db.olderFiles[logRecordPos.Fid]
	}

	// 数据文件不存在
	if storageFile == nil {
		return nil, errs.ErrDataFileNotFound
	}

	// 根据便宜读取对应的数据
	logRecord, _, err := storageFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, errs.ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize == 0 {
		return errors.New("database data file size must be greater than 0")
	}

	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}
