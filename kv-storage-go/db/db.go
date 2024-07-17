package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/gofrs/flock"
	"github.com/tClown11/kv-storage/errs"
	"github.com/tClown11/kv-storage/fio"
	"github.com/tClown11/kv-storage/index"
	"github.com/tClown11/kv-storage/structure"
	"github.com/tClown11/kv-storage/utils"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB bitcask 存储引擎
type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIDs         []int                             // 文件 id ，只用在加载索引的时候
	activeFile      *structure.StorageFile            // 当前活跃数据文件，可以用于写入
	olderFiles      map[uint32]*structure.StorageFile // 旧数据文件，只用于读
	index           index.Indexer                     // 内存索引
	seqNo           uint64                            // 事务序列号，全局递增
	isMerging       bool                              // 是否正在 merge
	seqNoFileExists bool                              // 存储事务序列号的文件是否存在
	isInitial       bool                              // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock                      // 文件锁保证多进程之间的互斥
	bytesWrite      uint                              // 累计写了多少个字节
	reclaimSize     int64                             // 表示有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // key 的总数量
	DataFileNum     uint  // 数据文件的数量
	ReclaimableSize int64 // 可以进行 merge 回收的数据量，字节为单位
	DiskSize        int64 // 数据目录所占磁盘空间大小
}

func newDB(options Options) *DB {
	return &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*structure.StorageFile),
		index: index.NewIndexer(&index.IndexOpts{
			Type:    options.IndexType,
			DirPath: options.DirPath,
			Sync:    options.SyncWrites,
		}),
	}
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, errs.ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := newDB(options)
	db.isInitial = isInitial
	db.fileLock = fileLock

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadStorageFiles(); err != nil {
		return nil, err
	}

	// 索引加载

	// 从 hint 索引文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromStorageFiles(); err != nil {
		return nil, err
	}

	// 获取事务已操作的序列号
	if err := db.loadSeqNo(); err != nil {
		return nil, err
	}
	if db.activeFile != nil {
		size, err := db.activeFile.IoManager.Size()
		if err != nil {
			return nil, err
		}
		db.activeFile.WriteOff = size
	}

	// // 重置 IO 类型为标准文件 IO
	// if db.options.MMapAtStartup {
	// 	if err := db.resetIoType(); err != nil {
	// 		return nil, err
	// 	}
	// }

	return db, nil
}

func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	log_record := &structure.LogRecord{
		Key:   structure.EncodeKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  structure.LogRecordNormal,
	}

	// 追加写入到当前活跃的数据文件中
	pos, err := db.appendLogRecordLock(log_record)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	// 检查 key 是否存在，如果不存在则返回
	if pos := db.index.Get(key); pos == nil {
		return errs.ErrKeyNotFound
	}

	// 构造 LogRecord, 表示其是被删除的
	logRecord := &structure.LogRecord{
		Key:  structure.EncodeKeyWithSeq(key, nonTransactionSeqNo),
		Type: structure.LogRecordDeleted,
	}
	// 写入到数据文件中
	_, err := db.appendLogRecordLock(logRecord)
	if err != nil {
		return err
	}

	// 从内存索引中删除对应的 key
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return errs.ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Get 根据 key 读取数据
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

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Backup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		// 释放文件锁
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}

		// 关闭索引
		if err := db.index.Close(); err != nil {
			panic("failed to close index")
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 事务处理

	seqNoFile, err := structure.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &structure.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := record.EncodeLogRecord()
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//	关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 持久化数据到文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) appendLogRecordLock(logRecord *structure.LogRecord) (*structure.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *structure.LogRecord) (*structure.LogRecordPos, error) {
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
	pos := &structure.LogRecordPos{
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
	dataFile, err := structure.OpenStorageFile(db.options.DirPath, initialFileID, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 根据索引信息获取对应的 value
func (db *DB) getValueByPosition(logRecordPos *structure.LogRecordPos) ([]byte, error) {
	// 根据文件 id 找到对应的数据文件
	var storageFile *structure.StorageFile
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

	if logRecord.Type == structure.LogRecordDeleted {
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

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, structure.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := structure.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true
	return os.Remove(fileName)
}
