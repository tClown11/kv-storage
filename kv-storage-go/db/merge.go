package db

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/tClown11/kv-storage/errs"
	"github.com/tClown11/kv-storage/structure"
	"github.com/tClown11/kv-storage/utils"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
	// 如果数据库为空，则直接返回
	if db.activeFile == nil {
		return nil
	}

	// 如果 merge 正在进行中，则直接返回
	if db.isMerging {
		return errs.ErrMergeIsProgress
	}

	db.mu.Lock()

	// 查看 merge 的数据量是否已达到阈值
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return errs.ErrMergeRatioUnreached
	}

	// 查看剩余的空间是否可以容纳 merge 时生成的临时目录
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return errs.ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 将当前活跃文件转换为旧的数据文件
	db.olderFiles[db.activeFile.FileID] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 记录不参与 merge 的文件 id
	nonMergeFileID := db.activeFile.FileID

	// 取出所有需要 merge 的文件
	var mergeFiles []*structure.StorageFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 待 merge 的文件，从小到大进行排序，依次 merge ( 考虑是否可以优化 merge 顺序，以减少 IO 读取消耗 )
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过 merge，将其删除掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开一个新的临时 bitcask 实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开 hint 文件存储索引
	hintFile, err := structure.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 解析拿到实际的 key
			realKey, _ := structure.ParseKeyAndSeqFromLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 与内存中的索引位置进行比较，如果有效则重写
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileID &&
				logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = structure.EncodeKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将当前位置索引写到 Hint 文件当中
				if err = hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			// 增加 offset
			offset += size
		}
	}

	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinishedFile, err := structure.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &structure.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileID))),
	}
	encRecord, _ := mergeFinRecord.EncodeLogRecord()
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// merge 目录不存在，直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找表示 merge 完成的文件，判断 merge 是否处理完
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		switch entry.Name() {
		case structure.MergeFinishedfileName:
			mergeFinished = true
		case structure.SeqNoFileName, fileLockName:
			continue
		}

		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 没有 merge 完成则直接返回
	if !mergeFinished {
		return nil
	}

	nonMergeFiledID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return nil
	}

	// 删除旧的数据文件
	var fileID uint32 = 0
	for ; fileID < nonMergeFiledID; fileID++ {
		fileName := structure.GetStorageFileName(db.options.DirPath, fileID)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileID(dirPath string) (uint32, error) {
	mergeFinishedFile, err := structure.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}

	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileID), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, structure.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开 hint 索引文件
	hintFile, err := structure.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到实际的位置索引
		pos := structure.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
