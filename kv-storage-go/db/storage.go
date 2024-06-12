package db

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/tClown11/kv-storage/data"
	"github.com/tClown11/kv-storage/errs"
	"github.com/tClown11/kv-storage/fio"
)

func (db *DB) loadStorageFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIDs []int
	// 遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, item := range dirEntries {
		if strings.HasSuffix(item.Name(), data.StorageFileNameSuffix) {
			splitNames := strings.Split(item.Name(), ".")
			fileID, err := strconv.Atoi(splitNames[0])
			// 数据目录可能被损坏
			if err != nil {
				return errs.ErrDataDirectoryCorrupted
			}
			fileIDs = append(fileIDs, fileID)
		}
	}

	// 对文件 id 进行排序，从小到大依次加载
	sort.Ints(fileIDs)
	db.fileIDs = fileIDs

	// 遍历每个文件ID，打开对应的数据文件
	for i, fid := range fileIDs {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}

		dataFile, err := data.OpenStorageFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}

		if i == len(fileIDs)-1 { // 最后一个 ID 是最大的，说明是当前活跃文件
			db.activeFile = dataFile
		} else { // 说明是旧的文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromStorageFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIDs) == 0 {
		return nil
	}

	// todo: 查看是否发生过 merge
	//hasMerge, noMergeFileID := false, uint32(0)

	// 遍历所有的文件 id， 处理文件中的记录
	for i, fid := range db.fileIDs {
		var fileID = uint32(fid)
		var storageFile *data.StorageFile
		if fileID == db.activeFile.FileID {
			storageFile = db.activeFile
		} else {
			storageFile = db.olderFiles[fileID]
		}

		offset, err := db.writeCache(fileID, storageFile)
		if err != nil {
			return err
		}

		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOff = int64(offset)
		}
	}
	return nil
}

// writeCache 将
func (db *DB) writeCache(fileID uint32, file *data.StorageFile) (int64, error) {
	var offset int64 = 0

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	for {
		logRecord, size, err := file.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return offset, err
		}

		// 构造内存索引并保存
		logRecordPos := &data.LogRecordPos{
			Fid:    fileID,
			Offset: int64(offset),
			Size:   uint32(size),
		}

		// 更新索引
		updateIndex(logRecord.Key, logRecord.Type, logRecordPos)

		// 递增 offset，下一次从新的位置开始读取
		offset += size
	}
	return offset, nil
}
