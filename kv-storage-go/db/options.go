package db

import "github.com/tClown11/kv-storage/index"

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	// 索引类型
	IndexType index.IndexType

	// 启动时是否使用 MMap 加载数据
	MMapAtStartup bool

	//	数据文件合并的阈值
	DataFileMergeRatio float32
}
