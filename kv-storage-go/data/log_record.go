// Package data 是管理存储数据的包
package data

// LogRecordPos 数据内存索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储在哪个文件中
	Offset int64  // 偏移，表示将数据存储到了数据文件的具体位置偏移量
}
