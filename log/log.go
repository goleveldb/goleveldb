// Package log 实现日志读写操作.
package log

const (
	RecordFullType   = 1
	RecordFirstType  = 2
	RecordMiddleType = 3
	RecordLastType   = 4

	BlockSize  = 32768
	HeaderSize = 7 // 4 (checksum) + 2 (length) + 1 (type)
)
