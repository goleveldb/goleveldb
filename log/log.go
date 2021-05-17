// Package log 实现日志读写操作.
package log

const (
	RecordFullType   = uint8(1)
	RecordFirstType  = uint8(2)
	RecordMiddleType = uint8(3)
	RecordLastType   = uint8(4)

	BlockSize  = 32768
	HeaderSize = 7 // 4 (checksum) + 2 (length) + 1 (type)
)
