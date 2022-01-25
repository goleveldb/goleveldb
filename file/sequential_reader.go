package file

import "github.com/goleveldb/goleveldb/slice"

// SequentialReader 定义顺序读取文件操作.
type SequentialReader interface {
	// Read 顺序读取文件 n 个 byte，以 Slice 的形式返回.
	Read(n int) (slice.Slice, error)
	// Skip 跳过文件的 n 个 byte.
	Skip(n int) error
}
