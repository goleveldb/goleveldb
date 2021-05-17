// Package file 根据对文件的操作，定义了多种文件接口.
package file

import "github.com/goleveldb/goleveldb/slice"

// Writer 定义写文件操作
type Writer interface {
	// Append 将 data 追加到 写缓冲
	Append(data slice.Slice) error
	// Flush 将 写缓冲 内容同步到 文件系统
	Flush() error
	// Close 关闭文件
	Close() error
	// Sync flush 文件系统 buffer，保证内容被写入磁盘
	Sync() error
}
