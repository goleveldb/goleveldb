package log

import "github.com/goleveldb/goleveldb/slice"

// Writer 写日志.
type Writer interface {
	// AddRecord 向日志中添加条目.
	AddRecord(data slice.Slice) error
}
