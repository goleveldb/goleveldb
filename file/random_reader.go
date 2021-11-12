package file

import "github.com/goleveldb/goleveldb/slice"

type RandomReader interface {
	// 从file中<offset>处读取n个字节，以Slice的形式返回
	Read(offset, n uint64) (slice.Slice, error)
}