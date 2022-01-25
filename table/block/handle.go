package block

import (
	"encoding/binary"

	"github.com/goleveldb/goleveldb/slice"
)

// Handle : represents a specific block space in a file
type Handle struct {
	Offset uint64
	Size   uint64
}

const (
	HandleLength         = 2 * 8 // 一个blockHandle的实际大小 16B
	MaxBlockHandleLength = 20    // 序列化blockHandle所需要的最大空间 = 20B
)

func NewHandle(bytes []byte) *Handle {
	handle := Handle{}
	handle.Offset = binary.BigEndian.Uint64(bytes)
	handle.Size = binary.BigEndian.Uint64(bytes[8:])

	return &handle
}

func (b *Handle) ToSlice() slice.Slice {
	handle, pos := make([]byte, HandleLength), 0
	binary.BigEndian.PutUint64(handle, b.Offset)
	pos += 8
	binary.BigEndian.PutUint64(handle[pos:], b.Size)

	return handle
}
