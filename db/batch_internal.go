package db

import "encoding/binary"

func (b *batchImpl) getCount() uint32 {
	return binary.BigEndian.Uint32(b.data[8:12])
}

func (b *batchImpl) setCount(count uint32) {
	binary.BigEndian.PutUint32(b.data[8:12], count)
}
