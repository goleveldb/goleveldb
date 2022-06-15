package db

import (
	"github.com/goleveldb/goleveldb/internal/utils/varstr"
	"github.com/goleveldb/goleveldb/internal/utils/varstr/varint"
	"github.com/goleveldb/goleveldb/slice"
)

// NewBatch create a new batch.
func NewBatch() Batch {
	batch := &batchImpl{}
	batch.Clear()

	return batch
}

var kEmptyBatch = []byte{
	0, 0, 0, 0, 0, 0, 0, 0, // sequence.
	0, 0, 0, 0, // count.
}

const (
	kBatchHeadLen = 12
	kOpTypeLen    = 1

	// Operation type.
	kDeleteOp = 0x0
	kPutOp    = 0x1
)

type batchImpl struct {
	// data store operations in batch.
	// data: [head][datas], datas is a slice of operations.
	// head: [sequence][count]
	// 	- sequence is a uint64.
	// 	- count is a uint32, means number of operations in datas.
	// datas:
	// 	- [kDeleteOp][varSlice]
	// 	- [kPutOp][varSlice][varSlice]
	// kXXOp is a one byte tag.
	// varSlice: [length][data], length is a varint number.
	data []byte
}

func (b *batchImpl) Put(key, value slice.Slice) {
	b.setCount(b.getCount() + 1)

	totalLen := kOpTypeLen + varint.VarintLen(len(key)) + len(key) + varint.VarintLen(len(value)) + len(value)
	insertOp := make([]byte, totalLen)
	curPos := 0

	insertOp[0] = kPutOp
	curPos += kOpTypeLen

	// set key value.
	curPos += varstr.PutVarStr(insertOp[curPos:], key)
	curPos += varstr.PutVarStr(insertOp[curPos:], value)

	b.data = append(b.data, insertOp...)
}

func (b *batchImpl) Delete(key slice.Slice) {
	b.setCount(b.getCount() + 1)

	totalLen := kOpTypeLen + varint.VarintLen(len(key)) + len(key)
	insertOp := make([]byte, totalLen)
	curPos := 0

	insertOp[0] = kDeleteOp
	curPos += kOpTypeLen

	curPos += varstr.PutVarStr(insertOp[curPos:], key)

	b.data = append(b.data, insertOp...)
}

func (b *batchImpl) Clear() {
	if len(b.data) > kBatchHeadLen {
		b.data = b.data[:kBatchHeadLen]
	} else {
		b.data = make([]byte, kBatchHeadLen)
	}

	copy(b.data, kEmptyBatch)
}

func (b *batchImpl) Append(batch Batch) {
	appendBatch := batch.(*batchImpl)
	b.setCount(b.getCount() + appendBatch.getCount())

	b.data = append(b.data, appendBatch.data[kBatchHeadLen:]...)
}

func (b *batchImpl) Iterate(handler BatchHandler) {
	count := b.getCount()

	pos := kBatchHeadLen
	for i := 0; i < int(count); i++ {
		opType := b.data[pos]
		pos += 1

		key, keySize := varstr.GetVarStr(b.data[pos:])
		pos += keySize

		if opType == kDeleteOp {
			handler.Delete(key)
		} else if opType == kPutOp {
			value, valueSize := varstr.GetVarStr(b.data[pos:])
			pos += valueSize

			handler.Put(key, value)
		}
	}
}
