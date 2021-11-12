package table

import (
	"encoding/binary"
	"errors"

	"github.com/goleveldb/goleveldb/slice"
	"github.com/goleveldb/goleveldb/table/block"
)

type footer struct {
	indexHandle *block.Handle
	metaIndexHandle *block.Handle
}

const (
	TABLE_MAGIC_NUMBER uint64 = 0xdb4775248b80fb57
	FOOTER_PADDING_LENGTH = 2 * (block.MAX_BLOCK_HANDLE_LENGTH - block.BLOCK_HANDLE_LENGTH)
	FOOTER_LENGTH = 2 * block.MAX_BLOCK_HANDLE_LENGTH + 8
)

var ERR_INVALID_SSTABLE = errors.New("This sstable file is broken")

func newFooter(bytes []byte) (*footer, error) {
	if len(bytes) != FOOTER_LENGTH {
		return nil, ERR_INVALID_SSTABLE
	}
	if TABLE_MAGIC_NUMBER != binary.BigEndian.Uint64(bytes[2*block.MAX_BLOCK_HANDLE_LENGTH:]) {
		return nil, ERR_INVALID_SSTABLE
	}

	res := footer{}
	res.metaIndexHandle = block.NewHandle(bytes)
	res.indexHandle = block.NewHandle(bytes[block.BLOCK_HANDLE_LENGTH:])

	return &res, nil
}

func (f *footer) toSlice() slice.Slice {
	res := make([]byte, FOOTER_LENGTH)
	offset := 0
	offset += copy(res, f.indexHandle.ToSlice())
	offset += copy(res[offset:], f.metaIndexHandle.ToSlice())
	offset += FOOTER_PADDING_LENGTH
	binary.BigEndian.PutUint64(res[offset:], TABLE_MAGIC_NUMBER)

	return slice.Slice(res)
}