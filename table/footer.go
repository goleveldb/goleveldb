package table

import (
	"encoding/binary"
	"errors"

	"github.com/goleveldb/goleveldb/slice"
	"github.com/goleveldb/goleveldb/table/block"
)

type footer struct {
	indexHandle     *block.Handle
	metaIndexHandle *block.Handle
}

const (
	tableMagicNumber    uint64 = 0xdb4775248b80fb57
	footerPaddingLength        = 2 * (block.MaxBlockHandleLength - block.HandleLength)
	footerLength               = 2*block.MaxBlockHandleLength + 8
)

var errInvalidSSTable = errors.New("this sstable file is broken")

func newFooter(bytes []byte) (*footer, error) {
	if len(bytes) != footerLength {
		return nil, errInvalidSSTable
	}
	if tableMagicNumber != binary.BigEndian.Uint64(bytes[2*block.MaxBlockHandleLength:]) {
		return nil, errInvalidSSTable
	}

	res := footer{}
	res.indexHandle = block.NewHandle(bytes)
	res.metaIndexHandle = block.NewHandle(bytes[block.HandleLength:])

	return &res, nil
}

func (f *footer) toSlice() slice.Slice {
	res := make([]byte, footerLength)
	offset := 0
	offset += copy(res, f.indexHandle.ToSlice())
	// TODO resolve meta index instead of adding 16
	//offset += copy(res[offset:], f.metaIndexHandle.ToSlice())
	offset += block.HandleLength
	offset += footerPaddingLength
	binary.BigEndian.PutUint64(res[offset:], tableMagicNumber)

	return slice.Slice(res)
}
