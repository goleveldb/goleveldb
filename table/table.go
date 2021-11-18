package table

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
	"github.com/goleveldb/goleveldb/table/block"
)

type Table struct {
	IndexBlock *block.Block
	File file.RandomReader
}

var (
	ErrCrcValidation = "the stored crc32 value is %d, but the calculated value is %d, consider the block broken"
	ErrNoSuchKey = "cannot find the value for key %s"
)

func New(file file.RandomReader, size int) (*Table, error) {
	// decode footer information
	footerBytes, err := file.Read(uint64(size - footerLength), footerLength)
	if err != nil {
		return nil, err
	}
	
	footer, err := newFooter(footerBytes)
	if err != nil {
		return nil, err
	}

	indexBlockSlice, err := readBlock(footer.indexHandle, file)
	if err != nil {
		return nil, err
	}

	return &Table{
		IndexBlock: block.New(indexBlockSlice),
		File: file,
	}, nil
}

func readBlock(handle *block.Handle, file file.RandomReader) (slice.Slice, error) {
	content, err := file.Read(handle.Offset, handle.Size + blockTailSize)
	if err != nil {
		return nil, err
	}

	crc := binary.BigEndian.Uint32(content[handle.Size+1:])
	contentCrc := crc32.ChecksumIEEE(content[:handle.Size+1])
	if crc != contentCrc {
		return nil, fmt.Errorf(ErrCrcValidation, crc, contentCrc)
	}

	return content[:handle.Size], nil
}

func (t *Table) Get(key slice.Slice) (slice.Slice, error) {
	blockIter := block.NewIter(t.IndexBlock)
	blockIter.Find(key)
	if !blockIter.Success() {
		return nil, fmt.Errorf(ErrNoSuchKey, key)
	}

	handle := block.NewHandle(blockIter.Value())
	blockContent, err := readBlock(handle, t.File)
	if err != nil {
		return nil, err
	}

	dataBlock := block.New(blockContent)
	dataBlockIter := block.NewIter(dataBlock)
	dataBlockIter.Find(key)
	if !dataBlockIter.Success() {
		return nil, fmt.Errorf(ErrNoSuchKey, key)
	}
	// revalidate the keys
	if key.Compare(dataBlockIter.Key()) > 0 {
		return nil, fmt.Errorf(ErrNoSuchKey, key)
	}
	
	return dataBlockIter.Value(), nil
}