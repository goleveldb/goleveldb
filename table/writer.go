package table

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/goleveldb/goleveldb/config"
	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
	"github.com/goleveldb/goleveldb/table/block"
)

type Writer interface {
	Add(k, v slice.Slice)
	Finish() error
}

type writerImpl struct {
	indexBlock block.Writer
	dataBlock  block.Writer
	file       file.Writer
	offset     uint64
	lastKey    slice.Slice
}

const (
	blockTailSize = 4 + 1 // extra bytes (4 for crc validation info, 1 for compression type) for block serialization

	noCompression byte = 0 // do not compress the value
)

var _ Writer = (*writerImpl)(nil)

// NewWriter: create a concrete instrance for TableWriter interface
func NewWriter(file file.Writer) Writer {
	return &writerImpl{
		indexBlock: block.NewWriter(),
		dataBlock:  block.NewWriter(),
		file:       file,
	}
}

// Add: add an entry to current table
func (t *writerImpl) Add(key, value slice.Slice) {
	t.dataBlock.AddEntry(key, value)
	currentSize := t.dataBlock.Size()
	if currentSize >= config.BLOCK_MAX_SIZE {
		blockOffset, blockSize := t.flush()
		blockHandle := &block.Handle{
			Offset: uint64(blockOffset),
			Size:   uint64(blockSize),
		}
		t.indexBlock.AddEntry(key, blockHandle.ToSlice())
	}

	t.lastKey = key
}

// flush: flush the content in TableWriter to storage
func (t *writerImpl) flush() (offset, size int) {
	offset, size = t.writeBlockContent(t.dataBlock.Finish(), noCompression)
	t.dataBlock.Reset()

	return
}

//	writerBlockContent: append block content with its type and crc info to file
//	format:
//  block_data: Slice
//	type : uint8
//	crc : uint32
// 	returns <offset, size> of the block written in the file
func (t *writerImpl) writeBlockContent(content slice.Slice, cType byte) (offset, size int) {
	if err := t.file.Append(content); err != nil {
		return
	}

	tail := make([]byte, blockTailSize)
	tail[0] = cType
	// TODO compression is not needed at this moment, so we set tail[0] to 0 in a fixed way
	checksum := crc32.Update(
		crc32.ChecksumIEEE(content), crc32.IEEETable, []byte{tail[0]})
	binary.BigEndian.PutUint32(tail[1:], checksum)
	if err := t.file.Append(tail); err != nil {
		return
	}
	if err := t.file.Flush(); err != nil {
		return
	}

	blockOffset, blockSize := t.offset, len(content)
	t.offset += uint64(len(content) + blockTailSize)

	return int(blockOffset), blockSize
}

// Finish: flush everything in the table to its file storage
// TODO metaindex block.
// currently, only index block and footer are implemented
func (t *writerImpl) Finish() error {
	// TODO meta index block

	// flush remaining data block if any new entry is written in it
	if t.dataBlock.Size() > 0 {
		dataBlockOffset, dataBlockSize := t.flush()
		blockHandle := &block.Handle{
			Offset: uint64(dataBlockOffset),
			Size:   uint64(dataBlockSize),
		}
		t.indexBlock.AddEntry(t.lastKey, blockHandle.ToSlice())
	}

	indexBlockOffset, indexBlockSize := t.writeBlockContent(t.indexBlock.Finish(), noCompression)

	// writer sstable footer
	tableFooter := &footer{
		indexHandle: &block.Handle{
			Offset: uint64(indexBlockOffset),
			Size:   uint64(indexBlockSize),
		},
	}
	if err := t.file.Append(tableFooter.toSlice()); err != nil {
		return err
	}
	if err := t.file.Flush(); err != nil {
		return err
	}
	t.offset += footerLength

	return nil
}
