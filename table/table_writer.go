package table

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/goleveldb/goleveldb/table/block"
	"github.com/goleveldb/goleveldb/config"
	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
)

type TableWriter interface {
	Add(k,v slice.Slice)
	Finish() error
}

type tableWriterImpl struct {
	indexBlock block.Writer
	dataBlock block.Writer
	file file.Writer
	offset uint64
	lastKey slice.Slice
}

const (
	blockTailSize = 4 + 1	// 写块尾的大小，包括了4字节的crc校验码和1字节的压缩类型信息
)

var _ TableWriter = (*tableWriterImpl)(nil)

func NewWriter(file file.Writer) TableWriter {
	return &tableWriterImpl{
		indexBlock: block.NewWriter(),
		dataBlock: block.NewWriter(),
		file: file,
	}
}

func (t *tableWriterImpl) Add(key, value slice.Slice) {
	t.dataBlock.AddEntry(key, value)
	currentSize := t.dataBlock.Size()
	if currentSize >= config.BLOCK_MAX_SIZE {
		dbOffset, dbSize := t.flush()
		dbHandle := &block.Handle{
			Offset: uint64(dbOffset),
			Size: uint64(dbSize),
		}
		t.indexBlock.AddEntry(key, dbHandle.ToSlice())
	}

	t.lastKey = key
}

func (t *tableWriterImpl) flush() (offset, size int) {
	defer t.dataBlock.Reset()
	return t.writeBlockContent(t.dataBlock.Finish())
}

// 写块内容并且附加压缩和crc信息
//	格式
//  block_data
//	type : uint8
//	crc : uint32
// 返回该块的位置信息 <offset, size>, 注意size不包含额外的5B信息（crc校验&type信息）
func (t *tableWriterImpl) writeBlockContent(content slice.Slice) (offset, size int) {
	if err := t.file.Append(content); err != nil {
		return
	}

	tail := make([]byte, blockTailSize)
	// TODO 这里将压缩信息固定为0，表示不进行压缩
	checksum := crc32.Update(
		crc32.ChecksumIEEE(content), crc32.IEEETable, []byte{tail[0]})
	binary.BigEndian.PutUint32(tail[1:], checksum)
	if err := t.file.Append(tail); err != nil {
		return
	}
	
	blockOffset, blockSize := t.offset, len(content)
	t.offset += uint64(len(content) + blockTailSize)

	return int(blockOffset), blockSize
}

func (t *tableWriterImpl) Finish() error {
	// TODO meta index block

	// 写index block
	if t.dataBlock.Size() > 0 {
		lastBlockHandle := &block.Handle{
			Offset: t.offset,
			Size: uint64(t.dataBlock.Size()),
		}
		t.indexBlock.AddEntry(t.lastKey, lastBlockHandle.ToSlice())
	}
	ibOffset, ibSize := t.writeBlockContent(t.indexBlock.Finish())

	// 写sstable footer
	tableFooter := &footer{
		indexHandle: &block.Handle{
			Offset: uint64(ibOffset),
			Size: uint64(ibSize),
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