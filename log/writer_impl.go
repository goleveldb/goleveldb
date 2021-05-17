package log

import (
	"errors"
	"hash/crc32"

	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
)

const ()

// WriterImpl 实现 Writer 接口提供写日志服务.
type WriterImpl struct {
	fileWriter  file.Writer
	blockOffset int // 当前块内偏移.
}

// AddRecord 将data写入日志， 写入失败时返回 error.
func (w *WriterImpl) AddRecord(data slice.Slice) error {
	left := len(data)

	first := true
	for left != 0 {
		freeSize := BlockSize - w.blockOffset

		if freeSize < HeaderSize {
			if freeSize > 0 {
				w.fileWriter.Append(make(slice.Slice, freeSize))
			}
			w.blockOffset = 0
		}

		// 获取本次写入的长度.
		writableLen := BlockSize - w.blockOffset - HeaderSize
		writeLen := left
		if writeLen > writableLen {
			writeLen = writableLen
		}

		// 确定本次写入 Record 类型.
		var (
			last       = writeLen == left
			recordType uint8
		)
		if first && last {
			recordType = RecordFullType
		} else if first {
			recordType = RecordFirstType
		} else if last {
			recordType = RecordLastType
		} else {
			recordType = RecordMiddleType
		}

		if err := w.writeRecord(data[:writeLen], recordType); err != nil {
			return err
		}

		left -= writeLen
		first = false
		data = data[writeLen:]
	}

	return nil
}

// writeRecord 将 Record 头部与data封装后写入文件， 成功写入时会修改blockOffset.
func (w *WriterImpl) writeRecord(data slice.Slice, recordType uint8) error {
	if len(data)+HeaderSize+w.blockOffset > BlockSize {
		return errors.New("data toolong, can not write")
	}

	crc := crc32.ChecksumIEEE(data)
	lendata := len(data)
	header := []byte{
		byte(crc >> 24), byte(crc >> 16), byte(crc >> 8), byte(crc),
		byte(lendata >> 8), byte(lendata),
		recordType,
	}

	if err := w.fileWriter.Append(header); err != nil {
		return err
	}

	if err := w.fileWriter.Append(data); err != nil {
		return err
	}

	if err := w.fileWriter.Flush(); err != nil {
		return err
	}

	w.blockOffset += lendata + HeaderSize

	return nil
}
