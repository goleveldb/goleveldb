package log

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
	"github.com/pkg/errors"
)

// Reader 定义读日志操作.
type Reader interface {
	// ReadRecord 读取一个逻辑 Record.
	ReadRecord() (slice.Slice, error)
	// GetLastRecordOffset 获取最后读出的 Record 相对文件头部的偏移量.
	GetLastRecordOffset() int
}

// ReaderImpl 实现 Reader 接口, 读取日志.
type ReaderImpl struct {
	file.SequentialReader
	LastRecordOffset int
	endOfBufOffset   int
	buf              slice.Slice

	reporter Reporter
}

// ReadRecord 读取一个逻辑 Record.
func (r *ReaderImpl) ReadRecord() (record slice.Slice, err error) {
	inFragment := false
	for {
		data, recordType, err := r.readPhysicalRecord()
		if err != nil {
			r.reporter.Corruption(errors.Wrap(err, "read physical record error"))

			return nil, err
		}

		// 当前物理 Record 的起始偏移量.
		// 计算方法：当前 Record 起始地址 = buf 总偏移量 - 未读取数据长度 - 当前 Record 长度.
		physicalRecordOffset := r.endOfBufOffset - len(r.buf) - HeaderSize - len(data)

		switch recordType {
		case RecordFullType:
			if inFragment {
				r.reporter.Corruption(errors.New("get full type record, but in_fragment"))
			}

			r.LastRecordOffset = physicalRecordOffset
			return data, nil

		case RecordFirstType:
			if inFragment {
				r.reporter.Corruption(errors.New("get first type record, but in_fragment"))
			}

			inFragment = true
			r.LastRecordOffset = physicalRecordOffset
			record = data

		case RecordMiddleType:
			if !inFragment {
				r.reporter.Corruption(errors.New("get middle type record, but not in_fragment"))
			} else {
				record = append(record, data...)
			}

		case RecordLastType:
			if !inFragment {
				r.reporter.Corruption(errors.New("get last type record, but not in_fragment"))
			} else {
				record = append(record, data...)

				return record, nil
			}

		default:
			err = errors.New("unknown record type")
			r.reporter.Corruption(err)

			return nil, err
		}
	}
}

// readPhysicalRecord 读取一个物理 Record, 并返回该 Record 的 data 部分.
func (r *ReaderImpl) readPhysicalRecord() (record slice.Slice, recordType int, err error) {
	for {
		if len(r.buf) < HeaderSize {
			buf, err := r.SequentialReader.Read(BlockSize)
			if err != nil {
				return nil, 0, err
			}

			r.buf = buf
			r.endOfBufOffset += len(r.buf)
			continue
		}

		length := int(binary.BigEndian.Uint16(r.buf[4:6]))
		if length+HeaderSize > len(r.buf) {
			return nil, 0, errors.New("len(record) < header.length")
		}

		record = r.buf[HeaderSize : HeaderSize+length]
		crc := binary.BigEndian.Uint32(r.buf[:4])
		if crc != crc32.ChecksumIEEE(record) {
			return nil, 0, errors.New("checksum not equal")
		}

		recordType = int(r.buf[6])
		r.buf = r.buf[HeaderSize+length:]

		return record, recordType, nil
	}
}
