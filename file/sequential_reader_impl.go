package file

import (
	"fmt"
	"os"

	"github.com/goleveldb/goleveldb/slice"
)

// seek param, seek from current pos.
const seekFromCurrentPos = 1

// NewSequentialReader 生成用于顺序读取文件的对象.
func NewSequentialReader(fileName string) (SequentialReader, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &sequentialReaderImpl{file: file}, nil
}

type sequentialReaderImpl struct {
	file *os.File
	buf  []byte
}

// Read 顺序读取文件 n 个 byte，以 Slice 的形式返回.
func (r *sequentialReaderImpl) Read(n int) (slice.Slice, error) {
	if len(r.buf) < n {
		r.buf = make([]byte, n)
	}

	if _, err := r.file.Read(r.buf); err != nil {
		return nil, err
	}

	res := make([]byte, n)
	copy(res, r.buf[:n])

	return res, nil
}

// Skip 跳过文件的 n 个 byte.
func (r *sequentialReaderImpl) Skip(n int) error {
	ret, err := r.file.Seek(int64(n), seekFromCurrentPos)
	if err != nil {
		return err
	}

	if ret < int64(n) {
		return fmt.Errorf("can not skip %d bytes, only skip %d bytes", n, ret)
	}

	return nil
}
