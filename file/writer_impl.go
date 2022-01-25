package file

import (
	"bufio"
	"log"
	"os"

	"github.com/pkg/errors"

	"github.com/goleveldb/goleveldb/slice"
)

// 读写文件默认块大小.
const kFileBlockSize = (1 << 16)

var closedError = errors.New("file is closed")

// NewWriter 根据文件名创建写者.
func NewWriter(fileName string) (Writer, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	writer := &writerImpl{
		file:   file,
		writer: bufio.NewWriterSize(file, kFileBlockSize),
	}

	return writer, nil
}

type writerImpl struct {
	file   *os.File
	writer *bufio.Writer

	closed bool
}

// Append 将 data 追加到 写缓冲.
func (w *writerImpl) Append(data slice.Slice) error {
	if w.closed {
		return closedError
	}

	_, err := w.writer.Write(data)
	return err
}

// Flush 将 写缓冲 内容同步到 文件系统.
func (w *writerImpl) Flush() error {
	if w.closed {
		return closedError
	}

	return w.writer.Flush()
}

// Close 关闭文件.
func (w *writerImpl) Close() error {
	if w.closed {
		return closedError
	}

	if err := w.Flush(); err != nil {
		log.Println("before close file, flush error:", err)
	}

	if err := w.Sync(); err != nil {
		log.Println("before close file, sync error: ", err)
	}

	w.closed = true

	return w.file.Close()
}

// Sync flush 文件系统 buffer，保证内容被写入磁盘.
func (w *writerImpl) Sync() error {
	if w.closed {
		return closedError
	}

	if err := w.Flush(); err != nil {
		return errors.Wrap(err, "flush error")
	}

	return errors.Wrap(w.file.Sync(), "sync error")
}
