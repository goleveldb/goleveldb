package file

import (
	"errors"
	"os"

	"github.com/goleveldb/goleveldb/slice"
)

type RandomReaderImpl struct {
	file os.File
}

var _ RandomReader = (*RandomReaderImpl)(nil)

var (
	ErrOutOfBoundary = errors.New("read offset and size is out of boundary")
)

func NewRandomReader(fileName string) (RandomReader, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &RandomReaderImpl{
		file: *file,
	}, nil
}

func (r *RandomReaderImpl) Read(offset, n uint64) (slice.Slice, error) {
	fileInfo, err := r.file.Stat()
	if err != nil {
		return nil, err
	}

	size := fileInfo.Size()
	if offset+n > uint64(size) {
		return nil, ErrOutOfBoundary
	}

	buffer := make([]byte, n)
	if _, err := r.file.ReadAt(buffer, int64(offset)); err != nil {
		return nil, err
	}

	return buffer, nil
}
