package file

import (
	"fmt"
	"os"

	"github.com/goleveldb/goleveldb/slice"
)

type RandomReaderImpl struct {
	file os.File
}

var _ RandomReader = (*RandomReaderImpl)(nil)

var (
	ErrOutOfBoundary = "desired offset:%d, desired reading size:%d, file total size:%d"
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
	if offset + n > uint64(size) {
		return nil, fmt.Errorf(ErrOutOfBoundary, offset, n, size)
	}
	
	buffer := make([]byte, n)
	if _, err := r.file.ReadAt(buffer, int64(offset)); err != nil {
		return nil, err
	}

	return buffer, nil
}