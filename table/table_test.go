package table

import (
	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
)

type stringWriter struct {
	data   []byte
	reader *stringReader
}

var _ file.Writer = (*stringWriter)(nil)

func (s *stringWriter) Append(data slice.Slice) error {
	s.data = append(s.data, data...)
	s.reader.data = s.data

	return nil
}

func (*stringWriter) Flush() error {
	return nil
}

func (*stringWriter) Close() error {
	return nil
}

func (*stringWriter) Sync() error {
	return nil
}

func newStringWriter(s *stringReader) file.Writer {
	return &stringWriter{
		reader: s,
	}
}

type stringReader struct {
	data []byte
}

var _ file.RandomReader = (*stringReader)(nil)

func (s *stringReader) Read(offset, n uint64) (slice.Slice, error) {
	if offset+n > uint64(len(s.data)) {
		return nil, file.ErrOutOfBoundary
	}

	return s.data[offset : offset+n], nil
}

func newStringReader() *stringReader {
	return &stringReader{}
}
