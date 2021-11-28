package table

import (
	"fmt"
	"testing"

	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
)

type stringWriter struct {
	data []byte
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
	if offset + n > uint64(len(s.data)) {
		return nil, file.ErrOutOfBoundary
	}

	return s.data[offset:offset+n], nil
}

func newStringReader() *stringReader {
	return &stringReader{}
}

func Test_Add(t *testing.T) {
	fileReader := newStringReader()
	fileWriter := newStringWriter(fileReader)
	tableWriter := NewWriter(fileWriter)
	
	tableWriter.Add(makeSlice("20185081"), makeSlice("李峻宇"))
	if err := tableWriter.Finish(); err != nil {
		t.Fatal(err)
	}

	table := getTable(t, fileReader)
	res, err := table.Get(makeSlice("20185081"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("res: %s\n", res)
}

func getTable(t *testing.T, fileReader *stringReader) *Table {
	table, err := New(fileReader, len(fileReader.data))
	if err != nil {
		t.Fatal(err)
	}

	return table
}

func makeSlice(s string) slice.Slice {
	return []byte(s)
}