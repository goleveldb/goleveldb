package table

import (
	"fmt"
	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
	"testing"
)

type entry struct {
	key   slice.Slice
	value slice.Slice
}

type testCase struct {
	name         string
	writeEntries []*entry
}

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

func assertTrue(t *testing.T, boolVal bool, assertMsg string) {
	if !boolVal {
		t.Fatal(assertMsg)
	}
}

func assertFalse(t *testing.T, boolVal bool, assertMsg string) {
	assertTrue(t, !boolVal, assertMsg)
}

func newTable(t *testing.T, stringReader *stringReader) *Table {
	table, err := New(stringReader, len(stringReader.data))
	assertTrue(t, err == nil, fmt.Sprintf("%s", err))

	return table
}

func TestTable_Get(t *testing.T) {
	testCases := []*testCase{
		{
			name:         "prefix compression & same value",
			writeEntries: entriesWithFixedValue("gggggg", "wdnmd_%d", 20480),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fileReader := newStringReader()
			fileWriter := newStringWriter(fileReader)
			tableWriter := NewWriter(fileWriter)
			for _, entry := range testCase.writeEntries {
				assertTrue(t, nil == tableWriter.Add(entry.key, entry.value), "append failed")
			}

			err := tableWriter.Finish()
			assertTrue(t, nil == err, fmt.Sprintf("%v", err))

			table := newTable(t, fileReader)
			for _, entry := range testCase.writeEntries {
				getVal, err := table.Get(entry.key)
				assertTrue(t, nil == err, fmt.Sprintf("write %s, gotErr %s", entry.key, err))
				assertTrue(t, getVal.Compare(entry.value) == 0,
					fmt.Sprintf("write %s, got %s", entry.key, getVal))
			}
		})
	}
}

func makeEntry(k, v string) *entry {
	return &entry{
		key:   []byte(k),
		value: []byte(v),
	}
}

func sortEntries(entries []*entry) {
	for i, length := 0, len(entries); i < length-1; i++ {
		for j := 0; j < length-1-i; j++ {
			if entries[j].key.Compare(entries[j+1].key) <= 0 {
				continue
			}

			entries[j], entries[j+1] = entries[j+1], entries[j]
		}
	}
}

func entriesWithFixedValue(fixedValue string, prefix string, genCount int) []*entry {
	res := make([]*entry, genCount)
	for i := 0; i < genCount; i++ {
		res[i] = makeEntry(fmt.Sprintf(prefix, i), fixedValue)
	}

	sortEntries(res)
	return res
}
