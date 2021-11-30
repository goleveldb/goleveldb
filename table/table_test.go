package table

import (
	"testing"

	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
)

type opType bool

type testCase struct {
	name string
	ops []*op
}

type entry struct {
	key slice.Slice
	value slice.Slice
}

type op struct {
	operationType opType
	write []*entry
	read []*entry
}

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

const (
	opRead opType = false
	opWrite opType = true
)

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
	testCases := buildAddTestCases()
	for _, testCase := range testCases {
		fileReader := newStringReader()
		fileWriter := newStringWriter(fileReader)
		tableWriter := NewWriter(fileWriter)

		t.Run(testCase.name, func (t *testing.T)  {
			for _, op := range testCase.ops {
				if op.operationType == opRead {
					table := getTable(t, fileReader)
					for _, entry := range op.read {
						val, err := table.Get(entry.key)
						if err != nil {
							t.Fatal(err)
						}
						if val.Compare(entry.value) != 0 {
							t.Fatal(err)
						}
					}
				}else {
					// make sure the write slice keys are sorted
					sortEntries(op.write)
					for _, entry := range op.write {
						tableWriter.Add(entry.key, entry.value)
					}

					if err := tableWriter.Finish(); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func sortEntries(entries []*entry) {
	for i, length := 0, len(entries); i < length - 1; i++ {
		for j := 0; j < length - 1 - i; j++ {
			if entries[j].key.Compare(entries[j+1].key) <= 0 {
				continue
			}

			entries[j], entries[j+1] = entries[j+1], entries[j]
		}
	}
}

func buildAddTestCases() []*testCase {
	res := make([]*testCase, 0)
	// case 1. simple read and write
	commonEntries := []*entry{
		makeEntry("20185081", "li, junyu"),
		makeEntry("wdnmd", "wdnmd??"),
		makeEntry("apple", "banana"),
		makeEntry("&*^*&^*", "hhhhhhhhhh"),
	}
	res = append(res, &testCase{
		name: "simple read and write",
		ops: []*op {
			{
				operationType: opWrite,
				write:commonEntries,
			},
			{
				operationType: opRead,
				read: commonEntries,
			},
		},
	})

	return res
}

func makeEntry(key string, value string) *entry {
	return &entry{
		key: makeSlice(key),
		value: makeSlice(value),
	}
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