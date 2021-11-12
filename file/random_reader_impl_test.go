package file

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/goleveldb/goleveldb/slice"
)

func Test_RandomReaderImpl_NewRandomReader(t *testing.T) {
	t.Run("file exists", func (t *testing.T)  {
		tmpFile, err := ioutil.TempFile(".", "test_exist")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpFile.Name())

		_, err = NewRandomReader(tmpFile.Name())
		assert(t, err == nil)
	})

	t.Run("file not exist", func(t *testing.T) {
		wdnmdFileName := "qZ_WDNMD"
		if _, err := NewRandomReader(wdnmdFileName); err == nil {
			t.Fatal(fmt.Sprintf("file %s should not exist, but it does", wdnmdFileName))
		}
	})
}

func Test_RandomReaderImpl_Read(t *testing.T) {
	type eachRead struct {
		offset uint64
		size uint64
		hasErr bool
	}
	type testCase struct {
		writeContent string
		readings []eachRead
	}

	testCases := []testCase {
		{
			writeContent: "hello world",
			readings: []eachRead{
				{offset: 1, size: 3, hasErr: false},
				{offset: 0, size: 13, hasErr: true},
			},
		},
	}
	tmpFileName := "./tmp_file_wdnmd_%d"

	for i, testCase := range testCases {
		fileName := fmt.Sprintf(tmpFileName, i)
		writer, err := NewWriter(fileName)
		assert(t, nil == err)
		defer func() {
			assert(t, nil == os.Remove(fileName))
		}()

		writer.Append(slice.Slice(testCase.writeContent))
		assert(t, nil == writer.Close())

		reader, err := NewRandomReader(fileName)
		assert(t, nil == err)
		for _, reading := range testCase.readings {
			content, err := reader.Read(reading.offset, reading.size)
			hasErr := err != nil
			assert(t, hasErr == reading.hasErr)
			if hasErr {
				continue
			}

			memContent := testCase.writeContent[reading.offset:reading.offset+reading.size]
			assert(t, len(content) == len(memContent))
			for i := 0; i < len(content); i++ {
				assert(t, content[i] == memContent[i])
			}
		}
	}
}

func assert(t *testing.T, boolVal bool) {
	if !boolVal {
		t.Fatal()
	}
}