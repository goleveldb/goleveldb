package log

import (
	"io"
	"strings"
	"testing"

	"github.com/goleveldb/goleveldb/internal/mock/mock_file"
	"github.com/goleveldb/goleveldb/internal/mock/mock_log"

	"github.com/golang/mock/gomock"
	"github.com/goleveldb/goleveldb/slice"
	"github.com/pkg/errors"
)

func TestReaderImpl_ReadRecord(t *testing.T) {
	// 测试正常写入后的读取.
	testNormalReadAfterWrite_ReaderImpl_ReadRecord(t)
	// 测试在文件损坏的情况下进行读取时 报错信息是否正常.
	testLogFileDamagedRead_ReaderImpl_ReadRecord(t)
}

func TestReaderImpl_GetLastRecordOffset(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{"", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ReaderImpl{}
			if got := r.GetLastRecordOffset(); got != tt.want {
				t.Errorf("ReaderImpl.GetLastRecordOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}

// testNormalReadAfterWrite_ReaderImpl_ReadRecord 测试正常写入后的读取.
func testNormalReadAfterWrite_ReaderImpl_ReadRecord(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockReporter := mock_log.NewMockReporter(mockCtrl)
	mockSequentialReader := mock_file.NewMockSequentialReader(mockCtrl)

	tests := []struct {
		name                  string
		wantRecords           []slice.Slice
		sequentialReaderError bool
		wantErr               bool
	}{
		{
			name: "test signal full record",
			wantRecords: []slice.Slice{
				make(slice.Slice, 1000),
			},
			sequentialReaderError: false,
			wantErr:               false,
		},
		{
			name: "test two part record",
			wantRecords: []slice.Slice{
				make(slice.Slice, BlockSize),
			},
			sequentialReaderError: false,
			wantErr:               false,
		},
		{
			name: "test multiple part record",
			wantRecords: []slice.Slice{
				make(slice.Slice, BlockSize*2),
			},
			sequentialReaderError: false,
			wantErr:               false,
		},
		{
			name: "test write and read many records",
			wantRecords: []slice.Slice{
				make(slice.Slice, 1000),
				make(slice.Slice, BlockSize*2),
				make(slice.Slice, BlockSize),
			},
			sequentialReaderError: false,
			wantErr:               false,
		},
		{
			name: "test read file error",
			wantRecords: []slice.Slice{
				make(slice.Slice, BlockSize*2),
			},
			sequentialReaderError: true,
			wantErr:               true,
		},
	}

	var readCount int
	var blocks []slice.Slice
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readCount = 0
			blocks = generateLogFileBlocks(mockCtrl, tt.wantRecords)

			mockReporter.EXPECT().Corruption(gomock.Any()).AnyTimes()
			mockSequentialReader.EXPECT().Read(gomock.Any()).AnyTimes().DoAndReturn(
				func(n int) (slice.Slice, error) {
					if tt.sequentialReaderError {
						return nil, errors.New("err")
					}

					if readCount < len(blocks) {
						data := blocks[readCount]
						readCount++
						return data, nil
					}

					return nil, io.EOF
				},
			)

			r := &ReaderImpl{
				SequentialReader: mockSequentialReader,
				LastRecordOffset: 0,
				endOfBufOffset:   0,
				buf:              nil,
				reporter:         mockReporter,
			}

			// 顺序读取所有写入的 record.
			for index := 0; index < len(tt.wantRecords); index++ {
				record, err := r.ReadRecord()
				if (err != nil) != tt.wantErr {
					t.Errorf("want err = %v, but got err = %v", tt.wantErr, err)
				}

				if !tt.wantErr && record.Compare(tt.wantRecords[index]) != slice.CMPSame {
					t.Error("read record error: not equal")
				}
			}
		})
	}
}

// generateLogFileBlocks 将多条 record 以 block 的形式 写入 blocks.
func generateLogFileBlocks(mockCtrl *gomock.Controller, records []slice.Slice) (blocks []slice.Slice) {
	block := make(slice.Slice, 0)
	blocks = []slice.Slice{}
	mockWriter := mock_file.NewMockWriter(mockCtrl)

	mockWriter.EXPECT().Append(gomock.Any()).AnyTimes().DoAndReturn(func(data slice.Slice) error {
		block = append(block, data...)

		return nil
	})

	mockWriter.EXPECT().Flush().AnyTimes().DoAndReturn(func() error {
		blocks = append(blocks, block)
		block = make(slice.Slice, 0)

		return nil
	})

	writer := &WriterImpl{fileWriter: mockWriter}

	for _, record := range records {
		if err := writer.AddRecord(record); err != nil {
			mockCtrl.T.Fatalf("add record error ", err)
		}
	}

	if len(block) != 0 {
		if err := mockWriter.Flush(); err != nil {
			mockCtrl.T.Fatalf("flush error", err)
		}
	}

	return blocks
}

// testLogFileDamagedRead_ReaderImpl_ReadRecord 测试 log 文件损坏后的读取时 报错信息是否正常.
func testLogFileDamagedRead_ReaderImpl_ReadRecord(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockReporter := mock_log.NewMockReporter(mockCtrl)
	mockSequentialReader := mock_file.NewMockSequentialReader(mockCtrl)

	generateLogFileBlocks(mockCtrl, []slice.Slice{make(slice.Slice, 2)})

	// 本测试集中所有样例都期望通过reporter报告错误.
	tests := []struct {
		name         string
		block        slice.Slice
		errorKeyWord string // 通过 errorKeyWord 判断错误类型是否正确.
	}{
		{
			name: "test full type in fragment",
			block: []byte{
				// first type record
				65, 217, 18, 255, 0, 2, 2, 0, 0,
				// full type
				65, 217, 18, 255, 0, 2, 1, 0, 0,
			},
			errorKeyWord: "get full type record, but in_fragment",
		},
		{
			name: "test first type in fragment",
			block: []byte{
				// first type record
				65, 217, 18, 255, 0, 2, 2, 0, 0,
				// first type
				65, 217, 18, 255, 0, 2, 2, 0, 0,
			},
			errorKeyWord: "get first type record, but in_fragment",
		},
		{
			name: "test middle type not in fragment",
			block: []byte{
				// middle type
				65, 217, 18, 255, 0, 2, 3, 0, 0,
			},
			errorKeyWord: "get middle type record, but not in_fragment",
		},
		{
			name: "test last type not in fragment",
			block: []byte{
				// last type
				65, 217, 18, 255, 0, 2, 4, 0, 0,
			},
			errorKeyWord: "get last type record, but not in_fragment",
		},
		{
			name: "get unknown type record",
			block: []byte{
				65, 217, 18, 255, 0, 2, 5, 0, 0,
			},
			errorKeyWord: "unknown record type",
		},
		{
			name: "get checksum not equal record",
			block: []byte{
				66, 217, 18, 255, 0, 2, 1, 0, 0,
			},
			errorKeyWord: "checksum not equal",
		},
		{
			name: "get data not enouth record",
			block: []byte{
				66, 217, 18, 255, 0, 2, 1, 0,
			},
			errorKeyWord: "len(record) < header.length",
		},
	}

	var readCount int
	var getExpectedKeyWord bool
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readCount = 0
			getExpectedKeyWord = false

			mockReporter.EXPECT().Corruption(gomock.Any()).AnyTimes().DoAndReturn(
				func(err error) {
					if strings.Contains(err.Error(), tt.errorKeyWord) {
						getExpectedKeyWord = true
					}
				},
			)

			mockSequentialReader.EXPECT().Read(gomock.Any()).AnyTimes().DoAndReturn(
				func(n int) (slice.Slice, error) {
					if readCount == 0 {
						readCount++
						return tt.block, nil
					} else {
						return nil, io.EOF
					}
				},
			)

			r := &ReaderImpl{
				SequentialReader: mockSequentialReader,
				LastRecordOffset: 0,
				endOfBufOffset:   0,
				buf:              nil,
				reporter:         mockReporter,
			}

			_, err := r.ReadRecord()
			t.Log(tt.name, err)

			if !getExpectedKeyWord {
				t.Error("no expected key word in error", tt.errorKeyWord)
			}
		})
	}
}
