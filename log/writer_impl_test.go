package log

import (
	"errors"
	"hash/crc32"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goleveldb/goleveldb/file"
	mockfile "github.com/goleveldb/goleveldb/internal/mock/file"
	"github.com/goleveldb/goleveldb/slice"
)

//go:generate make gen_mock_file_writer -C ..

func TestWriterImpl_AddRecord(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockWriter := mockfile.NewMockWriter(mockCtrl)

	type fields struct {
		fileWriter  file.Writer
		blockOffset int
	}
	type recordHeader struct {
		crc        uint32
		length     int
		recordType int
	}
	type record struct {
		header recordHeader
		data   slice.Slice
	}
	tests := []struct {
		name           string
		fields         fields
		data           slice.Slice
		wantAppendList []record
		mockAppendErr  bool
		mockFlushErr   bool
		wantErr        bool
	}{
		{
			name: "test add FULL Record",
			fields: fields{
				fileWriter:  mockWriter,
				blockOffset: 0,
			},
			data: make(slice.Slice, BlockSize-HeaderSize),
			wantAppendList: []record{
				{
					header: recordHeader{
						crc:        crc32.ChecksumIEEE(make(slice.Slice, BlockSize-HeaderSize)),
						length:     BlockSize - HeaderSize,
						recordType: RecordFullType,
					},
					data: make(slice.Slice, BlockSize-HeaderSize),
				},
			},
		},
		{
			name: "test add two part Record",
			fields: fields{
				fileWriter:  mockWriter,
				blockOffset: 0,
			},
			data: make(slice.Slice, BlockSize),
			wantAppendList: []record{
				{
					header: recordHeader{
						crc:        crc32.ChecksumIEEE(make(slice.Slice, BlockSize-HeaderSize)),
						length:     BlockSize - HeaderSize,
						recordType: RecordFirstType,
					},
					data: make(slice.Slice, BlockSize-HeaderSize),
				},
				{
					header: recordHeader{
						crc:        crc32.ChecksumIEEE(make(slice.Slice, HeaderSize)),
						length:     HeaderSize,
						recordType: RecordLastType,
					},
					data: make(slice.Slice, HeaderSize),
				},
			},
		},
		{
			name: "test add three part Record",
			fields: fields{
				fileWriter:  mockWriter,
				blockOffset: 0,
			},
			data: make(slice.Slice, 2*BlockSize-HeaderSize),
			wantAppendList: []record{
				{
					header: recordHeader{
						crc:        crc32.ChecksumIEEE(make(slice.Slice, BlockSize-HeaderSize)),
						length:     BlockSize - HeaderSize,
						recordType: RecordFirstType,
					},
					data: make(slice.Slice, BlockSize-HeaderSize),
				},
				{
					header: recordHeader{
						crc:        crc32.ChecksumIEEE(make(slice.Slice, BlockSize-HeaderSize)),
						length:     BlockSize - HeaderSize,
						recordType: RecordMiddleType,
					},
					data: make(slice.Slice, BlockSize-HeaderSize),
				},
				{
					header: recordHeader{
						crc:        crc32.ChecksumIEEE(make(slice.Slice, HeaderSize)),
						length:     HeaderSize,
						recordType: RecordLastType,
					},
					data: make(slice.Slice, HeaderSize),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeTime := 0
			mockWriter.EXPECT().Flush().Times(len(tt.wantAppendList))
			mockWriter.EXPECT().Append(gomock.Any()).Times(len(tt.wantAppendList) * 2).DoAndReturn(func(s slice.Slice) error {
				expItem := tt.wantAppendList[writeTime/2]
				expData := expItem.data
				if writeTime%2 == 0 {
					var (
						expectHeader = expItem.header
						crc          = expectHeader.crc
						length       = expectHeader.length
						recordType   = expectHeader.recordType
					)

					cmpList := make(slice.Slice, 0)
					cmpList = append(cmpList, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
					cmpList = append(cmpList, byte(length>>8), byte(length))
					cmpList = append(cmpList, byte(recordType))

					expData = cmpList
				}

				if expData.Compare(s) != slice.CMPSame {
					return errors.New("write arg not equal")
				}

				writeTime++

				return nil
			})

			w := &WriterImpl{
				fileWriter:  tt.fields.fileWriter,
				blockOffset: tt.fields.blockOffset,
			}
			if err := w.AddRecord(tt.data); (err != nil) != tt.wantErr {
				t.Errorf("WriterImpl.AddRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
