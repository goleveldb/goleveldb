package file

import (
	"io/fs"
	"io/ioutil"
	"os"
	"testing"
)

const (
	operaWrite = 0
	operaClose = 1
	operaFlush = 2
	operaSync  = 3
)

type writerTestPoint struct {
	name       string
	operations []*writerOperation
	wantResult string
}

type writerOperation struct {
	name    string
	opera   int
	data    []byte
	wantErr bool
}

func TestNewWriter(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		f, err := ioutil.TempFile("./", "foobar.test")
		if err != nil {
			t.Fatal(err)
		}
		defer destoryTempFile(f)

		if _, err := NewWriter(f.Name()); err != nil {
			t.Error("TestNewWriter() => unexpect err:", err)
		}
	})

	t.Run("read only file", func(t *testing.T) {
		f, err := ioutil.TempFile("./", "foobar.test")
		if err != nil {
			t.Fatal(err)
		}
		defer destoryTempFile(f)

		// read only file.
		if err := f.Chmod(fs.FileMode(os.O_RDONLY)); err != nil {
			t.Fatal(err)
		}

		if _, err := NewWriter(f.Name()); err == nil {
			t.Error("TestNewWriter() => get err == nil, but expected error")
		}
	})
}

func Test_WriterImpl_All(t *testing.T) {
	tests := []*writerTestPoint{
		{
			name: "normal",
			operations: []*writerOperation{
				{
					name:    "normal write",
					opera:   operaWrite,
					data:    []byte("foobar"),
					wantErr: false,
				},
				{
					name:    "normal write",
					opera:   operaWrite,
					data:    []byte("foobar"),
					wantErr: false,
				},
				{
					name:    "normal close",
					opera:   operaClose,
					wantErr: false,
				},
				{
					name:    "appedn after close",
					opera:   operaWrite,
					wantErr: true,
				},
				{
					name:    "sync after close",
					opera:   operaSync,
					wantErr: true,
				},
				{
					name:    "flush after close",
					opera:   operaFlush,
					wantErr: true,
				},
				{
					name:    "close after close",
					opera:   operaClose,
					wantErr: true,
				},
			},
			wantResult: "foobarfoobar",
		},
		{
			name: "test sync",
			operations: []*writerOperation{
				{
					name:    "write",
					opera:   operaWrite,
					data:    []byte("foobar"),
					wantErr: false,
				},
				{
					name:    "sync",
					opera:   operaSync,
					wantErr: false,
				},
			},
			wantResult: "foobar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runWriterImplTestPoint(t, tt)
		})
	}
}

func runWriterImplTestPoint(t *testing.T, testPoint *writerTestPoint) {
	t.Helper()

	f, err := ioutil.TempFile("./", "foobar.test")
	if err != nil {
		t.Fatal(err)
	}
	defer destoryTempFile(f)

	writer, err := NewWriter(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range testPoint.operations {
		var err error
		switch tt.opera {
		case operaWrite:
			err = writer.Append(tt.data)
		case operaClose:
			err = writer.Close()
		case operaFlush:
			err = writer.Flush()
		case operaSync:
			err = writer.Sync()
		}

		if tt.wantErr != (err != nil) {
			t.Errorf("runWriterImplTestPoint() %s get err = %v, get wantErr = %v", tt.name, err, tt.wantErr)
		}
	}

	result, err := ioutil.ReadAll(f)
	if err != nil {
		t.Error("unexpected error", err)
	}

	if string(result) != testPoint.wantResult {
		t.Errorf("want result = %s, but get %s", testPoint.wantResult, string(result))
	}
}

func destoryTempFile(f *os.File) {
	f.Close()
	os.Remove(f.Name())
}
