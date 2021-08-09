package file

import (
	"io/ioutil"
	"testing"
)

const (
	operaRead = 0
	operaSkip = 1
)

type sequentialReaderTestPoint struct {
	name       string
	initStr    string
	opeartions []*sequentialReaderOperation
}

type sequentialReaderOperation struct {
	name      string
	opera     int
	n         int
	want      string
	wantError bool
}

func TestNewSequentialReader(t *testing.T) {
	t.Run("normal create", func(t *testing.T) {
		f, err := ioutil.TempFile("./", "foobar.test")
		if err != nil {
			t.Fatal(err)
		}
		defer destoryTempFile(f)

		if _, err := NewSequentialReader(f.Name()); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("file not exist", func(t *testing.T) {
		if _, err := NewSequentialReader("not_exist_file.foobar.test"); err == nil {
			t.Error("want error, but get err == nil")
		}
	})
}

func TestAll(t *testing.T) {
	tests := []*sequentialReaderTestPoint{
		{
			name:    "read1_skip1_read1",
			initStr: "foobar1foobar2foobar3",
			opeartions: []*sequentialReaderOperation{
				{
					name:      "read foobar1",
					opera:     operaRead,
					n:         7,
					want:      "foobar1",
					wantError: false,
				},
				{
					name:      "skip foobar2",
					opera:     operaSkip,
					n:         7,
					wantError: false,
				},
				{
					name:      "read foobar3",
					opera:     operaRead,
					n:         7,
					want:      "foobar3",
					wantError: false,
				},
				{
					name:      "read eof",
					opera:     operaRead,
					n:         5,
					want:      "",
					wantError: true,
				},
				{
					name:      "skip eof",
					opera:     operaSkip,
					n:         5,
					wantError: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runSequentialReaderImplTestPoint(t, tt)
		})
	}
}

func runSequentialReaderImplTestPoint(t *testing.T, testPoint *sequentialReaderTestPoint) {
	t.Helper()

	f, err := ioutil.TempFile("./", "foobar.test")
	if err != nil {
		t.Fatal(err)
	}
	defer destoryTempFile(f)

	if _, err := f.Write([]byte(testPoint.initStr)); err != nil {
		t.Fatal(err)
	}

	seqReader, err := NewSequentialReader(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	for _, op := range testPoint.opeartions {
		if op.opera == operaRead {
			res, err := seqReader.Read(op.n)
			if (err != nil) != op.wantError {
				t.Errorf("after %s, get err = %v, but wantErr = %v", op.name, err, op.wantError)
			}

			if string(res) != op.want {
				t.Errorf("after %s, want %s, but get %s", op.name, op.want, string(res))
			}
		} else {
			if err := seqReader.Skip(op.n); (err != nil) != op.wantError {
				t.Errorf("after %s, get err = %v, but wantErr = %v", op.name, err, op.wantError)
			}
		}
	}
}
