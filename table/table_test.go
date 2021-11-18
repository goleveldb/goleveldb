package table

import (
	"os"
	"testing"

	"github.com/goleveldb/goleveldb/file"
	"github.com/goleveldb/goleveldb/slice"
)

type opType bool

type entry struct {
	key string
	value string
}

type baseOperation struct {
	opType opType
	expectErr bool
	optionalErr error
	expect *entry
	tableWriter TableWriter
}

var fileWriter file.Writer
var fileReader file.RandomReader

const sstableFileName = "./0.sst"

const (
	opTypeWrite opType = false
	opTypeRead opType = true
)

func TestMain(m *testing.M) {
	fw, err := file.NewWriter(sstableFileName)
	if err != nil {
		panic(err)
	}
	fileWriter = fw
	defer func() {
		fw.Close()
		os.Remove(sstableFileName)
	}()

	fr, err := file.NewRandomReader(sstableFileName)
	if err != nil {
		panic(err)
	}
	fileReader = fr

	if code := m.Run(); code != 0 {
		panic(code)
	}
}

func Test_WriteAndGet(t *testing.T) {
	tableWriter := NewWriter(fileWriter)
	sequentialExecOperations := []*baseOperation{
		{
			opType: opTypeWrite,
			expectErr: false,
			expect: &entry{key: "name", value: "lijunyu",},
			tableWriter: tableWriter,
		},
	}

	for _, operation := range sequentialExecOperations {
		if operation.opType == opTypeWrite {
			doWrite(operation)
			continue
		}

		doRead(operation)
	}
}

func doWrite(operation *baseOperation) {
	
}

func doRead(operation *baseOperation) {

}