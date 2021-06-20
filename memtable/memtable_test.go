package memtable

import (
	"testing"

	"github.com/goleveldb/goleveldb/slice"
)

const methodGet = "get"

type insertArg struct {
	sequenceNumber uint64
	valueType      byte
	key            slice.Slice
	value          slice.Slice

	wantErr bool
}

type getArg struct {
	key slice.Slice

	wantRes slice.Slice
	wantErr bool
}

type memtableOperation struct {
	name   string
	method string

	insertArg *insertArg
	getArg    *getArg
}

type memtableTestPoint struct {
	name       string
	operations []*memtableOperation
}

func TestMemtable_Memtable_Iterator(t *testing.T) {
	New().Iterator()
}

func TestMemtable_Memtable_All(t *testing.T) {
	tests := []*memtableTestPoint{
		{
			name: "test",
			operations: []*memtableOperation{
				{
					name:   "get not exist key",
					method: methodGet,
					getArg: &getArg{
						key:     slice.Slice("not exist"),
						wantErr: true,
						wantRes: nil,
					},
				},
				{
					name:   "insert key value",
					method: methodInsert,
					insertArg: &insertArg{
						sequenceNumber: 1,
						valueType:      typeValue,
						key:            slice.Slice("foo"),
						value:          slice.Slice("bar"),
						wantErr:        false,
					},
				},
				{
					name:   "get exist key",
					method: methodGet,
					getArg: &getArg{
						key:     slice.Slice("foo"),
						wantRes: slice.Slice("bar"),
						wantErr: false,
					},
				},
				{
					name:   "delete key",
					method: methodInsert,
					insertArg: &insertArg{
						sequenceNumber: 2,
						valueType:      typeDelete, // 0 means delete
						key:            slice.Slice("foo"),
						wantErr:        false,
					},
				},
				{
					name:   "get deleted key",
					method: methodGet,
					getArg: &getArg{
						key:     slice.Slice("foo"),
						wantRes: nil,
						wantErr: true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		runTestPoint(t, tt)
	}
}

// runTestPoint 运行测试点.
func runTestPoint(t *testing.T, testPoint *memtableTestPoint) {
	table := New()

	t.Run(testPoint.name, func(t *testing.T) {
		for _, opera := range testPoint.operations {
			switch opera.method {
			case methodGet:
				got, err := table.Get(opera.getArg.key)
				if got.Compare(opera.getArg.wantRes) != slice.CMPSame || (err != nil) != opera.getArg.wantErr {
					t.Errorf("Memtable => testPoint %s want = (%s, %v), get = (%s, %v)",
						opera.name, string(opera.getArg.wantRes), opera.getArg.wantErr, string(got), err)
				}
			case methodInsert:
				err := table.Insert(opera.insertArg.sequenceNumber,
					opera.insertArg.valueType, opera.insertArg.key, opera.insertArg.value)
				if (err != nil) != opera.insertArg.wantErr {
					t.Errorf("Memtable => testPoint %s want = (%v), get = (%v)", opera.name, opera.getArg.wantErr, err)
				}
			}
		}
	})
}
