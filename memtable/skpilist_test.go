// Package memtable 定义并实现内存表.
package memtable

import (
	"testing"

	"github.com/goleveldb/goleveldb/slice"
)

const (
	methodInsert   = "insert"
	methodContains = "contains"
)

// operation 记录操作方法及其内容.
type opreation struct {
	name   string
	method string
	key    slice.Slice
	// expect true for bool, none nil for error.
	expectPositive bool
}

// testPoint 记录测试点数据.
type testPoint struct {
	name       string
	operations []*opreation
}

// TestMemtable_skiplistImpl_all 对 skiplistImpl 进行测试.
func TestMemtable_skiplistImpl_all(t *testing.T) {
	tests := []*testPoint{
		{
			name: "test normal insert and contains",
			operations: []*opreation{
				{"empty check", methodContains, slice.Slice("foobar"), false},
				{"insert foobar", methodInsert, slice.Slice("foobar"), true},
				{"check foobar", methodContains, slice.Slice("foobar"), true},
				{"re insert foobar", methodInsert, slice.Slice("foobar"), false},
				{"insert not foobar", methodInsert, slice.Slice("foo"), true},
				{"re check foobar", methodContains, slice.Slice("foobar"), true},
				{"check foo", methodContains, slice.Slice("foo"), true},
				{"check not exist", methodContains, slice.Slice("not exist"), false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, generateTestPointFunc(tt.operations))
	}
}

// generateTestPointFunc 根据测试点生成测试函数.
func generateTestPointFunc(operations []*opreation) func(t *testing.T) {
	return func(t *testing.T) {
		var isPositive bool
		list := newSkiplist()
		for _, opera := range operations {
			switch opera.method {
			case methodInsert:
				isPositive = list.insert(opera.key) == nil
			case methodContains:
				isPositive = list.contains(opera.key)
			default:
				t.Fatal("no such method: ", opera.method)
			}

			if isPositive != opera.expectPositive {
				t.Errorf("operation %s, expect positive is %v, get %v", opera.name, opera.expectPositive, isPositive)
			}
		}
	}
}
