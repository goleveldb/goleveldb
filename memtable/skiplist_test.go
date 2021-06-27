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

func compareString(a, b slice.Slice) int {
	return a.Compare(b)
}

func newTestSkipList() *skiplist {
	return &skiplist{
		header: &node{
			next: [maxHeight]*node{},
		},
		cmp: compareString,
	}
}

// skiplistOpreation 记录操作方法及其内容.
type skiplistOpreation struct {
	name   string
	method string
	key    slice.Slice
	// expect true for bool, none nil for error.
	expectPositive bool
}

// skiplistTestPoint 记录测试点数据.
type skiplistTestPoint struct {
	name       string
	operations []*skiplistOpreation
}

// TestMemtable_skiplistImpl_all 对 skiplistImpl 进行测试.
func TestMemtable_skiplistImpl_all(t *testing.T) {
	tests := []*skiplistTestPoint{
		{
			name: "test normal insert and contains",
			operations: []*skiplistOpreation{
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
func generateTestPointFunc(operations []*skiplistOpreation) func(t *testing.T) {
	return func(t *testing.T) {
		var isPositive bool
		list := newTestSkipList()
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
