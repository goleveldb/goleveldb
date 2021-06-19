// Package memtable 定义并实现内存表.
package memtable

import (
	"errors"

	"github.com/goleveldb/goleveldb/slice"
)

// skiplist 最大高度.
const maxHeight = 12

// skiplist 定义跳表, 包含针对 Slice的 插入, 包含 操作.
type skiplist interface {
	insert(slice.Slice) error
	contains(slice.Slice) bool
}

// skiplistImpl 实现 skiplist 接口.
type skiplistImpl struct {
	header *node
}

// node 描述 skiplist 节点.
type node struct {
	key  slice.Slice
	next [maxHeight]*node
}

// newSkiplist 实例化 skiplist 并返回.
func newSkiplist() skiplist {
	return &skiplistImpl{
		header: &node{
			next: [maxHeight]*node{},
		},
	}
}

// insert 将 key 插入 skiplist, key 不可重复.
func (l *skiplistImpl) insert(key slice.Slice) error {
	var prevs [maxHeight]*node

	cur := l.header
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.next[level] != nil && cur.next[level].key.Compare(key) != slice.CMPLarger {
			cur = cur.next[level]
		}
		prevs[level] = cur
	}

	if prevs[0].key.Compare(key) == slice.CMPSame {
		return errors.New("equal key")
	}

	insertedNode := &node{key: key}

	for level := 0; level < maxHeight; level++ {
		insertedNode.next[level] = prevs[level].next[level]
		prevs[level].next[level] = insertedNode
	}

	return nil
}

// contains 判断 key 是否存在于 skiplist 中.
func (l *skiplistImpl) contains(key slice.Slice) bool {
	cur := l.header
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.next[level] != nil && cur.next[level].key.Compare(key) != slice.CMPLarger {
			cur = cur.next[level]
		}

		if cur.key.Compare(key) == slice.CMPSame {
			return true
		}
	}

	return false
}
