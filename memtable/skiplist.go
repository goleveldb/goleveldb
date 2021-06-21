package memtable

import (
	"errors"
	"math/rand"

	"github.com/goleveldb/goleveldb/slice"
)

// skiplist 最大高度.
const maxHeight = 12

// skiplist 实现跳表，进行kv存储.
type skiplist struct {
	header *node
}

// node 描述 skiplist 节点.
type node struct {
	key  slice.Slice
	next [maxHeight]*node
}

// newSkiplist 实例化 skiplist 并返回.
func newSkiplist() *skiplist {
	return &skiplist{
		header: &node{
			next: [maxHeight]*node{},
		},
	}
}

// iterator 创建 skiplist 迭代器.
// 创建出的迭代器初始位置为第一个节点.
func (l *skiplist) iterator() *Iterator {
	it := &Iterator{list: l}
	it.SeekToFirst()

	return it
}

// insert 将 key 插入 skiplist, key 不可重复.
func (l *skiplist) insert(key slice.Slice) error {
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

	for level := 0; level <= (rand.Int() % maxHeight); level++ {
		insertedNode.next[level] = prevs[level].next[level]
		prevs[level].next[level] = insertedNode
	}

	return nil
}

// contains 判断 key 是否存在于 skiplist 中.
func (l *skiplist) contains(key slice.Slice) bool {
	res := l.seekGreaterOrEqual(key)

	return res != nil && res.key.Compare(key) == slice.CMPSame
}

// seekLessThanRule 获取小于 target 的第一个节点.
func (l *skiplist) seekLessThan(target slice.Slice) *node {
	cur := l.header
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.next[level] != nil && cur.next[level].key.Compare(target) == slice.CMPSmaller {
			cur = cur.next[level]
		}
	}

	return cur
}

// seekGreaterOrEqualRule 获取大于等于 target 的第一个节点.
func (l *skiplist) seekGreaterOrEqual(target slice.Slice) *node {
	return l.seekLessThan(target).next[0]
}

// seekLastRule 获取 skiplist 尾部的节点.
func (l *skiplist) seekLast() *node {
	cur := l.header
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.next[level] != nil {
			cur = cur.next[level]
		}
	}

	return cur
}
