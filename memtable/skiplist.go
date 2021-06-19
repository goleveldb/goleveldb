// Package memtable 定义并实现内存表.
package memtable

import (
	"errors"
	"math/rand"

	"github.com/goleveldb/goleveldb/slice"
)

// skiplist 最大高度.
const maxHeight = 12

// seekRule 判断是否向后移动.
// 返回 true 表示不满足最终条件, 需要继续移动.
type seekRule func(target, next slice.Slice) bool

// skiplist 实现 skiplist 接口.
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
func (l *skiplist) iterator() *iterator {
	it := &iterator{list: l}
	it.seekToFirst()

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
	res := l.find(key, seekGreaterOrEqualRule)

	return res != l.header && res.key.Compare(key) == slice.CMPSame
}

// find 寻找符合 rule 的节点.
func (l *skiplist) find(target slice.Slice, rule seekRule) *node {
	cur := l.header
	for level := maxHeight - 1; level >= 0; level-- {
		for cur.next[level] != nil && rule(target, cur.next[level].key) {
			cur = cur.next[level]
		}
	}

	return cur
}

// seekLessThanRule 获取小于 target 的规则.
// 下一个小于target就继续移动.
func seekLessThanRule(target, next slice.Slice) bool {
	return next.Compare(target) == slice.CMPSmaller
}

// seekGreaterOrEqualRule 获取大于等于 target 的规则.
// 下一个小于等于target就继续移动.
func seekGreaterOrEqualRule(target, next slice.Slice) bool {
	return next.Compare(target) != slice.CMPLarger
}

// seekLastRule 获取 skiplist 尾部的规则.
func seekLastRule(target, next slice.Slice) bool {
	return true
}
