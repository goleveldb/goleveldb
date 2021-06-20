package memtable

import (
	"errors"

	"github.com/goleveldb/goleveldb/slice"
)

// ErrNotValid 当前节点无效.
var ErrNotValid = errors.New("current node is not Valid")

// Iterator 遍历 skiplist.
type Iterator struct {
	list *skiplist
	node *node
}

// Valid 判断当前节点是否有效.
func (it *Iterator) Valid() bool {
	return it.node != nil
}

// Key 返回当前节点的 key.
func (it *Iterator) Key() (slice.Slice, error) {
	if !it.Valid() {
		return nil, ErrNotValid
	}

	return it.node.key, nil
}

// Next 访问下一个节点.
func (it *Iterator) Next() {
	if !it.Valid() {
		return
	}

	it.node = it.node.next[0]
}

// Prev 访问上一个节点.
func (it *Iterator) Prev() {
	if !it.Valid() {
		return
	}

	it.node = it.list.seekLessThan(it.node.key)
	if it.node == it.list.header {
		it.node = nil
	}
}

// Seek 访问大于等于 target 的第一个节点.
func (it *Iterator) Seek(target slice.Slice) {
	it.node = it.list.seekGreaterOrEqual(target)
}

// SeekToFirst 访问 skiplist 第一个节点.
func (it *Iterator) SeekToFirst() {
	it.node = it.list.header.next[0]
}

// SeekToLast 访问 skiplist 最后一个节点.
func (it *Iterator) SeekToLast() {
	it.node = it.list.seekLast()
	if it.node == it.list.header {
		it.node = nil
	}
}
