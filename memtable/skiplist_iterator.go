package memtable

import (
	"errors"

	"github.com/goleveldb/goleveldb/slice"
)

// ErrNotValid 当前节点无效.
var ErrNotValid = errors.New("current node is not valid")

// iterator 实现iterator接口, 遍历 skiplist.
type iterator struct {
	list *skiplist
	node *node
}

// valid 判断当前节点是否有效.
func (it *iterator) valid() bool {
	return it.node != nil
}

// key 返回当前节点的 key.
func (it *iterator) key() (slice.Slice, error) {
	if !it.valid() {
		return nil, ErrNotValid
	}

	return it.node.key, nil
}

// next 访问下一个节点.
func (it *iterator) next() {
	if !it.valid() {
		return
	}

	it.node = it.node.next[0]
}

// prev 访问上一个节点.
func (it *iterator) prev() {
	if !it.valid() {
		return
	}

	it.node = it.list.seek(it.node.key, seekLessThanRule)
	if it.node == it.list.header {
		it.node = nil
	}
}

// seek 访问大于等于 target 的第一个节点.
func (it *iterator) seek(target slice.Slice) {
	it.node = it.list.seek(target, seekGreaterOrEqualRule)

	// 如果找到的节点不符合条件, 置当前节点为空.
	if it.node.key.Compare(target) == slice.CMPSmaller {
		it.node = nil
	}
}

// seekToFirst 访问 skiplist 第一个节点.
func (it *iterator) seekToFirst() {
	it.node = it.list.header.next[0]
}

// seekToLast 访问 skiplist 最后一个节点.
func (it *iterator) seekToLast() {
	it.node = it.list.seek(nil, seekLastRule)
	if it.node == it.list.header {
		it.node = nil
	}
}
