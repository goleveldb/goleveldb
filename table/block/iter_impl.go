package block

import (
	"encoding/binary"

	"github.com/goleveldb/goleveldb/common"
	"github.com/goleveldb/goleveldb/slice"
)

type blockIteratorImpl struct {
	content        []byte
	numRestarts    uint32
	restartsOffset uint32

	current        uint32
	currentRestart uint32
	key            slice.Slice
	value          slice.Slice
	entryLen       uint32
}

var _ common.Iterator = (*blockIteratorImpl)(nil)

func NewIter(blk *Block) common.Iterator {
	return &blockIteratorImpl{
		content:        blk.Content,
		numRestarts:    blk.NumRestarts,
		restartsOffset: blk.RestartsOffset,
	}
}

func (i *blockIteratorImpl) Success() bool {
	return i.current < i.restartsOffset && i.currentRestart < i.numRestarts
}

func (i *blockIteratorImpl) Prev() {
	// seek the first restart point before current
	for i.getRestartOffset(i.currentRestart) >= i.current {
		if i.currentRestart == 0 {
			i.fail()
			return
		}
		i.currentRestart--
	}

	origOffset := i.current
	// clear the key and value to search from the restart point
	i.gotoRestart(i.currentRestart)
	// linear search from current restart point
	for i.parseCurrent() && i.current+i.entryLen < origOffset {
		i.gotoNext()
	}
}

func (i *blockIteratorImpl) gotoRestart(restartIndex uint32) {
	i.currentRestart = restartIndex
	i.current = i.getRestartOffset(restartIndex)
	i.key = nil
	i.value = nil
}

// get kv at current index
func (i *blockIteratorImpl) parseCurrent() bool {
	if i.current >= i.restartsOffset {
		i.fail()
		return false
	}

	entryLen, share, unshare, keyDelta, val := parseEntry(i.content[i.current:])
	currentKey, offset := make([]byte, share+unshare), 0
	if share > 0 {
		offset += copy(currentKey, i.key[:share])
	}
	copy(currentKey[offset:], keyDelta)
	i.key = currentKey
	i.value = val
	i.entryLen = entryLen

	return true
}

// parse the first entry from bytes
func parseEntry(bytes []byte) (entryLen uint32, share, unshare uint64, keyDelta, value slice.Slice) {
	// entry format :
	// shareLength : varint
	// unshareLength : varint
	// valueLength : varint
	// key_delta : []byte(length == unshareLength)
	// value : []byte(length == valueLength)
	offset := uint32(0)
	share = readUVarint(bytes, &offset)
	unshare = readUVarint(bytes, &offset)
	valueLen := readUVarint(bytes, &offset)

	keyDelta = bytes[offset : offset+uint32(unshare)]
	offset += uint32(unshare)
	value = bytes[offset : offset+uint32(valueLen)]
	offset += uint32(valueLen)

	entryLen = offset
	return
}

func readUVarint(data []byte, offset *uint32) uint64 {
	varint, incr := binary.Uvarint(data[*offset:])
	*offset += uint32(incr)

	return varint
}

func (i *blockIteratorImpl) fail() {
	i.current = i.restartsOffset
	i.currentRestart = i.numRestarts
}

func (i *blockIteratorImpl) getRestartOffset(restartIndex uint32) uint32 {
	return binary.BigEndian.Uint32(i.content[i.restartsOffset+4*restartIndex:])
}

func (i *blockIteratorImpl) Next() {
	i.gotoNext()
	i.parseCurrent()
}

func (i *blockIteratorImpl) gotoNext() {
	i.current += i.entryLen
	// find max restart point index < current
	for i.currentRestart+1 < i.numRestarts && i.getRestartOffset(i.currentRestart+1) <= i.current {
		i.currentRestart++
	}
}

func (i *blockIteratorImpl) Find(key slice.Slice) {
	// do binary search on restarts to determine the max restart point <= key
	left, right := uint32(0), i.numRestarts-1
	for left < right {
		mid := (left + right + 1) >> 1
		midOffset := i.getRestartOffset(mid)
		_, _, _, midK, _ := parseEntry(i.content[midOffset:])

		if midK.Compare(key) > 0 {
			right = mid - 1
		} else {
			left = mid
		}
	}

	// do linear search from the restart point
	// we find the first key k such that k.CompareTo(key) >= 0
	// cuz for index block kv, k.CompareTo(key) >= 0 satisfies our needs
	// for data block kv, k.CompareTo(key) == 0 satisfies our needs
	// to summarize the false condition is k.CompareTo(key) >= 0
	i.gotoRestart(left)
	for i.parseCurrent() && i.Key().Compare(key) < 0 {
		i.gotoNext()
	}
}

func (i *blockIteratorImpl) Key() slice.Slice {
	return i.key
}

func (i *blockIteratorImpl) Value() slice.Slice {
	return i.value
}
