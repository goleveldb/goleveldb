// Package memtable 定义并实现内存表, 及其插入、获取操作.
package memtable

import (
	"encoding/binary"
	"errors"

	"github.com/goleveldb/goleveldb/internal/utils/varstr"
	"github.com/goleveldb/goleveldb/slice"
)

// ErrNotFound 内存表中无法找到相应记录.
var ErrNotFound = errors.New("key not found")

const (
	kMaxUint64 uint64 = 0xffffffffffffffff
	// int64类型占用字节数.
	int64Len = 8

	// 当typeValue为1时数据有效.
	typeValue  = 1
	typeDelete = 0
)

// Memtable 在内存中存储kv数据.
type Memtable struct {
	table *skiplist
}

// New 创建并初始化 Memtable.
func New() *Memtable {
	return &Memtable{
		table: &skiplist{
			header: &node{
				next: [maxHeight]*node{},
			},
			cmp: compareKey,
		},
	}
}

// Iterator 创建用于遍历内存表的迭代器.
func (t *Memtable) Iterator() *Iterator {
	return t.table.iterator()
}

// Insert 向内存表中插入一条包含序列号, valueType 的kv记录.
// 插入的数据按照以下方式排列:
// - key length (varint).
// - key data.
// - sequenceNumber & valueType (uint64).
// - value length (varint).
// - value data.
func (t *Memtable) Insert(sequenceNumber uint64, valueType byte, key, value slice.Slice) error {
	totalLen := varstr.VarStrLen(key) + int64Len + varstr.VarStrLen(value)
	record := make([]byte, totalLen)
	curPos := 0

	curPos += varstr.PutVarStr(record[curPos:], key)

	binary.BigEndian.PutUint64(record[curPos:], ((sequenceNumber << 8) | uint64(valueType)))
	curPos += int64Len

	curPos += varstr.PutVarStr(record[curPos:], value)

	return t.table.insert(record)
}

// Get 从内存表中获取key对应的value.
func (t *Memtable) Get(key slice.Slice) (value slice.Slice, err error) {
	record, err := t.seekByKey(key)
	if err != nil {
		return nil, err
	}

	// 检查获取的 record 中的 key 与期望的 key 是否相同.
	recordKey, keyLength := varstr.GetVarStr(record)
	if key.Compare(recordKey) != slice.CMPSame {
		return nil, ErrNotFound
	}

	return parseTagAndValue(record[keyLength:])
}

// seekByKey 通过 key 在内存表中查找 record.
func (t *Memtable) seekByKey(key slice.Slice) (value slice.Slice, err error) {
	// build key.
	seekByKey := make([]byte, varstr.VarStrLen(key)+int64Len)

	pos := varstr.PutVarStr(seekByKey, key)
	binary.BigEndian.PutUint64(seekByKey[pos:], kMaxUint64)

	iter := t.table.iterator()
	iter.Seek(seekByKey)

	return iter.Key()
}

// parseTagAndValue 解析值.
func parseTagAndValue(record slice.Slice) (slice.Slice, error) {
	// 读取 sequenceNumber & valueType.
	tag := binary.BigEndian.Uint64(record)
	record = record[int64Len:]

	if tag&0xff != typeValue {
		return nil, ErrNotFound
	}

	valueLength, varintLen := binary.Varint(record)
	record = record[varintLen:]
	if len(record) != int(valueLength) {
		return nil, ErrNotFound
	}

	return record, nil
}

// compareKey 比较两个key的大小.
func compareKey(a, b slice.Slice) int {
	keyA, lenA := varstr.GetVarStr(a)
	keyB, lenB := varstr.GetVarStr(b)

	// not equal, return result.
	if res := slice.Slice(keyA).Compare(keyB); res != 0 {
		return res
	}

	// key equal, sequenceNumber 大的排在前面.
	if binary.BigEndian.Uint64(a[lenA:]) > binary.BigEndian.Uint64(b[lenB:]) {
		return slice.CMPSmaller
	} else if binary.BigEndian.Uint64(a[lenA:]) < binary.BigEndian.Uint64(b[lenB:]) {
		return slice.CMPLarger
	}

	return slice.CMPSame
}
