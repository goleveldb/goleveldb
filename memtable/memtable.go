// Package memtable 定义并实现内存表, 及其插入、获取操作.
package memtable

import (
	"encoding/binary"
	"errors"

	"github.com/goleveldb/goleveldb/slice"
)

// ErrNotFound 内存表中无法找到相应记录.
var ErrNotFound = errors.New("key not found")

const (
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
	totalLen := varintLen(len(key)) + len(key) + int64Len + varintLen(len(value)) + len(value)
	record := make([]byte, totalLen)
	curPos := 0

	// 添加 varint 编码key长度.
	byteLen := binary.PutVarint(record[curPos:], int64(len(key)))
	curPos += byteLen

	// 添加 key 数据.
	copy(record[curPos:curPos+len(key)], key)
	curPos += len(key)

	binary.BigEndian.PutUint64(record[curPos:], ((sequenceNumber << 8) | uint64(valueType)))
	curPos += int64Len

	// 添加 varint 编码value长度.
	byteLen = binary.PutVarint(record[curPos:], int64(len(value)))
	curPos += byteLen
	// 添加 value 数据.
	copy(record[curPos:curPos+len(value)], value)

	return t.table.insert(record)
}

// Get 从内存表中获取key对应的value.
func (t *Memtable) Get(key slice.Slice) (value slice.Slice, err error) {
	record, err := t.seekByKey(key)
	if err != nil {
		return nil, err
	}

	// 检查获取的 record 中的 key 与期望的 key 是否相同.
	recordKey, keyLength := loadKey(record)
	if key.Compare(recordKey) != slice.CMPSame {
		return nil, ErrNotFound
	}

	return parseTagAndValue(record[keyLength:])
}

// seekByKey 通过 key 在内存表中查找 record.
func (t *Memtable) seekByKey(key slice.Slice) (value slice.Slice, err error) {
	// put key.
	seekKey := make([]byte, varintLen(len(key)))
	binary.PutVarint(seekKey, int64(len(key)))
	seekKey = append(seekKey, key...)

	// put tag.
	tag := make([]byte, int64Len)
	for i := 0; i < int64Len; i++ {
		tag[i] = 0xff
	}
	seekKey = append(seekKey, tag...)

	iter := t.table.iterator()
	iter.Seek(seekKey)

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

// varintLen 返回 num 转化为 varintLen 类型后的字节数.
func varintLen(num int) int {
	res := 0
	for num != 0 {
		res++
		num = num >> 7
	}

	// if res == 0, return 1.
	if res == 0 {
		res++
	}

	return res
}

// compareKey 比较两个key的大小.
func compareKey(a, b slice.Slice) int {
	keyA, lenA := loadKey(a)
	keyB, lenB := loadKey(b)

	// not equal, return result.
	if res := keyA.Compare(keyB); res != 0 {
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

// 从一条record中读取 key 部分(指向的数据段相同).
func loadKey(record slice.Slice) (slice.Slice, int) {
	keyLength, varintLength := binary.Varint(record)

	return record[varintLength : varintLength+int(keyLength)], varintLength + int(keyLength)
}
