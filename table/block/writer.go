package block

import (
	"encoding/binary"

	"github.com/goleveldb/goleveldb/config"
	"github.com/goleveldb/goleveldb/slice"
)

type Writer interface {
	AddEntry(k,v slice.Slice)
	Finish() slice.Slice
	Reset()
	Size() int
}

type writerImpl struct {
	content []byte	// 当前块数据内容
	restartPoints []uint32	// 重新进行前缀压缩后的第一个key下标（即这些下标对应的key没有被压缩过，可以进行二分查找）
	lastInsertKey slice.Slice
	counter	uint32	// 前缀压缩计数
	isFinished bool
}

var _ Writer = (*writerImpl)(nil)

func NewWriter() Writer {
	restartPoints := []uint32{0}
	return &writerImpl{
		restartPoints: restartPoints,
	}
}

func (b *writerImpl) AddEntry(key, value slice.Slice) {
	// 计算key和lastInsertKey的最长匹配前缀长度
	share := 0
	if b.counter == config.BLOCK_RESTART_INTERVAL {
		b.counter = 0
		b.restartPoints = append(b.restartPoints, uint32(len(b.content)))
		b.lastInsertKey = slice.Slice{}
	}else {
		minKeyLen := minInt(len(key), len(b.lastInsertKey))
		for share < minKeyLen {
			if key[share] != b.lastInsertKey[share] {
				break
			}
			
			share++
		}
	}


	var (
		unshare = len(key) - share
		valueLen = len(value)
		varintShare = varintLen(share)
		varintUnshare = varintLen(unshare)
		varintValue = varintLen(valueLen)
		totalSpace = len(b.content) + varintShare + varintUnshare + varintValue + unshare + valueLen
	)

	newContent, newPos := make([]byte, totalSpace), 0
	newPos += copy(newContent, b.content)
	newPos += binary.PutVarint(newContent[newPos:], int64(varintLen(share)))
	newPos += binary.PutVarint(newContent[newPos:], int64(varintLen(unshare)))
	newPos += binary.PutVarint(newContent[newPos:], int64(varintLen(valueLen)))
	newPos += copy(newContent[newPos:], key[share:])
	copy(newContent[newPos:], value)

	b.lastInsertKey = key
	b.content = newContent
	b.counter++
}

func (b *writerImpl) Finish() slice.Slice {
	totalSpace := len(b.content) + len(b.restartPoints) * 4 + 4
	newContent, newPos := make([]byte, totalSpace), 0
	b.isFinished = true

	newPos += copy(newContent, b.content)
	for _, restartPoint := range b.restartPoints {
		binary.BigEndian.PutUint32(newContent[newPos:], restartPoint)
		newPos += 4
	}

	binary.BigEndian.PutUint32(newContent[newPos:], uint32(len(b.restartPoints)))
	return slice.Slice(newContent)
}

func (b *writerImpl) Reset() {
	b.isFinished = false
	b.content = nil
	b.counter = 0
	b.lastInsertKey = slice.Slice{}
	b.restartPoints = nil
}

func (b *writerImpl) Size() int{
	return len(b.content) + len(b.restartPoints) * 4 + 4
}

// 返回一个int的变长int类型长度
func varintLen(a int) int {
	if a == 0 {
		return 1
	}

	res := 0
	for a > 0 {
		res++
		a >>= 7
	}

	return res
}

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}