package block

import (
	"encoding/binary"
	"errors"

	"github.com/goleveldb/goleveldb/config"
	"github.com/goleveldb/goleveldb/slice"
)

type Writer interface {
	AddEntry(k, v slice.Slice) error
	Finish() slice.Slice
	Reset()
	Size() int
}

type writerImpl struct {
	content       []byte      // data in current block waiting to be finished
	restartPoints []uint32    // stores all indexes where lastInsertKey is recalculated
	lastInsertKey slice.Slice // used for prefix compression
	counter       uint32      // used for prefix compression, see config.BLOCK_RESTART_INTERVAL
	isFinished    bool
}

var _ Writer = (*writerImpl)(nil)

// NewWriter: create a concrete instance of Writer interface
func NewWriter() Writer {
	restartPoints := []uint32{0}
	return &writerImpl{
		restartPoints: restartPoints,
	}
}

var ErrBlockFinished = errors.New("unable to perform actions on finished block")

// AddEntry: append a new entry to the pending data block in BlockWriter
func (b *writerImpl) AddEntry(key, value slice.Slice) error {
	if b.isFinished {
		return ErrBlockFinished
	}
	// get the prefix length of current key and last insert key
	share := 0
	if b.counter == config.BLOCK_RESTART_INTERVAL {
		b.counter = 0
		b.restartPoints = append(b.restartPoints, uint32(len(b.content)))
		b.lastInsertKey = nil
	} else {
		minKeyLen := minInt(len(key), len(b.lastInsertKey))
		for share < minKeyLen {
			if key[share] != b.lastInsertKey[share] {
				break
			}

			share++
		}
	}

	var (
		unshare          = len(key) - share
		valueLen         = len(value)
		varintLenShare   = varintLen(share)
		varintLenUnshare = varintLen(unshare)
		varintLenValue   = varintLen(valueLen)
		totalSpace       = len(b.content) + varintLenShare + varintLenUnshare + varintLenValue + unshare + valueLen
	)

	newContent, newPos := make([]byte, totalSpace), 0
	newPos += copy(newContent, b.content)
	newPos += binary.PutUvarint(newContent[newPos:], uint64(share))
	newPos += binary.PutUvarint(newContent[newPos:], uint64(unshare))
	newPos += binary.PutUvarint(newContent[newPos:], uint64(valueLen))
	newPos += copy(newContent[newPos:], key[share:])
	copy(newContent[newPos:], value)

	b.lastInsertKey = key
	b.content = newContent
	b.counter++

	return nil
}

// Finish: build a slice containing the block content and restart point array info
// If Finish() is called, new entries shouldn't be appended until Reset() is called.
func (b *writerImpl) Finish() slice.Slice {
	b.isFinished = true
	newContent, newPos := make([]byte, b.Size()), 0
	newPos += copy(newContent, b.content)

	for _, restartPoint := range b.restartPoints {
		binary.BigEndian.PutUint32(newContent[newPos:], restartPoint)
		newPos += 4
	}
	binary.BigEndian.PutUint32(newContent[newPos:], uint32(len(b.restartPoints)))

	return slice.Slice(newContent)
}

// Reset: reset the block to its initial status
func (b *writerImpl) Reset() {
	b.isFinished = false
	b.content = nil
	b.counter = 0
	b.lastInsertKey = nil
	b.restartPoints = []uint32{0}
}

// Size: return the estimated size of the built slice if Finish() is called
func (b *writerImpl) Size() int {
	return len(b.content) + len(b.restartPoints)*4 + 4
}

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
