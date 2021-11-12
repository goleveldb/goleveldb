package block

import (
	"encoding/binary"

	"github.com/goleveldb/goleveldb/slice"
)

type Block struct {
	Content slice.Slice	// all data in the block
	NumRestarts uint32
	RestartsOffset uint32
}

func New(content slice.Slice) *Block {
	numRestarts := binary.BigEndian.Uint32(content[len(content)-4:])
	return &Block{
		Content: content,
		NumRestarts: numRestarts,
		RestartsOffset: uint32(len(content)) - 4 - 4 * numRestarts,
	}
}