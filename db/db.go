// Package db define impl db read/write op.
package db

import "github.com/goleveldb/goleveldb/slice"

type BatchHandler interface {
	// Put handle put operation.
	Put(key, value slice.Slice)
	// Delete handle delete operation.
	Delete(key slice.Slice)
}

// Batch submitted in Write method, record update data.
type Batch interface {
	// Put add put operation to current batch.
	Put(key, value slice.Slice)
	// Delete add delete operation to current batch.
	Delete(key slice.Slice)
	// Clear all operations in current batch.
	Clear()
	// Append operations in b to current batch.
	Append(batch Batch)
	// Iterate batch by iterator.
	Iterate(iter BatchHandler)
}

// DB define db crud ops.
type DB interface {
	Put(key, value slice.Slice) error
	Write(b *Batch) error
}
