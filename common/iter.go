package common

import "github.com/goleveldb/goleveldb/slice"

type Iterator interface {
	// Success indicates the iterator has done its job
	Success() bool
	// Prev move the iterator backward
	Prev()
	// Next move the iterator forward
	Next()
	// Find given the specific key, find the corresponding value
	// if (iter.Success()) {
	// 	value := iter.Value()
	// }
	Find(key slice.Slice)
	// Key returns key of the pair to which the iterator is pointing
	Key() slice.Slice
	// Value returns value of the pair to which the iterator is pointing
	Value() slice.Slice
}
