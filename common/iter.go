package common

import "github.com/goleveldb/goleveldb/slice"

type Iterator interface {
	// indicates the iterator has done its job
	Success() bool
	// move the iterator backward
	Prev()
	// move the iterator forward
	Next()
	// given the specific key, find the corresponding value
	// eg.
	// if (iter.Success()) {
	// 	value := iter.Value()
	// }
	Find(key slice.Slice)
	// returns key of the pair to which the iterator is pointing
	Key() slice.Slice
	// returns value of the pair to which the iterator is pointing
	Value() slice.Slice
}