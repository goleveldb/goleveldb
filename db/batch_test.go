package db

import (
	"fmt"
	"testing"

	"github.com/goleveldb/goleveldb/slice"
)

type batchTestSingleOp struct {
	clear  bool
	put    bool
	delete bool
	key    slice.Slice
	value  slice.Slice
}

type batchTestIterateResult struct {
	opType byte
	key    slice.Slice
	value  slice.Slice
}

func TestBatchOperations(t *testing.T) {
	tests := []struct {
		name string
		ops  []batchTestSingleOp
		want []*batchTestIterateResult
	}{
		{
			name: "put and delete test",
			ops: []batchTestSingleOp{
				{put: true, key: []byte("key1"), value: []byte("value1")},
				{put: true, key: []byte("key2"), value: []byte("value2")},
				{put: true, key: []byte("key1"), value: []byte("value1-1")},
				{delete: true, key: []byte("key1")},
			},
			want: []*batchTestIterateResult{
				{opType: kPutOp, key: []byte("key1"), value: []byte("value1")},
				{opType: kPutOp, key: []byte("key2"), value: []byte("value2")},
				{opType: kPutOp, key: []byte("key1"), value: []byte("value1-1")},
				{opType: kDeleteOp, key: []byte("key1")},
			},
		},
		{
			name: "no operation test",
			ops:  []batchTestSingleOp{},
			want: []*batchTestIterateResult{},
		},
		{
			name: "clear test",
			ops: []batchTestSingleOp{
				{put: true, key: []byte("key1"), value: []byte("value1")},
				{put: true, key: []byte("key2"), value: []byte("value2")},
				{put: true, key: []byte("key1"), value: []byte("value1-1")},
				{delete: true, key: []byte("key1")},
				{clear: true},
				{put: true, key: []byte("key1"), value: []byte("value1")},
				{put: true, key: []byte("key2"), value: []byte("value2")},
			},
			want: []*batchTestIterateResult{
				{opType: kPutOp, key: []byte("key1"), value: []byte("value1")},
				{opType: kPutOp, key: []byte("key2"), value: []byte("value2")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := NewBatch()

			for _, op := range tt.ops {
				if op.put {
					batch.Put(op.key, op.value)
				} else if op.delete {
					batch.Delete(op.key)
				} else if op.clear {
					batch.Clear()
				}
			}

			iter := &testBatchHnalderImpl{}
			batch.Iterate(iter)

			if err := batchResultEqual(iter.results, tt.want); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestBatchAppend(t *testing.T) {
	tests := []struct {
		name        string
		buildBatchA func() Batch
		buildBatchB func() Batch
		want        []*batchTestIterateResult
	}{
		{
			name: "normal append",
			buildBatchA: func() Batch {
				batch := NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Put([]byte("key2"), []byte("value2"))
				return batch
			},
			buildBatchB: func() Batch {
				batch := NewBatch()
				batch.Put([]byte("key3"), []byte("value3"))
				batch.Put([]byte("key4"), []byte("value4"))
				return batch
			},
			want: []*batchTestIterateResult{
				{opType: kPutOp, key: []byte("key1"), value: []byte("value1")},
				{opType: kPutOp, key: []byte("key2"), value: []byte("value2")},
				{opType: kPutOp, key: []byte("key3"), value: []byte("value3")},
				{opType: kPutOp, key: []byte("key4"), value: []byte("value4")},
			},
		},
		{
			name: "append empty batch",
			buildBatchA: func() Batch {
				batch := NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Put([]byte("key2"), []byte("value2"))
				return batch
			},
			buildBatchB: func() Batch {
				return NewBatch()
			},
			want: []*batchTestIterateResult{
				{opType: kPutOp, key: []byte("key1"), value: []byte("value1")},
				{opType: kPutOp, key: []byte("key2"), value: []byte("value2")},
			},
		},
		{
			name: "empty append not empty",
			buildBatchA: func() Batch {
				return NewBatch()
			},
			buildBatchB: func() Batch {
				batch := NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Put([]byte("key2"), []byte("value2"))
				return batch
			},

			want: []*batchTestIterateResult{
				{opType: kPutOp, key: []byte("key1"), value: []byte("value1")},
				{opType: kPutOp, key: []byte("key2"), value: []byte("value2")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchA := tt.buildBatchA()
			batchB := tt.buildBatchB()
			batchA.Append(batchB)

			iter := &testBatchHnalderImpl{}
			batchA.Iterate(iter)

			if err := batchResultEqual(iter.results, tt.want); err != nil {
				t.Error(err)
			}
		})
	}
}

func batchResultEqual(want, get []*batchTestIterateResult) error {
	if len(get) != len(want) {
		return fmt.Errorf("result size error, got %d, want %d", len(get), len(want))
	}

	for i, result := range get {
		if result.opType != want[i].opType {
			return fmt.Errorf("op type error, got %d, want %d", result.opType, want[i].opType)
		}

		if want[i].key.Compare(result.key) != slice.CMPSame {
			return fmt.Errorf("key error, got %s, want %s", result.key, want[i].key)
		}

		if want[i].value != nil && result.value.Compare(want[i].value) != slice.CMPSame {
			return fmt.Errorf("value error, got %s, want %s", result.value, want[i].value)
		}
	}

	return nil
}

// hanle batch iteration, remind iterat result.
type testBatchHnalderImpl struct {
	results []*batchTestIterateResult
}

func (h *testBatchHnalderImpl) Put(key, value slice.Slice) {
	h.results = append(h.results, &batchTestIterateResult{
		opType: kPutOp,
		key:    key,
		value:  value,
	})
}

func (h *testBatchHnalderImpl) Delete(key slice.Slice) {
	h.results = append(h.results, &batchTestIterateResult{
		opType: kDeleteOp,
		key:    key,
	})
}
