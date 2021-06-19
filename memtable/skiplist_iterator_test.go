package memtable

import (
	"errors"
	"testing"

	"github.com/goleveldb/goleveldb/slice"
)

func TestMemtable_SkipListIterator_seekToFirst(t *testing.T) {
	tests := []struct {
		name    string
		list    []slice.Slice
		wantRes slice.Slice
		wantErr error
	}{
		{
			name:    "test normal",
			list:    []slice.Slice{slice.Slice("1"), slice.Slice("2")},
			wantRes: slice.Slice("1"),
			wantErr: nil,
		},
		{
			name:    "test nil skiplist",
			list:    []slice.Slice{},
			wantRes: nil,
			wantErr: ErrNotValid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := generateSkiplistIterator(tt.list)
			it.seekToFirst()
			if got, err := it.key(); (got.Compare(tt.wantRes) != slice.CMPSame) || !errors.Is(err, tt.wantErr) {
				t.Errorf("seekToFirst() => want = (%s, %v), but got = (%s, %v)", string(tt.wantRes), tt.wantErr, string(got), err)
			}
		})
	}
}

func TestMemtable_SkipListIterator_seekToLast(t *testing.T) {
	tests := []struct {
		name    string
		list    []slice.Slice
		wantRes slice.Slice
		wantErr error
	}{
		{
			name:    "test normal",
			list:    []slice.Slice{slice.Slice("1"), slice.Slice("2")},
			wantRes: slice.Slice("2"),
			wantErr: nil,
		},
		{
			name:    "test nil skiplist",
			list:    []slice.Slice{},
			wantRes: nil,
			wantErr: ErrNotValid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := generateSkiplistIterator(tt.list)
			it.seekToLast()
			if got, err := it.key(); (got.Compare(tt.wantRes) != slice.CMPSame) || !errors.Is(err, tt.wantErr) {
				t.Errorf("seekToLast() => want = (%s, %v), but got = (%s, %v)", string(tt.wantRes), tt.wantErr, string(got), err)
			}
		})
	}
}

func TestMemtable_SkipListIterator_seek(t *testing.T) {
	tests := []struct {
		name       string
		list       []slice.Slice
		seekTarget slice.Slice
		wantRes    slice.Slice
		wantErr    error
	}{
		{
			name:       "test normal, get first",
			list:       []slice.Slice{slice.Slice("1"), slice.Slice("2")},
			seekTarget: slice.Slice("1"),
			wantRes:    slice.Slice("1"),
			wantErr:    nil,
		},
		{
			name:       "test normal, get last",
			list:       []slice.Slice{slice.Slice("1"), slice.Slice("2")},
			seekTarget: slice.Slice("2"),
			wantRes:    slice.Slice("2"),
			wantErr:    nil,
		},
		{
			name:       "test normal, get not exist",
			list:       []slice.Slice{slice.Slice("1"), slice.Slice("2")},
			seekTarget: slice.Slice("3"),
			wantRes:    nil,
			wantErr:    ErrNotValid,
		},
		{
			name:       "test nil skiplist",
			list:       []slice.Slice{},
			seekTarget: slice.Slice("2"),
			wantRes:    nil,
			wantErr:    ErrNotValid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := generateSkiplistIterator(tt.list)
			it.seek(tt.seekTarget)
			if got, err := it.key(); (got.Compare(tt.wantRes) != slice.CMPSame) || !errors.Is(err, tt.wantErr) {
				t.Errorf("seek() => want = (%s, %v), but got = (%s, %v)", string(tt.wantRes), tt.wantErr, string(got), err)
			}
		})
	}
}

func TestMemtable_SkipListIterator_next(t *testing.T) {
	// 顺序遍历一次.
	it := generateSkiplistIterator([]slice.Slice{slice.Slice("1"), slice.Slice("2"), slice.Slice("3")})
	it.seekToFirst()
	if got, err := it.key(); (got.Compare(slice.Slice("1")) != slice.CMPSame) || err != nil {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "1", false, string(got), err)
	}

	it.next()
	if got, err := it.key(); (got.Compare(slice.Slice("2")) != slice.CMPSame) || err != nil {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "2", false, string(got), err)
	}

	it.next()
	if got, err := it.key(); (got.Compare(slice.Slice("3")) != slice.CMPSame) || err != nil {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "3", false, string(got), err)
	}

	it.next()
	if got, err := it.key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}

	it.next()
	if got, err := it.key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}
}

func TestMemtable_SkipListIterator_prev(t *testing.T) {
	// 顺序遍历一次.
	it := generateSkiplistIterator([]slice.Slice{slice.Slice("1"), slice.Slice("2"), slice.Slice("3")})
	it.seekToLast()
	if got, err := it.key(); (got.Compare(slice.Slice("3")) != slice.CMPSame) || err != nil {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "1", false, string(got), err)
	}

	it.prev()
	if got, err := it.key(); (got.Compare(slice.Slice("2")) != slice.CMPSame) || err != nil {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "2", false, string(got), err)
	}

	it.prev()
	if got, err := it.key(); (got.Compare(slice.Slice("1")) != slice.CMPSame) || err != nil {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "3", false, string(got), err)
	}

	it.prev()
	if got, err := it.key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}

	it.prev()
	if got, err := it.key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}
}

func generateSkiplistIterator(datas []slice.Slice) *iterator {
	list := newSkiplist()
	for _, data := range datas {
		list.insert(data)
	}

	return list.iterator()
}
