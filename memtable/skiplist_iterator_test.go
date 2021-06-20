package memtable

import (
	"errors"
	"log"
	"testing"

	"github.com/goleveldb/goleveldb/slice"
)

func TestMemtable_SkipListIterator_SeekToFirst(t *testing.T) {
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
			it.SeekToFirst()
			if got, err := it.Key(); (got.Compare(tt.wantRes) != slice.CMPSame) || !errors.Is(err, tt.wantErr) {
				t.Errorf("seekToFirst() => want = (%s, %v), but got = (%s, %v)", string(tt.wantRes), tt.wantErr, string(got), err)
			}
		})
	}
}

func TestMemtable_SkipListIterator_SeekToLast(t *testing.T) {
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
			it.SeekToLast()
			if got, err := it.Key(); (got.Compare(tt.wantRes) != slice.CMPSame) || !errors.Is(err, tt.wantErr) {
				t.Errorf("seekToLast() => want = (%s, %v), but got = (%s, %v)", string(tt.wantRes), tt.wantErr, string(got), err)
			}
		})
	}
}

func TestMemtable_SkipListIterator_Seek(t *testing.T) {
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
			name:       "test one seek",
			list:       []slice.Slice{slice.Slice("1"), slice.Slice("3")},
			seekTarget: slice.Slice("2"),
			wantRes:    slice.Slice("3"),
			wantErr:    nil,
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
			it.Seek(tt.seekTarget)
			if got, err := it.Key(); (got.Compare(tt.wantRes) != slice.CMPSame) || !errors.Is(err, tt.wantErr) {
				t.Errorf("seek() => want = (%s, %v), but got = (%s, %v)", string(tt.wantRes), tt.wantErr, string(got), err)
			}
		})
	}
}

func TestMemtable_SkipListIterator_Next(t *testing.T) {
	// 顺序遍历一次.
	it := generateSkiplistIterator([]slice.Slice{slice.Slice("1"), slice.Slice("2"), slice.Slice("3")})
	it.SeekToFirst()
	if got, err := it.Key(); (got.Compare(slice.Slice("1")) != slice.CMPSame) || err != nil {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "1", false, string(got), err)
	}

	it.Next()
	if got, err := it.Key(); (got.Compare(slice.Slice("2")) != slice.CMPSame) || err != nil {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "2", false, string(got), err)
	}

	it.Next()
	if got, err := it.Key(); (got.Compare(slice.Slice("3")) != slice.CMPSame) || err != nil {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "3", false, string(got), err)
	}

	it.Next()
	if got, err := it.Key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}

	it.Next()
	if got, err := it.Key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("next() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}
}

func TestMemtable_SkipListIterator_Prev(t *testing.T) {
	// 顺序遍历一次.
	it := generateSkiplistIterator([]slice.Slice{slice.Slice("1"), slice.Slice("2"), slice.Slice("3")})
	it.SeekToLast()
	if got, err := it.Key(); (got.Compare(slice.Slice("3")) != slice.CMPSame) || err != nil {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "1", false, string(got), err)
	}

	it.Prev()
	if got, err := it.Key(); (got.Compare(slice.Slice("2")) != slice.CMPSame) || err != nil {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "2", false, string(got), err)
	}

	it.Prev()
	if got, err := it.Key(); (got.Compare(slice.Slice("1")) != slice.CMPSame) || err != nil {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "3", false, string(got), err)
	}

	it.Prev()
	if got, err := it.Key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}

	it.Prev()
	if got, err := it.Key(); (got != nil) || !errors.Is(err, ErrNotValid) {
		t.Errorf("prev() => want = (%s, %v), but got = (%s, %v)", "", false, string(got), err)
	}
}

func generateSkiplistIterator(datas []slice.Slice) *Iterator {
	list := newSkiplist()
	for _, data := range datas {
		if err := list.insert(data); err != nil {
			log.Panicln(err)
		}
	}

	return list.iterator()
}
