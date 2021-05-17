// Package slice 封装golang切片类型，添加比较操作.
package slice

import "testing"

func TestSlice_Compare(t *testing.T) {
	tests := []struct {
		name string
		s    Slice
		arg  Slice
		want int
	}{
		{
			name: "test larger cmp",
			s:    []byte{1, 2, 3},
			arg:  []byte{0, 2, 3},
			want: CMPLarger,
		},
		{
			name: "test euqal cmp",
			s:    []byte{1, 2, 3},
			arg:  []byte{1, 2, 3},
			want: CMPSame,
		},
		{
			name: "test smaller cmp",
			s:    []byte{1, 2, 3},
			arg:  []byte{2, 2, 3},
			want: CMPSmaller,
		},
		{
			name: "test cmp with empty",
			s:    []byte{1, 2, 3},
			arg:  nil,
			want: CMPLarger,
		},
		{
			name: "test both empty",
			s:    nil,
			arg:  nil,
			want: CMPSame,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Compare(tt.arg); got != tt.want {
				t.Errorf("Slice.Compare() = %v, want %v", got, tt.want)
			}
		})
	}
}
