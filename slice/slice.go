// Package slice 封装golang切片类型，添加比较操作.
package slice

const (
	Larger  = int8(+1)
	Smaller = int8(-1)
	Same    = int8(0)
)

// Slice 是基于[]byte的切片类型，实现了其比较操作.
type Slice []byte

// Compare 比较两个切片，返回两者比较结果.
func (s Slice) Compare(b Slice) int8 {
	if string(s) > string(b) {
		return Larger
	} else if string(s) < string(b) {
		return Smaller
	}

	return Same
}
