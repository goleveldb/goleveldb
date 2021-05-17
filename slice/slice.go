// Package slice 封装golang切片类型，添加比较操作.
package slice

const (
	CMPLarger  = int8(+1) // 当前Slice大于目标字符串
	CMPSmaller = int8(-1) // 当前Slice小于目标字符串
	CMPSame    = int8(0)  // 当前Slice等于目标字符串
)

// Slice 是基于[]byte的切片类型，实现了其比较操作.
type Slice []byte

// Compare 比较两个切片，返回两者比较结果.
func (s Slice) Compare(b Slice) int8 {
	if string(s) > string(b) {
		return CMPLarger
	} else if string(s) < string(b) {
		return CMPSmaller
	}

	return CMPSame
}
