// Package varint impl utils for varint.
package varint

// VarintLen 返回 num 转化为 varintLen 类型后的字节数.
func VarintLen(num int) int {
	res := 0
	for num != 0 {
		res++
		num = num >> 7
	}

	// if res == 0, return 1.
	if res == 0 {
		res++
	}

	return res
}
