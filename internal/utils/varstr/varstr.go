// Package varstr define and impl varstr related ops.
package varstr

import (
	"encoding/binary"

	"github.com/goleveldb/goleveldb/internal/utils/varstr/varint"
)

// VarStrLen return varstr length of src string.
func VarStrLen(src []byte) int {
	return varint.VarintLen(len(src)) + len(src)
}

// PutVarStr put src string into dst slice, return put size.
func PutVarStr(dst []byte, src []byte) int {
	dstLen := binary.PutVarint(dst, int64(len(src)))
	copy(dst[dstLen:], src)

	return dstLen + len(src)
}

// GetVarStr get src string from src slice, return get size.
func GetVarStr(src []byte) ([]byte, int) {
	strLen, varintSize := binary.Varint(src)
	varstrEndPos := varintSize + int(strLen)

	return src[varintSize:varstrEndPos], varstrEndPos
}
