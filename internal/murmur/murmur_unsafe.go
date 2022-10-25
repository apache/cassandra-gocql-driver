//go:build !appengine && !s390x
// +build !appengine,!s390x

package murmur

import (
	"unsafe"
)

func getBlock(data []byte, n int) (int64, int64) {
	block := (*[2]int64)(unsafe.Pointer(&data[n*16]))

	k1 := block[0]
	k2 := block[1]
	return k1, k2
}
