// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

import (
	"fmt"
)

type ExampleStruct struct {
	Val0 int32
	Val1 int32
	Val2 int32
}

func (e *ExampleStruct) UnmarshalCQL2(data []byte) int32 {
	write := int32(0)
	elemLen := int32(0)
	e.Val0, write = decodeIntToInt32(data[:8])
	e.Val1, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	e.Val2, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	return write
}

func unmarshalToExample(e *ExampleStruct, data []byte) int32 {
	write := int32(0)
	elemLen := int32(0)
	e.Val0, write = decodeIntToInt32(data[:8])
	e.Val1, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	e.Val2, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	return write
}

func unmarshalExample(data []byte) (int32, ExampleStruct) {
	out := ExampleStruct{}
	write := int32(0)
	elemLen := int32(0)
	out.Val0, write = decodeIntToInt32(data[:8])
	out.Val1, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	out.Val2, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	return write, out
}

func unmarshalRefExample(data []byte) (int32, *ExampleStruct) {
	out := ExampleStruct{}
	write := int32(0)
	elemLen := int32(0)
	out.Val0, write = decodeIntToInt32(data[:8])
	out.Val1, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	out.Val2, elemLen = decodeIntToInt32(data[write : write+8])
	write += elemLen
	return write, &out
}

func (e *ExampleStruct) GetID() int32 {
	return e.Val0
}

func decodeIntToInt32(data []byte) (out int32, read int32) {
	valueLen := dec4ToInt32(data[:4])
	switch {
	case valueLen == 4:
		return dec4ToInt32(data[4:8]), 8
	case valueLen <= 0:
		return int32(0), 4
	default:
		panic(fmt.Sprintf("wrong value len <int> type should:4 have:%d, data:%v", valueLen, data[:8]))
	}
}

func dec4ToInt32(data []byte) int32 {
	return int32(data[0])<<24 | int32(data[1])<<16 | int32(data[2])<<8 | int32(data[3])
}
