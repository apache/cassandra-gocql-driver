// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

// ExampleRefSlice represent example implemented Slice interface with processing by reference of slice for slice.
// Comparison of slice processing in various ways you can see in bench_slice_test.go.
type ExampleRefSlice struct {
	rows   *[]ExampleStruct
	rowIdx int
}

func (s *ExampleRefSlice) ReUse(rows *[]ExampleStruct) {
	s.rowIdx = 0
	s.rows = rows
}

func (s *ExampleRefSlice) Init(count int32) {
	*s.rows = make([]ExampleStruct, count)
}

func (s *ExampleRefSlice) Append(count int32) {
	*s.rows = append(*s.rows, make([]ExampleStruct, count)...)
}

func (s *ExampleRefSlice) UnmarshalElem(data []byte) int32 {
	if s.rowIdx > s.Len()-1 {
		s.Append(1)
	}
	return (*s.rows)[s.rowIdx].UnmarshalCQL2(data)
}

func (s *ExampleRefSlice) NextElem() {
	s.rowIdx++
}

func (s *ExampleRefSlice) OnElemError() {
	(*s.rows)[s.rowIdx] = *new(ExampleStruct)
}

func (s *ExampleRefSlice) CutOnDone() {
	if s.rowIdx+1 < s.Len() {
		*s.rows = (*s.rows)[:s.rowIdx+1]
	}
}

func (s *ExampleRefSlice) Len() int {
	return len(*s.rows)
}
