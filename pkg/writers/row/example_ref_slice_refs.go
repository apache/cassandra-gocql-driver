// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

// ExampleRefSliceRefs represent example implemented Slice interface with processing by reference of slice for slice of references.
// Comparison of slice processing in various ways you can see in bench_slice_test.go.
type ExampleRefSliceRefs struct {
	rows   *[]*ExampleStruct
	rowIdx int
}

func (s *ExampleRefSliceRefs) ReUse(rows *[]*ExampleStruct) {
	s.rowIdx = 0
	s.rows = rows
}

func (s *ExampleRefSliceRefs) Init(count int32) {
	*s.rows = make([]*ExampleStruct, count)
}

func (s *ExampleRefSliceRefs) Append(count int32) {
	*s.rows = append(*s.rows, make([]*ExampleStruct, count)...)
}

func (s *ExampleRefSliceRefs) UnmarshalElem(data []byte) int32 {
	if s.rowIdx > s.Len()-1 {
		s.Append(1)
	}
	(*s.rows)[s.rowIdx] = &ExampleStruct{}
	return (*s.rows)[s.rowIdx].UnmarshalCQL2(data)
}

func (s *ExampleRefSliceRefs) NextElem() {
	s.rowIdx++
}

func (s *ExampleRefSliceRefs) OnElemError() {
	(*s.rows)[s.rowIdx] = new(ExampleStruct)
}

func (s *ExampleRefSliceRefs) CutOnDone() {
	if s.rowIdx+1 < s.Len() {
		*s.rows = (*s.rows)[:s.rowIdx+1]
	}
}

func (s *ExampleRefSliceRefs) Len() int {
	return len(*s.rows)
}
