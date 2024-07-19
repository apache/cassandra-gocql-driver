// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

// ExampleSliceRefs represent example implemented Slice interface with direct slice processing for slice of references.
// Comparison of slice processing in various ways you can see in bench_slice_test.go.
type ExampleSliceRefs struct {
	Rows   []*ExampleStruct
	rowIdx int
}

func (s *ExampleSliceRefs) Init(count int32) {
	s.rowIdx = 0
	s.Rows = make([]*ExampleStruct, count)
}

func (s *ExampleSliceRefs) Append(count int32) {
	s.Rows = append(s.Rows, make([]*ExampleStruct, count)...)
}

func (s *ExampleSliceRefs) UnmarshalElem(data []byte) int32 {
	if s.rowIdx > s.Len()-1 {
		s.Append(1)
	}
	s.Rows[s.rowIdx] = &ExampleStruct{}
	return s.Rows[s.rowIdx].UnmarshalCQL2(data)
}

func (s *ExampleSliceRefs) NextElem() {
	s.rowIdx++
}

func (s *ExampleSliceRefs) OnElemError() {
	*s.Rows[s.rowIdx] = *new(ExampleStruct)
}

func (s *ExampleSliceRefs) CutOnDone() {
	if s.rowIdx+1 < s.Len() {
		s.Rows = s.Rows[:s.rowIdx+1]
	}
}

func (s *ExampleSliceRefs) Len() int {
	return len(s.Rows)
}
