// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

type ExampleMap map[int32]ExampleStruct

func (s ExampleMap) UnmarshalMapElem(data []byte) int32 {
	tmp := ExampleStruct{}
	write := tmp.UnmarshalCQL2(data)
	s[tmp.GetID()] = tmp
	return write
}

func (s ExampleMap) OnMapElemError() {
}

type ExampleMapRefs map[int32]ExampleStruct

func (s *ExampleMapRefs) UnmarshalMapElem(data []byte) int32 {
	tmp := ExampleStruct{}
	write := tmp.UnmarshalCQL2(data)
	(*s)[tmp.GetID()] = tmp
	return write
}

func (s *ExampleMapRefs) OnMapElemError() {
}

type ExampleRefMap map[int32]*ExampleStruct

func (s ExampleRefMap) UnmarshalMapElem(data []byte) int32 {
	tmp := ExampleStruct{}
	write := tmp.UnmarshalCQL2(data)
	s[tmp.GetID()] = &tmp
	return write
}

func (s ExampleRefMap) OnMapElemError() {
}

type ExamplesRefMapRefs map[int32]*ExampleStruct

func (s *ExamplesRefMapRefs) UnmarshalMapElem(data []byte) int32 {
	tmp := ExampleStruct{}
	write := tmp.UnmarshalCQL2(data)
	(*s)[tmp.GetID()] = &tmp
	return write
}
func (s *ExamplesRefMapRefs) OnMapElemError() {
}
