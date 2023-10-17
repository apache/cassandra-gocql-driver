// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package writers

import "errors"

var NAP = errors.New("write operation is completed, but there is still data left")

type Struct interface {
	UnmarshalCQL2(data []byte) int32
}

// Rows describes interaction between callers and Rows`s designed for row by row write.
// Rows useful if expect response with undefined rows count and callers fill the data gradual.
// Callers must call call Prepare(n) before call WriteRow n times and/or before WriteRows call.
// Implementations must not modify the slice data, even temporarily and not retain data.
type Rows interface {
	// Prepare helps inform Rows`s about count of rows that callers have. So callers can help Rows`s have better resource pre allocation.
	Prepare(rows int32)
	// WriteRow write a single row.
	// If write is done and len(data)>w must return error=nil.
	// If data is not enough to write a row must return io.ErrUnexpectedEOF
	WriteRow(data []byte) (w int32, err error)
	// WriteRows write all rows from data.
	// If write is not done or is partial done because data not enough must return io.ErrUnexpectedEOF
	WriteRows(data []byte) (w int32, err error)
	ColumnsInRow() int
}

// Row describes interaction between callers and writers designed for solitary row write.
// Row useful if expect response with rows=1 or 0.
// Implementations must not modify the slice data, even temporarily and not retain data.
type Row interface {
	// WriteRow write a single row.
	// If write is done and len(data)>w must return error=nil.
	// If data is not enough to write a row must return io.ErrUnexpectedEOF
	WriteRow(data []byte) (write int32, err error)
	ColumnsInRow() int
}

// Slice describes interaction between Rows and go slices (actually structures with slices).
// Rows must raise code from any panic due UnmarshalElem calls and call OnElemError.
// Implementations must not modify the slice data, even temporarily and not retain data.
// On wrong data cases implementations must use panic with detailed description.
// Implementations must independently monitor the sufficiency of the slice length.
type Slice interface {
	// Init same as ...make(slice,count).
	Init(count int32)
	// Append same as build in append func.
	Append(count int32)
	// UnmarshalElem writes data to slice elem, returns write bites count.
	UnmarshalElem(data []byte) int32
	// NextElem switch Slice to next elem.
	NextElem()
	// OnElemError helps ToSliceWriter tell to Slice about panic due UnmarshalElem call.
	OnElemError()
	// CutOnDone helps Slice to free pre allocated resources due all Expand and Init calls.
	CutOnDone()
	// Len returns current Slice len.
	Len() int
}

// Map describes interaction between Rows and go maps.
// Rows must raise code from any panic due UnmarshalMapElem calls and call OnMapElemError.
// Implementations must not modify the slice data, even temporarily and not retain data.
// On wrong data cases implementations must use panic with detailed description.
type Map interface {
	// UnmarshalMapElem writes data to map elem, returns write bites count.
	UnmarshalMapElem(data []byte) int32
	// OnMapElemError helps ToMapWriter tell to Map about panic due UnmarshalMapElem call.
	OnMapElemError()
}
