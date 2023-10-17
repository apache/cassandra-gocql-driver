// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

import (
	"fmt"
	"github.com/gocql/gocql/pkg/writers"
	"io"
	"strings"
)

func InitToSliceWriter(rows writers.Slice, columnsInRow int) ToSliceWriter {
	return ToSliceWriter{
		rows:    rows,
		columns: columnsInRow,
	}
}

// ToSliceWriter it`s implementation of writers.Rows interface, designed for write rows to the Slice`s.
type ToSliceWriter struct {
	rows    writers.Slice
	columns int
}

func (r *ToSliceWriter) ColumnsInRow() int {
	return r.columns
}

func (r *ToSliceWriter) ReUse(rows writers.Slice) {
	r.rows = rows
}

func (r *ToSliceWriter) Prepare(rows int32) {
	if rows == 0 {
		return
	}
	if r.rows.Len() < 1 {
		r.rows.Init(rows)
	}
	r.rows.Append(rows)
}

func (r *ToSliceWriter) WriteRow(data []byte) (write int32, err error) {
	// In usual case don`t need to check data on every read operation, because if unmarshalers already tested and data r right we should just read.
	// In case wrong data or wrong unmarshaler will result to wrong all rows in response, that`s why we can just catch panic once and make error.
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%T", errF), "runtime.boundsError") {
				err = io.ErrUnexpectedEOF
			} else {
				err = fmt.Errorf("%s", errF)
			}
			r.rows.OnElemError()
		}
	}()
	write = r.rows.UnmarshalElem(data)
	r.rows.NextElem()
	return
}

func (r *ToSliceWriter) WriteRows(data []byte) (write int32, err error) {
	// In usual case don`t need to check data on every read operation, because if unmarshalers already tested and data r right we should just read.
	// In case wrong data or wrong unmarshaler will result to wrong all rows in response, that`s why we can just catch panic once and make error.
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%T", errF), "runtime.boundsError") {
				err = io.ErrUnexpectedEOF
			} else {
				err = fmt.Errorf("%s", errF)
			}
			r.rows.OnElemError()
		}
	}()
	for int(write) < len(data) {
		write += r.rows.UnmarshalElem(data[write:])
		r.rows.NextElem()
	}
	return
}
