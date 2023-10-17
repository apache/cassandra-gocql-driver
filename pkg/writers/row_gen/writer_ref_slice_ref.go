// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row_gen

import (
	"fmt"
	"io"
	"strings"
)

type InitFunc[V any] func() V

func InitRefSliceRefsWriter[V any](rows *[]*V, writer WriteToFunc[V], columnsInRow int) ToSliceRefsWriter[V] {
	return ToSliceRefsWriter[V]{
		write:   writer,
		rows:    rows,
		columns: columnsInRow,
	}
}

// ToSliceRefsWriter it`s implementation of writers.RowsWriter interface, designed for write rows to the go slices of references.
type ToSliceRefsWriter[V any] struct {
	write     WriteToFunc[V]
	rows      *[]*V
	rowIdx    int
	minRowLen int
	columns   int
}

func (r *ToSliceRefsWriter[V]) ColumnsInRow() int {
	return r.columns
}

func (r *ToSliceRefsWriter[V]) Prepare(rows int32) {
	if rows == 0 {
		return
	}
	if len(*r.rows) < 1 {
		*r.rows = make([]*V, rows)
	}
	*r.rows = append(*r.rows, make([]*V, rows)...)
}

func (r *ToSliceRefsWriter[V]) CutOnDone() {
	if r.rowIdx+1 < len(*r.rows) {
		*r.rows = (*r.rows)[:r.rowIdx+1]
	}
}

func (r *ToSliceRefsWriter[V]) ReUse(rows *[]*V) {
	r.rows = rows
	r.rowIdx = 0
}

func (r *ToSliceRefsWriter[V]) WriteRow(data []byte) (write int32, err error) {
	// In usual case don`t need to check data on every read operation, because if unmarshalers already tested and data r right we should just read.
	// In case wrong data or wrong unmarshaler will result to wrong all rows in response, that`s why we can just catch panic once and make error.
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%T", errF), "runtime.boundsError") {
				err = io.ErrUnexpectedEOF
			} else {
				err = fmt.Errorf("%s", errF)
			}
		}
	}()
	if r.rowIdx > len(*r.rows)-1 {
		*r.rows = append(*r.rows, new(V))
	} else {
		(*r.rows)[r.rowIdx] = new(V)
	}
	write = r.write((*r.rows)[r.rowIdx], data)
	r.rowIdx++
	return
}

func (r *ToSliceRefsWriter[V]) WriteRows(data []byte) (write int32, err error) {
	// In usual case don`t need to check data on every read operation, because if unmarshalers already tested and data r right we should just read.
	// In case wrong data or wrong unmarshaler will result to wrong all rows in response, that`s why we can just catch panic once and make error.
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%T", errF), "runtime.boundsError") {
				err = io.ErrUnexpectedEOF
			} else {
				err = fmt.Errorf("%s", errF)
			}
		}
	}()
	for int(write) < len(data) {
		if r.rowIdx > len(*r.rows)-1 {
			*r.rows = append(*r.rows, new(V))
		} else {
			(*r.rows)[r.rowIdx] = new(V)
		}
		write += r.write((*r.rows)[r.rowIdx], data[write:])
		r.rowIdx++
	}
	return
}
