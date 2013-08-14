// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
)

type queryContext interface {
	executeQuery(query *Query) (frame, error)
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo *TypeInfo
}

type BatchType int

const (
	LoggedBatch   BatchType = 0
	UnloggedBatch BatchType = 1
	CounterBatch  BatchType = 2
)

/*
type Batch struct {
	queries []*Query
	ctx     queryContext
	cons    Consistency
}

func (b *Batch) Query(stmt string, args ...interface{}) *Query {
	return &Query{
		stmt: stmt,
		args: args,
		cons: b.cons,
		//ctx:  b,
	}
}

func (b *Batch) Apply() error {
	return nil
} */

type Consistency uint16

const (
	Any Consistency = iota
	One
	Two
	Three
	Quorum
	All
	LocalQuorum
	EachQuorum
	Serial
	LocalSerial
)

type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

var (
	ErrNotFound        = errors.New("not found")
	ErrNoHostAvailable = errors.New("no host available")
	ErrProtocol        = errors.New("protocol error")
)
