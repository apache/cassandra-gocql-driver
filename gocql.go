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
	ConAny         Consistency = 0x0000
	ConOne         Consistency = 0x0001
	ConTwo         Consistency = 0x0002
	ConThree       Consistency = 0x0003
	ConQuorum      Consistency = 0x0004
	ConAll         Consistency = 0x0005
	ConLocalQuorum Consistency = 0x0006
	ConEachQuorum  Consistency = 0x0007
	ConSerial      Consistency = 0x0008
	ConLocalSerial Consistency = 0x0009
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
	ErrQueryUnbound    = errors.New("can not execute unbound query")
	ErrProtocol        = errors.New("protocol error")
)
