/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"errors"
	"fmt"
	"github.com/gocql/gocql/internal/protocol"
	"time"
)

// type unsetColumn= protocol.UnsetColumn
// UnsetValue represents a value used in a query binding that will be ignored by Cassandra.
//
// By setting a field to the unset value Cassandra will ignore the write completely.
// The main advantage is the ability to keep the same prepared statement even when you don't
// want to update some fields, where before you needed to make another prepared statement.
//
// UnsetValue is only available when using the version 4 of the protocol.
var UnsetValue = protocol.UnsetColumn{}

type Consistency = protocol.Consistency

type SerialConsistency = protocol.SerialConsistency

type frame = protocol.Frame
type frameBuilder = protocol.FrameBuilder

const (
	Any         = protocol.Any
	One         = protocol.One
	Two         = protocol.Two
	Three       = protocol.Three
	Quorum      = protocol.Quorum
	All         = protocol.All
	LocalQuorum = protocol.LocalQuorum
	EachQuorum  = protocol.EachQuorum
	LocalOne    = protocol.LocalOne
)

const (
	protoDirectionMask = protocol.ProtoDirectionMask
	protoVersionMask   = protocol.ProtoVersionMask
	protoVersion1      = protocol.ProtoVersion1
	protoVersion2      = protocol.ProtoVersion2
	protoVersion3      = protocol.ProtoVersion3
	protoVersion4      = protocol.ProtoVersion4
	protoVersion5      = protocol.ProtoVersion5

	maxFrameSize = protocol.MaxFrameSize
)

// NamedValue produce a value which will bind to the named parameter in a query
func NamedValue(name string, value interface{}) interface{} {
	return &protocol.NamedValue{
		Name:  name,
		Value: value,
	}
}

var (
	ErrFrameTooBig = errors.New("frame length is bigger than the maximum allowed")
)

type ObservedFrameHeader struct {
	Version protocol.ProtoVersion
	Flags   byte
	Stream  int16
	Opcode  protocol.FrameOp
	Length  int32

	// StartHeader is the time we started reading the frame header off the network connection.
	Start time.Time
	// EndHeader is the time we finished reading the frame header off the network connection.
	End time.Time

	// Host is Host of the connection the frame header was read from.
	Host *HostInfo
}

func (f ObservedFrameHeader) String() string {
	return fmt.Sprintf("[observed header version=%s flags=0x%x stream=%d op=%s length=%d]", f.Version, f.Flags, f.Stream, f.Opcode, f.Length)
}

// FrameHeaderObserver is the interface implemented by frame observers / stat collectors.
//
// Experimental, this interface and use may change
type FrameHeaderObserver interface {
	// ObserveFrameHeader gets called on every received frame header.
	ObserveFrameHeader(context.Context, ObservedFrameHeader)
}
