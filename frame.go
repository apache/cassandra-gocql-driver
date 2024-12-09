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
	"fmt"
	"github.com/gocql/gocql/internal"
	"strings"
	"time"
)

// UnsetValue represents a value used in a query binding that will be ignored by Cassandra.
//
// By setting a field to the unset value Cassandra will ignore the write completely.
// The main advantage is the ability to keep the same prepared statement even when you don't
// want to update some fields, where before you needed to make another prepared statement.
//
// UnsetValue is only available when using the version 4 of the protocol.
var UnsetValue = internal.UnsetColumn{}

// NamedValue produce a value which will bind to the named parameter in a query
func NamedValue(name string, value interface{}) interface{} {
	return &internal.NamedValue{
		Name:  name,
		Value: value,
	}
}

// TODO: Deprecate consystency, and use it from package consistency
type Consistency uint16

const (
	Any         Consistency = 0x00
	One         Consistency = 0x01
	Two         Consistency = 0x02
	Three       Consistency = 0x03
	Quorum      Consistency = 0x04
	All         Consistency = 0x05
	LocalQuorum Consistency = 0x06
	EachQuorum  Consistency = 0x07
	LocalOne    Consistency = 0x0A
)

func (c Consistency) String() string {
	switch c {
	case Any:
		return "ANY"
	case One:
		return "ONE"
	case Two:
		return "TWO"
	case Three:
		return "THREE"
	case Quorum:
		return "QUORUM"
	case All:
		return "ALL"
	case LocalQuorum:
		return "LOCAL_QUORUM"
	case EachQuorum:
		return "EACH_QUORUM"
	case LocalOne:
		return "LOCAL_ONE"
	default:
		return fmt.Sprintf("UNKNOWN_CONS_0x%x", uint16(c))
	}
}

func (c Consistency) MarshalText() (text []byte, err error) {
	return []byte(c.String()), nil
}

func (c *Consistency) UnmarshalText(text []byte) error {
	switch string(text) {
	case "ANY":
		*c = Any
	case "ONE":
		*c = One
	case "TWO":
		*c = Two
	case "THREE":
		*c = Three
	case "QUORUM":
		*c = Quorum
	case "ALL":
		*c = All
	case "LOCAL_QUORUM":
		*c = LocalQuorum
	case "EACH_QUORUM":
		*c = EachQuorum
	case "LOCAL_ONE":
		*c = LocalOne
	default:
		return fmt.Errorf("invalid consistency %q", string(text))
	}

	return nil
}

func ParseConsistency(s string) Consistency {
	var c Consistency
	if err := c.UnmarshalText([]byte(strings.ToUpper(s))); err != nil {
		panic(err)
	}
	return c
}

// ParseConsistencyWrapper wraps gocql.ParseConsistency to provide an err
// return instead of a panic
func ParseConsistencyWrapper(s string) (consistency Consistency, err error) {
	err = consistency.UnmarshalText([]byte(strings.ToUpper(s)))
	return
}

// MustParseConsistency is the same as ParseConsistency except it returns
// an error (never). It is kept here since breaking changes are not good.
// DEPRECATED: use ParseConsistency if you want a panic on parse error.
func MustParseConsistency(s string) (Consistency, error) {
	c, err := ParseConsistencyWrapper(s)
	if err != nil {
		panic(err)
	}
	return c, nil
}

type SerialConsistency uint16

const (
	Serial      SerialConsistency = 0x08
	LocalSerial SerialConsistency = 0x09
)

func (s SerialConsistency) String() string {
	switch s {
	case Serial:
		return "SERIAL"
	case LocalSerial:
		return "LOCAL_SERIAL"
	default:
		return fmt.Sprintf("UNKNOWN_SERIAL_CONS_0x%x", uint16(s))
	}
}

func (s SerialConsistency) MarshalText() (text []byte, err error) {
	return []byte(s.String()), nil
}

func (s *SerialConsistency) UnmarshalText(text []byte) error {
	switch string(text) {
	case "SERIAL":
		*s = Serial
	case "LOCAL_SERIAL":
		*s = LocalSerial
	default:
		return fmt.Errorf("invalid consistency %q", string(text))
	}

	return nil
}

// TODO

type ObservedFrameHeader struct {
	Version internal.ProtoVersion
	Flags   byte
	Stream  int16
	Opcode  internal.FrameOp
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
