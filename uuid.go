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

// The uuid package can be used to generate and parse universally unique
// identifiers, a standardized format in the form of a 128 bit number.
//
// http://tools.ietf.org/html/rfc4122

import (
	"crypto/rand"
	"github.com/gocql/gocql/internal/protocol"
	"io"
	"net"
	"sync/atomic"
	"time"
)

type UUID = protocol.UUID

var hardwareAddr []byte
var clockSeq uint32

func init() {
	if interfaces, err := net.Interfaces(); err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagLoopback == 0 && len(i.HardwareAddr) > 0 {
				hardwareAddr = i.HardwareAddr
				break
			}
		}
	}
	if hardwareAddr == nil {
		// If we failed to obtain the MAC address of the current computer,
		// we will use a randomly generated 6 byte sequence instead and set
		// the multicast bit as recommended in RFC 4122.
		hardwareAddr = make([]byte, 6)
		_, err := io.ReadFull(rand.Reader, hardwareAddr)
		if err != nil {
			panic(err)
		}
		hardwareAddr[0] = hardwareAddr[0] | 0x01
	}

	// initialize the clock sequence with a random number
	var clockSeqRand [2]byte
	io.ReadFull(rand.Reader, clockSeqRand[:])
	clockSeq = uint32(clockSeqRand[1])<<8 | uint32(clockSeqRand[0])
}

var ParseUUID = protocol.ParseUUID

var UUIDFromBytes = protocol.UUIDFromBytes

func MustRandomUUID() UUID {
	uuid, err := RandomUUID()
	if err != nil {
		panic(err)
	}
	return uuid
}

// RandomUUID generates a totally random UUID (version 4) as described in
// RFC 4122.
func RandomUUID() (UUID, error) {
	var u UUID
	_, err := io.ReadFull(rand.Reader, u[:])
	if err != nil {
		return u, err
	}
	u[6] &= 0x0F // clear version
	u[6] |= 0x40 // set version to 4 (random uuid)
	u[8] &= 0x3F // clear variant
	u[8] |= 0x80 // set to IETF variant
	return u, nil
}

var timeBase = protocol.TimeBase

// getTimestamp converts time to UUID (version 1) timestamp.
// It must be an interval of 100-nanoseconds since timeBase.
func getTimestamp(t time.Time) int64 {
	utcTime := t.In(time.UTC)
	ts := int64(utcTime.Unix()-timeBase)*10000000 + int64(utcTime.Nanosecond()/100)

	return ts
}

// TimeUUID generates a new time based UUID (version 1) using the current
// time as the timestamp.
func TimeUUID() UUID {
	return UUIDFromTime(time.Now())
}

// The min and max clock values for a UUID.
//
// Cassandra's TimeUUIDType compares the lsb parts as signed byte arrays.
// Thus, the min value for each byte is -128 and the max is +127.
const (
	minClock = 0x8080
	maxClock = 0x7f7f
)

// The min and max node values for a UUID.
//
// See explanation about Cassandra's TimeUUIDType comparison logic above.
var (
	minNode = []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
	maxNode = []byte{0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f}
)

// MinTimeUUID generates a "fake" time based UUID (version 1) which will be
// the smallest possible UUID generated for the provided timestamp.
//
// UUIDs generated by this function are not unique and are mostly suitable only
// in queries to select a time range of a Cassandra's TimeUUID column.
func MinTimeUUID(t time.Time) UUID {
	return TimeUUIDWith(getTimestamp(t), minClock, minNode)
}

// MaxTimeUUID generates a "fake" time based UUID (version 1) which will be
// the biggest possible UUID generated for the provided timestamp.
//
// UUIDs generated by this function are not unique and are mostly suitable only
// in queries to select a time range of a Cassandra's TimeUUID column.
func MaxTimeUUID(t time.Time) UUID {
	return TimeUUIDWith(getTimestamp(t), maxClock, maxNode)
}

// UUIDFromTime generates a new time based UUID (version 1) as described in
// RFC 4122. This UUID contains the MAC address of the node that generated
// the UUID, the given timestamp and a sequence number.
func UUIDFromTime(t time.Time) UUID {
	ts := getTimestamp(t)
	clock := atomic.AddUint32(&clockSeq, 1)

	return TimeUUIDWith(ts, clock, hardwareAddr)
}

// TimeUUIDWith generates a new time based UUID (version 1) as described in
// RFC4122 with given parameters. t is the number of 100's of nanoseconds
// since 15 Oct 1582 (60bits). clock is the number of clock sequence (14bits).
// node is a slice to gurarantee the uniqueness of the UUID (up to 6bytes).
// Note: calling this function does not increment the static clock sequence.
func TimeUUIDWith(t int64, clock uint32, node []byte) UUID {
	var u UUID

	u[0], u[1], u[2], u[3] = byte(t>>24), byte(t>>16), byte(t>>8), byte(t)
	u[4], u[5] = byte(t>>40), byte(t>>32)
	u[6], u[7] = byte(t>>56)&0x0F, byte(t>>48)

	u[8] = byte(clock >> 8)
	u[9] = byte(clock)

	copy(u[10:], node)

	u[6] |= 0x10 // set version to 1 (time based uuid)
	u[8] &= 0x3F // clear variant
	u[8] |= 0x80 // set to IETF variant

	return u
}

// String returns the UUID in it's canonical form, a 32 digit hexadecimal
// number in the form of xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
