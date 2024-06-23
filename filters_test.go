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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"net"
	"testing"
)

func TestFilter_WhiteList(t *testing.T) {
	f := WhiteListHostFilter("127.0.0.1", "127.0.0.2")
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), true},
		{net.ParseIP("127.0.0.2"), true},
		{net.ParseIP("127.0.0.3"), false},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{connectAddress: test.addr}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_AllowAll(t *testing.T) {
	f := AcceptAllFilter()
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), true},
		{net.ParseIP("127.0.0.2"), true},
		{net.ParseIP("127.0.0.3"), true},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{connectAddress: test.addr}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_DenyAll(t *testing.T) {
	f := DenyAllFilter()
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), false},
		{net.ParseIP("127.0.0.2"), false},
		{net.ParseIP("127.0.0.3"), false},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{connectAddress: test.addr}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_DataCentre(t *testing.T) {
	f := DataCentreHostFilter("dc1")
	tests := [...]struct {
		dc     string
		accept bool
	}{
		{"dc1", true},
		{"dc2", false},
	}

	for i, test := range tests {
		if f.Accept(&HostInfo{dataCenter: test.dc}) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}
