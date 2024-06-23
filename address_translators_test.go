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

func TestIdentityAddressTranslator_NilAddrAndZeroPort(t *testing.T) {
	var tr AddressTranslator = IdentityTranslator()
	hostIP := net.ParseIP("")
	if hostIP != nil {
		t.Errorf("expected host ip to be (nil) but was (%+v) instead", hostIP)
	}

	addr, port := tr.Translate(hostIP, 0)
	if addr != nil {
		t.Errorf("expected translated host to be (nil) but was (%+v) instead", addr)
	}
	assertEqual(t, "translated port", 0, port)
}

func TestIdentityAddressTranslator_HostProvided(t *testing.T) {
	var tr AddressTranslator = IdentityTranslator()
	hostIP := net.ParseIP("10.1.2.3")
	if hostIP == nil {
		t.Error("expected host ip not to be (nil)")
	}

	addr, port := tr.Translate(hostIP, 9042)
	if !hostIP.Equal(addr) {
		t.Errorf("expected translated addr to be (%+v) but was (%+v) instead", hostIP, addr)
	}
	assertEqual(t, "translated port", 9042, port)
}
