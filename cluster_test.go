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
	"errors"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestNewCluster_Defaults(t *testing.T) {
	cfg := NewCluster()
	assertEqual(t, "cluster config cql version", "3.0.0", cfg.CQLVersion)
	assertEqual(t, "cluster config timeout", 11*time.Second, cfg.Timeout)
	assertEqual(t, "cluster config port", 9042, cfg.Port)
	assertEqual(t, "cluster config num-conns", 2, cfg.NumConns)
	assertEqual(t, "cluster config consistency", Quorum, cfg.Consistency)
	assertEqual(t, "cluster config max prepared statements", defaultMaxPreparedStmts, cfg.MaxPreparedStmts)
	assertEqual(t, "cluster config max routing key info", 1000, cfg.MaxRoutingKeyInfo)
	assertEqual(t, "cluster config page-size", 5000, cfg.PageSize)
	assertEqual(t, "cluster config default timestamp", true, cfg.DefaultTimestamp)
	assertEqual(t, "cluster config max wait schema agreement", 60*time.Second, cfg.MaxWaitSchemaAgreement)
	assertEqual(t, "cluster config reconnect interval", 60*time.Second, cfg.ReconnectInterval)
	assertTrue(t, "cluster config conviction policy",
		reflect.DeepEqual(&SimpleConvictionPolicy{}, cfg.ConvictionPolicy))
	assertTrue(t, "cluster config reconnection policy",
		reflect.DeepEqual(&ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second}, cfg.ReconnectionPolicy))
}

func TestNewCluster_WithHosts(t *testing.T) {
	cfg := NewCluster("addr1", "addr2")
	assertEqual(t, "cluster config hosts length", 2, len(cfg.Hosts))
	assertEqual(t, "cluster config host 0", "addr1", cfg.Hosts[0])
	assertEqual(t, "cluster config host 1", "addr2", cfg.Hosts[1])
}

func TestClusterConfig_translateAddressAndPort_NilTranslator(t *testing.T) {
	cfg := NewCluster()
	assertNil(t, "cluster config address translator", cfg.AddressTranslator)
	newAddr, newPort := cfg.translateAddressPort(net.ParseIP("10.0.0.1"), 1234)
	assertTrue(t, "same address as provided", net.ParseIP("10.0.0.1").Equal(newAddr))
	assertEqual(t, "translated host and port", 1234, newPort)
}

func TestClusterConfig_translateAddressAndPort_EmptyAddr(t *testing.T) {
	cfg := NewCluster()
	cfg.AddressTranslator = staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
	newAddr, newPort := cfg.translateAddressPort(net.IP([]byte{}), 0)
	assertTrue(t, "translated address is still empty", len(newAddr) == 0)
	assertEqual(t, "translated port", 0, newPort)
}

func TestClusterConfig_translateAddressAndPort_Success(t *testing.T) {
	cfg := NewCluster()
	cfg.AddressTranslator = staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
	newAddr, newPort := cfg.translateAddressPort(net.ParseIP("10.0.0.1"), 2345)
	assertTrue(t, "translated address", net.ParseIP("10.10.10.10").Equal(newAddr))
	assertEqual(t, "translated port", 5432, newPort)
}

func TestEmptyRack(t *testing.T) {
	s := &Session{}
	host := &HostInfo{}

	row := make(map[string]interface{})

	row["preferred_ip"] = "172.3.0.2"
	row["rpc_address"] = "172.3.0.2"
	row["host_id"] = UUIDFromTime(time.Now())
	row["data_center"] = "dc1"
	row["tokens"] = []string{"t1", "t2"}
	row["rack"] = "rack1"

	validHost, err := s.hostInfoFromMap(row, host)
	if err != nil {
		t.Fatal(err)
	}
	if !isValidPeer(validHost) {
		t.Fatal(errors.New("expected valid host"))
	}

	row["rack"] = ""

	validHost, err = s.hostInfoFromMap(row, host)
	if err != nil {
		t.Fatal(err)
	}
	if !isValidPeer(validHost) {
		t.Fatal(errors.New("expected valid host"))
	}

	strPtr := new(string)
	*strPtr = "rack"
	row["rack"] = strPtr

	validHost, err = s.hostInfoFromMap(row, host)
	if err != nil {
		t.Fatal(err)
	}
	if !isValidPeer(validHost) {
		t.Fatal(errors.New("expected valid host"))
	}

	strPtr = new(string)
	strPtr = nil
	row["rack"] = strPtr

	validHost, err = s.hostInfoFromMap(row, host)
	if err != nil {
		t.Fatal(err)
	}
	if isValidPeer(validHost) {
		t.Fatal(errors.New("expected valid host"))
	}
}
