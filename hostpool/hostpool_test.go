//go:build all || unit
// +build all unit

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

package hostpool

import (
	"fmt"
	"net"
	"testing"

	"github.com/hailocab/go-hostpool"

	"github.com/gocql/gocql"
)

func TestHostPolicy_HostPool(t *testing.T) {
	policy := HostPoolHostPolicy(hostpool.New(nil))

	//hosts := []*gocql.HostInfo{
	//	{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 0)},
	//	{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 1)},
	//}

	firstHost, err := gocql.NewHostInfo(net.IPv4(10, 0, 0, 0), 9042)
	if err != nil {
		t.Errorf("Error creating first host: %v", err)
	}
	firstHost.SetHostID("0")

	secHost, err := gocql.NewHostInfo(net.IPv4(10, 0, 0, 1), 9042)
	if err != nil {
		t.Errorf("Error creating second host: %v", err)
	}
	secHost.SetHostID("1")
	hosts := []*gocql.HostInfo{firstHost, secHost}
	// Using set host to control the ordering of the hosts as calling "AddHost" iterates the map
	// which will result in an unpredictable ordering
	policy.SetHosts(hosts)

	// the first host selected is actually at [1], but this is ok for RR
	// interleaved iteration should always increment the host
	iter := policy.Pick(nil)
	actualA := iter()
	if actualA.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualA.Info().HostID())
	}
	actualA.Mark(nil)

	actualB := iter()
	if actualB.Info().HostID() != "1" {
		t.Errorf("Expected hosts[1] but was hosts[%s]", actualB.Info().HostID())
	}
	actualB.Mark(fmt.Errorf("error"))

	actualC := iter()
	if actualC.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualC.Info().HostID())
	}
	actualC.Mark(nil)

	actualD := iter()
	if actualD.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualD.Info().HostID())
	}
	actualD.Mark(nil)
}
