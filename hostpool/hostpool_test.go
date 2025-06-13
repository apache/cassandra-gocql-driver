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
	//	{hostId: "f1935733-af5f-4995-bd1e-94a7a3e67bfd", connectAddress: net.ParseIP("10.0.0.0")},
	//	{hostId: "93ca4489-b322-4fda-b5a5-12d4436271df", connectAddress: net.ParseIP("10.0.0.1")},
	//}
	firstHostId, err1 := gocql.ParseUUID("f1935733-af5f-4995-bd1e-94a7a3e67bfd")
	secondHostId, err2 := gocql.ParseUUID("93ca4489-b322-4fda-b5a5-12d4436271df")

	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}

	firstHost, err := gocql.NewTestHostInfoFromRow(
		map[string]interface{}{
			"peer":        net.ParseIP("10.0.0.0"),
			"native_port": 9042,
			"host_id":     firstHostId})
	if err != nil {
		t.Errorf("Error creating first host: %v", err)
	}

	secHost, err := gocql.NewTestHostInfoFromRow(
		map[string]interface{}{
			"peer":        net.ParseIP("10.0.0.1"),
			"native_port": 9042,
			"host_id":     secondHostId})
	if err != nil {
		t.Errorf("Error creating second host: %v", err)
	}
	hosts := []*gocql.HostInfo{firstHost, secHost}
	// Using set host to control the ordering of the hosts as calling "AddHost" iterates the map
	// which will result in an unpredictable ordering
	policy.SetHosts(hosts)

	// the first host selected is actually at [1], but this is ok for RR
	// interleaved iteration should always increment the host
	iter := policy.Pick(nil)
	actualA := iter()
	if actualA.Info().HostID() != firstHostId.String() {
		t.Errorf("Expected first host id but was %s", actualA.Info().HostID())
	}
	actualA.Mark(nil)

	actualB := iter()
	if actualB.Info().HostID() != secondHostId.String() {
		t.Errorf("Expected second host id but was %s", actualB.Info().HostID())
	}
	actualB.Mark(fmt.Errorf("error"))

	actualC := iter()
	if actualC.Info().HostID() != firstHostId.String() {
		t.Errorf("Expected first host id but was %s", actualC.Info().HostID())
	}
	actualC.Mark(nil)

	actualD := iter()
	if actualD.Info().HostID() != firstHostId.String() {
		t.Errorf("Expected first host id but was %s", actualD.Info().HostID())
	}
	actualD.Mark(nil)
}
