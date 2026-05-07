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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlacementStrategy_SimpleStrategy(t *testing.T) {
	host0 := &HostInfo{hostId: "0"}
	host25 := &HostInfo{hostId: "25"}
	host50 := &HostInfo{hostId: "50"}
	host75 := &HostInfo{hostId: "75"}

	tokens := []hostToken{
		{intToken(0), host0},
		{intToken(25), host25},
		{intToken(50), host50},
		{intToken(75), host75},
	}

	hosts := []*HostInfo{host0, host25, host50, host75}

	strat := newSimpleStrategy(2)
	tokenReplicas := strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens}, nopLoggerSingleton)
	if len(tokenReplicas) != len(tokens) {
		t.Fatalf("expected replica map to have %d items but has %d", len(tokens), len(tokenReplicas))
	}

	for _, replicas := range tokenReplicas {
		if len(replicas.hosts) != strat.rf {
			t.Errorf("expected to have %d replicas got %d for token=%v", strat.rf, len(replicas.hosts), replicas.token)
		}
	}

	for i, token := range tokens {
		ht := tokenReplicas.replicasFor(token.token)
		if ht.token != token.token {
			t.Errorf("token %v not in replica map: %v", token, ht.hosts)
		}

		for j, replica := range ht.hosts {
			exp := tokens[(i+j)%len(tokens)].host
			if exp != replica {
				t.Errorf("expected host %v to be a replica of %v got %v", exp.hostId, token, replica.hostId)
			}
		}
	}
}

func TestPlacementStrategy_NetworkStrategy(t *testing.T) {
	const (
		totalDCs   = 3
		racksPerDC = 3
		hostsPerDC = 5
	)

	tests := []struct {
		name                   string
		strat                  *networkTopology
		expectedReplicaMapSize int
	}{
		{
			name: "full",
			strat: newNetworkTopology(map[string]int{
				"dc1": 1,
				"dc2": 2,
				"dc3": 3,
			}),
			expectedReplicaMapSize: hostsPerDC * totalDCs,
		},
		{
			name: "missing",
			strat: newNetworkTopology(map[string]int{
				"dc2": 2,
				"dc3": 3,
			}),
			expectedReplicaMapSize: hostsPerDC * 2,
		},
		{
			name: "zero",
			strat: newNetworkTopology(map[string]int{
				"dc1": 0,
				"dc2": 2,
				"dc3": 3,
			}),
			expectedReplicaMapSize: hostsPerDC * 2,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var (
				hosts  []*HostInfo
				tokens []hostToken
			)
			dcRing := make(map[string][]hostToken, totalDCs)
			for i := 0; i < totalDCs; i++ {
				var dcTokens []hostToken
				dc := fmt.Sprintf("dc%d", i+1)

				for j := 0; j < hostsPerDC; j++ {
					rack := fmt.Sprintf("rack%d", (j%racksPerDC)+1)

					h := &HostInfo{hostId: fmt.Sprintf("%s:%s:%d", dc, rack, j), dataCenter: dc, rack: rack}

					token := hostToken{
						token: orderedToken([]byte(h.hostId)),
						host:  h,
					}

					tokens = append(tokens, token)
					dcTokens = append(dcTokens, token)

					hosts = append(hosts, h)
				}

				sort.Sort(&tokenRing{tokens: dcTokens})
				dcRing[dc] = dcTokens
			}

			if len(tokens) != hostsPerDC*totalDCs {
				t.Fatalf("expected %d tokens in the ring got %d", hostsPerDC*totalDCs, len(tokens))
			}
			sort.Sort(&tokenRing{tokens: tokens})

			var expReplicas int
			for _, rf := range test.strat.dcs {
				expReplicas += rf
			}

			tokenReplicas := test.strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens}, nopLoggerSingleton)
			if len(tokenReplicas) != test.expectedReplicaMapSize {
				t.Fatalf("expected replica map to have %d items but has %d", test.expectedReplicaMapSize,
					len(tokenReplicas))
			}
			if !sort.IsSorted(tokenReplicas) {
				t.Fatal("replica map was not sorted by token")
			}

			for token, replicas := range tokenReplicas {
				if len(replicas.hosts) != expReplicas {
					t.Fatalf("expected to have %d replicas got %d for token=%v", expReplicas, len(replicas.hosts), token)
				}
			}

			for dc, rf := range test.strat.dcs {
				if rf == 0 {
					continue
				}
				dcTokens := dcRing[dc]
				for i, th := range dcTokens {
					token := th.token
					allReplicas := tokenReplicas.replicasFor(token)
					if allReplicas.token != token {
						t.Fatalf("token %v not in replica map", token)
					}

					var replicas []*HostInfo
					for _, replica := range allReplicas.hosts {
						if replica.dataCenter == dc {
							replicas = append(replicas, replica)
						}
					}

					if len(replicas) != rf {
						t.Fatalf("expected %d replicas in dc %q got %d", rf, dc, len(replicas))
					}

					var lastRack string
					for j, replica := range replicas {
						// expected is in the next rack
						var exp *HostInfo
						if lastRack == "" {
							// primary, first replica
							exp = dcTokens[(i+j)%len(dcTokens)].host
						} else {
							for k := 0; k < len(dcTokens); k++ {
								// walk around the ring from i + j to find the next host the
								// next rack
								p := (i + j + k) % len(dcTokens)
								h := dcTokens[p].host
								if h.rack != lastRack {
									exp = h
									break
								}
							}
							if exp.rack == lastRack {
								panic("no more racks")
							}
						}
						lastRack = replica.rack
					}
				}
			}
		})
	}
}

// Regression test for CASSGO-122:
// when the token ring only contains hosts from a DC that has RF=0/unspecified for a keyspace,
// networkTopology.replicaMap should return an empty replica map.
func TestPlacementStrategy_NetworkStrategy_ReturnEmptyReplicaMapWhenNoReplicasInRing(t *testing.T) {
	strat := newNetworkTopology(map[string]int{
		"dc1": 3, // replicated only in dc1
	})

	// Hosts in ring only from dc2, so no replicas should be returned.
	// hostId format: dc:rack:host which is used as a token in the token ring.
	// It makes sense to use the hostId as a token in the token ring because it is unique and deterministic for test purpose.
	hosts := []*HostInfo{
		{hostId: "dc2:rack1:0", dataCenter: "dc2", rack: "rack1"},
		{hostId: "dc2:rack2:1", dataCenter: "dc2", rack: "rack2"},
		{hostId: "dc2:rack3:2", dataCenter: "dc2", rack: "rack3"},
	}

	tokens := make([]hostToken, 0, len(hosts))
	for _, h := range hosts {
		tokens = append(tokens, hostToken{
			token: orderedToken(h.hostId),
			host:  h,
		})
	}
	sort.Sort(&tokenRing{tokens: tokens})

	replicas := strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens}, nopLoggerSingleton)
	require.Empty(t, replicas, "expected no replicas, got %d", len(replicas))
}
