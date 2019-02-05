// Copyright 2019 Gocql Owners

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gocql

import (
	"net"
	"testing"
)

func TestRing_AddHostIfMissing_Missing(t *testing.T) {
	ring := &ring{}

	host := &HostInfo{connectAddress: net.IPv4(1, 1, 1, 1)}
	h1, ok := ring.addHostIfMissing(host)
	if ok {
		t.Fatal("host was reported as already existing")
	} else if !h1.Equal(host) {
		t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
	} else if h1 != host {
		t.Fatalf("returned host same pointer: %p != %p", h1, host)
	}
}

func TestRing_AddHostIfMissing_Existing(t *testing.T) {
	ring := &ring{}

	host := &HostInfo{connectAddress: net.IPv4(1, 1, 1, 1)}
	ring.addHostIfMissing(host)

	h2 := &HostInfo{connectAddress: net.IPv4(1, 1, 1, 1)}

	h1, ok := ring.addHostIfMissing(h2)
	if !ok {
		t.Fatal("host was not reported as already existing")
	} else if !h1.Equal(host) {
		t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
	} else if h1 != host {
		t.Fatalf("returned host same pointer: %p != %p", h1, host)
	}
}
