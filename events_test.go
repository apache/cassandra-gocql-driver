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
	"sync"
	"testing"
)

func TestEventDebounce(t *testing.T) {
	const eventCount = 150
	wg := &sync.WaitGroup{}
	wg.Add(1)

	eventsSeen := 0
	debouncer := newEventDebouncer("testDebouncer", func(events []frame) {
		defer wg.Done()
		eventsSeen += len(events)
	})
	defer debouncer.stop()

	for i := 0; i < eventCount; i++ {
		debouncer.debounce(&statusChangeEventFrame{
			change: "UP",
			host:   net.IPv4(127, 0, 0, 1),
			port:   9042,
		})
	}

	wg.Wait()
	if eventCount != eventsSeen {
		t.Fatalf("expected to see %d events but got %d", eventCount, eventsSeen)
	}
}
