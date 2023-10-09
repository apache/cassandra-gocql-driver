//go:build all || unit
// +build all unit

package gocql

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnmarshalCassVersion(t *testing.T) {
	tests := [...]struct {
		data    string
		version cassVersion
	}{
		{"3.2", cassVersion{3, 2, 0}},
		{"2.10.1-SNAPSHOT", cassVersion{2, 10, 1}},
		{"1.2.3", cassVersion{1, 2, 3}},
	}

	for i, test := range tests {
		v := &cassVersion{}
		if err := v.UnmarshalCQL(nil, []byte(test.data)); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if *v != test.version {
			t.Errorf("%d: expected %#+v got %#+v", i, test.version, *v)
		}
	}
}

func TestCassVersionBefore(t *testing.T) {
	tests := [...]struct {
		version             cassVersion
		major, minor, patch int
	}{
		{cassVersion{1, 0, 0}, 0, 0, 0},
		{cassVersion{0, 1, 0}, 0, 0, 0},
		{cassVersion{0, 0, 1}, 0, 0, 0},

		{cassVersion{1, 0, 0}, 0, 1, 0},
		{cassVersion{0, 1, 0}, 0, 0, 1},
		{cassVersion{4, 1, 0}, 3, 1, 2},
	}

	for i, test := range tests {
		if test.version.Before(test.major, test.minor, test.patch) {
			t.Errorf("%d: expected v%d.%d.%d to be before %v", i, test.major, test.minor, test.patch, test.version)
		}
	}

}

func TestIsValidPeer(t *testing.T) {
	host := &HostInfo{
		rpcAddress: net.ParseIP("0.0.0.0"),
		rack:       "myRack",
		hostId:     "0",
		dataCenter: "datacenter",
		tokens:     []string{"0", "1"},
	}

	if !isValidPeer(host) {
		t.Errorf("expected %+v to be a valid peer", host)
	}

	host.rack = ""
	if isValidPeer(host) {
		t.Errorf("expected %+v to NOT be a valid peer", host)
	}
}

func TestHostInfo_ConnectAddress(t *testing.T) {
	var localhost = net.IPv4(127, 0, 0, 1)
	tests := []struct {
		name          string
		connectAddr   net.IP
		rpcAddr       net.IP
		broadcastAddr net.IP
		peer          net.IP
	}{
		{name: "rpc_address", rpcAddr: localhost},
		{name: "connect_address", connectAddr: localhost},
		{name: "broadcast_address", broadcastAddr: localhost},
		{name: "peer", peer: localhost},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &HostInfo{
				connectAddress:   test.connectAddr,
				rpcAddress:       test.rpcAddr,
				broadcastAddress: test.broadcastAddr,
				peer:             test.peer,
			}

			if addr := host.ConnectAddress(); !addr.Equal(localhost) {
				t.Fatalf("expected ConnectAddress to be %s got %s", localhost, addr)
			}
		})
	}
}

// This test sends debounce requests and waits until the refresh function is called (which should happen when the timer elapses).
func TestRefreshDebouncer_MultipleEvents(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 1 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	wg := sync.WaitGroup{}
	d := newRefreshDebouncer(2*time.Second, fn)
	defer d.stop()
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.debounce()
		}()
	}
	wg.Wait()
	timeoutCh := time.After(2500 * time.Millisecond) // extra time to avoid flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called")
	}
	afterFunctionCall := time.Now()

	// use 1.5 seconds instead of 2 seconds to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 1500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~2 seconds", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	// wait another 2 seconds and check if function was called again
	time.Sleep(2500 * time.Millisecond)
	if len(channel) > 0 {
		t.Fatalf("function was called more than once")
	}
}

// This test:
//
//	1 - Sends debounce requests when test starts
//	2 - Calls refreshNow() before the timer elapsed (which stops the timer) about 1.5 seconds after test starts
//
// The end result should be 1 refresh function call when refreshNow() is called.
func TestRefreshDebouncer_RefreshNow(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 1 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	eventsWg := sync.WaitGroup{}
	d := newRefreshDebouncer(2*time.Second, fn)
	defer d.stop()
	for i := 0; i < numberOfEvents; i++ {
		eventsWg.Add(1)
		go func() {
			defer eventsWg.Done()
			d.debounce()
		}()
	}

	refreshNowWg := sync.WaitGroup{}
	refreshNowWg.Add(1)
	go func() {
		defer refreshNowWg.Done()
		time.Sleep(1500 * time.Millisecond)
		d.refreshNow()
	}()

	eventsWg.Wait()
	select {
	case <-channel:
		t.Fatalf("function was called before the expected time")
	default:
	}

	refreshNowWg.Wait()

	timeoutCh := time.After(200 * time.Millisecond) // allow for 200ms of delay to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called")
	}
	afterFunctionCall := time.Now()

	// use 1 second instead of 1.5s to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 1000*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~1.5 seconds", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	// wait some time and check if function was called again
	time.Sleep(2500 * time.Millisecond)
	if len(channel) > 0 {
		t.Fatalf("function was called more than once")
	}
}

// This test:
//
//	1 - Sends debounce requests when test starts
//	2 - Calls refreshNow() before the timer elapsed (which stops the timer) about 1 second after test starts
//	3 - Sends more debounce requests (which resets the timer with a 3-second interval) about 2 seconds after test starts
//
// The end result should be 2 refresh function calls:
//
//	1 - When refreshNow() is called (1 second after the test starts)
//	2 - When the timer elapses after the second "wave" of debounce requests (5 seconds after the test starts)
func TestRefreshDebouncer_EventsAfterRefreshNow(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 2 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	wg := sync.WaitGroup{}
	d := newRefreshDebouncer(3*time.Second, fn)
	defer d.stop()
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.debounce()
			time.Sleep(2000 * time.Millisecond)
			d.debounce()
		}()
	}

	go func() {
		time.Sleep(1 * time.Second)
		d.refreshNow()
	}()

	wg.Wait()
	timeoutCh := time.After(1500 * time.Millisecond) // extra 500ms to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called after refreshNow()")
	}
	afterFunctionCall := time.Now()

	// use 500ms instead of 1s to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~1 second", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	timeoutCh = time.After(4 * time.Second) // extra 1s to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called after debounce requests")
	}
	afterSecondFunctionCall := time.Now()

	// use 2.5s instead of 3s to avoid timer precision issues
	if afterSecondFunctionCall.Sub(afterFunctionCall) < 2500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~3 seconds", afterSecondFunctionCall.Sub(afterFunctionCall).Milliseconds())
	}

	if len(channel) > 0 {
		t.Fatalf("function was called more than twice")
	}
}

func TestErrorBroadcaster_MultipleListeners(t *testing.T) {
	b := newErrorBroadcaster()
	defer b.stop()
	const numberOfListeners = 10
	var listeners []<-chan error
	for i := 0; i < numberOfListeners; i++ {
		listeners = append(listeners, b.newListener())
	}

	err := errors.New("expected error")
	wg := sync.WaitGroup{}
	result := atomic.Value{}
	for _, listener := range listeners {
		currentListener := listener
		wg.Add(1)
		go func() {
			defer wg.Done()
			receivedErr, ok := <-currentListener
			if !ok {
				result.Store(errors.New("listener was closed"))
			} else if receivedErr != err {
				result.Store(errors.New("expected received error to be the same as the one that was broadcasted"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.broadcast(err)
		b.stop()
	}()
	wg.Wait()
	if loadedVal := result.Load(); loadedVal != nil {
		t.Errorf(loadedVal.(error).Error())
	}
}

func TestErrorBroadcaster_StopWithoutBroadcast(t *testing.T) {
	var b = newErrorBroadcaster()
	defer b.stop()
	const numberOfListeners = 10
	var listeners []<-chan error
	for i := 0; i < numberOfListeners; i++ {
		listeners = append(listeners, b.newListener())
	}

	wg := sync.WaitGroup{}
	result := atomic.Value{}
	for _, listener := range listeners {
		currentListener := listener
		wg.Add(1)
		go func() {
			defer wg.Done()
			// broadcaster stopped, expect listener to be closed
			_, ok := <-currentListener
			if ok {
				result.Store(errors.New("expected listener to be closed"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// call stop without broadcasting anything to current listeners
		b.stop()
	}()
	wg.Wait()
	if loadedVal := result.Load(); loadedVal != nil {
		t.Errorf(loadedVal.(error).Error())
	}
}
