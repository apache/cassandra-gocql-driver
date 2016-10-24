package gocql

import (
	"net"
	"testing"
)

func TestHostInfo_Lookup(t *testing.T) {
	hostLookupPreferV4 = true
	defer func() { hostLookupPreferV4 = false }()

	tests := [...]struct {
		addr string
		ip   net.IP
	}{
		{"127.0.0.1", net.IPv4(127, 0, 0, 1)},
		{"localhost", net.IPv4(127, 0, 0, 1)}, // TODO: this may be host dependant
	}

	for i, test := range tests {
		host, err := hostInfo(test.addr, 1)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		if !host.peer.Equal(test.ip) {
			t.Errorf("expected ip %v got %v for addr %q", test.ip, host.peer, test.addr)
		}
	}
}
