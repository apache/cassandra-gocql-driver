package hostpoolhostpolicy

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
	firstHost := &gocql.HostInfo{}
	firstHost.SetHostID("0")
	firstHost.SetConnectAddress(net.IPv4(10, 0, 0, 0))
	secHost := &gocql.HostInfo{}
	secHost.SetHostID("1")
	secHost.SetConnectAddress(net.IPv4(10, 0, 0, 1))
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
