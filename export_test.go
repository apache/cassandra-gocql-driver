//go:build integration || unit
// +build integration unit

package gocql

var FlagRunSslTest = flagRunSslTest
var CreateCluster = createCluster
var TestLogger = &testLogger{}
var WaitUntilPoolsStopFilling = waitUntilPoolsStopFilling

func GetRingAllHosts(sess *Session) []*HostInfo {
	return sess.ring.allHosts()
}
