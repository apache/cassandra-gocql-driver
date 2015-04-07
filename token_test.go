// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"math/big"
	"strconv"
	"testing"
)

func TestMurmur3H1(t *testing.T) {
	assertMurmur3H1(t, []byte{}, 0x000000000000000)
	assertMurmur3H1(t, []byte{0}, 0x4610abe56eff5cb5)
	assertMurmur3H1(t, []byte{0, 1}, 0x7cb3f5c58dab264c)
	assertMurmur3H1(t, []byte{0, 1, 2}, 0xb872a12fef53e6be)
	assertMurmur3H1(t, []byte{0, 1, 2, 3}, 0xe1c594ae0ddfaf10)
	assertMurmur3H1(t, []byte("hello"), 0xcbd8a7b341bd9b02)
	assertMurmur3H1(t, []byte("hello, world"), 0x342fac623a5ebc8e)
	assertMurmur3H1(t, []byte("19 Jan 2038 at 3:14:07 AM"), 0xb89e5988b737affc)
	assertMurmur3H1(t, []byte("The quick brown fox jumps over the lazy dog."), 0xcd99481f9ee902c9)
}

func assertMurmur3H1(t *testing.T, data []byte, expected uint64) {
	actual := murmur3H1(data)
	if actual != expected {
		t.Errorf("Expected h1 = %x for data = %x, but was %x", expected, data, actual)
	}
}

func BenchmarkMurmur3H1(b *testing.B) {
	var h1 uint64
	var data [1024]byte
	for i := 0; i < 1024; i++ {
		data[i] = byte(i)
	}
	for i := 0; i < b.N; i++ {
		b.ResetTimer()
		h1 = murmur3H1(data[:])
		_ = murmur3Token(int64(h1))
	}
}

func TestMurmur3Partitioner(t *testing.T) {
	token := murmur3Partitioner{}.ParseString("-1053604476080545076")

	if "-1053604476080545076" != token.String() {
		t.Errorf("Expected '-1053604476080545076' but was '%s'", token)
	}

	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := marshalInt(nil, 1)
	token = murmur3Partitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

func TestMurmur3Token(t *testing.T) {
	if murmur3Token(42).Less(murmur3Token(42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !murmur3Token(-42).Less(murmur3Token(42)) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if murmur3Token(42).Less(murmur3Token(-42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

func TestOrderPreservingPartitioner(t *testing.T) {
	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := marshalInt(nil, 1)
	token := orderPreservingPartitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

func TestOrderPreservingToken(t *testing.T) {
	if orderPreservingToken([]byte{0, 0, 4, 2}).Less(orderPreservingToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !orderPreservingToken([]byte{0, 0, 3}).Less(orderPreservingToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if orderPreservingToken([]byte{0, 0, 4, 2}).Less(orderPreservingToken([]byte{0, 0, 3})) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

func TestRandomPartitioner(t *testing.T) {
	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := marshalInt(nil, 1)
	token := randomPartitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

func TestRandomToken(t *testing.T) {
	if ((*randomToken)(big.NewInt(42))).Less((*randomToken)(big.NewInt(42))) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !((*randomToken)(big.NewInt(41))).Less((*randomToken)(big.NewInt(42))) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if ((*randomToken)(big.NewInt(42))).Less((*randomToken)(big.NewInt(41))) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

type intToken int

func (i intToken) String() string {
	return strconv.Itoa(int(i))
}

func (i intToken) Less(token token) bool {
	return i < token.(intToken)
}

func TestIntTokenRing(t *testing.T) {
	// test based on example at the start of this page of documentation:
	// http://www.datastax.com/docs/0.8/cluster_architecture/partitioning
	host0 := &HostInfo{}
	host25 := &HostInfo{}
	host50 := &HostInfo{}
	host75 := &HostInfo{}
	tokenRing := &tokenRing{
		partitioner: nil,
		tokens: []token{
			intToken(0),
			intToken(25),
			intToken(50),
			intToken(75),
		},
		hosts: []*HostInfo{
			host0,
			host25,
			host50,
			host75,
		},
	}

	if tokenRing.GetHostForToken(intToken(0)) != host0 {
		t.Error("Expected host 0 for token 0")
	}
	if tokenRing.GetHostForToken(intToken(1)) != host25 {
		t.Error("Expected host 25 for token 1")
	}
	if tokenRing.GetHostForToken(intToken(24)) != host25 {
		t.Error("Expected host 25 for token 24")
	}
	if tokenRing.GetHostForToken(intToken(25)) != host25 {
		t.Error("Expected host 25 for token 25")
	}
	if tokenRing.GetHostForToken(intToken(26)) != host50 {
		t.Error("Expected host 50 for token 26")
	}
	if tokenRing.GetHostForToken(intToken(49)) != host50 {
		t.Error("Expected host 50 for token 49")
	}
	if tokenRing.GetHostForToken(intToken(50)) != host50 {
		t.Error("Expected host 50 for token 50")
	}
	if tokenRing.GetHostForToken(intToken(51)) != host75 {
		t.Error("Expected host 75 for token 51")
	}
	if tokenRing.GetHostForToken(intToken(74)) != host75 {
		t.Error("Expected host 75 for token 74")
	}
	if tokenRing.GetHostForToken(intToken(75)) != host75 {
		t.Error("Expected host 75 for token 75")
	}
	if tokenRing.GetHostForToken(intToken(76)) != host0 {
		t.Error("Expected host 0 for token 76")
	}
	if tokenRing.GetHostForToken(intToken(99)) != host0 {
		t.Error("Expected host 0 for token 99")
	}
	if tokenRing.GetHostForToken(intToken(100)) != host0 {
		t.Error("Expected host 0 for token 100")
	}
}
