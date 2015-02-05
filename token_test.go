// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"math/big"
	"strconv"
	"testing"
)

func TestMurmur3Partitioner(t *testing.T) {
	token := Murmur3Partitioner{}.ParseString("-1053604476080545076")

	if "-1053604476080545076" != token.String() {
		t.Errorf("Expected '-1053604476080545076' but was '%s'", token)
	}

	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := marshalInt(nil, 1)
	token = Murmur3Partitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

func TestMurmur3Token(t *testing.T) {
	if Murmur3Token(42).Less(Murmur3Token(42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !Murmur3Token(-42).Less(Murmur3Token(42)) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if Murmur3Token(42).Less(Murmur3Token(-42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

func TestOrderPreservingPartitioner(t *testing.T) {
	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := marshalInt(nil, 1)
	token := OrderPreservingPartitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

func TestOrderPreservingToken(t *testing.T) {
	if OrderPreservingToken([]byte{0, 0, 4, 2}).Less(OrderPreservingToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !OrderPreservingToken([]byte{0, 0, 3}).Less(OrderPreservingToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if OrderPreservingToken([]byte{0, 0, 4, 2}).Less(OrderPreservingToken([]byte{0, 0, 3})) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

func TestRandomPartitioner(t *testing.T) {
	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := marshalInt(nil, 1)
	token := RandomPartitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

func TestRandomToken(t *testing.T) {
	if (RandomToken{big.NewInt(42)}).Less(RandomToken{big.NewInt(42)}) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !(RandomToken{big.NewInt(41)}).Less(RandomToken{big.NewInt(42)}) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if (RandomToken{big.NewInt(42)}).Less(RandomToken{big.NewInt(41)}) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

type IntToken int

func (i IntToken) String() string {
	return strconv.Itoa(int(i))
}

func (i IntToken) Less(token Token) bool {
	return i < token.(IntToken)
}

func TestIntTokenRing(t *testing.T) {
	// test based on example at the start of this page of documentation:
	// http://www.datastax.com/docs/0.8/cluster_architecture/partitioning
	host0 := &HostInfo{}
	host25 := &HostInfo{}
	host50 := &HostInfo{}
	host75 := &HostInfo{}
	tokenRing := &TokenRing{
		partitioner: nil,
		tokens: []Token{
			IntToken(0),
			IntToken(25),
			IntToken(50),
			IntToken(75),
		},
		hosts: []*HostInfo{
			host0,
			host25,
			host50,
			host75,
		},
	}

	if tokenRing.GetHostForToken(IntToken(0)) != host0 {
		t.Error("Expected host 0 for token 0")
	}
	if tokenRing.GetHostForToken(IntToken(1)) != host25 {
		t.Error("Expected host 25 for token 1")
	}
	if tokenRing.GetHostForToken(IntToken(24)) != host25 {
		t.Error("Expected host 25 for token 24")
	}
	if tokenRing.GetHostForToken(IntToken(25)) != host25 {
		t.Error("Expected host 25 for token 25")
	}
	if tokenRing.GetHostForToken(IntToken(26)) != host50 {
		t.Error("Expected host 50 for token 26")
	}
	if tokenRing.GetHostForToken(IntToken(49)) != host50 {
		t.Error("Expected host 50 for token 49")
	}
	if tokenRing.GetHostForToken(IntToken(50)) != host50 {
		t.Error("Expected host 50 for token 50")
	}
	if tokenRing.GetHostForToken(IntToken(51)) != host75 {
		t.Error("Expected host 75 for token 51")
	}
	if tokenRing.GetHostForToken(IntToken(74)) != host75 {
		t.Error("Expected host 75 for token 74")
	}
	if tokenRing.GetHostForToken(IntToken(75)) != host75 {
		t.Error("Expected host 75 for token 75")
	}
	if tokenRing.GetHostForToken(IntToken(76)) != host0 {
		t.Error("Expected host 0 for token 76")
	}
	if tokenRing.GetHostForToken(IntToken(99)) != host0 {
		t.Error("Expected host 0 for token 99")
	}
	if tokenRing.GetHostForToken(IntToken(100)) != host0 {
		t.Error("Expected host 0 for token 100")
	}
}
