//go:build all || unit
// +build all unit

package gocql

import (
	"sync"
	"testing"
)

var tablets = []*TabletInfo{
	{
		sync.RWMutex{},
		"test1",
		"table1",
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		sync.RWMutex{},
		"test1",
		"table1",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		sync.RWMutex{},
		"test1",
		"table1",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		sync.RWMutex{},
		"test1",
		"table1",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		sync.RWMutex{},
		"test1",
		"table1",
		-1,
		2305843009213693951,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		sync.RWMutex{},
		"test1",
		"table1",
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		sync.RWMutex{},
		"test1",
		"table1",
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
	{
		sync.RWMutex{},
		"test1",
		"table1",
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{TimeUUID(), 9}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{TimeUUID(), 8}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		-1,
		2305843009213693951,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{TimeUUID(), 3}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
	{
		sync.RWMutex{},
		"test2",
		"table1",
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{TimeUUID(), 7}},
	},
}

func TestFindTablets(t *testing.T) {
	id, id2 := findTablets(tablets, "test1", "table1")
	assertEqual(t, "id", 0, id)
	assertEqual(t, "id2", 7, id2)

	id, id2 = findTablets(tablets, "test2", "table1")
	assertEqual(t, "id", 8, id)
	assertEqual(t, "id2", 15, id2)

	id, id2 = findTablets(tablets, "test3", "table1")
	assertEqual(t, "id", -1, id)
	assertEqual(t, "id2", -1, id2)
}

func TestFindTabletForToken(t *testing.T) {
	tablet := findTabletForToken(tablets, parseInt64Token("0"), 0, 7)
	assertTrue(t, "tablet.lastToken == 2305843009213693951", tablet.lastToken == 2305843009213693951)

	tablet = findTabletForToken(tablets, parseInt64Token("9223372036854775807"), 0, 7)
	assertTrue(t, "tablet.lastToken == 9223372036854775807", tablet.lastToken == 9223372036854775807)

	tablet = findTabletForToken(tablets, parseInt64Token("-4611686018427387904"), 0, 7)
	assertTrue(t, "tablet.lastToken == -2305843009213693953", tablet.lastToken == -2305843009213693953)
}

func CompareRanges(tablets []*TabletInfo, ranges [][]int64) bool {
	if len(tablets) != len(ranges) {
		return false
	}

	for idx, tablet := range tablets {
		if tablet.FirstToken() != ranges[idx][0] || tablet.LastToken() != ranges[idx][1] {
			return false
		}
	}
	return true
}
func TestAddTabletToEmptyTablets(t *testing.T) {
	tablets := []*TabletInfo{}

	tablets = addTabletToTabletsList(tablets, &TabletInfo{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheBeggining(t *testing.T) {
	tablets := []*TabletInfo{{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = addTabletToTabletsList(tablets, &TabletInfo{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857}, {-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheEnd(t *testing.T) {
	tablets := []*TabletInfo{{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = addTabletToTabletsList(tablets, &TabletInfo{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-1, 2305843009213693951}}))
}

func TestAddTabletInTheMiddle(t *testing.T) {
	tablets := []*TabletInfo{{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = addTabletToTabletsList(tablets, &TabletInfo{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-4611686018427387905, -2305843009213693953},
		{-1, 2305843009213693951}}))
}

func TestAddTabletIntersecting(t *testing.T) {
	tablets := []*TabletInfo{{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{},
	}, {
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{},
	}, {
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = addTabletToTabletsList(tablets, &TabletInfo{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-3611686018427387905,
		-6,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
			{-3611686018427387905, -6},
			{-1, 2305843009213693951}}))
}

func TestAddTabletIntersectingWithFirst(t *testing.T) {
	tablets := []*TabletInfo{{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = addTabletToTabletsList(tablets, &TabletInfo{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-8011686018427387905,
		-7987529027641081857,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8011686018427387905, -7987529027641081857},
		{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletIntersectingWithLast(t *testing.T) {
	tablets := []*TabletInfo{{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = addTabletToTabletsList(tablets, &TabletInfo{
		sync.RWMutex{},
		"test_ks",
		"test_tb",
		-5011686018427387905,
		-2987529027641081857,
		[]ReplicaInfo{},
	})

	assertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857},
		{-5011686018427387905, -2987529027641081857}}))
}
