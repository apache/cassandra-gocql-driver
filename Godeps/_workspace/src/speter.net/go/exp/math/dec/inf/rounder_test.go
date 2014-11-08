package inf

import (
	"math/big"
	"testing"
)

var decRounderInputs = [...]struct {
	quo    *Dec
	rA, rB *big.Int
}{
	// examples from go language spec
	{NewDec(1, 0), big.NewInt(2), big.NewInt(3)},   //  5 /  3
	{NewDec(-1, 0), big.NewInt(-2), big.NewInt(3)}, // -5 /  3
	{NewDec(-1, 0), big.NewInt(2), big.NewInt(-3)}, //  5 / -3
	{NewDec(1, 0), big.NewInt(-2), big.NewInt(-3)}, // -5 / -3
	// examples from godoc
	{NewDec(-1, 1), big.NewInt(-8), big.NewInt(10)},
	{NewDec(-1, 1), big.NewInt(-5), big.NewInt(10)},
	{NewDec(-1, 1), big.NewInt(-2), big.NewInt(10)},
	{NewDec(0, 1), big.NewInt(-8), big.NewInt(10)},
	{NewDec(0, 1), big.NewInt(-5), big.NewInt(10)},
	{NewDec(0, 1), big.NewInt(-2), big.NewInt(10)},
	{NewDec(0, 1), big.NewInt(0), big.NewInt(1)},
	{NewDec(0, 1), big.NewInt(2), big.NewInt(10)},
	{NewDec(0, 1), big.NewInt(5), big.NewInt(10)},
	{NewDec(0, 1), big.NewInt(8), big.NewInt(10)},
	{NewDec(1, 1), big.NewInt(2), big.NewInt(10)},
	{NewDec(1, 1), big.NewInt(5), big.NewInt(10)},
	{NewDec(1, 1), big.NewInt(8), big.NewInt(10)},
}

var decRounderResults = [...]struct {
	rounder Rounder
	results [len(decRounderInputs)]*Dec
}{
	{RoundExact, [...]*Dec{nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		NewDec(0, 1), nil, nil, nil, nil, nil, nil}},
	{RoundDown, [...]*Dec{
		NewDec(1, 0), NewDec(-1, 0), NewDec(-1, 0), NewDec(1, 0),
		NewDec(-1, 1), NewDec(-1, 1), NewDec(-1, 1),
		NewDec(0, 1), NewDec(0, 1), NewDec(0, 1),
		NewDec(0, 1),
		NewDec(0, 1), NewDec(0, 1), NewDec(0, 1),
		NewDec(1, 1), NewDec(1, 1), NewDec(1, 1)}},
	{RoundUp, [...]*Dec{
		NewDec(2, 0), NewDec(-2, 0), NewDec(-2, 0), NewDec(2, 0),
		NewDec(-2, 1), NewDec(-2, 1), NewDec(-2, 1),
		NewDec(-1, 1), NewDec(-1, 1), NewDec(-1, 1),
		NewDec(0, 1),
		NewDec(1, 1), NewDec(1, 1), NewDec(1, 1),
		NewDec(2, 1), NewDec(2, 1), NewDec(2, 1)}},
	{RoundHalfDown, [...]*Dec{
		NewDec(2, 0), NewDec(-2, 0), NewDec(-2, 0), NewDec(2, 0),
		NewDec(-2, 1), NewDec(-1, 1), NewDec(-1, 1),
		NewDec(-1, 1), NewDec(0, 1), NewDec(0, 1),
		NewDec(0, 1),
		NewDec(0, 1), NewDec(0, 1), NewDec(1, 1),
		NewDec(1, 1), NewDec(1, 1), NewDec(2, 1)}},
	{RoundHalfUp, [...]*Dec{
		NewDec(2, 0), NewDec(-2, 0), NewDec(-2, 0), NewDec(2, 0),
		NewDec(-2, 1), NewDec(-2, 1), NewDec(-1, 1),
		NewDec(-1, 1), NewDec(-1, 1), NewDec(0, 1),
		NewDec(0, 1),
		NewDec(0, 1), NewDec(1, 1), NewDec(1, 1),
		NewDec(1, 1), NewDec(2, 1), NewDec(2, 1)}},
	{RoundHalfEven, [...]*Dec{
		NewDec(2, 0), NewDec(-2, 0), NewDec(-2, 0), NewDec(2, 0),
		NewDec(-2, 1), NewDec(-2, 1), NewDec(-1, 1),
		NewDec(-1, 1), NewDec(0, 1), NewDec(0, 1),
		NewDec(0, 1),
		NewDec(0, 1), NewDec(0, 1), NewDec(1, 1),
		NewDec(1, 1), NewDec(2, 1), NewDec(2, 1)}},
	{RoundFloor, [...]*Dec{
		NewDec(1, 0), NewDec(-2, 0), NewDec(-2, 0), NewDec(1, 0),
		NewDec(-2, 1), NewDec(-2, 1), NewDec(-2, 1),
		NewDec(-1, 1), NewDec(-1, 1), NewDec(-1, 1),
		NewDec(0, 1),
		NewDec(0, 1), NewDec(0, 1), NewDec(0, 1),
		NewDec(1, 1), NewDec(1, 1), NewDec(1, 1)}},
	{RoundCeil, [...]*Dec{
		NewDec(2, 0), NewDec(-1, 0), NewDec(-1, 0), NewDec(2, 0),
		NewDec(-1, 1), NewDec(-1, 1), NewDec(-1, 1),
		NewDec(0, 1), NewDec(0, 1), NewDec(0, 1),
		NewDec(0, 1),
		NewDec(1, 1), NewDec(1, 1), NewDec(1, 1),
		NewDec(2, 1), NewDec(2, 1), NewDec(2, 1)}},
}

func TestDecRounders(t *testing.T) {
	for i, a := range decRounderResults {
		for j, input := range decRounderInputs {
			q := new(Dec).Set(input.quo)
			rA, rB := new(big.Int).Set(input.rA), new(big.Int).Set(input.rB)
			res := a.rounder.Round(new(Dec), q, rA, rB)
			if a.results[j] == nil && res == nil {
				continue
			}
			if (a.results[j] == nil && res != nil) ||
				(a.results[j] != nil && res == nil) ||
				a.results[j].Cmp(res) != 0 {
				t.Errorf("#%d,%d Rounder got %v; expected %v", i, j, res, a.results[j])
			}
		}
	}
}
