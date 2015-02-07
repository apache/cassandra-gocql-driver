package inf

import (
	"math/big"
)

// Rounder represents a method for rounding the (possibly infinite decimal)
// result of a division to a finite Dec. It is used by Dec.Round() and
// Dec.Quo().
//
type Rounder rounder

type rounder interface {

	// When UseRemainder() returns true, the Round() method is passed the
	// remainder of the division, expressed as the numerator and denominator of
	// a rational.
	UseRemainder() bool

	// Round sets the rounded value of a quotient to z, and returns z.
	// quo is rounded down (truncated towards zero) to the scale obtained from
	// the Scaler in Quo().
	//
	// When the remainder is not used, remNum and remDen are nil.
	// When used, the remainder is normalized between -1 and 1; that is:
	//
	//  -|remDen| < remNum < |remDen|
	//
	// remDen has the same sign as y, and remNum is zero or has the same sign
	// as x.
	Round(z, quo *Dec, remNum, remDen *big.Int) *Dec
}

type rndr struct {
	useRem bool
	round  func(z, quo *Dec, remNum, remDen *big.Int) *Dec
}

func (r rndr) UseRemainder() bool {
	return r.useRem
}

func (r rndr) Round(z, quo *Dec, remNum, remDen *big.Int) *Dec {
	return r.round(z, quo, remNum, remDen)
}

// RoundExact returns quo if rem is zero, or nil otherwise. It is intended to
// be used with ScaleQuoExact when it is guaranteed that the result can be
// obtained without rounding. QuoExact is a shorthand for such a quotient
// operation.
//
var RoundExact Rounder = roundExact

// RoundDown rounds towards 0; that is, returns the Dec with the greatest
// absolute value not exceeding that of the result represented by quo and rem.
//
// The following table shows examples of the results for
// QuoRound(x, y, scale, RoundDown).
//
//      x      y    scale   result
//  ------------------------------
//    -1.8    10        1     -0.1
//    -1.5    10        1     -0.1
//    -1.2    10        1     -0.1
//    -1.0    10        1     -0.1
//    -0.8    10        1     -0.0
//    -0.5    10        1     -0.0
//    -0.2    10        1     -0.0
//     0.0    10        1      0.0
//     0.2    10        1      0.0
//     0.5    10        1      0.0
//     0.8    10        1      0.0
//     1.0    10        1      0.1
//     1.2    10        1      0.1
//     1.5    10        1      0.1
//     1.8    10        1      0.1
//
var RoundDown Rounder = roundDown

// RoundUp rounds away from 0; that is, returns the Dec with the smallest
// absolute value not smaller than that of the result represented by quo and
// rem.
//
// The following table shows examples of the results for
// QuoRound(x, y, scale, RoundUp).
//
//      x      y    scale   result
//  ------------------------------
//    -1.8    10        1     -0.2
//    -1.5    10        1     -0.2
//    -1.2    10        1     -0.2
//    -1.0    10        1     -0.1
//    -0.8    10        1     -0.1
//    -0.5    10        1     -0.1
//    -0.2    10        1     -0.1
//     0.0    10        1      0.0
//     0.2    10        1      0.1
//     0.5    10        1      0.1
//     0.8    10        1      0.1
//     1.0    10        1      0.1
//     1.2    10        1      0.2
//     1.5    10        1      0.2
//     1.8    10        1      0.2
//
var RoundUp Rounder = roundUp

// RoundHalfDown rounds to the nearest Dec, and when the remainder is 1/2, it
// rounds to the Dec with the lower absolute value.
//
// The following table shows examples of the results for
// QuoRound(x, y, scale, RoundHalfDown).
//
//      x      y    scale   result
//  ------------------------------
//    -1.8    10        1     -0.2
//    -1.5    10        1     -0.1
//    -1.2    10        1     -0.1
//    -1.0    10        1     -0.1
//    -0.8    10        1     -0.1
//    -0.5    10        1     -0.0
//    -0.2    10        1     -0.0
//     0.0    10        1      0.0
//     0.2    10        1      0.0
//     0.5    10        1      0.0
//     0.8    10        1      0.1
//     1.0    10        1      0.1
//     1.2    10        1      0.1
//     1.5    10        1      0.1
//     1.8    10        1      0.2
//
var RoundHalfDown Rounder = roundHalfDown

// RoundHalfUp rounds to the nearest Dec, and when the remainder is 1/2, it
// rounds to the Dec with the greater absolute value.
//
// The following table shows examples of the results for
// QuoRound(x, y, scale, RoundHalfUp).
//
//      x      y    scale   result
//  ------------------------------
//    -1.8    10        1     -0.2
//    -1.5    10        1     -0.2
//    -1.2    10        1     -0.1
//    -1.0    10        1     -0.1
//    -0.8    10        1     -0.1
//    -0.5    10        1     -0.1
//    -0.2    10        1     -0.0
//     0.0    10        1      0.0
//     0.2    10        1      0.0
//     0.5    10        1      0.1
//     0.8    10        1      0.1
//     1.0    10        1      0.1
//     1.2    10        1      0.1
//     1.5    10        1      0.2
//     1.8    10        1      0.2
//
var RoundHalfUp Rounder = roundHalfUp

// RoundHalfEven rounds to the nearest Dec, and when the remainder is 1/2, it
// rounds to the Dec with even last digit.
//
// The following table shows examples of the results for
// QuoRound(x, y, scale, RoundHalfEven).
//
//      x      y    scale   result
//  ------------------------------
//    -1.8    10        1     -0.2
//    -1.5    10        1     -0.2
//    -1.2    10        1     -0.1
//    -1.0    10        1     -0.1
//    -0.8    10        1     -0.1
//    -0.5    10        1     -0.0
//    -0.2    10        1     -0.0
//     0.0    10        1      0.0
//     0.2    10        1      0.0
//     0.5    10        1      0.0
//     0.8    10        1      0.1
//     1.0    10        1      0.1
//     1.2    10        1      0.1
//     1.5    10        1      0.2
//     1.8    10        1      0.2
//
var RoundHalfEven Rounder = roundHalfEven

// RoundFloor rounds towards negative infinity; that is, returns the greatest
// Dec not exceeding the result represented by quo and rem.
//
// The following table shows examples of the results for
// QuoRound(x, y, scale, RoundFloor).
//
//      x      y    scale   result
//  ------------------------------
//    -1.8    10        1     -0.2
//    -1.5    10        1     -0.2
//    -1.2    10        1     -0.2
//    -1.0    10        1     -0.1
//    -0.8    10        1     -0.1
//    -0.5    10        1     -0.1
//    -0.2    10        1     -0.1
//     0.0    10        1      0.0
//     0.2    10        1      0.0
//     0.5    10        1      0.0
//     0.8    10        1      0.0
//     1.0    10        1      0.1
//     1.2    10        1      0.1
//     1.5    10        1      0.1
//     1.8    10        1      0.1
//
var RoundFloor Rounder = roundFloor

// RoundCeil rounds towards positive infinity; that is, returns the
// smallest Dec not smaller than the result represented by quo and rem.
//
// The following table shows examples of the results for
// QuoRound(x, y, scale, RoundCeil).
//
//      x      y    scale   result
//  ------------------------------
//    -1.8    10        1     -0.1
//    -1.5    10        1     -0.1
//    -1.2    10        1     -0.1
//    -1.0    10        1     -0.1
//    -0.8    10        1     -0.0
//    -0.5    10        1     -0.0
//    -0.2    10        1     -0.0
//     0.0    10        1      0.0
//     0.2    10        1      0.1
//     0.5    10        1      0.1
//     0.8    10        1      0.1
//     1.0    10        1      0.1
//     1.2    10        1      0.2
//     1.5    10        1      0.2
//     1.8    10        1      0.2
//
var RoundCeil Rounder = roundCeil

var intSign = []*big.Int{big.NewInt(-1), big.NewInt(0), big.NewInt(1)}

var roundExact = rndr{true,
	func(z, q *Dec, rA, rB *big.Int) *Dec {
		if rA.Sign() != 0 {
			return nil
		}
		return z.Set(q)
	}}

var roundDown = rndr{false,
	func(z, q *Dec, rA, rB *big.Int) *Dec {
		return z.Set(q)
	}}

var roundUp = rndr{true,
	func(z, q *Dec, rA, rB *big.Int) *Dec {
		z.Set(q)
		if rA.Sign() != 0 {
			z.UnscaledBig().Add(z.UnscaledBig(), intSign[rA.Sign()*rB.Sign()+1])
		}
		return z
	}}

func roundHalf(f func(c int, odd uint) (roundUp bool)) func(z, q *Dec, rA, rB *big.Int) *Dec {
	return func(z, q *Dec, rA, rB *big.Int) *Dec {
		z.Set(q)
		brA, brB := rA.BitLen(), rB.BitLen()
		if brA < brB-1 {
			// brA < brB-1 => |rA| < |rB/2|
			return z
		}
		roundUp := false
		srA, srB := rA.Sign(), rB.Sign()
		s := srA * srB
		if brA == brB-1 {
			rA2 := new(big.Int).Lsh(rA, 1)
			if s < 0 {
				rA2.Neg(rA2)
			}
			roundUp = f(rA2.Cmp(rB)*srB, z.UnscaledBig().Bit(0))
		} else {
			// brA > brB-1 => |rA| > |rB/2|
			roundUp = true
		}
		if roundUp {
			z.UnscaledBig().Add(z.UnscaledBig(), intSign[s+1])
		}
		return z
	}
}

var roundHalfDown = rndr{true, roundHalf(
	func(c int, odd uint) bool {
		return c > 0
	})}

var roundHalfUp = rndr{true, roundHalf(
	func(c int, odd uint) bool {
		return c >= 0
	})}

var roundHalfEven = rndr{true, roundHalf(
	func(c int, odd uint) bool {
		return c > 0 || c == 0 && odd == 1
	})}

var roundFloor = rndr{true,
	func(z, q *Dec, rA, rB *big.Int) *Dec {
		z.Set(q)
		if rA.Sign()*rB.Sign() < 0 {
			z.UnscaledBig().Add(z.UnscaledBig(), intSign[0])
		}
		return z
	}}

var roundCeil = rndr{true,
	func(z, q *Dec, rA, rB *big.Int) *Dec {
		z.Set(q)
		if rA.Sign()*rB.Sign() > 0 {
			z.UnscaledBig().Add(z.UnscaledBig(), intSign[2])
		}
		return z
	}}
