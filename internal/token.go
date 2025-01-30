package internal

import (
	"crypto/md5"
	"fmt"
	"github.com/gocql/gocql/internal/murmur"
	"math/big"
	"strconv"
)

// a Token Partitioner
type Partitioner interface {
	Name() string
	Hash([]byte) Token
	ParseString(string) Token
}

// a Token
type Token interface {
	fmt.Stringer
	Less(Token) bool
}

// murmur3 partitioner and Token
type Murmur3Partitioner struct{}
type Murmur3Token int64

func (p Murmur3Partitioner) Name() string {
	return "Murmur3Partitioner"
}

func (p Murmur3Partitioner) Hash(partitionKey []byte) Token {
	h1 := murmur.Murmur3H1(partitionKey)
	return Murmur3Token(h1)
}

// murmur3 little-endian, 128-bit hash, but returns only h1
func (p Murmur3Partitioner) ParseString(str string) Token {
	val, _ := strconv.ParseInt(str, 10, 64)
	return Murmur3Token(val)
}

func (m Murmur3Token) String() string {
	return strconv.FormatInt(int64(m), 10)
}

func (m Murmur3Token) Less(Token Token) bool {
	return m < Token.(Murmur3Token)
}

// order preserving partitioner and Token
type OrderedPartitioner struct{}
type OrderedToken string

func (p OrderedPartitioner) Name() string {
	return "OrderedPartitioner"
}

func (p OrderedPartitioner) Hash(partitionKey []byte) Token {
	// the partition key is the Token
	return OrderedToken(partitionKey)
}

func (p OrderedPartitioner) ParseString(str string) Token {
	return OrderedToken(str)
}

func (o OrderedToken) String() string {
	return string(o)
}

func (o OrderedToken) Less(Token Token) bool {
	return o < Token.(OrderedToken)
}

// random partitioner and Token
type RandomPartitioner struct{}
type RandomToken big.Int

func (r RandomPartitioner) Name() string {
	return "RandomPartitioner"
}

// 2 ** 128
var maxHashInt, _ = new(big.Int).SetString("340282366920938463463374607431768211456", 10)

func (p RandomPartitioner) Hash(partitionKey []byte) Token {
	sum := md5.Sum(partitionKey)
	val := new(big.Int)
	val.SetBytes(sum[:])
	if sum[0] > 127 {
		val.Sub(val, maxHashInt)
		val.Abs(val)
	}

	return (*RandomToken)(val)
}

func (p RandomPartitioner) ParseString(str string) Token {
	val := new(big.Int)
	val.SetString(str, 10)
	return (*RandomToken)(val)
}

func (r *RandomToken) String() string {
	return (*big.Int)(r).String()
}

func (r *RandomToken) Less(Token Token) bool {
	return -1 == (*big.Int)(r).Cmp((*big.Int)(Token.(*RandomToken)))
}
