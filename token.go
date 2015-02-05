// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/spaolacci/murmur3"
)

// a token partitioner
type Partitioner interface {
	Hash([]byte) Token
	ParseString(string) Token
}

// a token
type Token interface {
	fmt.Stringer
	Less(Token) bool
}

// murmur3 partitioner and token
type Murmur3Partitioner struct{}
type Murmur3Token int64

func (p Murmur3Partitioner) Hash(partitionKey []byte) Token {
	hash := murmur3.New128()
	hash.Write(partitionKey)
	// cassandra only uses h1
	h1, _ := hash.Sum128()
	return Murmur3Token(int64(h1))
}

func (p Murmur3Partitioner) ParseString(str string) Token {
	val, _ := strconv.ParseInt(str, 10, 64)
	return Murmur3Token(val)
}

func (m Murmur3Token) String() string {
	return strconv.FormatInt(int64(m), 10)
}

func (m Murmur3Token) Less(token Token) bool {
	return m < token.(Murmur3Token)
}

// order preserving partitioner and token
type OrderPreservingPartitioner struct{}
type OrderPreservingToken []byte

func (p OrderPreservingPartitioner) Hash(partitionKey []byte) Token {
	// the partition key is the token
	return OrderPreservingToken(partitionKey)
}

func (p OrderPreservingPartitioner) ParseString(str string) Token {
	return OrderPreservingToken([]byte(str))
}

func (o OrderPreservingToken) String() string {
	return string([]byte(o))
}

func (o OrderPreservingToken) Less(token Token) bool {
	return -1 == bytes.Compare(o, token.(OrderPreservingToken))
}

// random partitioner and token
type RandomPartitioner struct{}
type RandomToken struct {
	*big.Int
}

func (p RandomPartitioner) Hash(partitionKey []byte) Token {
	hash := md5.New()
	sum := hash.Sum(partitionKey)

	val := new(big.Int)
	val = val.SetBytes(sum)
	val = val.Abs(val)

	return RandomToken{val}
}

func (p RandomPartitioner) ParseString(str string) Token {
	val := new(big.Int)
	val.SetString(str, 10)
	return RandomToken{val}
}

func (r RandomToken) Less(token Token) bool {
	return -1 == r.Int.Cmp(token.(RandomToken).Int)
}

// a data structure for organizing the relationship between tokens and hosts
type TokenRing struct {
	partitioner Partitioner
	tokens      []Token
	hosts       []*HostInfo
}

func NewTokenRing(partitioner string, hosts []*HostInfo) (*TokenRing, error) {
	tokenRing := &TokenRing{
		tokens: []Token{},
		hosts:  []*HostInfo{},
	}

	if strings.HasSuffix(partitioner, "Murmur3Partitioner") {
		tokenRing.partitioner = Murmur3Partitioner{}
	} else if strings.HasSuffix(partitioner, "OrderedPartitioner") {
		tokenRing.partitioner = OrderPreservingPartitioner{}
	} else if strings.HasSuffix(partitioner, "RandomPartitioner") {
		tokenRing.partitioner = RandomPartitioner{}
	} else {
		return nil, fmt.Errorf("Unsupported partitioner '%s'", partitioner)
	}

	for _, host := range hosts {
		for _, strToken := range host.Tokens {
			token := tokenRing.partitioner.ParseString(strToken)
			tokenRing.tokens = append(tokenRing.tokens, token)
			tokenRing.hosts = append(tokenRing.hosts, host)
		}
	}

	sort.Sort(tokenRing)

	return tokenRing, nil
}

func (t *TokenRing) Len() int {
	return len(t.tokens)
}

func (t *TokenRing) Less(i, j int) bool {
	return t.tokens[i].Less(t.tokens[j])
}

func (t *TokenRing) Swap(i, j int) {
	t.tokens[i], t.hosts[i], t.tokens[j], t.hosts[j] =
		t.tokens[j], t.hosts[j], t.tokens[i], t.hosts[i]
}

func (t *TokenRing) String() string {
	buf := &bytes.Buffer{}
	buf.WriteString("TokenRing={")
	sep := ""
	for i := range t.tokens {
		buf.WriteString(sep)
		sep = ","
		buf.WriteString("\n\t[")
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString("]")
		buf.WriteString(t.tokens[i].String())
		buf.WriteString(":")
		buf.WriteString(t.hosts[i].Peer)
	}
	buf.WriteString("\n}")
	return string(buf.Bytes())
}

func (t *TokenRing) GetHostForPartitionKey(partitionKey []byte) *HostInfo {
	token := t.partitioner.Hash(partitionKey)
	return t.GetHostForToken(token)
}

func (t *TokenRing) GetHostForToken(token Token) *HostInfo {
	// find the primary repica
	ringIndex := sort.Search(
		len(t.tokens),
		func(i int) bool {
			return !t.tokens[i].Less(token)
		},
	)
	if ringIndex == len(t.tokens) {
		// wrap around to the first in the ring
		ringIndex = 0
	}
	host := t.hosts[ringIndex]
	return host
}
