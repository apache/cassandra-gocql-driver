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

	"github.com/gocql/gocql/internal/murmur"
)

// a token partitioner
type partitioner interface {
	Name() string
	Hash([]byte) Token
	ParseString(string) Token
}

// Token is a Cassandra token.
// It can be used in queries such as:
//  session.Query("SELECT * FROM my_table WHERE TOKEN(id) > ?", token)
type Token interface {
	fmt.Stringer
	Marshaler
	Less(Token) bool
}

// murmur3 partitioner and token
type murmur3Partitioner struct{}
type murmur3Token int64

func (p murmur3Partitioner) Name() string {
	return "Murmur3Partitioner"
}

func (p murmur3Partitioner) Hash(partitionKey []byte) Token {
	h1 := murmur.Murmur3H1(partitionKey)
	return murmur3Token(h1)
}

// murmur3 little-endian, 128-bit hash, but returns only h1
func (p murmur3Partitioner) ParseString(str string) Token {
	val, _ := strconv.ParseInt(str, 10, 64)
	return murmur3Token(val)
}

func (m murmur3Token) String() string {
	return strconv.FormatInt(int64(m), 10)
}

func (m murmur3Token) Less(token Token) bool {
	return m < token.(murmur3Token)
}

func (m murmur3Token) MarshalCQL(info TypeInfo) ([]byte, error) {
	return Marshal(info, int64(m))
}

// order preserving partitioner and token
type orderedPartitioner struct{}
type orderedToken string

func (p orderedPartitioner) Name() string {
	return "OrderedPartitioner"
}

func (p orderedPartitioner) Hash(partitionKey []byte) Token {
	// the partition key is the Token
	return orderedToken(partitionKey)
}

func (p orderedPartitioner) ParseString(str string) Token {
	return orderedToken(str)
}

func (o orderedToken) String() string {
	return string(o)
}

func (o orderedToken) Less(token Token) bool {
	return o < token.(orderedToken)
}

func (o orderedToken) MarshalCQL(info TypeInfo) ([]byte, error) {
	return Marshal(info, string(o))
}

// random partitioner and token
type randomPartitioner struct{}
type randomToken big.Int

func (r randomPartitioner) Name() string {
	return "RandomPartitioner"
}

// 2 ** 128
var maxHashInt, _ = new(big.Int).SetString("340282366920938463463374607431768211456", 10)

func (p randomPartitioner) Hash(partitionKey []byte) Token {
	sum := md5.Sum(partitionKey)
	val := new(big.Int)
	val.SetBytes(sum[:])
	if sum[0] > 127 {
		val.Sub(val, maxHashInt)
		val.Abs(val)
	}

	return (*randomToken)(val)
}

func (p randomPartitioner) ParseString(str string) Token {
	val := new(big.Int)
	val.SetString(str, 10)
	return (*randomToken)(val)
}

func (r *randomToken) String() string {
	return (*big.Int)(r).String()
}

func (r *randomToken) Less(token Token) bool {
	return -1 == (*big.Int)(r).Cmp((*big.Int)(token.(*randomToken)))
}

func (r *randomToken) MarshalCQL(info TypeInfo) ([]byte, error) {
	return Marshal(info, (*big.Int)(r))
}

type hostToken struct {
	token Token
	host  *HostInfo
}

func (ht hostToken) String() string {
	return fmt.Sprintf("{token=%v host=%v}", ht.token, ht.host.HostID())
}

// TokenRing is a data structure for organizing the relationship between tokens and hosts
type TokenRing struct {
	partitioner partitioner

	// tokens map token range to primary replica.
	// The elements in tokens are sorted by token ascending.
	// The range for a given item in tokens starts after preceding range and ends with the token specified in
	// token. The end token is part of the range.
	// The lowest (i.e. index 0) range wraps around the ring (its preceding range is the one with largest index).
	tokens []hostToken

	hosts []*HostInfo
}

func newTokenRing(partitioner string, hosts []*HostInfo) (*TokenRing, error) {
	tokenRing := &TokenRing{
		hosts: hosts,
	}

	if strings.HasSuffix(partitioner, "Murmur3Partitioner") {
		tokenRing.partitioner = murmur3Partitioner{}
	} else if strings.HasSuffix(partitioner, "OrderedPartitioner") {
		tokenRing.partitioner = orderedPartitioner{}
	} else if strings.HasSuffix(partitioner, "RandomPartitioner") {
		tokenRing.partitioner = randomPartitioner{}
	} else {
		return nil, fmt.Errorf("unsupported partitioner '%s'", partitioner)
	}

	for _, host := range hosts {
		for _, strToken := range host.Tokens() {
			token := tokenRing.partitioner.ParseString(strToken)
			tokenRing.tokens = append(tokenRing.tokens, hostToken{token, host})
		}
	}

	sort.Sort(tokenRing)

	return tokenRing, nil
}

func (t *TokenRing) Len() int {
	return len(t.tokens)
}

func (t *TokenRing) Less(i, j int) bool {
	return t.tokens[i].token.Less(t.tokens[j].token)
}

func (t *TokenRing) Swap(i, j int) {
	t.tokens[i], t.tokens[j] = t.tokens[j], t.tokens[i]
}

func (t *TokenRing) String() string {
	buf := &bytes.Buffer{}
	buf.WriteString("TokenRing(")
	if t.partitioner != nil {
		buf.WriteString(t.partitioner.Name())
	}
	buf.WriteString("){")
	sep := ""
	for i, th := range t.tokens {
		buf.WriteString(sep)
		sep = ","
		buf.WriteString("\n\t[")
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString("]")
		buf.WriteString(th.token.String())
		buf.WriteString(":")
		buf.WriteString(th.host.ConnectAddress().String())
	}
	buf.WriteString("\n}")
	return string(buf.Bytes())
}

// Tokens returns the token range corresponding to the primary replica.
// The elements are sorted by token ascending.
// The range for a given item starts after preceding range and ends with the token at the current position.
// The end token is part of the range.
// The lowest (i.e. index 0) range wraps around the ring (its preceding range is the one with the largest index).
// You can obtain the owner host/vnode of the range by calling HostForToken with the end token.
//
// The following example constructs one TOKEN-based query for each token range:
//	func buildTokenQueries(s *Session, t *TokenRing) []*Query {
//		tokens := t.Tokens()
//		if len(tokens) == 0 {
//			return nil
//		}
//
//		// If there's a single token (practically impossible), there is no need for a token range query
//		// because all the tokens are assigned to the same host/vnode anyway
//		if len(tokens) == 1 {
//			return []*Query{
//				s.Query("SELECT * FROM my_table"),
//			}
//		}
//
//		queries := make([]*Query, 0, len(tokens)+1)
//		for i, token := range tokens {
//			var query *Query
//			if i == 0 {
//				// The first range contains all the tokens less than or equal to tokens[0]
//				query = s.Query("SELECT * FROM my_table WHERE TOKEN(id) <= ?", token)
//			} else {
//				// Tokens in intermediate ranges are between (tokens[i-1], tokens[i]]
//				prevToken := tokens[i-1]
//				query = s.Query("SELECT * FROM my_table WHERE TOKEN(id) > ? AND TOKEN(id) <= ?", prevToken, token)
//			}
//			queries = append(queries, query)
//		}
//
//		// Add a final range containing all the tokens greater than the last token,
//		// to complete the wraparound to tokens[0].
//		queries = append(queries,
//			s.Query("SELECT * FROM my_table WHERE TOKEN(id) > ?", tokens[len(tokens)-1]),
//		)
//		return queries
//	}
func (t *TokenRing) Tokens() []Token {
	if len(t.tokens) == 0 {
		return nil
	}

	ranges := make([]Token, len(t.tokens))
	for i := range t.tokens {
		ranges[i] = t.tokens[i].token
	}

	return ranges
}

func buildTokenQueries(s *Session, t *TokenRing) []*Query {
	tokens := t.Tokens()
	if len(tokens) == 0 {
		return nil
	}

	// If there's a single token (practically impossible), there is no need for a token range query
	// because all the tokens are assigned to the same host/vnode anyway
	if len(tokens) == 1 {
		return []*Query{
			s.Query("SELECT * FROM my_table"),
		}
	}

	queries := make([]*Query, 0, len(tokens)+1)
	for i, token := range tokens {
		var query *Query
		if i == 0 {
			// The first range contains all the tokens less than or equal to tokens[0]
			query = s.Query("SELECT * FROM my_table WHERE TOKEN(id) <= ?", token)
		} else {
			// Tokens in intermediate ranges are between (tokens[i-1], tokens[i]]
			prevToken := tokens[i-1]
			query = s.Query("SELECT * FROM my_table WHERE TOKEN(id) > ? AND TOKEN(id) <= ?", prevToken, token)
		}
		queries = append(queries, query)
	}

	// The last range contains all the tokens greater than the last token (completing the wraparound to tokens[0]).
	queries = append(queries,
		s.Query("SELECT * FROM my_table WHERE TOKEN(id) > ?", tokens[len(tokens)-1]),
	)
	return queries
}

// HostForToken returns the host to which a token belongs
func (t *TokenRing) HostForToken(token Token) (host *HostInfo, endToken Token) {
	if t == nil || len(t.tokens) == 0 {
		return nil, nil
	}

	// find the primary replica
	p := sort.Search(len(t.tokens), func(i int) bool {
		return !t.tokens[i].token.Less(token)
	})

	if p == len(t.tokens) {
		// wrap around to the first in the ring
		p = 0
	}

	v := t.tokens[p]
	return v.host, v.token
}
