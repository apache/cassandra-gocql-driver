package gocql

import (
	"math"
	"strconv"
)

const noSharding = -1

const (
	scyllaShard             = "SCYLLA_SHARD"
	scyllaNrShards          = "SCYLLA_NR_SHARDS"
	scyllaPartitioner       = "SCYLLA_PARTITIONER"
	scyllaShardingAlgorithm = "SCYLLA_SHARDING_ALGORITHM"
	scyllaShardingIgnoreMSB = "SCYLLA_SHARDING_IGNORE_MSB"
)

type scyllaConnPicker struct {
	conns        []*Conn
	connsCount   int32
	shardingInfo shardingInfo
}

func NewScyllaConnPicker() *scyllaConnPicker {
	return &scyllaConnPicker{}
}

func (p *scyllaConnPicker) Remove(conn *Conn) {
	p.conns[conn.shard] = nil
}

func (p *scyllaConnPicker) Close() {
	conns := p.conns
	p.conns = nil
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

func (p *scyllaConnPicker) Size() (int, int) {
	sz := int(p.connsCount)
	return sz, p.shardingInfo.nrShards - sz
}

func (p *scyllaConnPicker) Pick(t token) *Conn {
	if t == nil {
		for _, conn := range p.conns {
			if conn != nil {
				return conn
			}
		}
		return nil
	}

	mmt, ok := t.(murmur3Token)
	// double check if that's murmur3 token
	if !ok {
		return nil
	}

	idx := p.shardingInfo.ShardOf(mmt)
	return p.conns[idx]
}

func (p *scyllaConnPicker) Put(conn *Conn) {
	si := conn.host.ShardingInfo()
	if size := si.nrShards; size != len(p.conns) {
		p.shardingInfo = si
		conns := p.conns
		p.conns = make([]*Conn, size, size)
		copy(p.conns, conns)
	}
	if c := p.conns[conn.shard]; c != nil {
		conn.Close()
		return
	}
	p.conns[conn.shard] = conn
	p.connsCount++
}

type shardingInfo struct {
	nrShards  int
	msbIgnore uint64
}

func newShardingInfo(m map[string][]string) (int, shardingInfo) {
	// default shard is noSharding
	shard := noSharding

	if s, ok := m[scyllaShard]; ok {
		if sh, err := strconv.Atoi(s[0]); err == nil {
			shard = sh
		} else if gocqlDebug {
			Logger.Printf("sharding: failed to parse %s value %v: %s", scyllaShard, s, err)
		}
	}
	if shard == noSharding {
		return noSharding, shardingInfo{}
	}

	var (
		partitioner string
		algorithm   string
		si          shardingInfo
		err         error
	)

	if s, ok := m[scyllaPartitioner]; ok {
		partitioner = s[0]
	}
	if s, ok := m[scyllaShardingAlgorithm]; ok {
		algorithm = s[0]
	}
	if s, ok := m[scyllaNrShards]; ok {
		if si.nrShards, err = strconv.Atoi(s[0]); err != nil {
			if gocqlDebug {
				Logger.Printf("sharding: failed to parse %s value %v: %s", scyllaNrShards, s, err)
			}
		}
	}
	if s, ok := m[scyllaShardingIgnoreMSB]; ok {
		if si.msbIgnore, err = strconv.ParseUint(s[0], 10, 64); err != nil {
			if gocqlDebug {
				Logger.Printf("sharding: failed to parse %s value %v: %s", scyllaShardingIgnoreMSB, s, err)
			}
		}
	}

	if partitioner != "org.apache.cassandra.dht.Murmur3Partitioner" || algorithm != "biased-token-round-robin" || si.nrShards == 0 || si.msbIgnore == 0 {
		return noSharding, shardingInfo{}
	}

	return shard, si
}

func (si shardingInfo) ShardOf(token murmur3Token) int {
	shards := uint64(si.nrShards)
	z := uint64(token+math.MinInt64) << si.msbIgnore
	lo := z & 0xffffffff
	hi := (z >> 32) & 0xffffffff
	mul1 := lo * shards
	mul2 := hi * shards
	sum := (mul1 >> 32) + mul2
	return int(sum >> 32)
}
