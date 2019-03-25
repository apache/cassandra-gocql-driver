package gocql

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
)

// scyllaSupported represents Scylla connection options as sent in SUPPORTED
// frame.
type scyllaSupported struct {
	shard     int
	nrShards  int
	msbIgnore uint64
}

func parseSupported(supported map[string][]string) scyllaSupported {
	const (
		scyllaShard             = "SCYLLA_SHARD"
		scyllaNrShards          = "SCYLLA_NR_SHARDS"
		scyllaPartitioner       = "SCYLLA_PARTITIONER"
		scyllaShardingAlgorithm = "SCYLLA_SHARDING_ALGORITHM"
		scyllaShardingIgnoreMSB = "SCYLLA_SHARDING_IGNORE_MSB"
	)

	var (
		si  scyllaSupported
		err error
	)

	if s, ok := supported[scyllaShard]; ok {
		if si.shard, err = strconv.Atoi(s[0]); err != nil {
			if GoCQLDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShard, s, err)
			}
		}
	}
	if s, ok := supported[scyllaNrShards]; ok {
		if si.nrShards, err = strconv.Atoi(s[0]); err != nil {
			if GoCQLDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaNrShards, s, err)
			}
		}
	}
	if s, ok := supported[scyllaShardingIgnoreMSB]; ok {
		if si.msbIgnore, err = strconv.ParseUint(s[0], 10, 64); err != nil {
			if GoCQLDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShardingIgnoreMSB, s, err)
			}
		}
	}

	var (
		partitioner string
		algorithm   string
	)
	if s, ok := supported[scyllaPartitioner]; ok {
		partitioner = s[0]
	}
	if s, ok := supported[scyllaShardingAlgorithm]; ok {
		algorithm = s[0]
	}

	if partitioner != "org.apache.cassandra.dht.Murmur3Partitioner" || algorithm != "biased-token-round-robin" || si.nrShards == 0 || si.msbIgnore == 0 {
		if GoCQLDebug {
			Logger.Printf("scylla: unsupported sharding configuration")
		}
		return scyllaSupported{}
	}

	return si
}

// isScyllaConn checks if conn is suitable for scyllaConnPicker.
func isScyllaConn(conn *Conn) bool {
	s := parseSupported(conn.supported)
	return s.nrShards != 0
}

// scyllaConnPicker is a specialised ConnPicker that selects connections based
// on token trying to get connection to a shard containing the given token.
type scyllaConnPicker struct {
	muConnsPerShard sync.RWMutex
	connsPerShard   [][]*Conn

	nrConns   int
	nrShards  int
	msbIgnore uint64
	pos       int32
}

func newScyllaConnPicker(conn *Conn) *scyllaConnPicker {
	s := parseSupported(conn.supported)
	if s.nrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", conn.Address()))
	}

	if GoCQLDebug {
		Logger.Printf("scylla: %s sharding options %+v", conn.Address(), s)
	}
	v := &scyllaConnPicker{
		nrShards:      s.nrShards,
		msbIgnore:     s.msbIgnore,
		connsPerShard: make([][]*Conn, s.nrShards),
	}
	return v
}

func (p *scyllaConnPicker) Remove(conn *Conn) {
	s := parseSupported(conn.supported)
	if s.nrShards == 0 {
		// It is possible for Remove to be called before the connection is added to the pool.
		// Ignoring these connections here is safe.
		if GoCQLDebug {
			Logger.Printf("scylla: %s has unknown sharding state, ignoring it", conn.Address())
		}
		return
	}
	if GoCQLDebug {
		Logger.Printf("scylla: %s remove shard %d connection", conn.Address(), s.shard)
	}

	p.muConnsPerShard.Lock()
	if len(p.connsPerShard[s.shard]) > 0 {
		p.nrConns--
	}
	p.connsPerShard[s.shard] = p.connsPerShard[s.shard][:]
	p.muConnsPerShard.Unlock()
}

func (p *scyllaConnPicker) Close() {
	connsPerShard := p.connsPerShard
	p.connsPerShard = nil

	p.muConnsPerShard.RLock()
	for _, conns := range connsPerShard {
		for _, c := range conns {
			c.Close()
		}
	}
	p.muConnsPerShard.RUnlock()
}

func (p *scyllaConnPicker) Size() (int, int) {
	return p.nrConns, p.nrShards - p.nrConns
}

func (p *scyllaConnPicker) Pick(t token) *Conn {
	if p.nrConns == 0 {
		return nil
	}

	if t == nil {
		idx := int(atomic.AddInt32(&p.pos, 1))
		conn := (*Conn)(nil)
		p.muConnsPerShard.RLock()
		for i := range p.connsPerShard {
			conns := p.connsPerShard[(idx+i)%len(p.connsPerShard)]
			if len(conns) > 0 {
				conn = conns[0]
				break
			}
		}
		p.muConnsPerShard.RUnlock()
		return conn
	}

	mmt, ok := t.(murmur3Token)
	// double check if that's murmur3 token
	if !ok {
		return nil
	}

	idx := p.shardOf(mmt)

	conn := (*Conn)(nil)
	p.muConnsPerShard.RLock()
	conns := p.connsPerShard[idx]
	if len(conns) > 0 {
		conn = conns[0]
	}
	p.muConnsPerShard.RUnlock()
	return conn
}

func (p *scyllaConnPicker) shardOf(token murmur3Token) int {
	shards := uint64(p.nrShards)
	z := uint64(token+math.MinInt64) << p.msbIgnore
	lo := z & 0xffffffff
	hi := (z >> 32) & 0xffffffff
	mul1 := lo * shards
	mul2 := hi * shards
	sum := (mul1 >> 32) + mul2
	return int(sum >> 32)
}

func (p *scyllaConnPicker) Put(conn *Conn) {
	s := parseSupported(conn.supported)
	if s.nrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", conn.Address()))
	}
	if s.nrShards != p.nrShards {
		panic(fmt.Sprintf("scylla: %s invalid number of shards", conn.Address()))
	}

	p.muConnsPerShard.Lock()
	conns := p.connsPerShard[s.shard]
	wasEmpty := len(conns) == 0
	conns = append(conns, conn)
	p.connsPerShard[s.shard] = conns
	if wasEmpty {
		p.nrConns++
	}
	if p.nrConns == p.nrShards {
		for i, conns := range p.connsPerShard {
			for _, c := range conns[1:] {
				c.Close()
			}
			p.connsPerShard[i] = conns[:1] // keep the first connection
		}
	}
	p.muConnsPerShard.Unlock()

	if GoCQLDebug {
		Logger.Printf("scylla: %s put shard %d connection total: %d missing: %d", conn.Address(), s.shard, p.nrConns, p.nrShards-p.nrConns)
	}
}
