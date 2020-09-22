package gocql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
)

// scyllaSupported represents Scylla connection options as sent in SUPPORTED
// frame.
// FIXME: Should also follow `cqlProtocolExtension` interface.
type scyllaSupported struct {
	shard             int
	nrShards          int
	msbIgnore         uint64
	partitioner       string
	shardingAlgorithm string
	lwtFlagMask       int
}

// CQL Protocol extension interface for Scylla.
// Each extension is identified by a name and defines a way to serialize itself
// in STARTUP message payload.
type cqlProtocolExtension interface {
	name() string
	serialize() map[string]string
}

func findCQLProtoExtByName(exts []cqlProtocolExtension, name string) cqlProtocolExtension {
	for i := range exts {
		if exts[i].name() == name {
			return exts[i]
		}
	}
	return nil
}

// Top-level keys used for serialization/deserialization of CQL protocol
// extensions in SUPPORTED/STARTUP messages.
// Each key identifies a single extension.
const (
	lwtAddMetadataMarkKey = "SCYLLA_LWT_ADD_METADATA_MARK"
)

// "LWT prepared statements metadata mark" CQL Protocol Extension.
// This extension, if enabled (properly negotiated), allows Scylla server
// to set a special bit in prepared statements metadata, which would indicate
// whether the statement at hand is LWT statement or not.
//
// This is further used to consistently choose primary replicas in a predefined
// order for these queries, which can reduce contention over hot keys and thus
// increase LWT performance.
//
// Implements cqlProtocolExtension interface.
type lwtAddMetadataMarkExt struct {
	lwtOptMetaBitMask int
}

var _ cqlProtocolExtension = &lwtAddMetadataMarkExt{}

// Factory function to deserialize and create an `lwtAddMetadataMarkExt` instance
// from SUPPORTED message payload.
func newLwtAddMetaMarkExt(supported map[string][]string) *lwtAddMetadataMarkExt {
	const lwtOptMetaBitMaskKey = "LWT_OPTIMIZATION_META_BIT_MASK"

	if v, found := supported[lwtAddMetadataMarkKey]; found {
		for i := range v {
			splitVal := strings.Split(v[i], "=")
			if splitVal[0] == lwtOptMetaBitMaskKey {
				var (
					err     error
					bitMask int
				)
				if bitMask, err = strconv.Atoi(splitVal[1]); err != nil {
					if gocqlDebug {
						Logger.Printf("scylla: failed to parse %s value %v: %s", lwtOptMetaBitMaskKey, splitVal[1], err)
						return nil
					}
				}
				return &lwtAddMetadataMarkExt{
					lwtOptMetaBitMask: bitMask,
				}
			}
		}
	}
	return nil
}

func (ext *lwtAddMetadataMarkExt) serialize() map[string]string {
	return map[string]string{
		lwtAddMetadataMarkKey: fmt.Sprintf("LWT_OPTIMIZATION_META_BIT_MASK=%d", ext.lwtOptMetaBitMask),
	}
}

func (ext *lwtAddMetadataMarkExt) name() string {
	return lwtAddMetadataMarkKey
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
			if gocqlDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShard, s, err)
			}
		}
	}
	if s, ok := supported[scyllaNrShards]; ok {
		if si.nrShards, err = strconv.Atoi(s[0]); err != nil {
			if gocqlDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaNrShards, s, err)
			}
		}
	}
	if s, ok := supported[scyllaShardingIgnoreMSB]; ok {
		if si.msbIgnore, err = strconv.ParseUint(s[0], 10, 64); err != nil {
			if gocqlDebug {
				Logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShardingIgnoreMSB, s, err)
			}
		}
	}

	if s, ok := supported[scyllaPartitioner]; ok {
		si.partitioner = s[0]
	}
	if s, ok := supported[scyllaShardingAlgorithm]; ok {
		si.shardingAlgorithm = s[0]
	}

	if si.partitioner != "org.apache.cassandra.dht.Murmur3Partitioner" || si.shardingAlgorithm != "biased-token-round-robin" || si.nrShards == 0 || si.msbIgnore == 0 {
		if gocqlDebug {
			Logger.Printf("scylla: unsupported sharding configuration, partitioner=%s, algorithm=%s, no_shards=%d, msb_ignore=%d",
				si.partitioner, si.shardingAlgorithm, si.nrShards, si.msbIgnore)
		}
		return scyllaSupported{}
	}

	return si
}

func parseCQLProtocolExtensions(supported map[string][]string) []cqlProtocolExtension {
	exts := []cqlProtocolExtension{}

	lwtExt := newLwtAddMetaMarkExt(supported)
	if lwtExt != nil {
		exts = append(exts, lwtExt)
	}

	return exts
}

// isScyllaConn checks if conn is suitable for scyllaConnPicker.
func isScyllaConn(conn *Conn) bool {
	return conn.scyllaSupported.nrShards != 0
}

// scyllaConnPicker is a specialised ConnPicker that selects connections based
// on token trying to get connection to a shard containing the given token.
// A list of excess connections is maintained to allow for lazy closing of
// connections to already opened shards. Keeping excess connections open helps
// reaching equilibrium faster since the likelihood of hitting the same shard
// decreases with the number of connections to the shard.
type scyllaConnPicker struct {
	conns       []*Conn
	excessConns []*Conn
	nrConns     int
	nrShards    int
	msbIgnore   uint64
	pos         uint64
}

func newScyllaConnPicker(conn *Conn) *scyllaConnPicker {
	if conn.scyllaSupported.nrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", conn.Address()))
	}

	if gocqlDebug {
		Logger.Printf("scylla: %s sharding options %+v", conn.Address(), conn.scyllaSupported)
	}

	return &scyllaConnPicker{
		nrShards:  conn.scyllaSupported.nrShards,
		msbIgnore: conn.scyllaSupported.msbIgnore,
	}
}

func (p *scyllaConnPicker) Pick(t token) *Conn {
	if len(p.conns) == 0 {
		return nil
	}

	if t == nil {
		return p.randomConn()
	}

	mmt, ok := t.(murmur3Token)
	// double check if that's murmur3 token
	if !ok {
		return nil
	}

	idx := p.shardOf(mmt)
	if c := p.conns[idx]; c != nil {
		// We have this shard's connection
		// so let's give it to the caller.
		return c
	}
	return p.randomConn()
}

func (p *scyllaConnPicker) randomConn() *Conn {
	idx := int(atomic.AddUint64(&p.pos, 1))
	for i := 0; i < len(p.conns); i++ {
		if conn := p.conns[(idx+i)%len(p.conns)]; conn != nil {
			return conn
		}
	}
	return nil
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
	const maxExcessConnsFactor = 10

	nrShards := conn.scyllaSupported.nrShards
	shard := conn.scyllaSupported.shard

	if nrShards == 0 {
		panic(fmt.Sprintf("scylla: %s not a sharded connection", conn.Address()))
	}

	if nrShards != len(p.conns) {
		if nrShards != p.nrShards {
			panic(fmt.Sprintf("scylla: %s invalid number of shards", conn.Address()))
		}
		conns := p.conns
		p.conns = make([]*Conn, nrShards, nrShards)
		copy(p.conns, conns)
	}
	if c := p.conns[shard]; c != nil {
		p.excessConns = append(p.excessConns, conn)
		if len(p.excessConns) > maxExcessConnsFactor*p.nrShards {
			if gocqlDebug {
				Logger.Printf("scylla: excess connections limit reached (%d)", maxExcessConnsFactor*p.nrShards)
			}
			p.closeExcessConns()
		}
		return
	}
	p.conns[shard] = conn
	p.nrConns++
	if p.nrConns >= p.nrShards {
		// We have reached one connection to each shard and
		// it's time to close the excess connections.
		p.closeExcessConns()
	}
	if gocqlDebug {
		Logger.Printf("scylla: %s put shard %d connection total: %d missing: %d", conn.Address(), shard, p.nrConns, p.nrShards-p.nrConns)
	}
}

// closeExcessConns closes the excess connections and clears
// the excessConns slice. This function needs to be called
// in a goroutine safe context, i.e. when the external pool
// write lock is held or other synchronization is needed.
func (p *scyllaConnPicker) closeExcessConns() {
	if gocqlDebug {
		Logger.Printf("scylla: closing %d excess connections", len(p.excessConns))
	}
	for _, c := range p.excessConns {
		c.Close()
	}
	p.excessConns = nil
}

func (p *scyllaConnPicker) Remove(conn *Conn) {
	shard := conn.scyllaSupported.shard

	if conn.scyllaSupported.nrShards == 0 {
		// It is possible for Remove to be called before the connection is added to the pool.
		// Ignoring these connections here is safe.
		if gocqlDebug {
			Logger.Printf("scylla: %s has unknown sharding state, ignoring it", conn.Address())
		}
		return
	}
	if gocqlDebug {
		Logger.Printf("scylla: %s remove shard %d connection", conn.Address(), shard)
	}

	if p.conns[shard] != nil {
		p.conns[shard] = nil
		p.nrConns--
	}
}

func (p *scyllaConnPicker) Size() (int, int) {
	return p.nrConns, p.nrShards - p.nrConns
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
