package gocql

// ClusterMeta holds metadata about cluster topology.
// All of its operations are read-only.
type ClusterMeta interface {
	GetTokenRing() *tokenRing
}

// clusterMeta holds metadata about cluster topology.
// It is used inside atomic.Value and shallow copies are used when replacing it,
// so fields should not be modified in-place. Instead, to modify a field a copy of the field should be made
// and the pointer in clusterMeta updated to point to the new value.
type clusterMeta struct {
	// replicas is map[keyspace]map[Token]hosts
	replicas  map[string]tokenRingReplicas
	tokenRing *tokenRing
}

// resetTokenRing creates a new tokenRing.
// It must be called with t.mu locked.
func (m *clusterMeta) resetTokenRing(partitioner string, hosts []*HostInfo, logger StdLogger) {
	if partitioner == "" {
		// partitioner not yet set
		return
	}

	// create a new Token ring
	tokenRing, err := newTokenRing(partitioner, hosts)
	if err != nil {
		logger.Printf("Unable to update the Token ring due to error: %s", err)
		return
	}

	// replace the Token ring
	m.tokenRing = tokenRing
}

//GetTokenRing returns the token ring
func (m *clusterMeta) GetTokenRing() TokenRing {
	return m.tokenRing
}
