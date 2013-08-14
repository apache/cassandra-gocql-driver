package gocql

import (
	"sync"
	"sync/atomic"
	"time"
)

type Node interface {
	ExecuteQuery(qry *Query) (*Iter, error)
	ExecuteBatch(batch *Batch) error
	Close()
}

type NodePicker interface {
	AddNode(node Node)
	RemoveNode(node Node)
	Pick(qry *Query) Node
}

type RoundRobinPicker struct {
	pool []Node
	pos  uint32
	mu   sync.RWMutex
}

func NewRoundRobinPicker() *RoundRobinPicker {
	return &RoundRobinPicker{}
}

func (r *RoundRobinPicker) AddNode(node Node) {
	r.mu.Lock()
	r.pool = append(r.pool, node)
	r.mu.Unlock()
}

func (r *RoundRobinPicker) RemoveNode(node Node) {
	r.mu.Lock()
	n := len(r.pool)
	for i := 0; i < n; i++ {
		if r.pool[i] == node {
			r.pool[i], r.pool[n-1] = r.pool[n-1], r.pool[i]
			r.pool = r.pool[:n-1]
			break
		}
	}
	r.mu.Unlock()
}

func (r *RoundRobinPicker) Pick(query *Query) Node {
	pos := atomic.AddUint32(&r.pos, 1)
	var node Node
	r.mu.RLock()
	if len(r.pool) > 0 {
		node = r.pool[pos%uint32(len(r.pool))]
	}
	r.mu.RUnlock()
	return node
}

type Reconnector interface {
	Reconnect(session *Session, address string)
}

type ExponentialReconnector struct {
	baseDelay time.Duration
	maxDelay  time.Duration
}

func NewExponentialReconnector(baseDelay, maxDelay time.Duration) *ExponentialReconnector {
	return &ExponentialReconnector{baseDelay, maxDelay}
}

func (e *ExponentialReconnector) Reconnect(session *Session, address string) {
	delay := e.baseDelay
	for {
		conn, err := Connect(address, session.cfg)
		if err != nil {
			<-time.After(delay)
			if delay *= 2; delay > e.maxDelay {
				delay = e.maxDelay
			}
			continue
		}
		node := &Host{conn}
		go func() {
			conn.Serve()
			session.pool.RemoveNode(node)
			e.Reconnect(session, address)
		}()
		session.pool.AddNode(node)
		return
	}
}

type Host struct {
	conn *Conn
}

func (h *Host) ExecuteQuery(qry *Query) (*Iter, error) {
	return h.conn.ExecuteQuery(qry)
}

func (h *Host) ExecuteBatch(batch *Batch) error {
	return nil
}

func (h *Host) Close() {
	h.conn.conn.Close()
}
