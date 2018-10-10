package gocql

import (
	"sync"
	"sync/atomic"
)

type ConnPicker interface {
	Pick(token) *Conn
	Put(*Conn)
	Remove(conn *Conn)
	Size() (int, int)
	Close()
}

type DefaultConnPicker struct {
	conns []*Conn
	pos   uint32
	size  int
	mu    sync.RWMutex
}

func NewDefaultConnPicker(size int) *DefaultConnPicker {
	return &DefaultConnPicker{
		size: size,
	}
}

func (p *DefaultConnPicker) Remove(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, candidate := range p.conns {
		if candidate == conn {
			p.conns[i] = nil
			return
		}
	}
}

func (p *DefaultConnPicker) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	conns := p.conns
	p.conns = nil
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

func (p *DefaultConnPicker) Size() (int, int) {
	sz := len(p.conns)
	return sz, p.size - sz
}

func (p *DefaultConnPicker) Pick(token) *Conn {
	pos := int(atomic.AddUint32(&p.pos, 1) - 1)
	size := len(p.conns)

	var (
		leastBusyConn    *Conn
		streamsAvailable int
	)

	// find the conn which has the most available streams, this is racy
	for i := 0; i < size; i++ {
		conn := p.conns[(pos+i)%size]
		if conn == nil {
			continue
		}
		if streams := conn.AvailableStreams(); streams > streamsAvailable {
			leastBusyConn = conn
			streamsAvailable = streams
		}
	}

	return leastBusyConn
}

func (p *DefaultConnPicker) Put(conn *Conn) {
	p.mu.Lock()
	p.conns = append(p.conns, conn)
	p.mu.Unlock()
}
