package gocql

import (
	"log"
	"net"
)

func (s *Session) handleEvent(framer *framer) {
	defer framerPool.Put(framer)

	frame, err := framer.parseFrame()
	if err != nil {
		// TODO: logger
		log.Printf("gocql: unable to parse event frame: %v\n", err)
		return
	}

	// TODO: handle medatadata events
	switch f := frame.(type) {
	case *schemaChangeKeyspace:
	case *schemaChangeFunction:
	case *schemaChangeTable:
	case *topologyChangeEventFrame:
		switch f.change {
		case "NEW_NODE":
			s.handleNewNode(f.host, f.port)
		case "REMOVED_NODE":
			s.handleRemovedNode(f.host, f.port)
		case "MOVED_NODE":
			// java-driver handles this, not mentioned in the spec
			// TODO(zariel): refresh token map
		}
	case *statusChangeEventFrame:
		// TODO(zariel): is it worth having 2 methods for these?
		switch f.change {
		case "UP":
			s.handleNodeUp(f.host, f.port)
		case "DOWN":
		}
	default:
		log.Printf("gocql: invalid event frame (%T): %v\n", f, f)
	}
}

func (s *Session) handleNewNode(host net.IP, port int) {
	if !s.cfg.DiscoverHosts || s.control == nil {
		return
	}

	if s.control.addr() == host.String() {
		go s.control.reconnect(false)
	}

	hostInfo, err := s.control.fetchHostInfo(host, port)
	if err != nil {
		log.Printf("gocql: unable to fetch host info for %v: %v\n", host, err)
		return
	}

	s.pool.addHost(hostInfo)
}

func (s *Session) handleRemovedNode(host net.IP, port int) {
	if !s.cfg.DiscoverHosts {
		return
	}

	s.pool.removeHost(host.String())
}

func (s *Session) handleNodeUp(host net.IP, port int) {
	// even if were not disconvering new nodes we should still handle nodes going
	// up.

	s.pool.hostUp(host.String())
}

func (s *Session) handleNodeDown(host net.IP, port int) {
	s.pool.hostDown(host.String())
}
