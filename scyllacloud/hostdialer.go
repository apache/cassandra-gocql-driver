// Copyright (C) 2021 ScyllaDB

package scyllacloud

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"

	"github.com/gocql/gocql"
	"golang.org/x/net/proxy"
)

// SniHostDialer is able to dial particular host through SNI proxy.
// TLS Config is build from ConnectionConfig based on datacenter where given node belongs.
// SNI is constructed from host_id of a node, and NodeDomain taken from cloud config.
type SniHostDialer struct {
	connConfig *ConnectionConfig
	dialer     gocql.Dialer
}

func NewSniHostDialer(connConfig *ConnectionConfig, dialer gocql.Dialer) *SniHostDialer {
	return &SniHostDialer{
		connConfig: connConfig,
		dialer:     dialer,
	}
}

func (s *SniHostDialer) DialHost(ctx context.Context, host *gocql.HostInfo) (*gocql.DialedHost, error) {
	hostID := host.HostID()
	if len(hostID) == 0 {
		return s.dialInitialContactPoint(ctx)
	}

	dcName := host.DataCenter()
	dcConf, ok := s.connConfig.Datacenters[dcName]
	if !ok {
		return nil, fmt.Errorf("datacenter %q configuration not found in connection bundle", dcName)
	}

	dialer := s.dialer

	if len(dcConf.ProxyURL) != 0 {
		u, err := url.Parse(dcConf.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("can't parse proxy URL %q: %w", dcConf.ProxyURL, err)
		}

		d, err := proxy.FromURL(u, proxyDialerFunc(func(network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		}))
		if err != nil {
			return nil, fmt.Errorf("can't create proxy dialer: %w", err)
		}

		dialer = d.(proxy.ContextDialer)
	}

	sni := fmt.Sprintf("%s.%s", host.HostID(), dcConf.NodeDomain)
	clientCertificate, err := s.connConfig.GetClientCertificate()
	if err != nil {
		return nil, fmt.Errorf("can't get client certificate from configuration: %w", err)
	}

	ca, err := s.connConfig.GetDatacenterCAPool(dcName)
	if err != nil {
		return nil, fmt.Errorf("can't get root CA from configuration: %w", err)
	}

	return s.connect(ctx, dialer, dcConf.Server, &tls.Config{
		ServerName:         sni,
		RootCAs:            ca,
		InsecureSkipVerify: dcConf.InsecureSkipTLSVerify,
		Certificates:       []tls.Certificate{*clientCertificate},
	})
}

func (s *SniHostDialer) dialInitialContactPoint(ctx context.Context) (*gocql.DialedHost, error) {
	insecureSkipVerify := false
	for _, dc := range s.connConfig.Datacenters {
		if dc.InsecureSkipTLSVerify {
			insecureSkipVerify = true
			break
		}
	}

	clientCertificate, err := s.connConfig.GetClientCertificate()
	if err != nil {
		return nil, fmt.Errorf("can't get client certificate from configuration: %w", err)
	}

	ca, err := s.connConfig.GetRootCAPool()
	if err != nil {
		return nil, fmt.Errorf("can't get root CA from configuration: %w", err)
	}

	dcConf, err := s.connConfig.GetCurrentDatacenterConfig()
	if err != nil {
		return nil, fmt.Errorf("can't get current datacenter config: %w", err)
	}

	serverName := dcConf.NodeDomain
	if len(serverName) == 0 {
		serverName = dcConf.Server
	}

	return s.connect(ctx, s.dialer, dcConf.Server, &tls.Config{
		ServerName:         serverName,
		RootCAs:            ca,
		InsecureSkipVerify: insecureSkipVerify,
		Certificates:       []tls.Certificate{*clientCertificate},
	})
}

func (s *SniHostDialer) connect(ctx context.Context, dialer gocql.Dialer, server string, tlsConfig *tls.Config) (*gocql.DialedHost, error) {
	conn, err := dialer.DialContext(ctx, "tcp", server)
	if err != nil {
		return nil, fmt.Errorf("can't connect to %q: %w", server, err)
	}

	tconn := tls.Client(conn, tlsConfig)
	if err := tconn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("can't finish TLS handshake with server %q SNI %q: %w", server, tlsConfig.ServerName, err)
	}

	return &gocql.DialedHost{
		Conn:            tconn,
		DisableCoalesce: true,
	}, nil
}

type proxyDialerFunc func(network, addr string) (net.Conn, error)

func (d proxyDialerFunc) Dial(network, addr string) (net.Conn, error) {
	return d(network, addr)
}
