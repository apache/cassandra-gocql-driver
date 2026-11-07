//go:build all || unit
// +build all unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

func (f dialContextFunc) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return f(ctx, network, addr)
}

func TestDefaultHostDialerTLSHandshakeTimeout(t *testing.T) {
	tests := []struct {
		name           string
		connectTimeout time.Duration
		parentTimeout  time.Duration
	}{
		{"connect timeout", 50 * time.Millisecond, 5 * time.Second},
		{"earlier parent deadline", 5 * time.Second, 50 * time.Millisecond},
		{"zero connect timeout", 0, 50 * time.Millisecond},
		{"negative connect timeout", -time.Second, 50 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			if err := server.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			// Read the ClientHello but never send a TLS response.
			done := make(chan error, 1)
			go func() {
				_, err := io.Copy(io.Discard, server)
				done <- err
			}()

			ctx, cancel := context.WithTimeout(context.Background(), test.parentTimeout)
			defer cancel()
			parentDeadline, _ := ctx.Deadline()
			var dialDeadline time.Time
			cfg := NewCluster("127.0.0.1")
			cfg.ConnectTimeout = test.connectTimeout
			cfg.SslOpts = &SslOptions{}
			cfg.Dialer = dialContextFunc(func(ctx context.Context, _, _ string) (net.Conn, error) {
				dialDeadline, _ = ctx.Deadline()
				return client, nil
			})
			connCfg, err := connConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			host := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042}
			start := time.Now()
			dialed, err := connCfg.HostDialer.DialHost(ctx, host)
			if dialed != nil {
				dialed.Conn.Close()
				t.Fatal("expected no connection after a stalled TLS handshake")
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected context deadline exceeded, got %v", err)
			}
			if test.connectTimeout > 0 && test.connectTimeout < test.parentTimeout {
				if dialDeadline.Before(start.Add(test.connectTimeout)) || !dialDeadline.Before(parentDeadline) {
					t.Fatalf("expected a connect timeout deadline before %v, got %v", parentDeadline, dialDeadline)
				}
				if time.Until(dialDeadline) > 0 {
					t.Fatal("TLS handshake returned before the shared dial deadline expired")
				}
				if ctx.Err() != nil {
					t.Fatalf("expected connect timeout before parent deadline, got %v", ctx.Err())
				}
			} else if !dialDeadline.Equal(parentDeadline) {
				t.Fatalf("expected parent deadline %v, got %v", parentDeadline, dialDeadline)
			}
			// A failed handshake must close the underlying connection.
			if err := <-done; err != nil {
				t.Fatalf("expected peer to observe a closed connection, got %v", err)
			}
		})
	}
}

func TestDefaultHostDialerConnectTimeout(t *testing.T) {
	cfg := NewCluster("127.0.0.1")
	cfg.ConnectTimeout = 50 * time.Millisecond
	cfg.Dialer = dialContextFunc(func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	connCfg, err := connConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	host := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042}
	_, err = connCfg.HostDialer.DialHost(ctx, host)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("expected connect timeout before parent deadline, got %v", ctx.Err())
	}
}

func TestDefaultHostDialerSuccess(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("testdata/pki/cassandra.crt", "testdata/pki/cassandra.key")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name           string
		tls            bool
		connectTimeout time.Duration
	}{
		{"plain", false, 5 * time.Second},
		{"TLS", true, 5 * time.Second},
		{"plain without timeout", false, 0},
		{"TLS without timeout", true, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			for _, conn := range []net.Conn{client, server} {
				if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				var conn net.Conn = server
				if test.tls {
					conn = tls.Server(server, &tls.Config{Certificates: []tls.Certificate{cert}})
				}
				var buf [1]byte
				_, err := io.ReadFull(conn, buf[:])
				done <- err
			}()

			cfg := NewCluster("127.0.0.1")
			cfg.ConnectTimeout = test.connectTimeout
			cfg.Dialer = dialContextFunc(func(context.Context, string, string) (net.Conn, error) {
				return client, nil
			})
			if test.tls {
				cfg.SslOpts = &SslOptions{}
			}
			connCfg, err := connConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			host := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042}
			dialed, err := connCfg.HostDialer.DialHost(ctx, host)
			if err != nil {
				t.Fatal(err)
			}
			if dialed.DisableCoalesce != test.tls {
				t.Fatalf("expected DisableCoalesce=%v, got %v", test.tls, dialed.DisableCoalesce)
			}
			// Cancelling the setup context must leave the established connection usable.
			cancel()
			if _, err := dialed.Conn.Write([]byte{1}); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
