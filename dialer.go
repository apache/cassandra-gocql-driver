// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"context"
	"net"
)


// Dialer is interface for establishing network connections
type Dialer interface {
	DialContext(ctx context.Context, network string, address string) (net.Conn, error)
}
