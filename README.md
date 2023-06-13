# Scylla shard-aware fork of gocql/gocql

![Build](https://github.com/scylladb/gocql/workflows/Build/badge.svg)

This is a fork of [gocql](https://github.com/gocql/gocql) package that we created at Scylla.
It contains extensions to tokenAwareHostPolicy supported by the Scylla 2.3 and onwards.
It allows driver to select a connection to a particular shard on a host based on the token.
This eliminates passing data between shards and significantly reduces latency. 
The protocol extension spec is available [here](https://github.com/scylladb/scylla/blob/master/docs/protocol-extensions.md).

There are open pull requests to merge the functionality to the upstream project:
 
* [gocql/gocql#1210](https://github.com/gocql/gocql/pull/1210)
* [gocql/gocql#1211](https://github.com/gocql/gocql/pull/1211).

It also provides support for shard aware ports, a faster way to connect to all shards, details available in [blogpost](https://www.scylladb.com/2021/04/27/connect-faster-to-scylla-with-a-shard-aware-port/).

Sunsetting Model
----------------

In general, the gocql team will focus on supporting the current and previous versions of Go. gocql may still work with older versions of Go, but official support for these versions will have been sunset.

Installation
------------

This is a drop-in replacement to gocql, it reuses the `github.com/gocql/gocql` import path.

Add the following line to your project `go.mod` file.

```
replace github.com/gocql/gocql => github.com/scylladb/gocql latest
```

and run 

```
go mod tidy
```

to evaluate `latest` to a concrete tag.

Your project now uses the Scylla driver fork, make sure you are using the `TokenAwareHostPolicy` to enable the shard-awareness, continue reading for details.

Configuration
-------------

In order to make shard-awareness work, token aware host selection policy has to be enabled.
Please make sure that the gocql configuration has `PoolConfig.HostSelectionPolicy` properly set like in the example below. 

__When working with a Scylla cluster, `PoolConfig.NumConns` option has no effect - the driver opens one connection for each shard and completely ignores this option.__

```go
c := gocql.NewCluster(hosts...)

// Enable token aware host selection policy, if using multi-dc cluster set a local DC.
fallback := gocql.RoundRobinHostPolicy()
if localDC != "" {
	fallback = gocql.DCAwareRoundRobinPolicy(localDC)
}
c.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

// If using multi-dc cluster use the "local" consistency levels. 
if localDC != "" {
	c.Consistency = gocql.LocalQuorum
}

// When working with a Scylla cluster the driver always opens one connection per shard, so `NumConns` is ignored.
// c.NumConns = 4
```

### Shard-aware port

This version of gocql supports a more robust method of establishing connection for each shard by using _shard aware port_ for native transport.
It greatly reduces time and the number of connections needed to establish a connection per shard in some cases - ex. when many clients connect at once, or when there are non-shard-aware clients connected to the same cluster.

If you are using a custom Dialer and if your nodes expose the shard-aware port, it is highly recommended to update it so that it uses a specific source port when connecting.

- If you are using a custom `net.Dialer`, you can make your dialer honor the source port by wrapping it in a `gocql.ScyllaShardAwareDialer`:
  ```go
  oldDialer := net.Dialer{...}
  clusterConfig.Dialer := &gocql.ScyllaShardAwareDialer{oldDialer}
  ```
- If you are using a custom type implementing `gocql.Dialer`, you can get the source port by using the `gocql.ScyllaGetSourcePort` function.
  An example:
  ```go
  func (d *myDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
      sourcePort := gocql.ScyllaGetSourcePort(ctx)
      localAddr, err := net.ResolveTCPAddr(network, fmt.Sprintf(":%d", sourcePort))
      if err != nil {
          return nil, err
      }
	  d := &net.Dialer{LocalAddr: localAddr}
	  return d.DialContext(ctx, network, addr)
  }
  ```
  The source port might be already bound by another connection on your system.
  In such case, you should return an appropriate error so that the driver can retry with a different port suitable for the shard it tries to connect to.

  - If you are using `net.Dialer.DialContext`, this function will return an error in case the source port is unavailable, and you can just return that error from your custom `Dialer`.
  - Otherwise, if you detect that the source port is unavailable, you can either return `gocql.ErrScyllaSourcePortAlreadyInUse` or `syscall.EADDRINUSE`.

For this feature to work correctly, you need to make sure the following conditions are met:

- Your cluster nodes are configured to listen on the shard-aware port (`native_shard_aware_transport_port` option),
- Your cluster nodes are not behind a NAT which changes source ports,
- If you have a custom Dialer, it connects from the correct source port (see the guide above).

The feature is designed to gracefully fall back to the using the non-shard-aware port when it detects that some of the above conditions are not met.
The driver will print a warning about misconfigured address translation if it detects it.
Issues with shard-aware port not being reachable are not reported in non-debug mode, because there is no way to detect it without false positives.

If you suspect that this feature is causing you problems, you can completely disable it by setting the `ClusterConfig.DisableShardAwarePort` flag to false.
