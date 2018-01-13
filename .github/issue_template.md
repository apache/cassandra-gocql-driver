Please answer these questions before submitting your issue. Thanks!

### What version of Cassandra are you using?


### What version of Gocql are you using?


### What did you do?


### What did you expect to see?


### What did you see instead?

---

If you are having connectivity related issues please share the following additional information

### Describe your Cassandra cluster
please provide the following information

- output of `nodetool status`
- output of `SELECT peer, rpc_address FROM system.peers`
- rebuild your application with the `gocql_debug` tag and post the output
