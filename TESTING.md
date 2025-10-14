# Testing Guide for gocql

This document explains how to run tests for the gocql project.

## Quick Start

### Run All Unit Tests

The easiest way to run all unit tests:

```bash
./run_unit_tests.sh
```

This script:
- Builds the test binary with the `unit` tag
- Runs all unit tests
- Reports pass/fail status

### Manual Test Execution

If you prefer to run tests manually:

```bash
# Build the test binary
go test -tags unit -c -o /tmp/gocql_unit_test

# Run the tests
/tmp/gocql_unit_test
```

## Test Types

### Unit Tests

Unit tests are tagged with `// +build all unit` and test individual components in isolation.

**Run unit tests:**
```bash
./run_unit_tests.sh
```

**What they test:**
- Marshaling/unmarshaling
- Connection logic
- Configuration propagation
- Individual component behavior

**Requirements:**
- No Cassandra instance needed
- No network connectivity required
- Fast execution

### Integration Tests

Integration tests require a running Cassandra cluster and are tagged with `// +build integration`.

**Run integration tests:**
```bash
# Requires ccm (Cassandra Cluster Manager) and a running cluster
./integration.sh <cassandra-version> <auth-enabled>

# Example:
./integration.sh 3.0.14 false
```

**What they test:**
- Full query execution
- Cluster connectivity
- Authentication
- SSL/TLS
- Multi-node scenarios

**Requirements:**
- Cassandra cluster running (via ccm)
- Network connectivity
- Longer execution time
