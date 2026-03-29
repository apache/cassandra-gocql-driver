SHELL := bash

CASSANDRA_VERSION ?= 4.1.8
TEST_CQL_PROTOCOL ?= 4
TEST_COMPRESSOR ?= no-compression
TEST_INTEGRATION_TAGS ?= integration cassandra tc
TEST_TIMEOUT ?= 60m
TEST_CLUSTER_SIZE ?= 3
TEST_AUTH_CLUSTER_SIZE ?= 1
GOLANGCI_VERSION = v2.1.6

.PHONY: test-integration test-integration-auth test-unit check fix install-golangci

test-integration:
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION} via Testcontainers"
	go test -v ${TEST_OPTS} -tags "${TEST_INTEGRATION_TAGS} gocql_debug" -timeout=${TEST_TIMEOUT} -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -rf=3 -clusterSize=${TEST_CLUSTER_SIZE} -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} ./...

test-integration-auth:
	@echo "Run auth integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION} via Testcontainers"
	go test -v ${TEST_OPTS} -run=TestAuthentication -tags "${TEST_INTEGRATION_TAGS} gocql_debug" -timeout=${TEST_TIMEOUT} -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -runauth -rf=3 -clusterSize=${TEST_AUTH_CLUSTER_SIZE} -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} ./...

test-unit:
	@echo "Run unit tests"
	@go clean -testcache
	go test -v -tags unit -timeout=5m -race ./...

check: .prepare-golangci
	@echo "Build"
	@go build -tags all .
	@echo "Check linting"
	@golangci-lint run

fix: .prepare-golangci
	@echo "Fix linting"
	golangci-lint run --fix

.prepare-golangci:
	@if ! golangci-lint --version 2>/dev/null | grep ${GOLANGCI_VERSION} >/dev/null; then \
		echo "Installing golangci-lint ${GOLANGCI_VERSION}"; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_VERSION}; \
	fi

install-golangci: .prepare-golangci
