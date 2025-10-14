#!/bin/bash

# Script to run all gocql unit tests
# This script works around the custom test framework flags issue

set -e

echo "========================================="
echo "Running gocql Unit Tests"
echo "========================================="
echo ""

# Build the test binary
echo "Building test binary..."
go test -tags unit -c -o /tmp/gocql_unit_test

# Run the tests
echo "Running tests..."
echo ""
/tmp/gocql_unit_test

# Check the result
if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "✅ All unit tests PASSED!"
    echo "========================================="
    echo ""
else
    echo ""
    echo "========================================="
    echo "❌ Tests FAILED"
    echo "========================================="
    exit 1
fi

