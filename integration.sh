#!/bin/bash

set -e

function run_tests() {
	local version=$1
	ccm create test -v $version -n 3 -s --debug

	ccm status

	if [[ $version == 1.2.* ]]; then
		echo "running protocol 1 test suite"
		go test -v ./... -proto 1
	else
		echo "running protocol 2 test suite"
		go test -v ./...
	fi

	ccm clear
}

run_tests $1
