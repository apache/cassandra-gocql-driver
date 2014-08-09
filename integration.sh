#!/bin/bash

set -e

function run_tests() {
	local version=$1
	ccm create test -v $version -n 3 -s -d --vnodes

	ccm status

	local proto=2
	if [[ $version == 1.2.* ]]; then
		proto=1
	fi

	go test -v -proto=$proto -rf=3 -cluster=$(ccm liveset) ./...

	ccm clear
}

run_tests $1
