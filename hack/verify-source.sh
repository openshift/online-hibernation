#!/bin/bash

# This script is meant to perform any number of verifications to ensure a Go
# source commit is sane (formatters, linters, etc.).

function find_files() {
	find . -not \( \
		\( \
		-wholename './.*' \
		-o -wholename '*/vendor/*' \
		\) -prune \
	\) -name '*.go' | sort -u
}

# Ensures that non-vendored Go source has been processed with gofmt.
function verify_gofmt() {
	local bad_files=$(find_files | xargs gofmt -l)
	if [[ -n "${bad_files}" ]]; then
		echo "!!! gofmt needs to be run on the following files: " >&2
		echo "${bad_files}"
		echo "Try running 'gofmt -d [path]'" >&2
		echo "Or autocorrect with 'hack/verify-gofmt.sh | xargs -n 1 gofmt -w'" >&2
		exit 1
	fi
}

verify_gofmt
