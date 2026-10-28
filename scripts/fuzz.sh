#!/bin/sh
# Runs every native Go fuzz target in the module for FUZZTIME (default 60s).
# go test -fuzz accepts one target per run, so the targets are discovered
# with go test -list and run one at a time; a new target can't be missed.
set -eu

fuzztime=${FUZZTIME:-60s}
ran=0
pkgs=$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...)
for pkg in $pkgs; do
	targets=$(go test -list '^Fuzz' "$pkg")
	for target in $(printf '%s\n' "$targets" | grep '^Fuzz'); do
		echo "fuzz: $pkg $target for $fuzztime"
		go test -run '^$' -fuzz "^$target\$" -fuzztime "$fuzztime" "$pkg"
		ran=$((ran + 1))
	done
done
echo "fuzz: ran $ran target(s)"
