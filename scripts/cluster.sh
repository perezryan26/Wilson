#!/bin/sh
# Runs a local cluster of 3 or 5 kvnode servers on 127.0.0.1, ports
# BASE_PORT+1 and up (default 8001), until Ctrl-C stops them all. Each
# server keeps its log in DATA/nN (default ${TMPDIR:-/tmp}/wilson-cluster),
# which outlives the run: running again with the same DATA restarts the
# same cluster from its disks. Remove the directory to start afresh.
#
#   scripts/cluster.sh      # three servers
#   scripts/cluster.sh 5    # five servers
set -eu

size=${1:-3}
case $size in
3 | 5) ;;
*)
	echo "usage: $0 [3|5]" >&2
	exit 2
	;;
esac
data=${DATA:-${TMPDIR:-/tmp}/wilson-cluster}
base=${BASE_PORT:-8000}

cd "$(dirname "$0")/.."
mkdir -p "$data"
go build -o "$data/kvnode" ./cmd/kvnode

peers=""
for i in $(seq 1 "$size"); do
	peers="$peers${peers:+,}$i=127.0.0.1:$((base + i))"
done

# Stop every server however the script ends; each shuts down gracefully
# on SIGTERM.
pids=""
trap 'kill $pids 2>/dev/null || true; wait' EXIT
trap 'exit 130' INT TERM
for i in $(seq 1 "$size"); do
	"$data/kvnode" -id "$i" -peers "$peers" -data "$data/n$i" &
	pids="$pids $!"
done

servers=""
for i in $(seq 1 "$size"); do
	servers="$servers${servers:+,}127.0.0.1:$((base + i))"
done
cat <<EOF
A cluster of $size servers, with data in $data. Ctrl-C stops them all.
  status:  go run ./cmd/kvctl -servers $servers status
  write:   go run ./cmd/kvctl -servers $servers put greeting hello
  read:    go run ./cmd/kvctl -servers $servers get greeting
EOF
wait
