#!/bin/sh
# Runs MIT 6.5840's Lab 3 Raft tests (src/raft1, parts 3A-3D) and Lab 4
# key-value tests (src/kvraft1/rsm, part 4A; src/kvraft1, parts 4B and 4C;
# TestSnapshot4C is in rsm) against this module's raft core, through an
# adapter kept in a separate private repository, because the course asks
# that solutions not be published (docs/TESTING.md). MITLAB_ADAPTER names
# the adapter's checkout, whose raft1/, kvraft1/, and kvraft1/rsm/ files
# replace the course's.
#
# Each invocation resets the course repository, cloned once into .mitlab/
# (which git ignores), to the pinned commit; copies in the adapter; points
# the course module at this checkout; and runs the part with -race. With
# -n it runs the part that many times, -p at a time, and reports how often
# each test passed and its longest time. Logs go to .mitlab/logs/.
#
#   MITLAB_ADAPTER=../wilson-mitlab scripts/mitlab.sh             # all of Lab 3
#   MITLAB_ADAPTER=../wilson-mitlab scripts/mitlab.sh 3B          # one part
#   MITLAB_ADAPTER=../wilson-mitlab scripts/mitlab.sh -n 20 -p 4  # 20 runs, 4 at a time
#   MITLAB_ADAPTER=../wilson-mitlab scripts/mitlab.sh 4           # all of Lab 4
set -eu

repo=git://g.csail.mit.edu/6.5840-golabs-2026
commit=513cac0aef010750fc74688da8a395974a83cf7a

usage() {
	echo "usage: MITLAB_ADAPTER=dir $0 [-n runs] [-p parallel] [3|3A|3B|3C|3D|4|4A|4B|4C]" >&2
	exit 2
}
runs=1
parallel=1
while getopts n:p: opt; do
	case $opt in
	n) runs=$OPTARG ;;
	p) parallel=$OPTARG ;;
	*) usage ;;
	esac
done
shift $((OPTIND - 1))
part=${1:-3}
# The packages whose tests the part runs, one after another.
case $part in
3 | 3A | 3B | 3C | 3D) pkgs=./raft1 ;;
4A) pkgs=./kvraft1/rsm ;;
4B) pkgs=./kvraft1 ;;
4 | 4C) pkgs="./kvraft1/rsm ./kvraft1" ;;
*) usage ;;
esac
[ -n "${MITLAB_ADAPTER:-}" ] || usage
adapter=$(cd "$MITLAB_ADAPTER" && pwd)

cd "$(dirname "$0")/.."
root=$(pwd)
course=$root/.mitlab/6.5840
if [ ! -d "$course" ]; then
	git clone -q "$repo" "$course"
fi
git -C "$course" cat-file -e "$commit^{commit}" 2>/dev/null || git -C "$course" fetch -q origin
git -C "$course" checkout -q --force --detach "$commit"
git -C "$course" clean -q -fdx
for dir in raft1 kvraft1 kvraft1/rsm; do
	cp "$adapter/$dir"/*.go "$course/src/$dir/"
done

cd "$course/src"
go mod edit -replace "github.com/perezryan26/wilson=$root"
# go get also raises the course module's go line to this module's if it
# is older, as Go requires, and its Porcupine to the version this module
# requires. The replace keeps the course's own Porcupine for Lab 4's
# checks; the adapter imports only the raft package, which needs none.
go get github.com/perezryan26/wilson@v0.0.0
go mod edit -replace github.com/anishathalye/porcupine=github.com/anishathalye/porcupine@v1.0.3
# Each server runs in its own process, so that is where the core runs and
# where the race detector must be.
for daemon in raft1d rsm1d kvraft1d; do
	go build -race -o "main/$daemon" "main/$daemon.go"
done

logs=$root/.mitlab/logs/$(date +%Y%m%d-%H%M%S)
mkdir -p "$logs"
echo "mitlab: course $commit, part $part, $runs run(s), $parallel at a time, logs in $logs"

# run N runs the part once, logging to run-N.log. A data race in a peer
# fails the run even if the tests pass, since peers are separate
# processes whose race reports never reach go test's exit status. Vet is
# off because the raised go line turns on a printf check that the
# course's own test files fail.
run() {
	log=$logs/run-$1.log
	# shellcheck disable=SC2086 # pkgs is a list of packages
	if go test -race -vet=off -count=1 -p 1 -v -timeout 30m -run "$part" $pkgs >"$log" 2>&1 &&
		! grep -q 'DATA RACE' "$log"; then
		echo "run $1: ok, $(awk '/^ok/ { sub(/s$/, "", $3); t += $3 } END { print t "s" }' "$log")"
	else
		echo "run $1: FAIL, see $log"
	fi
}
i=0
while [ "$i" -lt "$runs" ]; do
	j=0
	while [ "$j" -lt "$parallel" ] && [ "$i" -lt "$runs" ]; do
		i=$((i + 1))
		j=$((j + 1))
		run "$i" &
	done
	wait
done

# Every test's pass count and longest time, then the suite's: a run's
# time is the sum of its packages' times.
awk -v runs="$runs" '
/^--- (PASS|FAIL): / {
	name = $3
	secs = $4
	gsub(/[()s]/, "", secs)
	if (!(name in seen)) {
		seen[name] = 1
		names[++n] = name
	}
	if ($2 == "PASS:") passed[name]++
	if (secs + 0 > longest[name]) longest[name] = secs + 0
}
/^(ok|FAIL)[ \t]+6\.5840\// {
	secs = $3
	sub(/s$/, "", secs)
	total[FILENAME] += secs
	if ($1 == "FAIL") failed[FILENAME] = 1
}
END {
	for (k = 1; k <= n; k++)
		printf "%-66s %3d/%d passed, longest %6.1fs\n", names[k], passed[names[k]], runs, longest[names[k]]
	for (f in total) {
		if (!(f in failed)) ok++
		if (total[f] > slowest) slowest = total[f]
	}
	printf "%d/%d runs passed go test, slowest %.1fs\n", ok, runs, slowest
}' "$logs"/run-*.log
! grep -l 'DATA RACE\|^FAIL' "$logs"/run-*.log
