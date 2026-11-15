#!/bin/sh
# Runs MIT 6.5840's Lab 3 Raft tests (src/raft1, parts 3A-3D) against this
# module's raft core, through an adapter kept in a separate private
# repository, because the course asks that solutions not be published
# (docs/TESTING.md). MITLAB_ADAPTER names the adapter's checkout, whose
# raft1/ replaces the course's raft1/raft.go.
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
set -eu

repo=git://g.csail.mit.edu/6.5840-golabs-2026
commit=513cac0aef010750fc74688da8a395974a83cf7a

usage() {
	echo "usage: MITLAB_ADAPTER=dir $0 [-n runs] [-p parallel] [3|3A|3B|3C|3D]" >&2
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
case $part in
3 | 3A | 3B | 3C | 3D) ;;
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
cp "$adapter"/raft1/*.go "$course/src/raft1/"

cd "$course/src"
go mod edit -replace "github.com/perezryan26/wilson=$root"
# go get also raises the course module's go line to this module's if it
# is older, as Go requires.
go get github.com/perezryan26/wilson@v0.0.0
# Each peer runs in its own raft1d process, so that is where the core
# runs and where the race detector must be.
go build -race -o main/raft1d main/raft1d.go

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
	if (cd raft1 && go test -race -vet=off -count=1 -v -timeout 30m -run "$part") >"$log" 2>&1 &&
		! grep -q 'DATA RACE' "$log"; then
		echo "run $1: ok, $(tail -n 1 "$log" | awk '{print $NF}')"
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

# Every test's pass count and longest time, then the suite's.
cat "$logs"/run-*.log | awk -v runs="$runs" '
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
/^(ok|FAIL)[ \t]+6\.5840\/raft1[ \t]/ {
	secs = $3
	sub(/s$/, "", secs)
	if ($1 == "ok") ok++
	if (secs + 0 > slowest) slowest = secs + 0
}
END {
	for (k = 1; k <= n; k++)
		printf "%-34s %3d/%d passed, longest %6.1fs\n", names[k], passed[names[k]], runs, longest[names[k]]
	printf "%d/%d runs passed go test, slowest %.1fs\n", ok, runs, slowest
}'
! grep -l 'DATA RACE\|^FAIL' "$logs"/run-*.log
