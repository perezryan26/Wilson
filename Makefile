# go.mod's go and toolchain lines pin the compiler, and golangci-lint is
# pinned here and built on first use, so Go is the only prerequisite.
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

# Sizes for every push; heavier runs override them, e.g. make sim SEEDS=20000.
SEEDS ?= 1000
FUZZTIME ?= 60s

.PHONY: check fmt vet lint test sim fuzz

# check is everything CI runs on every push.
check: fmt vet lint test sim fuzz

fmt:
	@dirs=$$(go list -f '{{.Dir}}' ./...) && out=$$(gofmt -l $$dirs) && \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

lint:
	$(GOLANGCI_LINT) run

test:
	go test -race ./...

sim:
	go test ./internal/sim -run TestRandomized -seeds=$(SEEDS)

fuzz:
	FUZZTIME=$(FUZZTIME) ./scripts/fuzz.sh
