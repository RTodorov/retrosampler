# Harness entry point. Protected by PreToolUse hook (ADR-001 r3).
# Tool versions are pinned in internal/tools/go.mod.
TOOLS_DIR       := $(CURDIR)/.tools
export PATH     := $(TOOLS_DIR):$(PATH)
MODULE          := github.com/rtodorov/retrosampler
LICENSE_HOLDER  := The retrosampler Authors

TOOL_PKGS := \
	go.opentelemetry.io/collector/cmd/mdatagen \
	go.opentelemetry.io/collector/cmd/builder \
	github.com/evilmartians/lefthook/v2 \
	github.com/google/addlicense \
	golang.org/x/vuln/cmd/govulncheck \
	golang.org/x/perf/cmd/benchstat \
	mvdan.cc/gofumpt \
	github.com/daixiang0/gci \
	github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen

.PHONY: install-tools fmt lint test cover generate build e2e testbed golden \
        vuln bench bench-gate bench-baseline license license-check

install-tools:
	mkdir -p $(TOOLS_DIR)
	cd internal/tools && GOBIN=$(TOOLS_DIR) GOFLAGS=-tags=tools go install $(TOOL_PKGS)

fmt:
	golangci-lint fmt

lint:
	golangci-lint run

test:
	go test -race ./...

cover:
	go test -race -coverprofile=coverage.out ./...
	scripts/coverage_floor.sh

generate:
	go generate ./...

build:
	mkdir -p bin
	$(TOOLS_DIR)/builder --config builder-config.yaml
	cp bin/ocb-dist/retrosamplercol bin/retrosamplercol

e2e: build
	scripts/e2e.sh

testbed:
	@echo "testbed scenario not implemented yet (ADR-004 r3 floors pending)" >&2; exit 1

golden:
	@echo "regenerate goldens per package: go test <pkg> -update" >&2; exit 1

vuln:
	$(TOOLS_DIR)/govulncheck ./...

bench:
	go test -run '^$$' -bench '^(BenchmarkIngest|BenchmarkKeepFlush|BenchmarkExpiry)$$' \
	  -benchmem -count=10 ./... | tee bench-new.txt

bench-gate: bench
	scripts/bench_gate.sh compare

bench-baseline: bench
	scripts/bench_gate.sh baseline

license:
	addlicense -c "$(LICENSE_HOLDER)" -l apache -s=only $$(find . -name '*.go' -not -path './.tools/*' -not -path './bin/*')

license-check:
	addlicense -check -c "$(LICENSE_HOLDER)" -l apache -s=only $$(find . -name '*.go' -not -path './.tools/*' -not -path './bin/*')
