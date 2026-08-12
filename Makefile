# Harness entry point. Protected by PreToolUse hook (ADR-001 r3).
# Tool versions are pinned in internal/tools/go.mod.
TOOLS_DIR       := $(CURDIR)/.tools
export PATH     := $(TOOLS_DIR):$(PATH)
MODULE          := github.com/rtodorov/retrosampler
LICENSE_HOLDER  := The retrosampler Authors

GOLANGCI_LINT_V := v2.8.0

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

.PHONY: install-tools fmt lint test cover generate build build-linux e2e \
        e2e-compose testbed golden vuln bench bench-gate bench-baseline \
        license license-check

install-tools:
	mkdir -p $(TOOLS_DIR)
	cd internal/tools && GOBIN=$(TOOLS_DIR) GOFLAGS=-tags=tools GOTOOLCHAIN=auto go install $(TOOL_PKGS)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
	  | sh -s -- -b $(TOOLS_DIR) $(GOLANGCI_LINT_V)

fmt:
	$(TOOLS_DIR)/golangci-lint fmt

lint:
	$(TOOLS_DIR)/golangci-lint run

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

build-linux:
	mkdir -p bin
	GOOS=linux CGO_ENABLED=0 $(TOOLS_DIR)/builder --config builder-config.yaml
	cp bin/ocb-dist/retrosamplercol bin/retrosamplercol-linux

e2e-compose: build-linux
	docker build -f e2e/compose/Dockerfile -t retrosampler-e2e:local bin
	scripts/e2e_compose.sh

testbed:
	@echo "testbed scenario not implemented yet (ADR-004 r3 floors pending)" >&2; exit 1

golden:
	@echo "regenerate goldens per package: go test <pkg> -update" >&2; exit 1

vuln:
	$(TOOLS_DIR)/govulncheck ./...

bench:
	go test -p 1 -run '^$$' -bench '^(BenchmarkIngest|BenchmarkKeepFlush|BenchmarkExpiry|BenchmarkOffer|BenchmarkDecode)$$' \
	  -benchmem -count=10 ./... | tee bench-new.txt

bench-gate: bench
	scripts/bench_gate.sh compare

bench-baseline: bench
	scripts/bench_gate.sh baseline

license:
	addlicense -c "$(LICENSE_HOLDER)" -l apache -s=only $$(find . -name '*.go' -not -path './.tools/*' -not -path './bin/*')

license-check:
	addlicense -check -c "$(LICENSE_HOLDER)" -l apache -s=only $$(find . -name '*.go' -not -path './.tools/*' -not -path './bin/*')
