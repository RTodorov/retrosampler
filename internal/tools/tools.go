// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

//go:build tools

// Package tools pins build tooling versions (ADR-001 stack).
package tools

import (
	_ "github.com/daixiang0/gci"
	_ "github.com/evilmartians/lefthook/v2"
	_ "github.com/google/addlicense"
	_ "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen"
	_ "go.opentelemetry.io/collector/cmd/builder"
	_ "go.opentelemetry.io/collector/cmd/mdatagen"
	_ "golang.org/x/perf/cmd/benchstat"
	_ "golang.org/x/vuln/cmd/govulncheck"
	_ "mvdan.cc/gofumpt"
)
