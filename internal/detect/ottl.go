// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package detect

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/bus"
)

// compiledPolicy pairs one parsed condition with its counters. matches
// and evalErrors are addressed by policy index everywhere — the name is
// resolved only at telemetry collect time.
type compiledPolicy struct {
	name       string
	cond       *ottl.Condition[*ottlspan.TransformContext]
	matches    atomic.Uint64
	evalErrors atomic.Uint64
	warned     atomic.Bool
}

// compilePolicies parses every condition with the given settings. Names
// must be unique and non-empty: they key telemetry attribution and the
// per-policy-chain benchmark baselines (ADR-004 r2 / ADR-008 r2).
func compilePolicies(policies []Policy, set component.TelemetrySettings) ([]*compiledPolicy, error) {
	if len(policies) == 0 {
		return nil, nil
	}
	parser, err := ottlspan.NewParser(
		ottlfuncs.StandardConverters[*ottlspan.TransformContext](), set,
	)
	if err != nil {
		return nil, fmt.Errorf("detect: ottl parser: %w", err)
	}
	seen := make(map[string]struct{}, len(policies))
	out := make([]*compiledPolicy, 0, len(policies))
	for _, p := range policies {
		if p.Name == "" {
			return nil, errors.New("detect: policy name must not be empty")
		}
		if _, dup := seen[p.Name]; dup {
			return nil, fmt.Errorf("detect: duplicate policy name %q", p.Name)
		}
		seen[p.Name] = struct{}{}
		cond, err := parser.ParseCondition(p.Condition)
		if err != nil {
			return nil, fmt.Errorf("detect: policy %q: %w", p.Name, err)
		}
		out = append(out, &compiledPolicy{name: p.Name, cond: cond})
	}
	return out, nil
}

// CheckPolicies is the config-load-time half of the parse-twice design:
// it compiles against throwaway nop settings so a bad expression fails
// Config.Validate loudly, long before start().
func CheckPolicies(policies []Policy) error {
	_, err := compilePolicies(policies, nopSettings())
	return err
}

// nopSettings builds the minimal TelemetrySettings the parser needs
// outside a running component. componenttest is test-only by policy, so
// this is assembled by hand.
func nopSettings() component.TelemetrySettings {
	return component.TelemetrySettings{
		Logger:         zap.NewNop(),
		MeterProvider:  mnoop.NewMeterProvider(),
		TracerProvider: tnoop.NewTracerProvider(),
	}
}

// evalPolicies runs the OTTL tail in config order, first match wins. An
// eval error is ignore-and-count (span does not match, counter moves,
// warn once per policy): propagating would turn one poison span into an
// endlessly retried batch. The pooled TransformContext is shared across
// the walk and must be closed on every path.
func (d *Detector) evalPolicies(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sp ptrace.Span) byte {
	if len(d.policies) == 0 {
		return 0
	}
	tCtx := ottlspan.NewTransformContextPtr(rs, ss, sp)
	defer tCtx.Close()
	for _, p := range d.policies {
		match, err := p.cond.Eval(context.Background(), tCtx)
		if err != nil {
			p.evalErrors.Add(1)
			if p.warned.CompareAndSwap(false, true) {
				d.logger.Warn("keep policy evaluation error; counting and continuing",
					zap.String("policy", p.name), zap.Error(err))
			}
			continue
		}
		if match {
			p.matches.Add(1)
			return d.hit(bus.ReasonPolicy)
		}
	}
	return 0
}

// PolicyNames returns the configured names in policy-index order.
func (d *Detector) PolicyNames() []string {
	names := make([]string, len(d.policies))
	for i, p := range d.policies {
		names[i] = p.name
	}
	return names
}

// PolicyMatches reports policy i's match count; out-of-range reads 0.
func (d *Detector) PolicyMatches(i int) uint64 {
	if i < 0 || i >= len(d.policies) {
		return 0
	}
	return d.policies[i].matches.Load()
}

// PolicyEvalErrors reports policy i's eval-error count.
func (d *Detector) PolicyEvalErrors(i int) uint64 {
	if i < 0 || i >= len(d.policies) {
		return 0
	}
	return d.policies[i].evalErrors.Load()
}
