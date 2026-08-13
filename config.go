// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"errors"
	"fmt"
	"time"

	"github.com/rtodorov/retrosampler/internal/detect"
)

// Config defines configuration for the retrosampler processor.
type Config struct {
	// StorageDir is the buffer segment directory. Required: the
	// processor emits only what it has buffered, so there is no
	// meaningful unbuffered mode.
	StorageDir string `mapstructure:"storage_dir"`
	// Window is the retention window W (ADR-006).
	Window time.Duration `mapstructure:"window"`
	// SegmentSize is the segment roll threshold in bytes.
	SegmentSize int `mapstructure:"segment_size"`
	// Shards caps the shard-worker count; the effective count is
	// min(GOMAXPROCS, shards), 0 meaning GOMAXPROCS (ADR-007 r4).
	Shards int `mapstructure:"shards"`
	// DiskBudget is the total buffer disk budget in bytes across all
	// shards (ADR-006); the overload ladder acts on it (ADR-007 r5).
	// Required.
	//
	// It is a target, soft in two directions. Above: only finalized
	// segments are reclaimable, so the shards x segment_size bytes held
	// by the active segments are a permanent floor — a budget whose
	// watermark does not clear it is rejected at startup. Below: usage
	// is sampled on each shard's tick, not on every roll, so the figure
	// the ladder acts on lags reality by up to one tick (~1s) per shard.
	DiskBudget int64 `mapstructure:"disk_budget"`
	// WatermarkPct is the disk-budget percentage above which shards
	// early-expire their oldest segments (ADR-007 r5).
	WatermarkPct int `mapstructure:"watermark_pct"`
	// WindowFloor is the minimum effective window early expiry may
	// leave; at the floor, ingest sheds instead (ADR-007 r5).
	WindowFloor time.Duration `mapstructure:"window_floor"`
	// KeepOnError keeps any trace containing a span with error status —
	// the keep-on-error built-in (ADR-009), evaluated per span at
	// ingest, guaranteed alloc-free. Default true.
	KeepOnError bool `mapstructure:"keep_on_error"`
	// SpanLatencyThreshold keeps any trace containing a span longer than
	// this. 0 disables. Guaranteed alloc-free — for span latency, use
	// this setting, never an OTTL arithmetic expression (ADR-008).
	SpanLatencyThreshold time.Duration `mapstructure:"span_latency_threshold"`
	// TraceLatencyThreshold keeps a trace whose accumulated baggage
	// elapsed_ms exceeds it. 0 disables. Skew-free, blind to inter-hop
	// gaps (ADR-008 r1).
	TraceLatencyThreshold time.Duration `mapstructure:"trace_latency_threshold"`
	// TraceAgeThreshold keeps a trace older than now−T0 (baggage). 0
	// disables. Catches queue-wait latency; wall-clock skew accepted.
	TraceAgeThreshold time.Duration `mapstructure:"trace_age_threshold"`
	// BaselineRate is the deterministic head-sample fraction in [0, 1];
	// identical verdict on every instance, never broadcast. 0 disables.
	BaselineRate float64 `mapstructure:"baseline_rate"`
	// Policies are named OTTL span conditions, OR-composed: any match
	// keeps the whole trace. ~30–110 ns and 0–3 allocs per span per
	// condition, expression-shape-dependent — never guaranteed
	// alloc-free (ADR-008 r2).
	Policies []PolicyConfig `mapstructure:"policies"`
	// T0Attribute is the span attribute carrying baggage T0 as unix
	// epoch milliseconds (int or decimal string).
	T0Attribute string `mapstructure:"t0_attribute"`
	// ElapsedMSAttribute is the span attribute carrying baggage
	// elapsed_ms (int or decimal string).
	ElapsedMSAttribute string `mapstructure:"elapsed_ms_attribute"`
	// Bus selects the keep-notification transport. Absent means the
	// in-process Loopback (single-instance mode, ADR-009 r4). The block
	// is discriminated by Type so a future backend is additive, never a
	// breaking change (ADR-011 r1).
	Bus *BusConfig `mapstructure:"bus"`
}

// BusConfig is the discriminated bus block. Type names the backend;
// exactly the matching sub-block must be set.
type BusConfig struct {
	// Type is the backend: "nats" is the only value this stage.
	Type string `mapstructure:"type"`
	// NATS configures the NATS client (required when type is "nats").
	NATS *NATSConfig `mapstructure:"nats"`
}

// NATSConfig configures the natsbus client (ADR-011 r1-r2).
type NATSConfig struct {
	// URL is the NATS server address, e.g. nats://host:4222. TLS rides
	// the scheme (tls://); auth rides URL userinfo or CredsFile.
	URL string `mapstructure:"url"`
	// Mode is the ADR-008 r6 operator choice: "durable" (JetStream,
	// default — reconnects replay missed keeps) or "at_most_once"
	// (core pub/sub — a reconnect gap silently loses fragments of kept
	// traces; counted, documented trade).
	Mode string `mapstructure:"mode"`
	// Subject is the keep broadcast subject.
	Subject string `mapstructure:"subject"`
	// Stream is the JetStream stream name (durable mode only).
	Stream string `mapstructure:"stream"`
	// CredsFile is an optional NATS credentials file path.
	CredsFile string `mapstructure:"creds_file"`
}

const (
	busModeDurable    = "durable"
	busModeAtMostOnce = "at_most_once"
)

// withDefaults fills the optional fields; URL stays required.
func (n *NATSConfig) withDefaults() NATSConfig {
	out := *n
	if out.Mode == "" {
		out.Mode = busModeDurable
	}
	if out.Subject == "" {
		out.Subject = "retrosampler.keeps"
	}
	if out.Stream == "" {
		out.Stream = "retrosampler-keeps"
	}
	return out
}

// validBusToken refuses NATS wildcard/space characters in a subject or
// stream name; empty is allowed (defaults fill it).
func validBusToken(s string) bool {
	for _, r := range s {
		switch r {
		case '*', '>', ' ', '\t':
			return false
		}
	}
	return true
}

// PolicyConfig is one named OTTL span condition. Names must be unique
// and non-empty: they key the per-policy telemetry attribution.
type PolicyConfig struct {
	Name      string `mapstructure:"name"`
	Condition string `mapstructure:"condition"`
}

// policyList maps the config surface onto detect's policy type, in
// config order — which is evaluation order and telemetry index order.
func policyList(ps []PolicyConfig) []detect.Policy {
	out := make([]detect.Policy, len(ps))
	for i, p := range ps {
		out[i] = detect.Policy{Name: p.Name, Condition: p.Condition}
	}
	return out
}

// Validate checks that the configuration is usable.
func (cfg *Config) Validate() error {
	if cfg.StorageDir == "" {
		return errors.New("storage_dir is required (a retroactive sampler that cannot buffer cannot sample)")
	}
	if cfg.DiskBudget <= 0 {
		return errors.New("disk_budget is required")
	}
	if cfg.Window <= 0 {
		return errors.New("window must be positive")
	}
	if cfg.SegmentSize < 1<<20 {
		return errors.New("segment_size must be at least 1 MiB")
	}
	// Record offsets are u32 (internal/buffer.lenU32): a segment materially
	// larger than 1 GiB would let offsets wrap and silently corrupt later
	// records on recovery.
	if cfg.SegmentSize > 1<<30 {
		return errors.New("segment_size must be at most 1 GiB")
	}
	if cfg.Shards < 0 {
		return errors.New("shards must be non-negative")
	}
	if cfg.WatermarkPct <= 0 || cfg.WatermarkPct > 100 {
		return errors.New("watermark_pct must be in (0, 100]")
	}
	if cfg.WindowFloor <= 0 || cfg.WindowFloor >= cfg.Window {
		return errors.New("window_floor must be positive and below window")
	}
	if cfg.SpanLatencyThreshold < 0 {
		return errors.New("span_latency_threshold must be non-negative (0 disables)")
	}
	if cfg.TraceLatencyThreshold < 0 {
		return errors.New("trace_latency_threshold must be non-negative (0 disables)")
	}
	if cfg.TraceAgeThreshold < 0 {
		return errors.New("trace_age_threshold must be non-negative (0 disables)")
	}
	// Nothing downstream catches an out-of-range rate: detect.Build's >0
	// gate silently disables a negative or NaN one, above 1 clears the
	// 56-bit id space and keeps every trace, and 256 or more makes the
	// float64->uint64 conversion implementation-defined. The negated form
	// is what rejects NaN, which passes both ordered comparisons.
	if !(cfg.BaselineRate >= 0 && cfg.BaselineRate <= 1) {
		return errors.New("baseline_rate must be in [0, 1] (above 1 keeps every trace; negative or NaN silently disables the baseline)")
	}
	if cfg.TraceAgeThreshold > 0 && cfg.T0Attribute == "" {
		return errors.New("t0_attribute is required when trace_age_threshold is set")
	}
	if cfg.TraceLatencyThreshold > 0 && cfg.ElapsedMSAttribute == "" {
		return errors.New("elapsed_ms_attribute is required when trace_latency_threshold is set")
	}
	if cfg.Bus != nil {
		if cfg.Bus.Type != "nats" {
			return fmt.Errorf("bus.type %q is not supported (this stage: nats; ADR-011 r1)", cfg.Bus.Type)
		}
		if cfg.Bus.NATS == nil {
			return errors.New("bus.nats is required when bus.type is nats")
		}
		n := cfg.Bus.NATS
		if n.URL == "" {
			return errors.New("bus.nats.url is required")
		}
		if n.Mode != "" && n.Mode != busModeDurable && n.Mode != busModeAtMostOnce {
			return fmt.Errorf("bus.nats.mode %q must be %q or %q", n.Mode, busModeDurable, busModeAtMostOnce)
		}
		if !validBusToken(n.Subject) {
			return errors.New("bus.nats.subject must not contain wildcards or spaces")
		}
		if !validBusToken(n.Stream) {
			return errors.New("bus.nats.stream must not contain wildcards or spaces")
		}
	}
	if err := detect.CheckPolicies(policyList(cfg.Policies)); err != nil {
		return fmt.Errorf("policies: %w", err)
	}
	return nil
}
