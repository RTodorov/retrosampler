// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/detect"
)

// validatableConfig is the default config plus the two now-required
// buffer fields, so a table row's mutation is the only thing under test.
func validatableConfig(t *testing.T) *Config {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = 1 << 30
	return cfg
}

func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Empty(t, cfg.StorageDir)
	assert.Equal(t, 5*time.Minute, cfg.Window)
	assert.Equal(t, 32<<20, cfg.SegmentSize)
	assert.Zero(t, cfg.Shards)
	assert.Zero(t, cfg.DiskBudget)
	assert.Equal(t, 80, cfg.WatermarkPct)
	assert.Equal(t, time.Minute, cfg.WindowFloor)
	assert.True(t, cfg.KeepOnError)
	assert.Zero(t, cfg.SpanLatencyThreshold)
	assert.Zero(t, cfg.TraceLatencyThreshold)
	assert.Zero(t, cfg.TraceAgeThreshold)
	assert.Zero(t, cfg.BaselineRate)
	assert.Nil(t, cfg.Policies)
	assert.Equal(t, "baggage.t0", cfg.T0Attribute)
	assert.Equal(t, "baggage.elapsed_ms", cfg.ElapsedMSAttribute)
	assert.Error(t, cfg.Validate(),
		"the defaults alone are incomplete: buffering is not optional")
}

func TestValidateRequiresStorageDirAndBudget(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.ErrorContains(t, cfg.Validate(), "storage_dir",
		"a sampler that cannot buffer cannot sample: empty storage_dir is a config error")
	cfg.StorageDir = "/tmp/x"
	require.ErrorContains(t, cfg.Validate(), "disk_budget")
	cfg.DiskBudget = 1 << 30
	require.NoError(t, cfg.Validate())
}

func TestDefaultKeepOnErrorIsTrue(t *testing.T) {
	assert.True(t, createDefaultConfig().(*Config).KeepOnError,
		"the headline condition is not opt-in")
}

func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		// wantErr is the substring the error must carry, which pins
		// down which check fired; empty means the row must validate.
		wantErr string
	}{
		{"complete", func(*Config) {}, ""},
		{"zero window", func(c *Config) { c.Window = 0 }, "window must be positive"},
		{"negative window", func(c *Config) { c.Window = -time.Second }, "window must be positive"},
		{"tiny segment", func(c *Config) { c.SegmentSize = 1 << 10 }, "segment_size must be at least 1 MiB"},
		{"segment at 1 GiB bound", func(c *Config) { c.SegmentSize = 1 << 30 }, ""},
		{"segment above 1 GiB bound", func(c *Config) { c.SegmentSize = 1<<30 + 1 }, "segment_size must be at most 1 GiB"},
		{"negative shards", func(c *Config) { c.Shards = -1 }, "shards must be non-negative"},
		{"no storage dir", func(c *Config) { c.StorageDir = "" }, "storage_dir is required"},
		{"no budget", func(c *Config) { c.DiskBudget = 0 }, "disk_budget is required"},
		{"negative budget", func(c *Config) { c.DiskBudget = -1 }, "disk_budget is required"},
		{"watermark zero", func(c *Config) { c.WatermarkPct = 0 }, "watermark_pct must be in (0, 100]"},
		{"watermark over 100", func(c *Config) { c.WatermarkPct = 101 }, "watermark_pct must be in (0, 100]"},
		{"floor zero", func(c *Config) { c.WindowFloor = 0 }, "window_floor must be positive and below window"},
		{"floor at window", func(c *Config) { c.WindowFloor = c.Window }, "window_floor must be positive and below window"},
		{"keep_on_error off", func(c *Config) { c.KeepOnError = false }, ""},
		{
			"negative span latency", func(c *Config) { c.SpanLatencyThreshold = -time.Second },
			"span_latency_threshold must be non-negative",
		},
		{
			"negative trace latency", func(c *Config) { c.TraceLatencyThreshold = -time.Second },
			"trace_latency_threshold must be non-negative",
		},
		{
			"negative trace age", func(c *Config) { c.TraceAgeThreshold = -time.Second },
			"trace_age_threshold must be non-negative",
		},
		{
			"baseline rate above one", func(c *Config) { c.BaselineRate = 1.1 },
			"baseline_rate must be in [0, 1]",
		},
		{
			"baseline rate negative", func(c *Config) { c.BaselineRate = -0.1 },
			"baseline_rate must be in [0, 1]",
		},
		{"baseline rate at one", func(c *Config) { c.BaselineRate = 1 }, ""},
		{
			// NaN passes both ordered comparisons, so an un-negated
			// range test would let it through to a silent disable.
			"baseline rate NaN", func(c *Config) { c.BaselineRate = math.NaN() },
			"baseline_rate must be in [0, 1]",
		},
		{"age threshold without t0 attribute", func(c *Config) {
			c.TraceAgeThreshold = time.Minute
			c.T0Attribute = ""
		}, "t0_attribute"},
		{"latency threshold without elapsed attribute", func(c *Config) {
			c.TraceLatencyThreshold = time.Minute
			c.ElapsedMSAttribute = ""
		}, "elapsed_ms_attribute"},
		{"unparseable policy", func(c *Config) {
			c.Policies = []PolicyConfig{{Name: "bad", Condition: "span.name =="}}
		}, "bad"},
		{"duplicate policy names", func(c *Config) {
			c.Policies = []PolicyConfig{
				{Name: "a", Condition: `span.name == "x"`},
				{Name: "a", Condition: `span.name == "y"`},
			}
		}, "duplicate"},
		{"sub-millisecond thresholds", func(c *Config) {
			c.TraceLatencyThreshold = time.Microsecond
			c.TraceAgeThreshold = time.Microsecond
		}, ""},
		{"policies and thresholds together", func(c *Config) {
			c.SpanLatencyThreshold = 2 * time.Second
			c.BaselineRate = 0.01
			c.Policies = []PolicyConfig{{Name: "slow", Condition: `span.name == "x"`}}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validatableConfig(t)
			tc.mut(cfg)
			if tc.wantErr == "" {
				assert.NoError(t, cfg.Validate())
			} else {
				assert.ErrorContains(t, cfg.Validate(), tc.wantErr)
			}
		})
	}
}

func TestValidatePolicyErrorNamesTheField(t *testing.T) {
	cfg := validatableConfig(t)
	cfg.Policies = []PolicyConfig{{Name: "bad", Condition: "span.name =="}}
	err := cfg.Validate()
	require.ErrorContains(t, err, "policies:",
		"an operator fixes the config field, not the internal package")
	require.ErrorContains(t, err, "bad", "the failing policy stays identifiable")
}

func TestPolicyList(t *testing.T) {
	assert.Empty(t, policyList(nil))
	assert.Equal(t, []detect.Policy{
		{Name: "a", Condition: `span.name == "x"`},
		{Name: "b", Condition: `span.name == "y"`},
	}, policyList([]PolicyConfig{
		{Name: "a", Condition: `span.name == "x"`},
		{Name: "b", Condition: `span.name == "y"`},
	}), "config order is policy-evaluation order and telemetry index order")
}
