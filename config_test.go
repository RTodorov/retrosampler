// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		ok   bool
	}{
		{"complete", func(*Config) {}, true},
		{"zero window", func(c *Config) { c.Window = 0 }, false},
		{"negative window", func(c *Config) { c.Window = -time.Second }, false},
		{"tiny segment", func(c *Config) { c.SegmentSize = 1 << 10 }, false},
		{"segment at 1 GiB bound", func(c *Config) { c.SegmentSize = 1 << 30 }, true},
		{"segment above 1 GiB bound", func(c *Config) { c.SegmentSize = 1<<30 + 1 }, false},
		{"negative shards", func(c *Config) { c.Shards = -1 }, false},
		{"no storage dir", func(c *Config) { c.StorageDir = "" }, false},
		{"no budget", func(c *Config) { c.DiskBudget = 0 }, false},
		{"negative budget", func(c *Config) { c.DiskBudget = -1 }, false},
		{"watermark zero", func(c *Config) { c.WatermarkPct = 0 }, false},
		{"watermark over 100", func(c *Config) { c.WatermarkPct = 101 }, false},
		{"floor zero", func(c *Config) { c.WindowFloor = 0 }, false},
		{"floor at window", func(c *Config) { c.WindowFloor = c.Window }, false},
		{"keep_on_error off", func(c *Config) { c.KeepOnError = false }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validatableConfig(t)
			tc.mut(cfg)
			if tc.ok {
				assert.NoError(t, cfg.Validate())
			} else {
				assert.Error(t, cfg.Validate())
			}
		})
	}
}
