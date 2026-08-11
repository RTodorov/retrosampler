// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Empty(t, cfg.StorageDir)
	assert.Equal(t, 5*time.Minute, cfg.Window)
	assert.Equal(t, 32<<20, cfg.SegmentSize)
	assert.Zero(t, cfg.Shards)
	assert.Zero(t, cfg.DiskBudget)
	assert.Equal(t, 80, cfg.WatermarkPct)
	assert.Equal(t, time.Minute, cfg.WindowFloor)
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		ok   bool
	}{
		{"defaults", func(*Config) {}, true},
		{"zero window", func(c *Config) { c.Window = 0 }, false},
		{"negative window", func(c *Config) { c.Window = -time.Second }, false},
		{"tiny segment", func(c *Config) { c.SegmentSize = 1 << 10 }, false},
		{"segment at 1 GiB bound", func(c *Config) { c.SegmentSize = 1 << 30 }, true},
		{"segment above 1 GiB bound", func(c *Config) { c.SegmentSize = 1<<30 + 1 }, false},
		{"negative shards", func(c *Config) { c.Shards = -1 }, false},
		{"dir without budget", func(c *Config) { c.StorageDir = t.TempDir() }, false},
		{"dir with budget", func(c *Config) { c.StorageDir = t.TempDir(); c.DiskBudget = 1 << 30 }, true},
		{"watermark zero", func(c *Config) { c.WatermarkPct = 0 }, false},
		{"watermark over 100", func(c *Config) { c.WatermarkPct = 101 }, false},
		{"floor zero", func(c *Config) { c.WindowFloor = 0 }, false},
		{"floor at window", func(c *Config) { c.WindowFloor = c.Window }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			tc.mut(cfg)
			if tc.ok {
				assert.NoError(t, cfg.Validate())
			} else {
				assert.Error(t, cfg.Validate())
			}
		})
	}
}
