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
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		ok   bool
	}{
		{"defaults", func(*Config) {}, true},
		{"with dir", func(c *Config) { c.StorageDir = t.TempDir() }, true},
		{"zero window", func(c *Config) { c.Window = 0 }, false},
		{"negative window", func(c *Config) { c.Window = -time.Second }, false},
		{"tiny segment", func(c *Config) { c.SegmentSize = 1 << 10 }, false},
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
