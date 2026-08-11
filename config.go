// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"errors"
	"time"
)

// Config defines configuration for the retrosampler processor.
type Config struct {
	// StorageDir is the buffer segment directory. Empty disables
	// buffering (passthrough only) — stage-1 shadow mode is opt-in.
	StorageDir string `mapstructure:"storage_dir"`
	// Window is the retention window W (ADR-006).
	Window time.Duration `mapstructure:"window"`
	// SegmentSize is the segment roll threshold in bytes.
	SegmentSize int `mapstructure:"segment_size"`
}

// Validate checks that the configuration is usable.
func (cfg *Config) Validate() error {
	if cfg.Window <= 0 {
		return errors.New("window must be positive")
	}
	if cfg.SegmentSize < 1<<20 {
		return errors.New("segment_size must be at least 1 MiB")
	}
	return nil
}
