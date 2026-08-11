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
	// Shards caps the shard-worker count; the effective count is
	// min(GOMAXPROCS, shards), 0 meaning GOMAXPROCS (ADR-007 r4).
	Shards int `mapstructure:"shards"`
	// DiskBudget is the total buffer disk budget in bytes across all
	// shards (ADR-006); the overload ladder acts on it (ADR-007 r5).
	// Required whenever storage_dir is set.
	DiskBudget int64 `mapstructure:"disk_budget"`
	// WatermarkPct is the disk-budget percentage above which shards
	// early-expire their oldest segments (ADR-007 r5).
	WatermarkPct int `mapstructure:"watermark_pct"`
	// WindowFloor is the minimum effective window early expiry may
	// leave; at the floor, ingest sheds instead (ADR-007 r5).
	WindowFloor time.Duration `mapstructure:"window_floor"`
}

// Validate checks that the configuration is usable.
func (cfg *Config) Validate() error {
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
	if cfg.StorageDir != "" && cfg.DiskBudget <= 0 {
		return errors.New("disk_budget is required when storage_dir is set")
	}
	return nil
}
