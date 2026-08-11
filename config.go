// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

// Config defines configuration for the retrosampler processor.
type Config struct{}

// Validate checks that the configuration is usable.
func (cfg *Config) Validate() error {
	return nil
}
