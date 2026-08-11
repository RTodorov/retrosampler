// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_DefaultIsValid(t *testing.T) {
	t.Parallel()
	cfg := &Config{}
	require.NoError(t, cfg.Validate())
}
