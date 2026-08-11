// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package retrosampler buffers marshaled spans on local disk and
// retroactively samples whole traces on locally detected keep conditions.
package retrosampler // import "github.com/rtodorov/retrosampler"
