// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

// gateFile is excluded from the reference scan: the gate reaching every
// field by reflection must not satisfy its own requirement.
const gateFile = "config_gate_test.go"

// fullSurface is the fixture: every field set to a valid value that
// differs from its default, spelled as the YAML parser would deliver it
// (durations as strings), so the decode hooks under test are the ones a
// collector config file exercises. Hand-authored on purpose — a
// generated fixture would satisfy the walk below by construction.
func fullSurface() map[string]any {
	return map[string]any{
		"storage_dir":             "/tmp/retrosampler-gate",
		"window":                  "4m",
		"keep_on_error":           false,
		"segment_size":            2097152,
		"shards":                  2,
		"disk_budget":             1073741824,
		"watermark_pct":           75,
		"window_floor":            "30s",
		"span_latency_threshold":  "750ms",
		"trace_latency_threshold": "3s",
		"trace_age_threshold":     "2m",
		"baseline_rate":           0.01,
		"policies": []any{map[string]any{
			"name":      "gate",
			"condition": `span.name == "gate"`,
		}},
		"t0_attribute":         "ctx.t0",
		"elapsed_ms_attribute": "ctx.elapsed_ms",
	}
}

// loadFullConfig unmarshals the fixture over the defaults, the way the
// collector does. Strict unmarshal makes an unknown key — a fixture
// entry whose mapstructure tag went stale — a load error.
func loadFullConfig(t *testing.T) *Config {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, confmap.NewFromStringMap(fullSurface()).Unmarshal(cfg))
	return cfg
}

func TestConfigFixtureValidates(t *testing.T) {
	require.NoError(t, loadFullConfig(t).Validate())
}

// TestConfigFixtureExercisesEveryField walks Config by reflection and
// requires every exported field to load a value different from its
// default (ADR-005 r6). A field added without a fixture entry sits at
// its default and fails here; no manifest to forget.
func TestConfigFixtureExercisesEveryField(t *testing.T) {
	dv := reflect.ValueOf(*createDefaultConfig().(*Config))
	full := loadFullConfig(t)
	fv := reflect.ValueOf(*full)
	for i := range dv.NumField() {
		f := dv.Type().Field(i)
		if !f.IsExported() {
			continue
		}
		assert.False(t, reflect.DeepEqual(dv.Field(i).Interface(), fv.Field(i).Interface()),
			"%s: the fixture value equals the default, so the load proves nothing about it", f.Name)
	}
	require.NotEmpty(t, full.Policies)
	pv := reflect.ValueOf(full.Policies[0])
	for i := range pv.NumField() {
		f := pv.Type().Field(i)
		if !f.IsExported() {
			continue
		}
		assert.False(t, pv.Field(i).IsZero(), "Policies[0].%s: unexercised by the fixture", f.Name)
	}
}

// TestConfigFieldsReferencedByTests requires each field's identifier to
// appear in some root test other than this gate: a field no test needs
// is a field to delete (ADR-005 r6).
func TestConfigFieldsReferencedByTests(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var src strings.Builder
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), "_test.go") || e.Name() == gateFile {
			continue
		}
		b, err := os.ReadFile(e.Name())
		require.NoError(t, err)
		src.Write(b)
	}
	all := src.String()
	for _, typ := range []reflect.Type{reflect.TypeFor[Config](), reflect.TypeFor[PolicyConfig]()} {
		for i := range typ.NumField() {
			f := typ.Field(i)
			if !f.IsExported() {
				continue
			}
			assert.True(t, regexp.MustCompile(`\b`+f.Name+`\b`).MatchString(all),
				"%s: no test outside this gate references the field", f.Name)
		}
	}
}
