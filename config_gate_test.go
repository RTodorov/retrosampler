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
		"bus": map[string]any{
			"type": "nats",
			"nats": map[string]any{
				"url":        "nats://fixture-host:4222",
				"mode":       "at_most_once",
				"subject":    "fixture.keeps",
				"stream":     "fixture-keeps",
				"creds_file": "/fixture/nats.creds",
			},
		},
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
		if !assert.False(t, reflect.DeepEqual(dv.Field(i).Interface(), fv.Field(i).Interface()),
			"%s: the fixture value equals the default, so the load proves nothing about it", f.Name) {
			continue
		}
		descendSubBlock(t, fv.Field(i), f.Name)
	}
	require.NotEmpty(t, full.Policies)
	exercisesEveryField(t, reflect.ValueOf(full.Policies[0]), "Policies[0]")
}

// exercisesEveryField requires every exported field of a loaded
// sub-block to carry a non-zero value. The differs-from-default check
// above cannot reach inside one: a nil-by-default pointer differs from
// its default the moment the fixture makes it non-nil, whatever — or
// however little — the block behind it holds.
func exercisesEveryField(t *testing.T, v reflect.Value, path string) {
	t.Helper()
	for i := range v.NumField() {
		f := v.Type().Field(i)
		if !f.IsExported() {
			continue
		}
		name := path + "." + f.Name
		if !assert.False(t, v.Field(i).IsZero(), "%s: unexercised by the fixture", name) {
			continue
		}
		descendSubBlock(t, v.Field(i), name)
	}
}

// descendSubBlock recurses when v holds a struct, behind a pointer or
// not, so a sub-block nested inside another one is walked to the
// leaves and neither spelling of "sub-block" escapes the requirement.
func descendSubBlock(t *testing.T, v reflect.Value, path string) {
	t.Helper()
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		exercisesEveryField(t, v, path)
	}
}

// configStructs collects the struct types the config surface is built
// from, reaching them through the pointer and slice fields that carry
// sub-blocks. Derived rather than listed: a list is a manifest to
// forget, and a sub-block missing from it is scanned by nothing.
func configStructs(typ reflect.Type, seen map[reflect.Type]bool) []reflect.Type {
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || seen[typ] {
		return nil
	}
	seen[typ] = true
	out := []reflect.Type{typ}
	for i := range typ.NumField() {
		if f := typ.Field(i); f.IsExported() {
			out = append(out, configStructs(f.Type, seen)...)
		}
	}
	return out
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
	for _, typ := range configStructs(reflect.TypeFor[Config](), map[reflect.Type]bool{}) {
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
