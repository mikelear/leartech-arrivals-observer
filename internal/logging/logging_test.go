package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog/log"
)

// decodeLast returns the last JSON object in the buffer. Used because
// Init installs the global logger which then writes to the writer;
// tests trigger a log call and pluck the emitted line back.
func decodeLast(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatalf("no log line emitted")
	}
	// If multiple lines were emitted, we want the last one.
	if i := strings.LastIndex(line, "\n"); i >= 0 {
		line = line[i+1:]
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("emitted line is not JSON: %v — line=%q", err, line)
	}
	return m
}

// TestInitTo_JSON_StampsAmbientFields verifies the three platform-
// standard fields (service/version/cluster) land on every record
// emitted after Init.
func TestInitTo_JSON_StampsAmbientFields(t *testing.T) {
	var buf bytes.Buffer
	format := InitTo(&buf, FormatJSON, "leartech-arrivals-observer", "1.2.3", "gcp")
	if format != FormatJSON {
		t.Fatalf("expected FormatJSON, got %q", format)
	}

	log.Info().Str("event", "arrival_created").Str("arrival", "ar-x").Msg("test")

	m := decodeLast(t, &buf)

	if m["service"] != "leartech-arrivals-observer" {
		t.Errorf("expected service field, got %v", m["service"])
	}
	if m["version"] != "1.2.3" {
		t.Errorf("expected version field, got %v", m["version"])
	}
	if m["cluster"] != "gcp" {
		t.Errorf("expected cluster field, got %v", m["cluster"])
	}
	if m["event"] != "arrival_created" {
		t.Errorf("expected event=arrival_created preserved, got %v", m["event"])
	}
	if m["arrival"] != "ar-x" {
		t.Errorf("expected arrival field preserved, got %v", m["arrival"])
	}
	if _, ok := m["time"]; !ok {
		t.Errorf("expected timestamp field")
	}
}

// TestInitTo_ConsoleFormatDoesNotShipJSON confirms console mode is NOT
// JSON — this is the whole point of the toggle (dev-local readability
// vs Loki queryability).
func TestInitTo_ConsoleFormatDoesNotShipJSON(t *testing.T) {
	var buf bytes.Buffer
	InitTo(&buf, FormatConsole, "svc", "v", "c")

	log.Info().Msg("hello")

	// Console output starts with a colored timestamp / level — not valid JSON.
	var probe map[string]any
	if err := json.Unmarshal(buf.Bytes(), &probe); err == nil {
		t.Fatalf("console format unexpectedly produced valid JSON: %q", buf.String())
	}
}

// TestNormalize collapses arbitrary inputs to one of the two formats.
// Anything not "json" is treated as console, matching the reference
// (leartech-plan-api) semantics.
func TestNormalize(t *testing.T) {
	tests := []struct {
		in   Format
		want Format
	}{
		{FormatJSON, FormatJSON},
		{Format("JSON"), FormatJSON},
		{Format("Json"), FormatJSON},
		{FormatConsole, FormatConsole},
		{Format(""), FormatConsole},
		{Format("logfmt"), FormatConsole},
		{Format("something-odd"), FormatConsole},
	}
	for _, tc := range tests {
		if got := normalize(tc.in); got != tc.want {
			t.Errorf("normalize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestInit_ReadsLogFormatEnv exercises the Init() wrapper's env-read
// path — mirrors what main.go does at process start. Since Init
// mutates the global logger, restore it after the test.
func TestInit_ReadsLogFormatEnv(t *testing.T) {
	// zerolog writes global logger; snapshot + restore.
	original := log.Logger
	defer func() { log.Logger = original }()

	// Preserve caller env.
	orig, hadOrig := lookupEnv(FormatEnvVar)
	defer func() {
		if hadOrig {
			setEnv(FormatEnvVar, orig)
		} else {
			unsetEnv(FormatEnvVar)
		}
	}()

	// Case 1: LOG_FORMAT=json → FormatJSON returned
	setEnv(FormatEnvVar, "json")
	if got := Init("svc", "v", "gcp"); got != FormatJSON {
		t.Errorf("Init with LOG_FORMAT=json returned %q, want json", got)
	}

	// Case 2: LOG_FORMAT unset → FormatConsole returned
	unsetEnv(FormatEnvVar)
	if got := Init("svc", "v", "gcp"); got != FormatConsole {
		t.Errorf("Init with LOG_FORMAT unset returned %q, want console", got)
	}

	// Case 3: LOG_FORMAT=logfmt (unrecognised) → FormatConsole
	setEnv(FormatEnvVar, "logfmt")
	if got := Init("svc", "v", "gcp"); got != FormatConsole {
		t.Errorf("Init with LOG_FORMAT=logfmt returned %q, want console", got)
	}
}

// TestInitTo_AllowsEmptyAmbientFields — Init should tolerate all three
// fields being empty (unit tests, dev environments without CLUSTER_ID).
// The record still emits, just without those fields.
func TestInitTo_AllowsEmptyAmbientFields(t *testing.T) {
	var buf bytes.Buffer
	InitTo(&buf, FormatJSON, "", "", "")

	log.Info().Str("event", "smoke").Msg("no ambient")

	m := decodeLast(t, &buf)
	if _, ok := m["service"]; ok {
		t.Errorf("expected no service field when empty, got %v", m["service"])
	}
	if _, ok := m["cluster"]; ok {
		t.Errorf("expected no cluster field when empty, got %v", m["cluster"])
	}
	if m["event"] != "smoke" {
		t.Errorf("event field lost: %v", m["event"])
	}
}
