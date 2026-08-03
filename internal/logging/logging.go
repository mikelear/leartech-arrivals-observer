// Package logging is the observer's structured-logging shim.
//
// # Why it exists
//
// Before this package the observer emitted zerolog CONSOLE format
// ("11:18AM INF request client_ip=…") through global log.Logger. That
// shape ships to Loki as opaque text with ANSI colour codes — so the
// obvious Loki query
//
//	{app="leartech-arrivals-observer"} | json
//
// silently fails ( `| json` can't parse the ANSI-coloured console lines),
// and Loki-side field filters like `| arrival="ar-…"` are impossible.
// Every debug session degrades to "grep the raw text on the pod".
//
// # Shape
//
// Init installs a JSON logger toggled by LOG_FORMAT=json (in-cluster
// default via the Helm chart's env) and stamps three fields on EVERY
// record so a Loki search can pivot without a join:
//
//	service:  "leartech-arrivals-observer"
//	version:  the ldflags-injected build version (or "dev")
//	cluster:  the CLUSTER_ID env var ("gcp" / "az")
//
// Locally (no LOG_FORMAT) we retain the human-readable ConsoleWriter
// shape so `go run ./cmd/server` output stays readable at the terminal.
//
// # Event convention
//
// The controller emits records with an `event=…` field so a single
// arrival is Loki-queryable end-to-end:
//
//	event="arrival_created"    (watcher)
//	event="arrival_pending"    (controller: reconcile fresh)
//	event="arrival_testing"    (controller: dispatched, phase=Testing)
//	event="pack_dispatched"    (controller: one pack Job created)
//	event="pack_result"        (controller: one pack Job settled)
//	event="arrival_passed"     (controller: terminal Passed)
//	event="arrival_failed"     (controller: terminal Failed)
//	event="arrival_timeout"    (controller: terminal Timeout)
//
// Mirror of the automated-agent's proven `event=run_start`/`run_end`
// pattern with a `run_id` ambient field — same query ergonomics.
//
// # Idempotence
//
// Init is safe to call more than once — subsequent calls replace the
// global logger. Tests that need to inspect emitted records call
// InitTo(w, format, …) instead, redirecting to a bytes.Buffer.
package logging

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// FormatEnvVar is the env var that toggles JSON output.
const FormatEnvVar = "LOG_FORMAT"

// Format is the wire format selected at Init time.
type Format string

// The two supported wire formats. `FormatConsole` is human-readable
// (dev), `FormatJSON` is line-delimited JSON (Loki-queryable).
const (
	FormatConsole Format = "console"
	FormatJSON    Format = "json"
)

// Init installs a global logger for the process. Should be called once
// at startup, before any log.* helpers fire.
//
// Reads LOG_FORMAT to select JSON vs console. Anything other than "json"
// (including empty) selects console — matches leartech-plan-api's
// internal/logging behaviour.
//
// The three ambient fields (service, version, cluster) are stamped on
// every record via zerolog's context — no caller needs to remember to
// pass them.
func Init(service, version, cluster string) Format {
	return InitTo(os.Stderr, Format(os.Getenv(FormatEnvVar)), service, version, cluster)
}

// InitTo is Init with an explicit writer + explicit format. Used by
// tests to redirect to a bytes.Buffer + assert JSON field presence.
func InitTo(w io.Writer, requested Format, service, version, cluster string) Format {
	// Loki works with second-precision RFC3339 timestamps; Unix seconds
	// (the observer's prior default) shipped as integers can't be range-
	// filtered visually. RFC3339 is the platform-wide standard.
	zerolog.TimeFieldFormat = time.RFC3339

	format := normalize(requested)

	var base zerolog.Logger
	switch format {
	case FormatJSON:
		base = zerolog.New(w)
	default:
		base = zerolog.New(zerolog.ConsoleWriter{Out: w, TimeFormat: time.RFC3339})
	}

	ctx := base.With().Timestamp()
	if service != "" {
		ctx = ctx.Str("service", service)
	}
	if version != "" {
		ctx = ctx.Str("version", version)
	}
	if cluster != "" {
		ctx = ctx.Str("cluster", cluster)
	}
	log.Logger = ctx.Logger()

	return format
}

// normalize collapses arbitrary LOG_FORMAT input to one of the two
// supported wire formats. Case-insensitive; anything other than "json"
// → console (matches plan-api's toggle semantics).
func normalize(f Format) Format {
	switch strings.ToLower(string(f)) {
	case "json":
		return FormatJSON
	default:
		return FormatConsole
	}
}
