// Package metrics registers the observer's Prometheus counters + histograms.
//
// Before this package /metrics on the observer exposed only the standard
// go/process collectors — the arrival lifecycle was invisible to any
// dashboard that couldn't read Arrival CRs directly. The counters below
// give a Prometheus-scraped view of every arrival + pack outcome so we
// can chart pass/fail rate, pack duration p99, and OOM counts alongside
// the Loki `event=…` timeline.
//
// # Naming
//
// Follows Prometheus best practice: `<subsystem>_<measurement>_<unit>`
// with `_total` on all counters. Labels stay under the 5-label ceiling
// per metric to keep cardinality manageable — service is the primary
// pivot, phase / status / pack are secondary.
//
// # Registration
//
// All metrics register into the DEFAULT Prometheus registry via
// promauto.With(prometheus.DefaultRegisterer). handlers.RegisterMetrics
// (which wires /metrics via promhttp.Handler) picks them up
// automatically — no explicit `MustRegister` call needed at the caller.
//
// # Idempotence + tests
//
// promauto panics on duplicate registration, which would explode under
// `go test ./...` when multiple test binaries pull the package in
// parallel. To keep tests hermetic, the metric constructors are
// `sync.Once`-guarded and the exported metric handles are re-assignable
// in tests via ResetForTest — which creates a fresh CustomRegistry and
// rebinds the package vars against it.
package metrics

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The exported metric handles the controller writes to. Package-scoped
// so any caller can `metrics.ArrivalFinalized.WithLabelValues(...).Inc()`.
//
// The nil-safe wrappers below (RecordArrivalFinalized etc.) let tests
// swap in a fresh registry without touching the raw handles.
var (
	// ArrivalFinalized counts arrivals that reached a terminal phase.
	// Labels: phase (Passed|Failed|Timeout|Skipped), service.
	ArrivalFinalized *prometheus.CounterVec

	// PackDuration measures wall-clock time from pack dispatch to pack
	// completion. Labels: service, pack. Buckets chosen for the observed
	// range (10s to ~10min).
	PackDuration *prometheus.HistogramVec

	// PackResult counts pack outcomes. Labels: status (Passed|Failed|Timeout),
	// service. Separate from ArrivalFinalized because one arrival can have
	// multiple packs.
	PackResult *prometheus.CounterVec

	// JobOOM counts K8s Job pods observed to have exited with OOMKilled
	// (reason=OOMKilled or exit code 137). Labels: service. Populated by
	// the dispatcher when it detects an OOM on a Job it's polling.
	JobOOM *prometheus.CounterVec
)

var initOnce sync.Once

func init() {
	initOnce.Do(func() {
		bind(prometheus.DefaultRegisterer)
	})
}

// bind creates and registers all metric handles against the given
// registerer. Extracted so tests can rebind against a fresh registry
// via ResetForTest.
func bind(reg prometheus.Registerer) {
	f := promauto.With(reg)
	ArrivalFinalized = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: "arrivals_observer",
		Name:      "arrival_finalized_total",
		Help:      "Count of Arrivals that reached a terminal phase, labelled by phase and service.",
	}, []string{"phase", "service"})

	PackDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "arrivals_observer",
		Name:      "pack_duration_seconds",
		Help:      "Wall-clock time from pack dispatch to completion, labelled by service and pack.",
		// Buckets tuned to observed pack duration range: 5s smoke tests
		// through 10min heavy Playwright suites. Log-ish spacing so both
		// ends land in a meaningful bucket.
		Buckets: []float64{5, 15, 30, 60, 120, 300, 600, 1200},
	}, []string{"service", "pack"})

	PackResult = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: "arrivals_observer",
		Name:      "pack_result_total",
		Help:      "Count of pack outcomes, labelled by status (Passed|Failed|Timeout) and service.",
	}, []string{"status", "service"})

	JobOOM = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: "arrivals_observer",
		Name:      "job_oom_total",
		Help:      "Count of pack Jobs whose pod exited OOMKilled (reason=OOMKilled or exit 137), labelled by service.",
	}, []string{"service"})
}

// ResetForTest rebinds the exported metric handles against a fresh
// registry. Returns the registry so tests can gather + assert on it.
// Never used in production — the initOnce guard in init() ensures the
// default registry keeps its production bindings.
func ResetForTest() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	bind(reg)
	return reg
}

// RecordArrivalFinalized increments the ArrivalFinalized counter with
// the given labels. Nil-safe — a mis-configured deployment that hasn't
// called init() won't panic (the counter is package-scoped and always
// non-nil after init, but the wrapper is defensive).
func RecordArrivalFinalized(phase, service string) {
	if ArrivalFinalized == nil {
		return
	}
	ArrivalFinalized.WithLabelValues(phase, service).Inc()
}

// ObservePackDuration records the pack completion time.
func ObservePackDuration(service, pack string, seconds float64) {
	if PackDuration == nil {
		return
	}
	PackDuration.WithLabelValues(service, pack).Observe(seconds)
}

// RecordPackResult increments the PackResult counter.
func RecordPackResult(status, service string) {
	if PackResult == nil {
		return
	}
	PackResult.WithLabelValues(status, service).Inc()
}

// RecordJobOOM increments the JobOOM counter.
func RecordJobOOM(service string) {
	if JobOOM == nil {
		return
	}
	JobOOM.WithLabelValues(service).Inc()
}

// IsOOMReason returns true when the pod / container termination reason
// looks like an OOMKill. Matches K8s' "OOMKilled" reason string
// case-insensitively — the exact string as emitted on
// containerStatuses[].lastState.terminated.reason.
//
// Kept in this package so both the controller (Job-status poller) and
// tests share the definition.
func IsOOMReason(reason string) bool {
	return strings.EqualFold(reason, "OOMKilled")
}
