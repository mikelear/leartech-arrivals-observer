package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestMetricsRegisterAndExpose registers the four canonical metrics
// against a fresh registry and verifies they appear in /metrics scrape
// output with the expected names + labels.
func TestMetricsRegisterAndExpose(t *testing.T) {
	reg := ResetForTest()

	// Emit one sample of each so the metric appears in the exposition.
	RecordArrivalFinalized("Passed", "leartech-canary")
	RecordArrivalFinalized("Failed", "leartech-canary")
	RecordArrivalFinalized("Timeout", "leartech-canary")
	ObservePackDuration("leartech-canary", "smoke", 12.5)
	RecordPackResult("Passed", "leartech-canary")
	RecordPackResult("Failed", "leartech-portal")
	RecordJobOOM("leartech-portal")

	// Scrape the fresh registry via a promhttp handler + test recorder —
	// mirrors what the real /metrics endpoint returns.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from metrics handler, got %d", rec.Code)
	}
	metricsText := rec.Body.String()

	requiredSubstrings := []string{
		`arrivals_observer_arrival_finalized_total{phase="Passed",service="leartech-canary"}`,
		`arrivals_observer_arrival_finalized_total{phase="Failed",service="leartech-canary"}`,
		`arrivals_observer_arrival_finalized_total{phase="Timeout",service="leartech-canary"}`,
		`arrivals_observer_pack_duration_seconds_bucket{pack="smoke",service="leartech-canary"`,
		`arrivals_observer_pack_result_total{service="leartech-canary",status="Passed"} 1`,
		`arrivals_observer_pack_result_total{service="leartech-portal",status="Failed"} 1`,
		`arrivals_observer_job_oom_total{service="leartech-portal"} 1`,
	}
	for _, sub := range requiredSubstrings {
		if !strings.Contains(metricsText, sub) {
			t.Errorf("metrics exposition missing %q — got:\n%s", sub, metricsText)
		}
	}
}

// TestArrivalFinalizedCounts verifies the counter increments correctly.
func TestArrivalFinalizedCounts(t *testing.T) {
	ResetForTest()

	RecordArrivalFinalized("Passed", "svc-a")
	RecordArrivalFinalized("Passed", "svc-a")
	RecordArrivalFinalized("Failed", "svc-a")

	if v := testutil.ToFloat64(ArrivalFinalized.WithLabelValues("Passed", "svc-a")); v != 2 {
		t.Errorf("Passed counter = %v, want 2", v)
	}
	if v := testutil.ToFloat64(ArrivalFinalized.WithLabelValues("Failed", "svc-a")); v != 1 {
		t.Errorf("Failed counter = %v, want 1", v)
	}
}

// TestIsOOMReason covers the pod-termination-reason match. Case-
// insensitive so "OOMKilled" / "oomkilled" both work — the K8s API
// yields exactly "OOMKilled" but we tolerate case drift.
func TestIsOOMReason(t *testing.T) {
	cases := map[string]bool{
		"OOMKilled":    true,
		"oomkilled":    true,
		"OOM":          false,
		"Error":        false,
		"":             false,
		"ContainerOOM": false, // partial match — must be exact
		"Completed":    false,
	}
	for input, want := range cases {
		if got := IsOOMReason(input); got != want {
			t.Errorf("IsOOMReason(%q) = %v, want %v", input, got, want)
		}
	}
}

// TestNilSafeWrappers ensures the exported helpers don't panic if
// (hypothetically) the package metric handles are nil — defensive
// belt-and-braces for tests that intentionally deregister.
func TestNilSafeWrappers(t *testing.T) {
	old := ArrivalFinalized
	oldDur := PackDuration
	oldResult := PackResult
	oldOOM := JobOOM
	defer func() {
		ArrivalFinalized = old
		PackDuration = oldDur
		PackResult = oldResult
		JobOOM = oldOOM
	}()
	ArrivalFinalized = nil
	PackDuration = nil
	PackResult = nil
	JobOOM = nil

	// Should not panic.
	RecordArrivalFinalized("x", "y")
	ObservePackDuration("x", "y", 1.0)
	RecordPackResult("x", "y")
	RecordJobOOM("y")
}
