package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/mikelear/leartech-arrivals-observer/internal/logging"
	"github.com/mikelear/leartech-arrivals-observer/internal/metrics"
)

// captureJSONLogs replaces the global zerolog logger with a JSON writer
// against the returned buffer, and restores the original on Cleanup.
// Every log record emitted while the test runs lands as one JSON line.
func captureJSONLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	logging.InitTo(buf, logging.FormatJSON, "leartech-arrivals-observer", "test", "gcp")
	t.Cleanup(func() {
		// Reset back to a discard-console logger — subsequent tests
		// (e.g. controller_test.go) don't need the JSON capture.
		logging.InitTo(&bytes.Buffer{}, logging.FormatConsole, "", "", "")
		_ = log.Logger // silence unused import when the file scope changes
	})
	return buf
}

// eventLines returns every JSON record whose `event` field matches the
// given prefix. Skips records without an event field.
func eventLines(t *testing.T, buf *bytes.Buffer, prefix string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line not JSON: %v — %q", err, line)
		}
		if ev, _ := m["event"].(string); strings.HasPrefix(ev, prefix) {
			out = append(out, m)
		}
	}
	return out
}

// TestHandlePending_EmitsArrivalSkippedEvent — no test packs should
// produce an `event=arrival_skipped` JSON log record with the ambient
// service/version/cluster fields, and bump the
// arrival_finalized_total{phase=Skipped} counter.
func TestHandlePending_EmitsArrivalSkippedEvent(t *testing.T) {
	reg := metrics.ResetForTest()
	buf := captureJSONLogs(t)

	arr := newArrival("svc-1-jx-staging", "leartech-svc-1", "1.0.0", "", nil)
	c := newTestController(t, arr)
	c.reconcileOne(context.Background(), arr)

	events := eventLines(t, buf, "arrival_skipped")
	if len(events) == 0 {
		t.Fatalf("expected at least one event=arrival_skipped record; got:\n%s", buf.String())
	}
	rec := events[0]
	assertField(t, rec, "arrival", "svc-1-jx-staging")
	assertField(t, rec, "service", "leartech-svc-1")
	assertField(t, rec, "version", "1.0.0")
	assertField(t, rec, "phase", PhaseSkipped)
	assertField(t, rec, "cluster", "gcp")

	if v := testutil.ToFloat64(metrics.ArrivalFinalized.WithLabelValues(PhaseSkipped, "leartech-svc-1")); v != 1 {
		t.Errorf("arrival_finalized_total{phase=Skipped,service=leartech-svc-1} = %v, want 1", v)
	}
	_ = reg
}

// TestHandlePending_EmitsPackDispatchedAndArrivalTesting — with a pack
// present, we should see one `event=pack_dispatched` per pack + one
// `event=arrival_testing`.
func TestHandlePending_EmitsPackDispatchedAndArrivalTesting(t *testing.T) {
	metrics.ResetForTest()
	buf := captureJSONLogs(t)

	packs := []map[string]any{
		{"name": "smoke", "type": "end2end"},
		{"name": "heavy", "type": "end2end-ui"},
	}
	arr := newArrival("svc-2-jx-staging", "leartech-svc-2", "2.0.0", "", packs)
	c := newTestController(t, arr)
	c.reconcileOne(context.Background(), arr)

	dispatched := eventLines(t, buf, "pack_dispatched")
	if len(dispatched) != 2 {
		t.Errorf("expected 2 pack_dispatched events (one per pack), got %d:\n%s", len(dispatched), buf.String())
	}
	testingEvents := eventLines(t, buf, "arrival_testing")
	if len(testingEvents) == 0 {
		t.Errorf("expected event=arrival_testing after dispatch")
	}
	if len(testingEvents) > 0 {
		assertField(t, testingEvents[0], "service", "leartech-svc-2")
		assertField(t, testingEvents[0], "arrival", "svc-2-jx-staging")
		assertField(t, testingEvents[0], "phase", PhaseTesting)
	}
}

// TestFinalize_EmitsTerminalEventAndMetrics exercises the finalize
// path (used by stub-mode + timeout) and confirms:
//   - one event=arrival_passed (or arrival_failed/timeout) is emitted
//   - one event=pack_result per test is emitted
//   - the arrival_finalized_total counter increments
//   - pack_result_total increments for each pack
//   - pack_duration_seconds observes when startedAt+completedAt present
func TestFinalize_EmitsTerminalEventAndMetrics(t *testing.T) {
	metrics.ResetForTest()
	buf := captureJSONLogs(t)

	arr := newArrival("svc-3-jx-staging", "leartech-svc-3", "3.0.0", PhaseTesting, nil)
	start := time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)
	_ = unstructured.SetNestedSlice(arr.Object, []any{
		map[string]any{"name": "smoke", "status": "Running", "startedAt": start},
	}, "status", "tests")

	c := newTestController(t, arr)
	c.finalize(context.Background(), arr, PhasePassed)

	// event=arrival_passed
	terminal := eventLines(t, buf, "arrival_passed")
	if len(terminal) == 0 {
		t.Errorf("expected event=arrival_passed; got:\n%s", buf.String())
	}
	if len(terminal) > 0 {
		assertField(t, terminal[0], "service", "leartech-svc-3")
		assertField(t, terminal[0], "phase", PhasePassed)
	}

	// One event=pack_result per pack
	packResults := eventLines(t, buf, "pack_result")
	if len(packResults) != 1 {
		t.Errorf("expected 1 pack_result event, got %d", len(packResults))
	}

	// Metrics: arrival_finalized_total{Passed, leartech-svc-3} = 1
	if v := testutil.ToFloat64(metrics.ArrivalFinalized.WithLabelValues(PhasePassed, "leartech-svc-3")); v != 1 {
		t.Errorf("arrival_finalized_total = %v, want 1", v)
	}
	// Metrics: pack_result_total{Passed, leartech-svc-3} = 1
	if v := testutil.ToFloat64(metrics.PackResult.WithLabelValues("Passed", "leartech-svc-3")); v != 1 {
		t.Errorf("pack_result_total = %v, want 1", v)
	}
	// Metrics: pack_duration_seconds observed at least once for smoke pack.
	// Histogram sample-count check via the vector's WithLabelValues + testutil.
	if v := testutil.CollectAndCount(metrics.PackDuration); v == 0 {
		t.Errorf("pack_duration_seconds never observed — histogram should have at least one sample")
	}
}

// assertField is a tight equality check that fails with a clear message
// naming the field so a diff in the observer's JSON shape doesn't hide
// behind an "expected true, got false" assertion.
func assertField(t *testing.T, m map[string]any, key, want string) {
	t.Helper()
	got, _ := m[key].(string)
	if got != want {
		t.Errorf("field %q: got %q, want %q — full record: %v", key, got, want, m)
	}
}
