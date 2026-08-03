package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dashboardPath is the on-disk location of the Grafana dashboard JSON
// shipped in the chart. Test walks up from the test's cwd (which under
// `go test ./internal/metrics` is that package's dir) to the repo root.
func dashboardPath(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	rel := filepath.Join(
		cwd,
		"..", "..",
		"charts", "leartech-arrivals-observer",
		"files", "grafana", "arrivals-observer-dashboard.json",
	)
	abs, err := filepath.Abs(rel)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	return abs
}

// loadDashboard reads and parses the dashboard JSON. Called by every
// dashboard test — kept tight so a JSON-invalid dashboard fails fast
// with a clear diagnostic.
func loadDashboard(t *testing.T) map[string]any {
	t.Helper()
	body, err := os.ReadFile(dashboardPath(t))
	if err != nil {
		t.Fatalf("read dashboard: %v (path=%s)", err, dashboardPath(t))
	}
	var dash map[string]any
	if err := json.Unmarshal(body, &dash); err != nil {
		t.Fatalf("dashboard JSON invalid: %v", err)
	}
	return dash
}

// TestDashboard_IsValidJSON is a smoke check — any drift to a
// non-parseable dashboard breaks Grafana import silently, so we fail
// on the parse itself.
func TestDashboard_IsValidJSON(t *testing.T) {
	loadDashboard(t)
}

// TestDashboard_ReferencesEveryProductionMetric guarantees the
// dashboard actually queries the four metrics this package exports.
// If someone renames arrivals_observer_pack_result_total in metrics.go
// but forgets to update the dashboard JSON, this test fires — the
// panel would silently show No Data in Grafana otherwise.
func TestDashboard_ReferencesEveryProductionMetric(t *testing.T) {
	body, err := os.ReadFile(dashboardPath(t))
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	text := string(body)

	expected := []string{
		"arrivals_observer_arrival_finalized_total",
		"arrivals_observer_pack_duration_seconds_bucket",
		"arrivals_observer_pack_result_total",
		"arrivals_observer_job_oom_total",
	}
	for _, name := range expected {
		if !strings.Contains(text, name) {
			t.Errorf("dashboard does not reference metric %q — add a panel or update the metric name", name)
		}
	}
}

// TestDashboard_HasJX3SidecarLabels validates the ConfigMap-side
// discovery contract by asserting the dashboard uses the label / UID
// shape the chart's grafana-dashboard.yaml expects. The
// ConfigMap is Helm-templated so this test can't parse it; instead we
// pin the dashboard's `uid` (Grafana ImportSpec key — must be stable)
// and `datasource.uid: loki` (matches every cluster's Grafana
// provisioned datasource).
func TestDashboard_HasJX3SidecarLabels(t *testing.T) {
	dash := loadDashboard(t)

	if uid, _ := dash["uid"].(string); uid != "leartech-arrivals-observer" {
		t.Errorf("expected stable dashboard uid=leartech-arrivals-observer, got %q", uid)
	}

	// Walk panels; assert at least one Loki-backed panel with the
	// prefilter+json shape the initiative requires.
	panels, ok := dash["panels"].([]any)
	if !ok || len(panels) == 0 {
		t.Fatalf("no panels array in dashboard")
	}
	sawLokiPrefilter := false
	sawPromMetric := false
	for _, p := range panels {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		targets, _ := pm["targets"].([]any)
		for _, tgt := range targets {
			tm, ok := tgt.(map[string]any)
			if !ok {
				continue
			}
			expr, _ := tm["expr"].(string)
			ds, _ := tm["datasource"].(map[string]any)
			dsType, _ := ds["type"].(string)
			if dsType == "loki" && strings.Contains(expr, "|~") && strings.Contains(expr, "| json") {
				sawLokiPrefilter = true
			}
			if dsType == "prometheus" && strings.Contains(expr, "arrivals_observer_") {
				sawPromMetric = true
			}
		}
	}
	if !sawLokiPrefilter {
		t.Errorf("no Loki panel with `|~` prefilter + `| json` — perf-degraded, initiative requires the prefilter shape")
	}
	if !sawPromMetric {
		t.Errorf("no Prometheus panel referencing arrivals_observer_* — dashboard would show empty for metric panels")
	}
}

// TestDashboard_HasNamespaceTemplateVar pins the initiative's
// requirement that the dashboard is namespace-scoped via a template
// variable — same shape as the automated-agent dashboards.
func TestDashboard_HasNamespaceTemplateVar(t *testing.T) {
	dash := loadDashboard(t)
	tpl, _ := dash["templating"].(map[string]any)
	list, _ := tpl["list"].([]any)
	names := make(map[string]bool)
	for _, v := range list {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if n, ok := vm["name"].(string); ok {
			names[n] = true
		}
	}
	if !names["namespace"] {
		t.Errorf("dashboard missing `namespace` template variable — initiative mandates a namespace pivot")
	}
}
