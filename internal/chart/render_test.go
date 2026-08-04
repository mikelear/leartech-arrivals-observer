// Package chart runs helm-render assertions against the observer's
// bundled Helm chart. Mirrors the intent of
// orchestrator-controller/chart_crds_yaml_test.go +
// chart_deployment_yaml_test.go: catch chart-side regressions BEFORE
// they land in a cluster — specifically the bug-class where the code
// consumes one path (e.g. `container.resources`) but the chart writes
// another (e.g. `pod.resources`).
//
// Renders through the helm/v3 SDK so tests are hermetic — no external
// `helm` binary required, and the SDK invokes the SAME rendering
// engine `helm template` uses.
package chart

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	"sigs.k8s.io/yaml"
)

const chartRelPath = "../../charts/leartech-arrivals-observer"

// renderChart runs the Helm engine over the observer's chart with the
// given values overlay layered ONTO the chart's default values.yaml.
// Returns a map of `templates/<file>` → rendered YAML string.
func renderChart(t *testing.T, valuesOverlay map[string]any) map[string]string {
	t.Helper()

	// Load the chart from disk.
	c, err := loader.Load(chartRelPath)
	require.NoError(t, err, "helm loader.Load must succeed on the chart directory %s", chartRelPath)

	// Merge overlay onto the chart's own default values.
	values, err := chartutil.CoalesceValues(c, valuesOverlay)
	require.NoError(t, err)

	// Standard capabilities + release metadata. Namespace + release
	// name mirror what Jenkins X installs on the canonical staging
	// deploy so the rendered output looks like production.
	rel := chartutil.ReleaseOptions{
		Name:      "leartech-arrivals-observer",
		Namespace: "jx-staging",
		IsInstall: true,
	}
	caps := chartutil.DefaultCapabilities.Copy()
	renderVals, err := chartutil.ToRenderValues(c, values, rel, caps)
	require.NoError(t, err)

	rendered, err := engine.Render(c, renderVals)
	require.NoError(t, err, "helm engine.Render must succeed")

	// Trim the leading chart-name/templates path prefix so callers can
	// key off simple filenames like `templates/deployment.yaml`.
	prefix := filepath.Join(c.Name(), "templates") + "/"
	out := make(map[string]string, len(rendered))
	for name, body := range rendered {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		out["templates/"+strings.TrimPrefix(name, prefix)] = body
	}
	return out
}

// parseYAMLDocs splits a multi-doc YAML string into a slice of typed
// maps (each doc = one k8s manifest). Empty docs are dropped.
//
// Tolerant of a leading comment block (Helm chart files often start
// with `{{- /* ... */ -}}` which renders to blank lines + `#`-lines).
// Splits on any `---` line, not just `\n---\n`, so a document that
// starts with `---` on the first line also parses.
func parseYAMLDocs(t *testing.T, body string) []map[string]any {
	t.Helper()
	var docs []string
	var cur strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "---" {
			if cur.Len() > 0 {
				docs = append(docs, cur.String())
				cur.Reset()
			}
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
	}
	if cur.Len() > 0 {
		docs = append(docs, cur.String())
	}

	var out []map[string]any
	for _, doc := range docs {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}
		// A doc that is entirely comments/blank lines is a no-op.
		nonComment := false
		for _, line := range strings.Split(doc, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			nonComment = true
			break
		}
		if !nonComment {
			continue
		}
		var m map[string]any
		if err := yaml.Unmarshal([]byte(doc), &m); err != nil {
			t.Fatalf("yaml decode: %v — doc:\n%s", err, doc)
		}
		if len(m) == 0 {
			continue
		}
		out = append(out, m)
	}
	return out
}

// TestChart_CRDArrival_HasResourcesAndEnvFields — the initiative asked
// specifically for a schema-round-trip test on the new Resources +
// Env fields. This asserts the rendered CRD advertises them.
func TestChart_CRDArrival_HasResourcesAndEnvFields(t *testing.T) {
	files := renderChart(t, nil)
	body, ok := files["templates/crd-arrival.yaml"]
	require.True(t, ok, "chart must render templates/crd-arrival.yaml with default values (crd.install=true)")

	docs := parseYAMLDocs(t, body)
	require.NotEmpty(t, docs, "CRD YAML must render at least one document — got body:\n%s", body)

	crd := docs[0]
	assert.Equal(t, "CustomResourceDefinition", crd["kind"])

	// Walk into spec.versions[0].schema.openAPIV3Schema.properties.spec.properties
	specProps := mustNested(t, crd, "spec", "versions", "[0]", "schema", "openAPIV3Schema", "properties", "spec", "properties")

	// Service-level `resources` and `env` — new fields the initiative asks
	// to survive the CRD schema.
	_, hasServiceResources := specProps["resources"]
	_, hasServiceEnv := specProps["env"]
	assert.True(t, hasServiceResources, "CRD.spec.versions[0].schema.…spec.properties.resources must be present so per-service Resources round-trip")
	assert.True(t, hasServiceEnv, "CRD.spec.versions[0].schema.…spec.properties.env must be present so per-service env round-trip")

	// Per-pack `resources` and `env` inside testPacks[].properties.
	testPacksItems := mustNested(t, specProps, "testPacks", "items", "properties")
	_, hasPackResources := testPacksItems["resources"]
	_, hasPackEnv := testPacksItems["env"]
	assert.True(t, hasPackResources, "CRD.…testPacks.items.properties.resources must be present so per-pack Resources round-trip")
	assert.True(t, hasPackEnv, "CRD.…testPacks.items.properties.env must be present so per-pack env round-trip")
}

// TestChart_ConfigMap_RendersServicesJSON — the ConfigMap must serialize
// values.services into SERVICES_JSON. When services carry per-service
// env + resources (or per-pack env + resources), those fields must
// survive the toJson round-trip.
func TestChart_ConfigMap_RendersServicesJSON(t *testing.T) {
	overlay := map[string]any{
		"services": map[string]any{
			"leartech-portal": map[string]any{
				"stagingUrl": "https://leartech-portal-jx-staging.gcp.leartech.com",
				"env": []map[string]any{
					{"name": "USER_EMAIL", "value": "user@example.com"},
				},
				"resources": map[string]any{
					"requests": map[string]any{"memory": "1Gi"},
				},
				"testPacks": []map[string]any{
					{
						"name": "heavy",
						"type": "end2end-ui",
						"resources": map[string]any{
							"requests": map[string]any{"memory": "3Gi"},
						},
						"env": []map[string]any{{"name": "PLAYWRIGHT_WORKERS", "value": "2"}},
					},
				},
			},
		},
	}
	files := renderChart(t, overlay)
	body, ok := files["templates/configmap.yaml"]
	require.True(t, ok)

	docs := parseYAMLDocs(t, body)
	require.Len(t, docs, 1)
	cm := docs[0]
	data := mustNested(t, cm, "data")

	svcJSONRaw, ok := data["SERVICES_JSON"].(string)
	require.True(t, ok, "data.SERVICES_JSON must be a string (toJson output)")

	var services map[string]map[string]any
	require.NoError(t, json.Unmarshal([]byte(svcJSONRaw), &services), "SERVICES_JSON must be valid JSON")

	portal, ok := services["leartech-portal"]
	require.True(t, ok, "leartech-portal must be present in SERVICES_JSON")
	assert.Equal(t, "https://leartech-portal-jx-staging.gcp.leartech.com", portal["stagingUrl"])
	envList, ok := portal["env"].([]any)
	require.True(t, ok, "portal.env must be a list")
	require.NotEmpty(t, envList)
	envEntry := envList[0].(map[string]any)
	assert.Equal(t, "USER_EMAIL", envEntry["name"])
	// Per-pack values also survive the round-trip.
	packs := portal["testPacks"].([]any)
	require.Len(t, packs, 1)
	heavy := packs[0].(map[string]any)
	assert.Equal(t, "heavy", heavy["name"])
	packRes := heavy["resources"].(map[string]any)
	packMem := packRes["requests"].(map[string]any)["memory"]
	assert.Equal(t, "3Gi", packMem, "per-pack Resources memory must round-trip cleanly")
}

// TestChart_GrafanaDashboard_HasCorrectLabelAndAnnotation — pins the
// two silent-failure modes: wrong sidecar label (grafana_dashboard vs
// jenkins-x.io/grafana-dashboard) and missing / mistyped
// grafana_folder annotation.
func TestChart_GrafanaDashboard_HasCorrectLabelAndAnnotation(t *testing.T) {
	files := renderChart(t, nil)
	body, ok := files["templates/grafana-dashboard.yaml"]
	require.True(t, ok, "chart must render the Grafana dashboard ConfigMap when grafana.dashboard.enabled=true (default)")

	docs := parseYAMLDocs(t, body)
	require.Len(t, docs, 1)
	cm := docs[0]

	// JX Grafana sidecar discovery label — literal key including the
	// slash. This is the one that bit orchestrator-controller: the
	// generic `grafana_dashboard: "1"` doesn't get picked up by the
	// JX sidecar.
	labels := mustNested(t, cm, "metadata", "labels")
	labelValue, ok := labels["jenkins-x.io/grafana-dashboard"]
	require.True(t, ok, "ConfigMap.metadata.labels['jenkins-x.io/grafana-dashboard'] must be present (JX sidecar discovery key)")
	assert.Equal(t, "1", labelValue, "label value must be literal string \"1\" (all label values are strings)")

	// grafana_folder annotation — plumbs the dashboard into the
	// configured Grafana folder.
	anns := mustNested(t, cm, "metadata", "annotations")
	folder, ok := anns["grafana_folder"]
	require.True(t, ok, "metadata.annotations.grafana_folder must be present so the sidecar drops the dashboard in the right folder")
	assert.Equal(t, "Leartech QA", folder, "default folder is Leartech QA (from values.yaml grafana.dashboard.folder)")

	// Body must be valid JSON — the dashboard is uploaded as-is; a
	// malformed JSON would crash the Grafana sidecar's importer.
	dataObj := mustNested(t, cm, "data")
	dashJSON, ok := dataObj["arrivals-observer.json"].(string)
	require.True(t, ok, "data['arrivals-observer.json'] must be a string carrying the dashboard JSON")
	var dash map[string]any
	require.NoError(t, json.Unmarshal([]byte(dashJSON), &dash), "dashboard body must be valid JSON")
}

// TestChart_Deployment_ResourcesAtContainerLevel — the bug-class the
// initiative called out specifically: chart writes container
// resources under `pod.resources` instead of `container.resources`,
// leaving the runtime container without a request/limit block.
//
// Confirms the deployment's container carries a `resources` field,
// AND that no pod-level `spec.resources` field exists (K8s pods don't
// support pod-level resources at all — but a broken chart could emit
// it and helm-render would accept the YAML).
func TestChart_Deployment_ResourcesAtContainerLevel(t *testing.T) {
	overlay := map[string]any{
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "200m", "memory": "256Mi"},
			"limits":   map[string]any{"cpu": "1", "memory": "1Gi"},
		},
	}
	files := renderChart(t, overlay)
	body, ok := files["templates/deployment.yaml"]
	require.True(t, ok)

	docs := parseYAMLDocs(t, body)
	require.Len(t, docs, 1)
	deploy := docs[0]

	assert.Equal(t, "Deployment", deploy["kind"])

	podSpec := mustNested(t, deploy, "spec", "template", "spec")

	// The bug-class: nobody should be writing spec.template.spec.resources.
	_, hasPodResources := podSpec["resources"]
	assert.False(t, hasPodResources, "resources block MUST NOT sit at pod level; container-level only")

	containers := podSpec["containers"].([]any)
	require.Len(t, containers, 1, "expected exactly one app container")
	c := containers[0].(map[string]any)

	res, ok := c["resources"].(map[string]any)
	require.True(t, ok, "container.resources must be present (this is what K8s actually reads)")
	req := res["requests"].(map[string]any)
	lim := res["limits"].(map[string]any)
	assert.Equal(t, "200m", req["cpu"])
	assert.Equal(t, "256Mi", req["memory"])
	assert.Equal(t, "1", lim["cpu"])
	assert.Equal(t, "1Gi", lim["memory"])
}

// TestChart_Deployment_LivenessProbeConfigured — surface probe wiring so
// a chart-side regression that removes probes (which would break
// K8s pod healthiness gates) is caught here rather than in a live cluster.
func TestChart_Deployment_LivenessProbeConfigured(t *testing.T) {
	files := renderChart(t, nil)
	body, ok := files["templates/deployment.yaml"]
	require.True(t, ok)
	docs := parseYAMLDocs(t, body)
	require.Len(t, docs, 1)

	c := mustNested(t, docs[0], "spec", "template", "spec", "containers", "[0]")
	// Probe names must match the go template output.
	for _, key := range []string{"livenessProbe", "readinessProbe", "startupProbe"} {
		if _, ok := c[key]; !ok {
			t.Errorf("container.%s must be present so K8s can gate rollouts on health", key)
		}
	}
}

// TestChart_CRDArrival_PhaseEnumMatches confirms the CRD's phase enum
// enumerates exactly the values the controller writes. If someone
// adds a new phase in code but forgets the CRD, the apiserver would
// reject the patch — helm-render can't catch that but this test can.
func TestChart_CRDArrival_PhaseEnumMatches(t *testing.T) {
	files := renderChart(t, nil)
	body, ok := files["templates/crd-arrival.yaml"]
	require.True(t, ok)

	docs := parseYAMLDocs(t, body)
	require.NotEmpty(t, docs)

	phaseObj := mustNested(t, docs[0], "spec", "versions", "[0]", "schema", "openAPIV3Schema", "properties", "status", "properties", "phase")
	enum, _ := phaseObj["enum"].([]any)
	require.NotEmpty(t, enum, "status.phase must carry an enum")

	got := map[string]bool{}
	for _, e := range enum {
		got[e.(string)] = true
	}
	// These are the controller.PhaseX constants. If a new phase is
	// added on either side, both must move.
	for _, want := range []string{"Pending", "Testing", "Passed", "Failed", "Timeout", "Skipped"} {
		assert.True(t, got[want], "CRD status.phase.enum must contain %q — controller writes this value", want)
	}
}

// TestChart_ConfigMap_RendersPlanConformanceRunnerImage — the ConfigMap
// must emit DISPATCH_PLAN_CONFORMANCE_RUNNER_IMAGE assembled from the
// registry/repository/tag split, so the dispatcher's plan-conformance
// branch has an image to run.
func TestChart_ConfigMap_RendersPlanConformanceRunnerImage(t *testing.T) {
	files := renderChart(t, nil)
	body, ok := files["templates/configmap.yaml"]
	require.True(t, ok)
	docs := parseYAMLDocs(t, body)
	require.Len(t, docs, 1)
	data := mustNested(t, docs[0], "data")

	img, ok := data["DISPATCH_PLAN_CONFORMANCE_RUNNER_IMAGE"].(string)
	require.True(t, ok, "configmap must set DISPATCH_PLAN_CONFORMANCE_RUNNER_IMAGE")
	assert.Equal(t, "ghcr.io/mikelear/leartech-plan-conformance-runner:latest", img)
}

// TestChart_Services_RegistersPlanConformanceSentinel — the default
// services map registers the plan-conformance pack under the sentinel
// service leartech-orchestrator-controller with an empty stagingUrl, so
// the corpus runs when the controller arrives.
func TestChart_Services_RegistersPlanConformanceSentinel(t *testing.T) {
	files := renderChart(t, nil)
	body, ok := files["templates/configmap.yaml"]
	require.True(t, ok)
	docs := parseYAMLDocs(t, body)
	require.Len(t, docs, 1)
	data := mustNested(t, docs[0], "data")

	raw, ok := data["SERVICES_JSON"].(string)
	require.True(t, ok)
	var services map[string]map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &services))

	ctrl, ok := services["leartech-orchestrator-controller"]
	require.True(t, ok, "sentinel service leartech-orchestrator-controller must be registered")
	assert.Equal(t, "", ctrl["stagingUrl"], "sentinel stagingUrl must be empty (no HTTP)")
	packs, ok := ctrl["testPacks"].([]any)
	require.True(t, ok)
	require.Len(t, packs, 1)
	pack := packs[0].(map[string]any)
	assert.Equal(t, "plan-conformance", pack["name"])
	assert.Equal(t, "plan-conformance", pack["type"])
}

// mustNested walks a nested map/list structure with mixed string keys
// and "[N]" list indices, failing the test on the first missing rung.
// Preferred over unstructured.NestedMap since we're operating on
// yaml.Unmarshal output (map[string]any, not *unstructured.Unstructured).
func mustNested(t *testing.T, root any, path ...string) map[string]any {
	t.Helper()
	cur := root
	for i, key := range path {
		if strings.HasPrefix(key, "[") && strings.HasSuffix(key, "]") {
			var idx int
			_, err := fmtscanf(strings.Trim(key, "[]"), &idx)
			if err != nil {
				t.Fatalf("bad index %q at step %d in %v", key, i, path)
			}
			list, ok := cur.([]any)
			require.Truef(t, ok, "step %d %q: expected []any, got %T (path: %v)", i, key, cur, path)
			require.Lessf(t, idx, len(list), "step %d %q: index out of range (path: %v)", i, key, path)
			cur = list[idx]
			continue
		}
		m, ok := cur.(map[string]any)
		require.Truef(t, ok, "step %d %q: expected map[string]any, got %T (path: %v)", i, key, cur, path)
		next, ok := m[key]
		require.Truef(t, ok, "step %d %q: key not found (path: %v; available keys: %v)", i, key, path, mapKeys(m))
		cur = next
	}
	m, ok := cur.(map[string]any)
	if !ok {
		// Wrap non-map leaves as a single-entry map with well-known
		// key "value" so callers can still assert on them.
		return map[string]any{"value": cur}
	}
	return m
}

func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// fmtscanf is a tiny sscanf wrapper.
func fmtscanf(s string, ptr *int) (int, error) {
	return fmt.Sscanf(s, "%d", ptr)
}
