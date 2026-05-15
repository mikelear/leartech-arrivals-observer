package dispatch

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderTemplate_PostDeployContract(t *testing.T) {
	got, err := renderTemplate(
		"results/v1/post-deploy/{{.Cluster}}/{{.Namespace}}/{{.Service}}/{{.Version}}/{{.Pack}}",
		pathVars{
			Cluster: "gcp", Namespace: "jx-staging",
			Service: "leartech-auth-ui", Version: "0.0.36", Pack: "end2end-ui",
		},
	)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := "results/v1/post-deploy/gcp/jx-staging/leartech-auth-ui/0.0.36/end2end-ui"
	if got != want {
		t.Errorf("\n got:  %s\n want: %s", got, want)
	}
}

func TestRenderTemplate_MissingKeyErrors(t *testing.T) {
	// missingkey=error should fail loud rather than render empty —
	// catches typos in the template (.Cluster → .Cluser).
	_, err := renderTemplate("{{.Cluser}}", pathVars{Cluster: "gcp"})
	if err == nil {
		t.Error("expected error on unknown template key, got nil")
	}
}

func TestRenderRefFallbacks(t *testing.T) {
	tmpls := []string{
		"v{{.Version}}-{{.Cluster}}",
		"v{{.Version}}",
		"{{.Version}}",
		"main",
		"", // empty entries dropped
	}
	got, err := renderRefFallbacks(tmpls, refVars{Version: "0.0.36", Cluster: "gcp"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := []string{"v0.0.36-gcp", "v0.0.36", "0.0.36", "main"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("\n got:  %v\n want: %v", got, want)
	}
}

func TestParseResources_Empty(t *testing.T) {
	for _, in := range []string{"", "{}"} {
		r, err := ParseResources(in)
		if err != nil {
			t.Errorf("input %q: %v", in, err)
		}
		if len(r.Requests) != 0 || len(r.Limits) != 0 {
			t.Errorf("input %q: expected empty struct, got %+v", in, r)
		}
	}
}

func TestStagingHostBase(t *testing.T) {
	tests := []struct {
		name, url, service, want string
	}{
		{"strips service prefix on AZ",
			"https://leartech-auth-service-jx-staging.az.leartech.com", "leartech-auth-service",
			"jx-staging.az.leartech.com"},
		{"strips service prefix on GCP",
			"https://leartech-auth-service-jx-staging.jx.leartech.com", "leartech-auth-service",
			"jx-staging.jx.leartech.com"},
		{"empty URL returns empty",
			"", "leartech-auth-service",
			""},
		{"empty service returns full host",
			"https://example.com:8080/path", "",
			"example.com:8080"},
		{"URL without service prefix returns full host",
			"https://only-host.example.com", "leartech-auth-service",
			"only-host.example.com"},
		{"malformed URL returns empty",
			"://bad", "leartech-auth-service",
			""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := stagingHostBase(tc.url, tc.service)
			if got != tc.want {
				t.Errorf("stagingHostBase(%q, %q) = %q, want %q", tc.url, tc.service, got, tc.want)
			}
		})
	}
}

func TestJobNameFor(t *testing.T) {
	tests := []struct {
		arrival, pack, want string
	}{
		{"foo", "smoke", "ar-foo-smoke"},
		{"With_Underscores", "End2End", "ar-with-underscores-end2end"},
		{"a-very-very-very-very-very-very-very-very-long-arrival", "p", "ar-a-very-very-very-very-very-very-very-very-long-arrival-p"},
	}
	for _, tc := range tests {
		got := jobNameFor(tc.arrival, tc.pack)
		if len(got) > 63 {
			t.Errorf("jobNameFor(%q, %q) = %q (len %d), exceeds k8s 63-char limit", tc.arrival, tc.pack, got, len(got))
		}
		if got != tc.want {
			t.Errorf("jobNameFor(%q, %q) = %q, want %q", tc.arrival, tc.pack, got, tc.want)
		}
	}
}

func TestJobNameFor_LongArrival_PreservesPackName(t *testing.T) {
	// The original bug: arrival names approaching ~50 chars + a pack name
	// like "end2end-ui" exceed 63, and blunt truncation chopped the pack
	// off entirely so the controller couldn't find the Job back.
	arr := "leartech-angular-service-template-0-0-13-jx-staging"
	got := jobNameFor(arr, "end2end-ui")
	if len(got) > 63 {
		t.Errorf("len(got)=%d exceeds 63: %q", len(got), got)
	}
	if !strings.HasSuffix(got, "-end2end-ui") {
		t.Errorf("pack name lost on truncation: %q", got)
	}
	if !strings.HasPrefix(got, "ar-") {
		t.Errorf("expected ar- prefix: %q", got)
	}
}

func TestJobNameFor_LongArrival_StableAcrossCalls(t *testing.T) {
	// The status-poll path retrieves jobs by name later; same input must
	// yield the same name across observer restarts.
	arr := "leartech-angular-service-template-0-0-13-jx-staging"
	a := jobNameFor(arr, "end2end-ui")
	b := jobNameFor(arr, "end2end-ui")
	if a != b {
		t.Errorf("expected deterministic output, got %q vs %q", a, b)
	}
}

func TestJobNameFor_LongArrival_DifferentArrivalsDiffer(t *testing.T) {
	// Hash should differentiate between two long-named arrivals — collisions
	// at the 4-byte-hash level are negligible per service.
	a := jobNameFor("leartech-angular-service-template-0-0-13-jx-staging", "end2end-ui")
	b := jobNameFor("leartech-angular-service-template-0-0-14-jx-staging", "end2end-ui")
	if a == b {
		t.Errorf("two distinct arrivals produced the same job name: %q", a)
	}
}

func TestJobNameFor_ExtremelyLongPack_TruncatedSafely(t *testing.T) {
	// Realistic pack names are short, but the truncation branch is exercised
	// here to lock the invariant.
	veryLongPack := strings.Repeat("x", 100)
	got := jobNameFor("a-very-very-very-very-very-very-very-very-long-arrival-name", veryLongPack)
	if len(got) > 63 {
		t.Errorf("len(got)=%d exceeds 63: %q", len(got), got)
	}
	if !strings.HasSuffix(got, "x") {
		t.Errorf("pack name must not end with trailing dash: %q", got)
	}
}

func TestSanitizeLabel(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"v1.2.3", "v1.2.3"},
		{"V1.2.3", "v1.2.3"},
		{"v1.2.3+build42", "v1.2.3-build42"},
		{"feature/branch_name", "feature-branch_name"},
	}
	for _, tc := range tests {
		got := sanitizeLabel(tc.in)
		if got != tc.want {
			t.Errorf("sanitizeLabel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseResources_Real(t *testing.T) {
	in := `{"requests":{"cpu":"250m","memory":"512Mi"},"limits":{"cpu":"1500m","memory":"2Gi"}}`
	r, err := ParseResources(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if r.Requests.Cpu().String() != "250m" {
		t.Errorf("requests.cpu = %s, want 250m", r.Requests.Cpu().String())
	}
	if r.Limits.Memory().String() != "2Gi" {
		t.Errorf("limits.memory = %s, want 2Gi", r.Limits.Memory().String())
	}
}

// TestRunnerScript_ResultsJSONOverridesTestExit locks the contract that the
// dispatcher's inline runner script translates results.json.success=false
// into a non-zero exit code for end2end packs — even when the catalog's
// shared run.sh exits 0 (its intentional design for PR-time uploads).
//
// Without this translation, K8s Job.Status.Succeeded=1, controller marks
// Arrival.phase=Passed, and forensics-runner is never triggered — exactly
// the bug discovered on 2026-05-11 (canary 0.0.7 deliberate-fail demo
// showed phase=Passed despite 1/2 checks failing).
//
// The script test runs the override fragment as a standalone bash snippet
// against synthetic results.json fixtures. Doesn't exercise the full
// runner (which needs a kube client + GCS + a real service); validates the
// translation logic in isolation.
func TestRunnerScript_ResultsJSONOverridesTestExit(t *testing.T) {
	// Mirrors the fragment in runnerScript (kept literal here so a test
	// failure pinpoints regressions — if you change the script, change
	// this too).
	fragment := `
if [ "$TEST_PACK_TYPE" = "end2end" ] && [ "$TEST_EXIT" -eq 0 ] && [ -f results.json ]; then
  REPORTED_SUCCESS=""
  if command -v jq >/dev/null 2>&1; then
    REPORTED_SUCCESS=$(jq -r '.success' results.json 2>/dev/null || true)
  else
    if grep -qE '"success"[[:space:]]*:[[:space:]]*false' results.json; then
      REPORTED_SUCCESS="false"
    fi
  fi
  if [ "$REPORTED_SUCCESS" = "false" ]; then
    TEST_EXIT=1
  fi
fi
exit $TEST_EXIT
`
	cases := []struct {
		name       string
		packType   string
		startExit  string
		results    string
		wantExit   int
		wantReason string
	}{
		{
			name:       "end2end success:false overrides exit 0 → 1",
			packType:   "end2end",
			startExit:  "0",
			results:    `{"success":false,"summary":"1/2 checks passed"}`,
			wantExit:   1,
			wantReason: "deliberate fail must produce Failed Arrival so forensics fires",
		},
		{
			name:       "end2end success:true keeps exit 0",
			packType:   "end2end",
			startExit:  "0",
			results:    `{"success":true,"summary":"2/2 checks passed"}`,
			wantExit:   0,
			wantReason: "passing tests must not be downgraded",
		},
		{
			name:       "end2end pre-existing non-zero exit preserved",
			packType:   "end2end",
			startExit:  "2",
			results:    `{"success":true}`,
			wantExit:   2,
			wantReason: "guard `TEST_EXIT -eq 0` prevents masking earlier failures (image pull, runner crash, etc.)",
		},
		{
			name:       "end2end-ui not gated (Playwright exit propagates natively)",
			packType:   "end2end-ui",
			startExit:  "0",
			results:    `{"success":false,"summary":"1/2 failed"}`,
			wantExit:   0,
			wantReason: "Playwright exit code is already correct; don't second-guess",
		},
		{
			name:       "end2end missing results.json keeps exit 0",
			packType:   "end2end",
			startExit:  "0",
			results:    "", // empty → don't write the file
			wantExit:   0,
			wantReason: "no results.json could mean runner crashed before write; preserve existing exit",
		},
		{
			name:       "end2end malformed results.json (jq fallback) preserves exit",
			packType:   "end2end",
			startExit:  "0",
			results:    `not even json`,
			wantExit:   0,
			wantReason: "jq returns 'true' for missing .success; grep fallback only triggers on exact pattern",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.results != "" {
				if err := os.WriteFile(filepath.Join(dir, "results.json"), []byte(tc.results), 0o600); err != nil {
					t.Fatalf("write fixture: %v", err)
				}
			}
			cmd := exec.Command("bash", "-eo", "pipefail", "-c", fragment)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(),
				"TEST_PACK_TYPE="+tc.packType,
				"TEST_EXIT="+tc.startExit,
			)
			err := cmd.Run()
			gotExit := 0
			if exitErr, ok := err.(*exec.ExitError); ok {
				gotExit = exitErr.ExitCode()
			} else if err != nil {
				t.Fatalf("bash run failed unexpectedly: %v", err)
			}
			if gotExit != tc.wantExit {
				t.Errorf("exit code = %d, want %d (%s)", gotExit, tc.wantExit, tc.wantReason)
			}
		})
	}
}

// TestRunnerScript_FragmentMatchesEmbedded is a defensive coupling check —
// the literal fragment in TestRunnerScript_ResultsJSONOverridesTestExit
// MUST stay in sync with the override block inside runnerScript. If you
// edit one, edit the other (no clever sharing because runnerScript is a
// large template literal interpolated with config; isolating the fragment
// without breaking that interpolation would obscure both).
//
// This test fails loudly if the canonical override string drifts away from
// what we test against — a forcing function to update both at once.
func TestRunnerScript_FragmentMatchesEmbedded(t *testing.T) {
	required := []string{
		`if [ "$TEST_PACK_TYPE" = "end2end" ] && [ "$TEST_EXIT" -eq 0 ] && [ -f results.json ]; then`,
		`REPORTED_SUCCESS=$(jq -r '.success' results.json 2>/dev/null || true)`,
		`if grep -qE '"success"[[:space:]]*:[[:space:]]*false' results.json; then`,
		`if [ "$REPORTED_SUCCESS" = "false" ]; then`,
		`TEST_EXIT=1`,
	}
	for _, want := range required {
		if !strings.Contains(runnerScript, want) {
			t.Errorf("runnerScript no longer contains expected fragment:\n  %s", want)
		}
	}
}
