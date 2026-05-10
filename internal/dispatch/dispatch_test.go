package dispatch

import (
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

func TestJobNameFor_TruncatesLongInput(t *testing.T) {
	got := jobNameFor("an-arrival-name-that-is-far-too-long-to-fit-in-the-k8s-limit", "endeavour")
	if len(got) != 63 {
		t.Errorf("expected exactly 63 chars after truncation, got %d: %q", len(got), got)
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
