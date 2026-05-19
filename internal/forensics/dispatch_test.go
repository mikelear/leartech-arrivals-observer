package forensics

import (
	"strings"
	"testing"
)

func TestRenderTemplate(t *testing.T) {
	got, err := renderTemplate(
		"forensics/v1/{{.Cluster}}/{{.Namespace}}/{{.Service}}/{{.Version}}",
		struct {
			Cluster, Namespace, Service, Version string
		}{"gcp", "jx-staging", "leartech-auth-ui", "0.0.36"},
	)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := "forensics/v1/gcp/jx-staging/leartech-auth-ui/0.0.36"
	if got != want {
		t.Errorf("\n got:  %s\n want: %s", got, want)
	}
}

func TestRenderTemplate_EmptyTemplate(t *testing.T) {
	got, err := renderTemplate("", nil)
	if err != nil || got != "" {
		t.Errorf("renderTemplate(empty) = (%q, %v), want (\"\", nil)", got, err)
	}
}

func TestRenderTemplate_MissingKeyErrors(t *testing.T) {
	_, err := renderTemplate("{{.Nonexistent}}", struct{ Cluster string }{"gcp"})
	if err == nil {
		t.Error("expected error on unknown template key, got nil")
	}
}

func TestJobNameFor(t *testing.T) {
	got := jobNameFor("Some_Arrival_Name")
	want := "forensics-some-arrival-name"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestJobNameFor_TruncatesAt63(t *testing.T) {
	got := jobNameFor(strings.Repeat("a", 80))
	if len(got) != 63 {
		t.Errorf("expected exactly 63 chars, got %d", len(got))
	}
}

func TestBuildJob_IssueCreationEnvWired(t *testing.T) {
	d := &Dispatcher{cfg: Config{
		Enabled:               true,
		RunnerImage:           "registry.example.com/forensics-runner:0.0.11",
		EnableIssueCreation:   true,
		IssueRepoOwner:        "leartech",
		ResultStoreBucket:     "test-bucket",
		ClusterID:             "gcp",
		WindowMinutes:         5,
		LatencyRatio:          1.5,
		ErrorRateDelta:        0.05,
		ContextTimeoutMinutes: 5,
	}}
	job := d.buildJob(Args{
		ArrivalName:      "svc-0-0-1-jx-staging",
		ArrivalNamespace: "jx-staging",
		Service:          "svc",
		Version:          "0.0.1",
	}, "forensics-svc")

	env := job.Spec.Template.Spec.Containers[0].Env
	get := func(name string) string {
		for _, e := range env {
			if e.Name == name {
				return e.Value
			}
		}
		return ""
	}
	if got := get("ENABLE_ISSUE_CREATION"); got != "true" {
		t.Errorf("ENABLE_ISSUE_CREATION = %q, want %q", got, "true")
	}
	if got := get("ISSUE_REPO_OWNER"); got != "leartech" {
		t.Errorf("ISSUE_REPO_OWNER = %q, want %q", got, "leartech")
	}
	// GITHUB_TOKEN comes via SecretKeyRef (no .Value) — verify the
	// var exists with a tekton-git/password SecretKeyRef.
	found := false
	for _, e := range env {
		if e.Name == "GITHUB_TOKEN" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			if e.ValueFrom.SecretKeyRef.Name == "tekton-git" && e.ValueFrom.SecretKeyRef.Key == "password" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("GITHUB_TOKEN env not wired via SecretKeyRef{Name:tekton-git, Key:password}")
	}
	_ = strings.Contains // keep strings import alive in case it was already used above
}

func TestBuildJob_MinBaselineSamplesEnvWired(t *testing.T) {
	d := &Dispatcher{cfg: Config{
		Enabled:            true,
		RunnerImage:        "registry.example.com/forensics-runner:0.0.12",
		MinBaselineSamples: 1, // canary opt-out
	}}
	job := d.buildJob(Args{ArrivalName: "n", ArrivalNamespace: "ns"}, "j")
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "MIN_BASELINE_SAMPLES" {
			if e.Value != "1" {
				t.Errorf("MIN_BASELINE_SAMPLES = %q, want %q", e.Value, "1")
			}
			return
		}
	}
	t.Error("MIN_BASELINE_SAMPLES env not present")
}

func TestBuildJob_IssueCreationDefaultFalse(t *testing.T) {
	d := &Dispatcher{cfg: Config{
		Enabled:     true,
		RunnerImage: "x:1",
		// EnableIssueCreation deliberately unset (zero value = false)
	}}
	job := d.buildJob(Args{ArrivalName: "n", ArrivalNamespace: "ns"}, "j")
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "ENABLE_ISSUE_CREATION" {
			if e.Value != "false" {
				t.Errorf("ENABLE_ISSUE_CREATION default = %q, want %q", e.Value, "false")
			}
			return
		}
	}
	t.Error("ENABLE_ISSUE_CREATION env not present")
}
