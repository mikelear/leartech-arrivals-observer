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
