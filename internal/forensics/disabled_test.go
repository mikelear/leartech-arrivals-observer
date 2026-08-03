package forensics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
)

// TestNew_ReturnsWiredInstance covers the trivial constructor + the
// disabled-guard branches of Dispatch that fire when the runner image
// is unset or forensics is disabled entirely — both are legitimate
// production modes and shouldn't error.
func TestNew_ReturnsWiredInstance(t *testing.T) {
	d := New(Config{}, fake.NewSimpleClientset())
	require.NotNil(t, d)
}

// TestDispatch_Disabled_ReturnsEmptyNoError — forensics.enabled=false
// short-circuits Dispatch. Returns (jobName="", err=nil).
func TestDispatch_Disabled_ReturnsEmptyNoError(t *testing.T) {
	d := &Dispatcher{
		clients: fake.NewSimpleClientset(),
		cfg:     Config{Enabled: false, RunnerImage: "runner:test"},
	}
	got, err := d.Dispatch(context.Background(), Args{ArrivalName: "a", ArrivalNamespace: "ns"})
	require.NoError(t, err)
	assert.Empty(t, got, "disabled dispatcher must return empty job name")
}

// TestDispatch_MissingRunnerImage_ReturnsEmpty — even if Enabled=true,
// an empty RunnerImage counts as disabled (chart bootstrap didn't wire
// the image). Prevents dispatching a broken Job.
func TestDispatch_MissingRunnerImage_ReturnsEmpty(t *testing.T) {
	d := &Dispatcher{
		clients: fake.NewSimpleClientset(),
		cfg:     Config{Enabled: true, RunnerImage: ""},
	}
	got, err := d.Dispatch(context.Background(), Args{ArrivalName: "a", ArrivalNamespace: "ns"})
	require.NoError(t, err)
	assert.Empty(t, got, "missing runner image must return empty job name (safe no-op)")
}

// TestBuildJob_PathTemplateRenderFailureFallsBackToEmpty — a malformed
// path template shouldn't crash the dispatch; runner falls back to the
// built-in default. The env var must be present with an empty value.
func TestBuildJob_PathTemplateRenderFailureFallsBackToEmpty(t *testing.T) {
	d := &Dispatcher{
		clients: fake.NewSimpleClientset(),
		cfg: Config{
			Enabled:               true,
			RunnerImage:           "runner:test",
			ForensicsPathTemplate: "{{.MissingKey}}", // will fail to render
			ClusterID:             "gcp",
		},
	}
	job := d.buildJob(Args{
		ArrivalName:      "a",
		ArrivalNamespace: "ns",
		Service:          "svc",
		Version:          "1.0",
	}, "forensics-a")

	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "RESULT_STORE_PATH_PREFIX" {
			assert.Empty(t, e.Value, "path-template render failure must produce empty RESULT_STORE_PATH_PREFIX (runner falls back)")
			return
		}
	}
	t.Error("RESULT_STORE_PATH_PREFIX env not present")
}
