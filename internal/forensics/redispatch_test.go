package forensics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// newTestDispatcher builds a forensics Dispatcher backed by a fake
// clientset (optionally seeded with pre-existing Jobs to simulate the
// rollout-restart re-dispatch case from #143). Config carries enough
// to render a real Job spec via buildJob.
func newTestDispatcher(seedJobs ...*batchv1.Job) *Dispatcher {
	var cs *fake.Clientset
	switch len(seedJobs) {
	case 0:
		cs = fake.NewSimpleClientset()
	case 1:
		cs = fake.NewSimpleClientset(seedJobs[0])
	case 2:
		cs = fake.NewSimpleClientset(seedJobs[0], seedJobs[1])
	default:
		cs = fake.NewSimpleClientset(seedJobs[0])
	}
	return &Dispatcher{
		clients: cs,
		cfg: Config{
			Enabled:               true,
			RunnerImage:           "ghcr.io/example/forensics-runner:test",
			TempoBaseURL:          "http://tempo.jx-observability:3200",
			WindowMinutes:         5,
			GCSKeySecret:          "test-artifacts-gcs-key",
			ResultStoreBucket:     "test-bucket",
			ClusterID:             "gcp",
			LatencyRatio:          1.5,
			ErrorRateDelta:        0.05,
			ContextTimeoutMinutes: 5,
			MinBaselineSamples:    1,
		},
	}
}

func TestDispatch_HappyPath_CreatesJob(t *testing.T) {
	d := newTestDispatcher()
	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName:      "canary-0-0-32-jx-staging",
		ArrivalNamespace: "jx-staging",
		Service:          "canary",
		Version:          "0.0.32",
		PreviousVersion:  "0.0.30",
		DeployedAt:       "2026-05-19T16:29:56Z",
	})
	require.NoError(t, err)
	assert.Equal(t, "forensics-canary-0-0-32-jx-staging", got)

	_, err = d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), got, metav1.GetOptions{})
	require.NoError(t, err, "forensics Job must be created in the fake clientset")
}

// TestDispatch_AlreadyExists_DeletesAndRecreates is the regression test
// for the bug that hid the canary 0.0.32 Issue-creation demo on 2026-05-20:
// when the controller re-dispatches forensics after a rollout-restart
// retest (per #143), the prior cycle's Job still exists at the
// deterministic name. The OLD code path silently logged "reusing" and
// returned success without creating a fresh Job — so Arrival.status.forensics
// kept showing the stale verdict and no new Issue could be filed.
//
// The fix mirrors the test-runner dispatcher's #144 pattern: on
// AlreadyExists, delete-then-recreate. This test pins that behaviour.
func TestDispatch_AlreadyExists_DeletesAndRecreates(t *testing.T) {
	staleJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "forensics-canary-0-0-32-jx-staging",
			Namespace:   "jx-staging",
			Annotations: map[string]string{"prior-run": "stale"},
		},
	}
	d := newTestDispatcher(staleJob)

	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName:      "canary-0-0-32-jx-staging",
		ArrivalNamespace: "jx-staging",
		Service:          "canary",
		Version:          "0.0.32",
		PreviousVersion:  "0.0.30",
		DeployedAt:       "2026-05-19T16:29:56Z",
	})
	require.NoError(t, err, "Dispatch must succeed by deleting + recreating, NOT silently reuse stale Job")
	assert.Equal(t, "forensics-canary-0-0-32-jx-staging", got)

	cur, err := d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), got, metav1.GetOptions{})
	require.NoError(t, err)
	_, hasStaleAnnotation := cur.Annotations["prior-run"]
	assert.False(t, hasStaleAnnotation, "recreated Job must not carry the stale Job's annotations")
}

// TestDispatch_AlreadyExists_RecreatedJobHasFreshSpec — confirms the
// recreated Job has the fresh runner container spec (with current env
// like MIN_BASELINE_SAMPLES, ENABLE_ISSUE_CREATION) rather than the
// stale Job's empty Spec being silently preserved.
func TestDispatch_AlreadyExists_RecreatedJobHasFreshSpec(t *testing.T) {
	staleJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "forensics-canary-0-0-32-jx-staging",
			Namespace: "jx-staging",
		},
		Spec: batchv1.JobSpec{}, // empty spec — pretend it's a malformed prior run
	}
	d := newTestDispatcher(staleJob)

	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName:      "canary-0-0-32-jx-staging",
		ArrivalNamespace: "jx-staging",
		Service:          "canary",
		Version:          "0.0.32",
		PreviousVersion:  "0.0.30",
		DeployedAt:       "2026-05-19T16:29:56Z",
	})
	require.NoError(t, err)

	cur, err := d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), got, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, cur.Spec.Template.Spec.Containers, "recreated Job must have the fresh runner container spec")

	// Verify MIN_BASELINE_SAMPLES env survived the recreate (this Dispatcher's
	// Config sets it to 1; if the recreate accidentally reused the stale empty
	// spec, this assertion fails).
	env := cur.Spec.Template.Spec.Containers[0].Env
	found := false
	for _, e := range env {
		if e.Name == "MIN_BASELINE_SAMPLES" && e.Value == "1" {
			found = true
			break
		}
	}
	assert.True(t, found, "recreated forensics Job must carry MIN_BASELINE_SAMPLES=1 from the current Config, proving the Spec was rebuilt — not silently reused")
}

func TestDeleteJobAndWait_Idempotent_GoneAlready(t *testing.T) {
	d := newTestDispatcher()
	err := d.deleteJobAndWait(context.Background(), "jx-staging", "nonexistent-forensics-job")
	require.NoError(t, err, "delete when already gone must be a no-op")
}

func TestDeleteJobAndWait_DeletesExisting(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "victim", Namespace: "jx-staging"},
	}
	d := newTestDispatcher(job)

	require.NoError(t, d.deleteJobAndWait(context.Background(), "jx-staging", "victim"))

	_, err := d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), "victim", metav1.GetOptions{})
	require.Error(t, err, "Job must be gone after deleteJobAndWait")
}

func TestDeleteJobAndWait_ContextCancelled(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "victim", Namespace: "jx-staging"},
	}
	d := newTestDispatcher(job)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.deleteJobAndWait(ctx, "jx-staging", "victim")
	// fake.Clientset may complete the delete before honouring the cancel —
	// both no-error AND context.Canceled are acceptable behaviour.
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}
