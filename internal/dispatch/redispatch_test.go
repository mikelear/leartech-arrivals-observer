package dispatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// newTestDispatcher builds a Dispatcher backed by a fake clientset
// (optionally seeded with pre-existing Jobs to simulate the rollout-
// restart re-dispatch case). Config carries enough to render a real
// Job spec via buildJob.
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
			RunnerImage:          "ghcr.io/example/runner:test",
			GCSKeySecret:         "test-artifacts-gcs-key",
			ResultStoreBucket:    "test-bucket",
			ClusterID:            "gcp",
			RepoHost:             "github.com",
			RepoOrg:              "mikelear",
			RefFallbackTemplates: []string{"v{{.Version}}"},
			HealthEndpoint:       "/health/live",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{"cpu": resource.MustParse("100m")},
				Limits:   corev1.ResourceList{"cpu": resource.MustParse("500m")},
			},
			PostDeployPathTemplate: "results/v1/post-deploy/{{.Cluster}}/{{.Namespace}}/{{.Service}}/{{.Version}}/{{.Pack}}",
		},
	}
}

func TestDispatch_HappyPath_CreatesJob(t *testing.T) {
	d := newTestDispatcher()
	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName: "canary-0-0-29-jx-staging",
		Namespace:   "jx-staging",
		Service:     "canary",
		Version:     "0.0.29",
		StagingURL:  "https://canary-jx-staging.gcp.leartech.com",
	}, []Test{{PackName: "smoke", PackType: "end2end"}})
	require.NoError(t, err)
	assert.Equal(t, "ar-canary-0-0-29-jx-staging-smoke", got["smoke"])

	_, err = d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), got["smoke"], metav1.GetOptions{})
	require.NoError(t, err, "Job must be created in the fake clientset")
}

func TestDispatch_AlreadyExists_DeletesAndRecreates(t *testing.T) {
	// Stale Failed Job sitting at the deterministic name — what the
	// rollout-restart retest path (#143) will see when it tries to
	// re-dispatch after operator runs `kubectl rollout restart`.
	staleJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "ar-canary-0-0-29-jx-staging-smoke",
			Namespace:   "jx-staging",
			Annotations: map[string]string{"prior-run": "stale-failed"},
		},
	}
	d := newTestDispatcher(staleJob)

	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName: "canary-0-0-29-jx-staging",
		Namespace:   "jx-staging",
		Service:     "canary",
		Version:     "0.0.29",
		StagingURL:  "https://canary-jx-staging.gcp.leartech.com",
	}, []Test{{PackName: "smoke", PackType: "end2end"}})
	require.NoError(t, err, "Dispatch must succeed by deleting + recreating, NOT fail with AlreadyExists")
	assert.Equal(t, "ar-canary-0-0-29-jx-staging-smoke", got["smoke"])

	cur, err := d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), got["smoke"], metav1.GetOptions{})
	require.NoError(t, err)
	_, hasStaleAnnotation := cur.Annotations["prior-run"]
	assert.False(t, hasStaleAnnotation, "recreated Job must not carry the stale Job's annotations")
}

func TestDispatch_AlreadyExists_RecreatedJobHasFreshSpec(t *testing.T) {
	staleJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ar-canary-0-0-29-jx-staging-smoke",
			Namespace: "jx-staging",
		},
		Spec: batchv1.JobSpec{},
	}
	d := newTestDispatcher(staleJob)

	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName: "canary-0-0-29-jx-staging",
		Namespace:   "jx-staging",
		Service:     "canary",
		Version:     "0.0.29",
		StagingURL:  "https://canary-jx-staging.gcp.leartech.com",
	}, []Test{{PackName: "smoke", PackType: "end2end"}})
	require.NoError(t, err)

	cur, err := d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), got["smoke"], metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, cur.Spec.Template.Spec.Containers, "recreated Job must have the fresh runner container spec, not the stale empty Spec")
	assert.NotEmpty(t, cur.Spec.Template.Spec.Containers[0].Env, "recreated Job must have env wired from current Dispatch call")
}

func TestDeleteJobAndWait_Idempotent_GoneAlready(t *testing.T) {
	d := newTestDispatcher()
	err := d.deleteJobAndWait(context.Background(), "jx-staging", "nonexistent-job")
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
