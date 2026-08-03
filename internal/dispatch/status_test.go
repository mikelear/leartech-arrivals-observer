// status_test.go covers Dispatcher.GetStatus + Dispatcher.GetFailureReason —
// the read-side of the controller's polling loop, previously
// under-tested because the fast unit path in controller_test uses a
// nil dispatcher (stub mode). These tests exercise the real polling
// path against a fake clientset.
package dispatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// TestNewDispatcher_ReturnsWiredInstance — bare constructor smoke test.
// Confirms Dispatcher's clients + cfg fields land where GetStatus /
// Dispatch will find them.
func TestNewDispatcher_ReturnsWiredInstance(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cfg := Config{RunnerImage: "runner:test"}
	d := New(cfg, cs)
	require.NotNil(t, d)
	// Round-trip a create so we know the client is wired.
	_, err := d.clients.BatchV1().Jobs("ns").Create(context.Background(),
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "smoke", Namespace: "ns"}},
		metav1.CreateOptions{})
	require.NoError(t, err)
}

// TestGetStatus_NotFound returns JobUnknown without error — the caller
// (controller.handleTesting) uses this to keep waiting.
func TestGetStatus_NotFound(t *testing.T) {
	d := newTestDispatcher()
	got, err := d.GetStatus(context.Background(), "jx-staging", "no-such-job")
	require.NoError(t, err)
	assert.Equal(t, JobUnknown, got)
}

// TestGetStatus_Succeeded_ReturnsPassed
func TestGetStatus_Succeeded_ReturnsPassed(t *testing.T) {
	d := newTestDispatcher(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "ok", Namespace: "jx-staging"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	})
	got, err := d.GetStatus(context.Background(), "jx-staging", "ok")
	require.NoError(t, err)
	assert.Equal(t, JobPassed, got)
}

// TestGetStatus_Failed_ReturnsFailed
func TestGetStatus_Failed_ReturnsFailed(t *testing.T) {
	d := newTestDispatcher(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "nope", Namespace: "jx-staging"},
		Status:     batchv1.JobStatus{Failed: 1},
	})
	got, err := d.GetStatus(context.Background(), "jx-staging", "nope")
	require.NoError(t, err)
	assert.Equal(t, JobFailed, got)
}

// TestGetStatus_Running_ReturnsRunning — Job exists, no terminal status.
func TestGetStatus_Running_ReturnsRunning(t *testing.T) {
	d := newTestDispatcher(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "mid", Namespace: "jx-staging"},
		Status:     batchv1.JobStatus{Active: 1},
	})
	got, err := d.GetStatus(context.Background(), "jx-staging", "mid")
	require.NoError(t, err)
	assert.Equal(t, JobRunning, got)
}

// TestGetFailureReason_OOMKilled — the observed pod carries
// containerStatuses[].state.terminated.reason=OOMKilled. Feeds the
// controller's OOM counter.
func TestGetFailureReason_OOMKilled(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "runner-pod",
			Namespace: "jx-staging",
			Labels:    map[string]string{"job-name": "target-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"},
				},
			}},
		},
	}
	d := newTestDispatcherWithObjects(pod)
	got, err := d.GetFailureReason(context.Background(), "jx-staging", "target-job")
	require.NoError(t, err)
	assert.Equal(t, "OOMKilled", got)
}

// TestGetFailureReason_LastTerminationStateFallback — if the current
// state is Running (mid-restart) but LastTerminationState carries the
// reason, we still return it.
func TestGetFailureReason_LastTerminationStateFallback(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "runner-pod",
			Namespace: "jx-staging",
			Labels:    map[string]string{"job-name": "restart-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "Error"},
				},
			}},
		},
	}
	d := newTestDispatcherWithObjects(pod)
	got, err := d.GetFailureReason(context.Background(), "jx-staging", "restart-job")
	require.NoError(t, err)
	assert.Equal(t, "Error", got)
}

// TestGetFailureReason_NoTerminatedState — pod exists but no
// terminated block; returns empty (caller treats as "no signal").
func TestGetFailureReason_NoTerminatedState(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "runner-pod",
			Namespace: "jx-staging",
			Labels:    map[string]string{"job-name": "running-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	d := newTestDispatcherWithObjects(pod)
	got, err := d.GetFailureReason(context.Background(), "jx-staging", "running-job")
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestGetFailureReason_NoPods — no pods matching the job label; empty
// reason (caller keeps polling).
func TestGetFailureReason_NoPods(t *testing.T) {
	d := newTestDispatcher()
	got, err := d.GetFailureReason(context.Background(), "jx-staging", "no-pods-job")
	require.NoError(t, err)
	assert.Empty(t, got)
}

// newTestDispatcherWithObjects seeds arbitrary runtime.Objects (Jobs,
// Pods, …) into the fake clientset. Complements newTestDispatcher
// (which only takes Jobs).
func newTestDispatcherWithObjects(objs ...runtime.Object) *Dispatcher {
	cs := fake.NewSimpleClientset(objs...)
	return &Dispatcher{
		clients: cs,
		cfg: Config{
			RunnerImage:            "runner:test",
			GCSKeySecret:           "gcs-key",
			ResultStoreBucket:      "test-bucket",
			ClusterID:              "gcp",
			PostDeployPathTemplate: "results/v1/post-deploy/{{.Cluster}}/{{.Namespace}}/{{.Service}}/{{.Version}}/{{.Pack}}",
		},
	}
}
