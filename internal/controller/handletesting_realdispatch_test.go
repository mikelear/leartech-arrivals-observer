// handletesting_realdispatch_test.go targets the real-dispatch branch
// of handleTesting — polling per-test Job status through a
// dispatch.Dispatcher and finalizing the Arrival when every pack
// settles. The existing controller_test.go covers stub-mode +
// wall-clock timeout; this file covers:
//
//   - all-passed → PhasePassed + finalize
//   - any-failed → PhaseFailed + finalize
//   - mixed running → status.tests patched, no finalize yet
//   - job jobName missing → skipped, no finalize
//   - already-settled tests are not re-polled
//
// Uses dispatch.Dispatcher against a fake kubernetes clientset seeded
// with the Job objects the controller is expected to look up. No
// envtest needed — the dynamic client uses dynamicfake, the Job status
// comes from the batchv1 fake.
package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/mikelear/leartech-arrivals-observer/internal/dispatch"
)

// buildRealDispatcher constructs a dispatch.Dispatcher backed by a fake
// kubernetes clientset carrying the given Jobs. The Dispatcher's
// Config carries just enough to keep buildJob happy — but since these
// tests only exercise GetStatus (not Dispatch), most config is unused.
func buildRealDispatcher(jobs ...*batchv1.Job) *dispatch.Dispatcher {
	objs := make([]runtime.Object, 0, len(jobs))
	for _, j := range jobs {
		objs = append(objs, j)
	}
	cs := fake.NewSimpleClientset(objs...)
	return dispatch.New(dispatch.Config{
		RunnerImage: "ghcr.io/example/runner:test",
		ClusterID:   "gcp",
	}, cs)
}

// jobWithStatus builds a batch.v1 Job with the requested succeeded/
// failed counts. Namespace fixed at jx-staging to match the fake
// dynamic client's namespace.
func jobWithStatus(name string, succeeded, failed int32) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "jx-staging"},
		Status:     batchv1.JobStatus{Succeeded: succeeded, Failed: failed},
	}
}

// arrivalWithTests builds a Testing-phase Arrival carrying the given
// per-test entries as status.tests. Uses the buildArrival helper so
// creationTimestamp / labels stay uniform.
func arrivalWithTests(t *testing.T, name string, tests []map[string]any) *unstructured.Unstructured {
	t.Helper()
	arr := newArrival(name, "canary", "0.0.29", PhaseTesting,
		[]map[string]any{{"name": "smoke", "type": "end2end"}})
	raw := make([]any, len(tests))
	for i, e := range tests {
		raw[i] = e
	}
	_ = unstructured.SetNestedSlice(arr.Object, raw, "status", "tests")
	return arr
}

// TestHandleTesting_RealDispatch_AllPassedFinalizesArrivalPassed —
// every test's Job is Succeeded → Arrival flips to Passed and
// finalizedAt is set.
func TestHandleTesting_RealDispatch_AllPassedFinalizesArrivalPassed(t *testing.T) {
	name := "a"
	arr := arrivalWithTests(t, name, []map[string]any{
		{"name": "smoke", "status": "Running", "jobName": "ar-smoke", "startedAt": time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)},
	})

	c := &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 30 * time.Second,
			Timeout:      10 * time.Minute,
			Dispatcher:   buildRealDispatcher(jobWithStatus("ar-smoke", 1, 0)),
		},
		dynamic:  newTestController(t, arr).dynamic,
		inFlight: map[string]time.Time{name: time.Now().Add(-1 * time.Second)},
	}

	c.handleTesting(context.Background(), arr, name)

	got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	finalized, _, _ := unstructured.NestedString(got.Object, "status", "finalizedAt")
	assert.Equal(t, PhasePassed, phase, "all-Succeeded Jobs must flip Arrival Testing → Passed")
	assert.NotEmpty(t, finalized, "finalizedAt must be recorded on terminal transition")

	c.mu.Lock()
	_, still := c.inFlight[name]
	c.mu.Unlock()
	assert.False(t, still, "inFlight must be cleared after finalize")
}

// TestHandleTesting_RealDispatch_AnyFailedFinalizesArrivalFailed —
// as soon as ONE Job is Failed, the Arrival goes Failed once all
// packs settle.
func TestHandleTesting_RealDispatch_AnyFailedFinalizesArrivalFailed(t *testing.T) {
	name := "b"
	arr := arrivalWithTests(t, name, []map[string]any{
		{"name": "smoke", "status": "Running", "jobName": "ar-smoke", "startedAt": time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)},
		{"name": "heavy", "status": "Running", "jobName": "ar-heavy", "startedAt": time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)},
	})

	c := &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 30 * time.Second,
			Timeout:      10 * time.Minute,
			Dispatcher: buildRealDispatcher(
				jobWithStatus("ar-smoke", 1, 0),
				jobWithStatus("ar-heavy", 0, 1),
			),
		},
		dynamic:  newTestController(t, arr).dynamic,
		inFlight: map[string]time.Time{name: time.Now().Add(-1 * time.Second)},
	}

	c.handleTesting(context.Background(), arr, name)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), name, metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseFailed, phase, "one Failed Job among many must produce Arrival.phase=Failed")

	tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
	require.Len(t, tests, 2)
	// Verify each pack got its terminal status.
	statuses := map[string]string{}
	for _, tt := range tests {
		m := tt.(map[string]any)
		statuses[m["name"].(string)] = m["status"].(string)
	}
	assert.Equal(t, "Passed", statuses["smoke"])
	assert.Equal(t, "Failed", statuses["heavy"])
}

// TestHandleTesting_RealDispatch_MixedRunningNoFinalize — one Job
// Running, one Passed. Must NOT finalize; must patch status.tests.
func TestHandleTesting_RealDispatch_MixedRunningNoFinalize(t *testing.T) {
	name := "c"
	arr := arrivalWithTests(t, name, []map[string]any{
		{"name": "smoke", "status": "Running", "jobName": "ar-smoke", "startedAt": time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)},
		{"name": "heavy", "status": "Running", "jobName": "ar-heavy", "startedAt": time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)},
	})

	c := &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 30 * time.Second,
			Timeout:      10 * time.Minute,
			Dispatcher: buildRealDispatcher(
				jobWithStatus("ar-smoke", 1, 0),
				jobWithStatus("ar-heavy", 0, 0), // still running
			),
		},
		dynamic:  newTestController(t, arr).dynamic,
		inFlight: map[string]time.Time{name: time.Now().Add(-1 * time.Second)},
	}

	c.handleTesting(context.Background(), arr, name)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), name, metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase, "Testing must persist while any Job is still Running")

	tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
	statuses := map[string]string{}
	for _, tt := range tests {
		m := tt.(map[string]any)
		statuses[m["name"].(string)] = m["status"].(string)
	}
	assert.Equal(t, "Passed", statuses["smoke"], "settled Job promoted to Passed")
	assert.Equal(t, "Running", statuses["heavy"], "still-Running Job stays Running")

	c.mu.Lock()
	_, still := c.inFlight[name]
	c.mu.Unlock()
	assert.True(t, still, "inFlight must be kept while Testing continues")
}

// TestHandleTesting_RealDispatch_MissingJobName — a pack with no
// jobName in status.tests must be treated as "still waiting for
// dispatch to be recorded" — no crash, no finalize.
func TestHandleTesting_RealDispatch_MissingJobName(t *testing.T) {
	name := "d"
	arr := arrivalWithTests(t, name, []map[string]any{
		{"name": "smoke", "status": "Running"}, // no jobName
	})

	c := &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 30 * time.Second,
			Timeout:      10 * time.Minute,
			Dispatcher:   buildRealDispatcher(),
		},
		dynamic:  newTestController(t, arr).dynamic,
		inFlight: map[string]time.Time{name: time.Now().Add(-1 * time.Second)},
	}

	c.handleTesting(context.Background(), arr, name)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), name, metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase, "missing jobName must not finalize the Arrival")
}

// TestHandleTesting_RealDispatch_AlreadySettledSkipped — a pack that's
// already Passed in status.tests must NOT be re-polled (the dispatcher
// hasn't been given that job at all — a real-dispatch re-poll would
// return JobUnknown and un-settle the pack).
func TestHandleTesting_RealDispatch_AlreadySettledSkipped(t *testing.T) {
	name := "e"
	arr := arrivalWithTests(t, name, []map[string]any{
		{"name": "smoke", "status": "Passed", "jobName": "ar-smoke", "startedAt": time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)},
	})

	c := &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 30 * time.Second,
			Timeout:      10 * time.Minute,
			Dispatcher:   buildRealDispatcher(),
		},
		dynamic:  newTestController(t, arr).dynamic,
		inFlight: map[string]time.Time{name: time.Now().Add(-1 * time.Second)},
	}

	c.handleTesting(context.Background(), arr, name)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), name, metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	// All packs already settled + Passed → Arrival finalizes to Passed.
	assert.Equal(t, PhasePassed, phase, "already-settled Passed pack must count as done → finalize Passed")
}

// TestHandleTesting_RealDispatch_EmptyStatusTests — a Testing Arrival
// with no status.tests (edge case if the dispatch patch was dropped)
// must not finalize; must just return without touching state.
func TestHandleTesting_RealDispatch_EmptyStatusTests(t *testing.T) {
	name := "f"
	arr := newArrival(name, "canary", "0.0.29", PhaseTesting, nil)
	_ = unstructured.SetNestedSlice(arr.Object, []any{}, "status", "tests")

	c := &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 30 * time.Second,
			Timeout:      10 * time.Minute,
			Dispatcher:   buildRealDispatcher(),
		},
		dynamic:  newTestController(t, arr).dynamic,
		inFlight: map[string]time.Time{name: time.Now().Add(-1 * time.Second)},
	}

	c.handleTesting(context.Background(), arr, name)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), name, metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase, "empty status.tests must be a no-op — don't finalize on no data")
}

// TestHandleTesting_RealDispatch_ControllerRestartReplaysTimer —
// dispatched Arrival, controller restart, first reconcile has no
// inFlight timer → must seed one and return without doing anything.
func TestHandleTesting_RealDispatch_ControllerRestartReplaysTimer(t *testing.T) {
	name := "g"
	arr := arrivalWithTests(t, name, []map[string]any{
		{"name": "smoke", "status": "Running", "jobName": "ar-smoke", "startedAt": time.Now().UTC().Format(time.RFC3339)},
	})

	c := &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 30 * time.Second,
			Timeout:      10 * time.Minute,
			Dispatcher:   buildRealDispatcher(jobWithStatus("ar-smoke", 1, 0)),
		},
		dynamic:  newTestController(t, arr).dynamic,
		inFlight: map[string]time.Time{}, // empty — as if the controller just restarted
	}

	c.handleTesting(context.Background(), arr, name)

	c.mu.Lock()
	_, exists := c.inFlight[name]
	c.mu.Unlock()
	assert.True(t, exists, "controller-restart reconcile of a Testing Arrival must seed inFlight")

	// Phase must not have advanced — the Arrival must be given one
	// full cycle before it's evaluated for finalize.
	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), name, metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase, "restart-replay must not advance phase on first reconcile")
}
