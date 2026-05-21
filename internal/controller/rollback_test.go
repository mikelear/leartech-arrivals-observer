package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/mikelear/leartech-arrivals-observer/internal/forensics"
)

// newTestControllerWithForensics extends newTestController by wiring a
// real forensics.Dispatcher backed by a fake kubernetes.Interface so
// tests can inspect the Job spec that maybeDispatchForensics produces.
// The dispatcher's runner image is set so Dispatch doesn't early-return
// as disabled. Pre-existing Arrival CRs are seeded into the dynamic
// fake via Create so findPreviousVersion's List can discover them.
func newTestControllerWithForensics(t *testing.T, objs ...*unstructured.Unstructured) (*Controller, *fake.Clientset) {
	t.Helper()
	c := newTestController(t)
	for _, u := range objs {
		_, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
			Create(context.Background(), u, metav1.CreateOptions{})
		require.NoError(t, err, "seed Arrival %s", u.GetName())
	}
	kc := fake.NewSimpleClientset()
	c.cfg.Forensics = forensics.New(forensics.Config{
		Enabled:               true,
		RunnerImage:           "ghcr.io/example/forensics-runner:test",
		ClusterID:             "gcp",
		TempoBaseURL:          "http://tempo.test:3200",
		WindowMinutes:         5,
		LatencyRatio:          1.5,
		ErrorRateDelta:        0.05,
		ContextTimeoutMinutes: 5,
		MinBaselineSamples:    1,
		ResultStoreBucket:     "test-bucket",
		GCSKeySecret:          "test-secret",
	}, kc)
	return c, kc
}

// envValue returns the value of the named env var on the dispatched
// forensics Job, or "" if absent. Asserts the Job exists.
func envValue(t *testing.T, kc *fake.Clientset, jobName, varName string) string {
	t.Helper()
	job, err := kc.BatchV1().Jobs("jx-staging").Get(context.Background(), jobName, metav1.GetOptions{})
	require.NoError(t, err, "forensics Job %s must exist", jobName)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers, "Job must have containers")
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == varName {
			return e.Value
		}
	}
	return ""
}

// TestMaybeDispatchForensics_RollbackPassesV13AsPrevious is the end-to-
// end pin for the rollback semantics discussion 2026-05-21. Full chain:
//
//	v1.3 Passed (CR day 0)       — the chronological predecessor
//	v1.4 Passed (CR day 1)       — the rollback target being re-tested
//	v1.5 Failed (CR day 2)       — the version rolled back FROM
//
// When the controller dispatches forensics on the v1.4 re-test, the Job
// must carry PREVIOUS_VERSION="1.3", NOT "1.5", because v1.5's CR was
// created after v1.4's and is filtered by the creationTimestamp cutoff.
// This test exercises the full code path (findPreviousVersion → Dispatch
// → buildJob) and asserts on the materialised Job spec, not just the
// helper function in isolation.
func TestMaybeDispatchForensics_RollbackPassesV13AsPrevious(t *testing.T) {
	now := time.Now()
	v14CreatedAt := now.Add(-48 * time.Hour)

	v13 := newArrival("svc-1-3-jx-staging", "svc", "1.3", PhasePassed, nil)
	v13.SetCreationTimestamp(metav1.NewTime(now.Add(-72 * time.Hour)))
	_ = unstructured.SetNestedField(v13.Object, now.Add(-71*time.Hour).Format(time.RFC3339), "status", "finalizedAt")

	v14 := newArrival("svc-1-4-jx-staging", "svc", "1.4", PhasePassed, nil)
	v14.SetCreationTimestamp(metav1.NewTime(v14CreatedAt))
	_ = unstructured.SetNestedField(v14.Object, now.Add(-47*time.Hour).Format(time.RFC3339), "status", "finalizedAt")
	_ = unstructured.SetNestedField(v14.Object, now.Format(time.RFC3339), "spec", "deployedAt")

	v15 := newArrival("svc-1-5-jx-staging", "svc", "1.5", PhaseFailed, nil)
	v15.SetCreationTimestamp(metav1.NewTime(now.Add(-24 * time.Hour)))
	_ = unstructured.SetNestedField(v15.Object, now.Add(-23*time.Hour).Format(time.RFC3339), "status", "finalizedAt")

	c, kc := newTestControllerWithForensics(t, v13, v14, v15)

	c.maybeDispatchForensics(context.Background(), v14)

	prev := envValue(t, kc, "forensics-svc-1-4-jx-staging", "PREVIOUS_VERSION")
	assert.Equal(t, "1.3", prev, "rollback retest must compare v1.4 against v1.3 (true predecessor), NOT v1.5 (just-rolled-back-from)")

	ver := envValue(t, kc, "forensics-svc-1-4-jx-staging", "VERSION")
	assert.Equal(t, "1.4", ver)
	svc := envValue(t, kc, "forensics-svc-1-4-jx-staging", "SERVICE")
	assert.Equal(t, "svc", svc)
}

// TestMaybeDispatchForensics_FirstDeploy_NoPrevious — when a service
// has never been deployed before, findPreviousVersion returns "" and the
// runner gets PREVIOUS_VERSION="" (treated as "single-window snapshot").
func TestMaybeDispatchForensics_FirstDeploy_NoPrevious(t *testing.T) {
	now := time.Now()
	first := newArrival("brandnew-0-0-1-jx-staging", "brandnew", "0.0.1", PhasePassed, nil)
	first.SetCreationTimestamp(metav1.NewTime(now.Add(-1 * time.Hour)))
	_ = unstructured.SetNestedField(first.Object, now.Add(-30*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	c, kc := newTestControllerWithForensics(t, first)
	c.maybeDispatchForensics(context.Background(), first)

	prev := envValue(t, kc, "forensics-brandnew-0-0-1-jx-staging", "PREVIOUS_VERSION")
	assert.Empty(t, prev, "first-deploy must pass empty PREVIOUS_VERSION (runner falls back to single-window snapshot)")
}

// TestMaybeDispatchForensics_NoForensicsCfg_NoOp — when c.cfg.Forensics
// is nil the controller short-circuits and never calls Dispatch. Pins the
// "graceful disable" pattern (same shape as the test-runner dispatcher).
func TestMaybeDispatchForensics_NoForensicsCfg_NoOp(t *testing.T) {
	now := time.Now()
	arr := newArrival("svc-1-0-jx-staging", "svc", "1.0", PhasePassed, nil)
	arr.SetCreationTimestamp(metav1.NewTime(now))

	c := newTestController(t, arr)
	// c.cfg.Forensics deliberately nil — should be safe to call finalize
	// path; the controller checks for nil before invoking the helper.
	c.finalize(context.Background(), arr, PhasePassed)
	// No assertion on a Job — the test passes by not panicking.
}

// TestFindPreviousVersion_FailedPredecessor — when the chronological
// predecessor is itself a Failed Arrival (not Passed), it still counts:
// Failed is a real terminal phase and a meaningful baseline (the test
// suite ran, just produced a Failed verdict). Only Skipped is excluded.
func TestFindPreviousVersion_FailedPredecessor(t *testing.T) {
	now := time.Now()

	prev := newArrival("svc-0-9-jx-staging", "svc", "0.9", PhaseFailed, nil)
	prev.SetCreationTimestamp(metav1.NewTime(now.Add(-2 * time.Hour)))
	_ = unstructured.SetNestedField(prev.Object, now.Add(-90*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	c := newTestController(t, prev)
	got := c.findPreviousVersion(context.Background(), "svc", "1.0", now)
	assert.Equal(t, "0.9", got, "Failed predecessor must be picked — phase=Failed is a real terminal signal, not noise like Skipped")
}

// TestFindPreviousVersion_TimeoutPredecessor — same as above for Timeout.
func TestFindPreviousVersion_TimeoutPredecessor(t *testing.T) {
	now := time.Now()
	prev := newArrival("svc-0-9-jx-staging", "svc", "0.9", PhaseTimeout, nil)
	prev.SetCreationTimestamp(metav1.NewTime(now.Add(-2 * time.Hour)))
	_ = unstructured.SetNestedField(prev.Object, now.Add(-90*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	c := newTestController(t, prev)
	got := c.findPreviousVersion(context.Background(), "svc", "1.0", now)
	assert.Equal(t, "0.9", got, "Timeout predecessor must be picked")
}

// TestFindPreviousVersion_AllCandidatesNewerThanCutoff — when every
// other Arrival was created AFTER the cutoff, none qualify. Returns
// empty string. This is the symmetric case to the rollback test:
// proves the cutoff filter is strict in both directions.
func TestFindPreviousVersion_AllCandidatesNewerThanCutoff(t *testing.T) {
	now := time.Now()
	cutoff := now.Add(-3 * time.Hour) // pretend "current" was created 3h ago

	newer := newArrival("svc-1-1-jx-staging", "svc", "1.1", PhasePassed, nil)
	newer.SetCreationTimestamp(metav1.NewTime(now.Add(-1 * time.Hour))) // 1h ago = AFTER cutoff
	_ = unstructured.SetNestedField(newer.Object, now.Add(-30*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	c := newTestController(t, newer)
	got := c.findPreviousVersion(context.Background(), "svc", "1.0", cutoff)
	assert.Empty(t, got, "no candidate older than cutoff → empty")
}

// TestFindPreviousVersion_PicksLatestByFinalizedAtWhenCreationsTie —
// when two prior Arrivals share the same creationTimestamp (unusual but
// possible if CRs were imported), the tie-breaker is finalizedAt.
func TestFindPreviousVersion_PicksLatestByFinalizedAtWhenCreationsTie(t *testing.T) {
	now := time.Now()
	sharedCreation := now.Add(-2 * time.Hour)

	earlierFinalize := newArrival("svc-0-8-jx-staging", "svc", "0.8", PhasePassed, nil)
	earlierFinalize.SetCreationTimestamp(metav1.NewTime(sharedCreation))
	_ = unstructured.SetNestedField(earlierFinalize.Object, now.Add(-105*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	laterFinalize := newArrival("svc-0-9-jx-staging", "svc", "0.9", PhasePassed, nil)
	laterFinalize.SetCreationTimestamp(metav1.NewTime(sharedCreation))
	_ = unstructured.SetNestedField(laterFinalize.Object, now.Add(-60*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	c := newTestController(t, earlierFinalize, laterFinalize)
	got := c.findPreviousVersion(context.Background(), "svc", "1.0", now)
	assert.Equal(t, "0.9", got, "when creationTimestamps tie, pick the one with the later finalizedAt")
}
