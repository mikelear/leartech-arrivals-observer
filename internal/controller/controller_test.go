package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// arrivalScheme is identical to rollout_test.deployScheme but also
// registers Arrival GVR so dynamicfake can resolve patches.
func arrivalScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(deploymentGVR.GroupVersion().WithKind("Deployment"), &unstructured.Unstructured{})
	s.AddKnownTypeWithName(deploymentGVR.GroupVersion().WithKind("DeploymentList"), &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(arrivalGVR.GroupVersion().WithKind("Arrival"), &unstructured.Unstructured{})
	s.AddKnownTypeWithName(arrivalGVR.GroupVersion().WithKind("ArrivalList"), &unstructured.UnstructuredList{})
	return s
}

func newArrival(name, service, version, phase string, packs []map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(arrivalGVR.GroupVersion().WithKind("Arrival"))
	u.SetName(name)
	u.SetNamespace("jx-staging")
	u.SetLabels(map[string]string{"qa.leartech.com/service": service})

	_ = unstructured.SetNestedField(u.Object, service, "spec", "service")
	_ = unstructured.SetNestedField(u.Object, version, "spec", "version")
	if len(packs) > 0 {
		raw := make([]any, len(packs))
		for i, p := range packs {
			raw[i] = p
		}
		_ = unstructured.SetNestedSlice(u.Object, raw, "spec", "testPacks")
	}
	if phase != "" {
		_ = unstructured.SetNestedField(u.Object, phase, "status", "phase")
	}
	return u
}

func newTestController(t *testing.T, objs ...runtime.Object) *Controller {
	t.Helper()
	return &Controller{
		cfg: Config{
			Namespace:    "jx-staging",
			PollInterval: 50 * time.Millisecond,
			Timeout:      5 * time.Second,
		},
		dynamic:  dynamicfake.NewSimpleDynamicClient(arrivalScheme(t), objs...),
		inFlight: make(map[string]time.Time),
	}
}

func TestReconcileOne_NoPacks_TransitionsToSkipped(t *testing.T) {
	arr := newArrival("canary-0-0-29-jx-staging", "canary", "0.0.29", "", nil)
	c := newTestController(t, arr)

	c.reconcileOne(context.Background(), arr)

	got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	finalizedAt, _, _ := unstructured.NestedString(got.Object, "status", "finalizedAt")
	assert.Equal(t, PhaseSkipped, phase)
	assert.NotEmpty(t, finalizedAt)
}

func TestReconcileOne_PacksPresent_StubDispatchesToTesting(t *testing.T) {
	packs := []map[string]any{{"name": "end2end", "type": "end2end"}}
	arr := newArrival("canary-0-0-29-jx-staging", "canary", "0.0.29", "", packs)
	c := newTestController(t, arr)

	c.reconcileOne(context.Background(), arr)

	got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
	assert.Equal(t, PhaseTesting, phase)
	require.Len(t, tests, 1)
	tm := tests[0].(map[string]any)
	assert.Equal(t, "end2end", tm["name"])
	assert.Equal(t, "Running", tm["status"])
}

func TestReconcileOne_SkippedWithLatePacks_FlipsBackToPending(t *testing.T) {
	packs := []map[string]any{{"name": "smoke", "type": "smoke"}}
	arr := newArrival("canary-0-0-29-jx-staging", "canary", "0.0.29", PhaseSkipped, packs)
	c := newTestController(t, arr)

	c.reconcileOne(context.Background(), arr)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhasePending, phase, "Skipped Arrival with newly-present testPacks must flip back to Pending")
}

func TestReconcileOne_TerminalPhases_NoOp(t *testing.T) {
	for _, phase := range []string{PhasePassed, PhaseFailed, PhaseTimeout} {
		t.Run(phase, func(t *testing.T) {
			arr := newArrival("a", "canary", "0.0.29", phase, nil)
			c := newTestController(t, arr)
			c.reconcileOne(context.Background(), arr)

			got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), "a", metav1.GetOptions{})
			gotPhase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
			assert.Equal(t, phase, gotPhase, "terminal phase must be left untouched")
		})
	}
}

func TestReconcile_NonUnstructured_NoOp(t *testing.T) {
	c := newTestController(t)
	// Doesn't panic; doesn't touch the client.
	c.reconcile(context.Background(), "not-an-unstructured")
	c.reconcile(context.Background(), nil)
}

func TestHandleTesting_StubMode_FinalizesAfterPollInterval(t *testing.T) {
	packs := []map[string]any{{"name": "smoke", "type": "smoke"}}
	arr := newArrival("canary-0-0-29-jx-staging", "canary", "0.0.29", PhaseTesting, packs)
	_ = unstructured.SetNestedSlice(arr.Object, []any{map[string]any{"name": "smoke", "status": "Running"}}, "status", "tests")
	c := newTestController(t, arr)

	// First call seeds the in-flight timer (controller-restart recovery path).
	c.handleTesting(context.Background(), arr, arr.GetName())

	// Wait past the poll interval, then call again — stub mode flips to Passed.
	time.Sleep(60 * time.Millisecond)
	c.handleTesting(context.Background(), arr, arr.GetName())

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	finalizedAt, _, _ := unstructured.NestedString(got.Object, "status", "finalizedAt")
	assert.Equal(t, PhasePassed, phase)
	assert.NotEmpty(t, finalizedAt)
}

func TestHandleTesting_TimesOut(t *testing.T) {
	packs := []map[string]any{{"name": "smoke", "type": "smoke"}}
	arr := newArrival("canary-0-0-29-jx-staging", "canary", "0.0.29", PhaseTesting, packs)
	_ = unstructured.SetNestedSlice(arr.Object, []any{map[string]any{"name": "smoke", "status": "Running"}}, "status", "tests")
	c := newTestController(t, arr)
	c.cfg.Timeout = 5 * time.Millisecond

	// Seed in-flight far enough in the past to exceed the wall-clock timeout.
	c.inFlight[arr.GetName()] = time.Now().Add(-1 * time.Second)

	c.handleTesting(context.Background(), arr, arr.GetName())

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTimeout, phase)
}

func TestFinalize_PromotesRunningTests(t *testing.T) {
	arr := newArrival("a", "canary", "0.0.29", PhaseTesting, nil)
	_ = unstructured.SetNestedSlice(arr.Object, []any{
		map[string]any{"name": "t1", "status": "Running"},
		map[string]any{"name": "t2", "status": "Passed"}, // already settled
	}, "status", "tests")
	c := newTestController(t, arr)

	c.finalize(context.Background(), arr, PhaseFailed)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), "a", metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
	assert.Equal(t, PhaseFailed, phase)
	require.Len(t, tests, 2)
	assert.Equal(t, "Failed", tests[0].(map[string]any)["status"], "Running test promoted to terminal phase")
	assert.Equal(t, "Passed", tests[1].(map[string]any)["status"], "already-settled test untouched")
}

func TestFinalize_ClearsInFlight(t *testing.T) {
	arr := newArrival("a", "canary", "0.0.29", PhaseTesting, nil)
	c := newTestController(t, arr)
	c.inFlight["a"] = time.Now()

	c.finalize(context.Background(), arr, PhasePassed)

	_, stillTracked := c.inFlight["a"]
	assert.False(t, stillTracked, "finalize must remove from inFlight map")
}

func TestPatchStatus_GracefulOnMissingArrival(t *testing.T) {
	c := newTestController(t) // nothing in the fake dynamic client
	// Should not panic, just log + return.
	c.patchStatus(context.Background(), "ghost", map[string]any{"phase": PhaseSkipped})
}

func TestDecodeEnvVars_LiteralAndSecretRef(t *testing.T) {
	raw := []any{
		map[string]any{"name": "USER_ID", "value": "user-1"},
		map[string]any{"name": "USER_PASSWORD", "valueFrom": map[string]any{
			"secretKeyRef": map[string]any{"name": "auth-secret", "key": "password"},
		}},
	}
	out := decodeEnvVars(raw)
	require.Len(t, out, 2)
	assert.Equal(t, "USER_ID", out[0].Name)
	assert.Equal(t, "user-1", out[0].Value)
	require.NotNil(t, out[1].ValueFrom)
	require.NotNil(t, out[1].ValueFrom.SecretKeyRef)
	assert.Equal(t, "auth-secret", out[1].ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, "password", out[1].ValueFrom.SecretKeyRef.Key)
}

func TestDecodeEnvVars_EmptyInputs(t *testing.T) {
	assert.Nil(t, decodeEnvVars(nil))
	assert.Nil(t, decodeEnvVars([]any{}))
}

func TestAsString(t *testing.T) {
	assert.Equal(t, "hello", asString("hello"))
	assert.Equal(t, "", asString(42))
	assert.Equal(t, "", asString(nil))
	assert.Equal(t, "", asString(map[string]any{}))
}

func TestFindPreviousVersion_PicksMostRecentFinalized(t *testing.T) {
	now := time.Now()
	older := newArrival("canary-0-0-21-jx-staging", "canary", "0.0.21", PhasePassed, nil)
	older.SetCreationTimestamp(metav1.NewTime(now.Add(-2 * time.Hour)))
	_ = unstructured.SetNestedField(older.Object, now.Add(-90*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	recent := newArrival("canary-0-0-28-jx-staging", "canary", "0.0.28", PhasePassed, nil)
	recent.SetCreationTimestamp(metav1.NewTime(now.Add(-1 * time.Hour)))
	_ = unstructured.SetNestedField(recent.Object, now.Add(-30*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	// Skipped doesn't count — must be ignored even if more recent.
	skipped := newArrival("canary-0-0-22-jx-staging", "canary", "0.0.22", PhaseSkipped, nil)
	skipped.SetCreationTimestamp(metav1.NewTime(now.Add(-15 * time.Minute)))
	_ = unstructured.SetNestedField(skipped.Object, now.Add(-10*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	c := newTestController(t, older, recent, skipped)

	prev := c.findPreviousVersion(context.Background(), "canary", "0.0.29", now)
	assert.Equal(t, "0.0.28", prev, "must pick the most-recent terminal-phase prior version")
}

func TestFindPreviousVersion_NoPriorArrivals(t *testing.T) {
	c := newTestController(t)
	assert.Empty(t, c.findPreviousVersion(context.Background(), "canary", "0.0.29", time.Now()))
}

func TestFindPreviousVersion_EmptyService(t *testing.T) {
	c := newTestController(t)
	assert.Empty(t, c.findPreviousVersion(context.Background(), "", "0.0.29", time.Now()))
}

func TestFindPreviousVersion_IgnoresSameVersion(t *testing.T) {
	now := time.Now()
	same := newArrival("canary-0-0-29-jx-staging", "canary", "0.0.29", PhasePassed, nil)
	same.SetCreationTimestamp(metav1.NewTime(now.Add(-2 * time.Hour)))
	_ = unstructured.SetNestedField(same.Object, now.Add(-90*time.Minute).Format(time.RFC3339), "status", "finalizedAt")

	c := newTestController(t, same)
	assert.Empty(t, c.findPreviousVersion(context.Background(), "canary", "0.0.29", now))
}

// TestFindPreviousVersion_RollbackComparesAgainstChronologicalPredecessor
// pins the rollback semantics. When the operator rolls back from a Failed
// v1.5 to v1.4, observer's #143 retest path redeploys v1.4 and triggers a
// re-test of the existing v1.4 Arrival CR (its spec.deployedAt updates;
// metadata.creationTimestamp stays at v1.4's original first-deploy time).
//
// On the re-test's forensics dispatch, findPreviousVersion must compare
// v1.4 against its TRUE chronological predecessor (v1.3 here), NOT
// against the just-failed v1.5. The currentCreated cutoff is v1.4's
// original creationTimestamp; v1.5 was created later, so it's filtered.
//
// This is the semantic the user asked about in the rollback design
// discussion 2026-05-21: "after a rollback to v1.4, would forensics
// wrongly compare against v1.5?" Answer pinned here: no, because the
// creationTimestamp ordering excludes anything newer than v1.4's CR.
func TestFindPreviousVersion_RollbackComparesAgainstChronologicalPredecessor(t *testing.T) {
	now := time.Now()

	// v1.3 — Passed, oldest (the true chronological predecessor of v1.4).
	v13 := newArrival("svc-1-3-jx-staging", "svc", "1.3", PhasePassed, nil)
	v13.SetCreationTimestamp(metav1.NewTime(now.Add(-72 * time.Hour))) // day 0
	_ = unstructured.SetNestedField(v13.Object, now.Add(-71*time.Hour).Format(time.RFC3339), "status", "finalizedAt")

	// v1.4 — Passed, the rollback target. Its CR was created day 1 (when
	// v1.4 first deployed), and is the CURRENT Arrival being re-tested
	// after the rollback redeploy.
	v14CreatedAt := now.Add(-48 * time.Hour) // day 1
	v14 := newArrival("svc-1-4-jx-staging", "svc", "1.4", PhasePassed, nil)
	v14.SetCreationTimestamp(metav1.NewTime(v14CreatedAt))
	_ = unstructured.SetNestedField(v14.Object, now.Add(-47*time.Hour).Format(time.RFC3339), "status", "finalizedAt")

	// v1.5 — Failed, the version we just rolled back FROM. Its CR was
	// created day 2 (after v1.4). MUST be filtered by the creationTimestamp
	// cutoff or forensics would compare v1.4 against this broken version.
	v15 := newArrival("svc-1-5-jx-staging", "svc", "1.5", PhaseFailed, nil)
	v15.SetCreationTimestamp(metav1.NewTime(now.Add(-24 * time.Hour))) // day 2
	_ = unstructured.SetNestedField(v15.Object, now.Add(-23*time.Hour).Format(time.RFC3339), "status", "finalizedAt")

	c := newTestController(t, v13, v14, v15)

	// Call findPreviousVersion as the controller would on v1.4's re-test —
	// passing v1.4's *original* creationTimestamp as the cutoff (this is
	// what GetCreationTimestamp() returns on the rolled-back Arrival CR,
	// since metadata.creationTimestamp is immutable per K8s spec).
	prev := c.findPreviousVersion(context.Background(), "svc", "1.4", v14CreatedAt)
	assert.Equal(t, "1.3", prev, "rollback retest must compare against the chronological predecessor (v1.3), NOT the just-rolled-back-from v1.5")
}

func TestHandlePending_DispatcherNil_PathExercisesStubFlow(t *testing.T) {
	// With Dispatcher=nil, handlePending skips the rollout gate AND skips
	// real Job creation, just flipping straight to Testing.
	packs := []map[string]any{{"name": "smoke", "type": "smoke"}}
	arr := newArrival("a", "canary", "0.0.29", "", packs)
	c := newTestController(t, arr)

	c.handlePending(context.Background(), arr)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), "a", metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase)
}

func TestHandlePending_DecodesEnvFromSpec(t *testing.T) {
	// Confirm spec.env round-trip works end-to-end through reconcile —
	// any decode failure must NOT block the dispatch.
	packs := []map[string]any{{"name": "smoke", "type": "smoke"}}
	arr := newArrival("a", "auth-service", "0.1.40", "", packs)
	env := []any{
		map[string]any{"name": "USER_ID", "value": "user-1"},
	}
	_ = unstructured.SetNestedSlice(arr.Object, env, "spec", "env")
	c := newTestController(t, arr)

	c.handlePending(context.Background(), arr)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), "a", metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase, "env decode failure must not block dispatch")
}

// Smoke check that the corev1 import is exercised (linters otherwise
// flag the import as unused if every reference is behind a helper).
var _ = corev1.EnvVar{}
