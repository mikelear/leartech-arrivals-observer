// envtest_reconcile_test.go is the high-fidelity reconcile scenario
// asked for in the initiative — a real kube-apiserver + etcd, no
// kubelet, driving the SAME controller code the production controller
// uses. Complements the fake-based unit tests, which are fast but
// forgiving on subtleties fakes elide (patch semantics, /status
// subresource, CRD schema rejection).
//
// Gated by envtest_harness_test.go's requireEnvtest — when assets are
// missing, each test SKIPS with a message pointing at KUBEBUILDER_ASSETS.
package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestEnvtest_ArrivalLifecycle_CreateDispatchTerminal drives an
// Arrival end-to-end against a live apiserver:
//
//  1. Create the CR (empty status).
//  2. Controller reconcile: with test packs present, Pending → Testing.
//     status.tests populated.
//  3. Force finalize with PhasePassed → terminal + finalizedAt set.
//
// The interesting difference vs the fake path: the apiserver returns
// real conflict / not-found errors, validates the CR against the
// installed CRD schema, and enforces the status-subresource split.
// If any assumption in reconcileOne / handlePending / finalize breaks
// against real k8s API semantics, this test flags it — the fake
// clients happily do things the apiserver won't.
func TestEnvtest_ArrivalLifecycle_CreateDispatchTerminal(t *testing.T) {
	_, _, dyn, ns := requireEnvtest(t)

	arr := newArrivalCR(ns, "envtest-lifecycle", "canary", "0.0.29",
		[]map[string]any{{"name": "smoke", "type": "end2end"}})

	ctx := context.Background()
	created, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Create(ctx, arr, metav1.CreateOptions{})
	require.NoError(t, err, "CRD schema must accept a well-formed Arrival")

	c := &Controller{
		cfg: Config{
			Namespace:    ns,
			PollInterval: 100 * time.Millisecond,
			Timeout:      1 * time.Second,
		},
		dynamic:  dyn,
		inFlight: make(map[string]time.Time),
	}

	// Reconcile — packs present → dispatch (stub-mode since no Dispatcher).
	c.reconcileOne(ctx, created)

	// Re-fetch and confirm phase advanced to Testing.
	got, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Get(ctx, "envtest-lifecycle", metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase, "reconcile against real apiserver must transition Pending → Testing")

	tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
	require.Len(t, tests, 1, "status.tests must carry the dispatched pack")
	tm := tests[0].(map[string]any)
	assert.Equal(t, "smoke", tm["name"])
	assert.Equal(t, "Running", tm["status"])

	// Force finalize as Passed — mirrors what handleTesting would do
	// after the poll interval elapses in stub mode.
	c.finalize(ctx, got, PhasePassed)

	// Refetch. Terminal phase + finalizedAt must be set.
	final, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Get(ctx, "envtest-lifecycle", metav1.GetOptions{})
	require.NoError(t, err)
	finalPhase, _, _ := unstructured.NestedString(final.Object, "status", "phase")
	finalizedAt, _, _ := unstructured.NestedString(final.Object, "status", "finalizedAt")
	assert.Equal(t, PhasePassed, finalPhase, "finalize must promote Testing → Passed")
	assert.NotEmpty(t, finalizedAt, "finalize must set status.finalizedAt on terminal transition")
}

// TestEnvtest_ArrivalLifecycle_SkippedWhenNoPacks — an Arrival with no
// testPacks must go straight to Skipped through the real apiserver
// (which enforces the CRD's phase enum + status subresource split).
func TestEnvtest_ArrivalLifecycle_SkippedWhenNoPacks(t *testing.T) {
	_, _, dyn, ns := requireEnvtest(t)

	arr := newArrivalCR(ns, "envtest-skipped", "svc", "0.0.1", nil)
	ctx := context.Background()
	created, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Create(ctx, arr, metav1.CreateOptions{})
	require.NoError(t, err)

	c := &Controller{
		cfg: Config{
			Namespace:    ns,
			PollInterval: 100 * time.Millisecond,
			Timeout:      1 * time.Second,
		},
		dynamic:  dyn,
		inFlight: make(map[string]time.Time),
	}
	c.reconcileOne(ctx, created)

	got, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Get(ctx, "envtest-skipped", metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseSkipped, phase, "empty packs → Skipped through real apiserver")
	finalizedAt, _, _ := unstructured.NestedString(got.Object, "status", "finalizedAt")
	assert.NotEmpty(t, finalizedAt, "Skipped is terminal — finalizedAt must be set")
}

// TestEnvtest_StatusSubresourceIsolatesSpec — the CRD marks status as
// a subresource, so patchStatus writes to /status. This test proves
// the split works against a real apiserver: reading spec after a
// status patch must still return the ORIGINAL spec (no clobber).
//
// Belt-and-braces regression guard for #144's earlier delete-recreate
// path: if we ever regressed patchStatus into patching the whole
// object, spec.testPacks could be silently wiped by a bare merge-patch.
func TestEnvtest_StatusSubresourceIsolatesSpec(t *testing.T) {
	_, _, dyn, ns := requireEnvtest(t)

	arr := newArrivalCR(ns, "envtest-subres", "svc", "0.0.1",
		[]map[string]any{{"name": "smoke", "type": "end2end"}})
	ctx := context.Background()
	created, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Create(ctx, arr, metav1.CreateOptions{})
	require.NoError(t, err)

	c := &Controller{
		cfg:      Config{Namespace: ns, PollInterval: 100 * time.Millisecond, Timeout: 1 * time.Second},
		dynamic:  dyn,
		inFlight: make(map[string]time.Time),
	}

	// Reconcile (patches status).
	c.reconcileOne(ctx, created)

	got, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Get(ctx, "envtest-subres", metav1.GetOptions{})
	require.NoError(t, err)

	// Spec must be UNCHANGED — status patches don't touch it.
	specPacks, _, _ := unstructured.NestedSlice(got.Object, "spec", "testPacks")
	require.Len(t, specPacks, 1, "spec.testPacks must survive status subresource patches")
	pm := specPacks[0].(map[string]any)
	assert.Equal(t, "smoke", pm["name"])
	assert.Equal(t, "end2end", pm["type"])

	// Status must show the transition.
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phase)
}
