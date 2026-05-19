package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clienttesting "k8s.io/client-go/testing"
)

// TestHandlePending_ReentryGuard reproduces the dotnet-template@0.0.8
// failure pattern (2026-05-18) where the leader pod's reconcile ticker
// observed Phase=Pending from a stale informer cache after Dispatch +
// patchStatus had already executed. Without the inFlight guard at the
// top of handlePending, this leads to double-dispatch → AlreadyExists →
// #144's delete+recreate path → Foreground propagation timeout →
// spurious Failed.
//
// Guard semantics: first call sets inFlight + patches Phase=Testing;
// concurrent / stale-cache re-entry short-circuits without re-patching.
func TestHandlePending_ReentryGuard_SecondCallShortCircuits(t *testing.T) {
	packs := []map[string]any{{"name": "end2end", "type": "end2end"}}
	arr := newArrival("dotnet-0-0-8-jx-staging", "dotnet", "0.0.8", "", packs)
	c := newTestController(t, arr)

	// First call simulates the initial reconcile after the Arrival was
	// created. With no Dispatcher configured, handlePending follows the
	// stub path: sets inFlight + patches status (Phase=Testing, tests).
	c.handlePending(context.Background(), arr)

	// Snapshot state after first call.
	c.mu.Lock()
	startedAt1 := c.inFlight["dotnet-0-0-8-jx-staging"]
	c.mu.Unlock()
	require.False(t, startedAt1.IsZero(), "first handlePending must populate inFlight")

	got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	tests1, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
	assert.Equal(t, PhaseTesting, phase)
	require.Len(t, tests1, 1, "first call must dispatch the test pack")

	// Count actions before the re-entry call so we can verify the guard
	// adds zero new actions (no re-patch, no dispatch).
	beforeActions := countPatchActions(c, "arrivals")

	// Simulate a slightly delayed re-reconcile from a stale informer
	// read — same Arrival snapshot (still has the original empty
	// status.phase from the test setup), called again.
	time.Sleep(1 * time.Millisecond) // ensure time.Now() would differ
	c.handlePending(context.Background(), arr)

	c.mu.Lock()
	startedAt2 := c.inFlight["dotnet-0-0-8-jx-staging"]
	c.mu.Unlock()
	assert.Equal(t, startedAt1, startedAt2, "inFlight time must NOT update on re-entry — second call should short-circuit before set")

	afterActions := countPatchActions(c, "arrivals")
	assert.Equal(t, beforeActions, afterActions, "second handlePending must add zero new patch actions (re-entry guard short-circuited)")
}

// TestHandlePending_NoPacks_NoInFlightLeak ensures the Skipped path
// doesn't populate inFlight — Skipped arrivals are terminal immediately,
// no dispatch happened, so no inFlight slot to clean up.
func TestHandlePending_NoPacks_NoInFlightLeak(t *testing.T) {
	arr := newArrival("canary-no-packs-jx-staging", "canary", "0.0.1", "", nil)
	c := newTestController(t, arr)

	c.handlePending(context.Background(), arr)

	c.mu.Lock()
	_, exists := c.inFlight["canary-no-packs-jx-staging"]
	c.mu.Unlock()
	assert.False(t, exists, "Skipped path must not populate inFlight (no dispatch happened)")

	got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseSkipped, phase)
}

// TestHandlePending_FinalizeClearsInFlight verifies that a terminal
// transition through finalize removes the inFlight entry, so a
// rollout-restart-retest (#143 → terminal → Pending → handlePending)
// can dispatch again without being short-circuited by stale inFlight.
func TestHandlePending_FinalizeClearsInFlight(t *testing.T) {
	packs := []map[string]any{{"name": "end2end", "type": "end2end"}}
	arr := newArrival("svc-0-0-1-jx-staging", "svc", "0.0.1", "", packs)
	c := newTestController(t, arr)

	c.handlePending(context.Background(), arr)
	c.mu.Lock()
	_, before := c.inFlight["svc-0-0-1-jx-staging"]
	c.mu.Unlock()
	require.True(t, before, "inFlight should be populated after handlePending")

	// Re-fetch the arrival so finalize sees Phase=Testing (else the
	// patch operates on a stale snapshot with Phase="").
	got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	c.finalize(context.Background(), got, PhaseFailed)

	c.mu.Lock()
	_, after := c.inFlight["svc-0-0-1-jx-staging"]
	c.mu.Unlock()
	assert.False(t, after, "finalize must clear inFlight so rollout-restart retest can re-dispatch")
}

// countPatchActions counts the number of patch operations the fake
// dynamic client has recorded for the given resource (e.g., "arrivals").
// dynamicfake records every Action through the tracker.
func countPatchActions(c *Controller, resource string) int {
	if t, ok := c.dynamic.(interface{ Actions() []clienttesting.Action }); ok {
		n := 0
		for _, a := range t.Actions() {
			if a.GetVerb() == "patch" && a.GetResource().Resource == resource {
				n++
			}
		}
		return n
	}
	return 0
}
