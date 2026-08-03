// invariants_test.go carries the property-style safety invariants of
// the Arrival lifecycle — properties that MUST hold for any input the
// controller might see in production. Complements
// state_machine_conformance_test.go (which pins the positive shape of
// each individual transition) by driving the machine with many inputs
// and asserting the properties never break.
//
// Mirrors orchestrator-controller's agentrun_terminal_invariants_test.go
// in spirit — "for any sequence of events, terminal outputs never lie."
// The state space is small enough that we can enumerate combinatorially
// rather than reach for a rapid/gopter-style property engine.
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

// TestInvariant_TerminalArrivalNeverSilentlyRegresses drives every
// terminal Arrival (Passed, Failed, Timeout) through repeated
// reconciles under conditions that do NOT satisfy the rollout-retest
// gate (missing timestamps, deployedAt earlier than finalizedAt, or
// within cooldown). None of them may un-terminalise the Arrival.
//
// The invariant: FOR ALL terminal phases × non-retest-satisfying
// preconditions × reconcile-count, the phase stays put.
func TestInvariant_TerminalArrivalNeverSilentlyRegresses(t *testing.T) {
	now := time.Now().UTC()

	preconditions := []struct {
		name        string
		deployedAt  string
		finalizedAt string
	}{
		{"no timestamps", "", ""},
		{"only deployedAt", now.Format(time.RFC3339), ""},
		{"only finalizedAt", "", now.Format(time.RFC3339)},
		{"deployedAt before finalizedAt", now.Add(-1 * time.Hour).Format(time.RFC3339), now.Add(-30 * time.Minute).Format(time.RFC3339)},
		{"deployedAt equal finalizedAt", now.Format(time.RFC3339), now.Format(time.RFC3339)},
		{"deployedAt inside cooldown", now.Add(-1 * time.Minute).Format(time.RFC3339), now.Add(-2 * time.Minute).Format(time.RFC3339)},
		{"deployedAt exactly at cooldown boundary", now.Format(time.RFC3339), now.Add(-5 * time.Minute).Format(time.RFC3339)},
	}

	for _, phase := range []string{PhasePassed, PhaseFailed, PhaseTimeout} {
		for _, pre := range preconditions {
			t.Run(phase+"/"+pre.name, func(t *testing.T) {
				arr := newArrival("invariant-arr", "canary", "0.0.29", phase, smokePack())
				if pre.deployedAt != "" {
					_ = unstructured.SetNestedField(arr.Object, pre.deployedAt, "spec", "deployedAt")
				}
				if pre.finalizedAt != "" {
					_ = unstructured.SetNestedField(arr.Object, pre.finalizedAt, "status", "finalizedAt")
				}
				c := newTestController(t, arr)

				// Reconcile 5 times — the invariant must hold under repeated
				// firings from ticker, informer add, informer update.
				for i := 0; i < 5; i++ {
					c.reconcileOne(context.Background(), arr)
					got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
						Get(context.Background(), arr.GetName(), metav1.GetOptions{})
					require.NoError(t, err)
					gotPhase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
					assert.Equal(t, phase, gotPhase,
						"iteration %d: terminal phase must not regress under precondition %q", i, pre.name)
				}
			})
		}
	}
}

// TestInvariant_RolloutRetestFiresAtMostOncePerCooldown asserts that a
// realistic burst of ReplicaSet-add events during a single rolling
// restart — which produces N reconcile events with the SAME deployedAt
// — triggers exactly ONE terminal→Pending transition. Repeated
// reconciles observing the same rollout must not each fire a fresh
// retest cycle.
//
// The mechanic: after the first reconcile flips terminal→Pending, the
// next reconcile sees Phase=Pending (not terminal) and either dispatches
// (packs present) or Skips. Neither of those observes deployedAt/
// finalizedAt again — so the rollout-retest gate cannot re-trigger from
// the same replica-add burst.
func TestInvariant_RolloutRetestFiresAtMostOncePerCooldown(t *testing.T) {
	for _, phase := range []string{PhasePassed, PhaseFailed, PhaseTimeout} {
		t.Run(phase, func(t *testing.T) {
			now := time.Now().UTC()
			finalizedAt := now.Add(-1 * time.Hour).Format(time.RFC3339)
			deployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339)

			packs := []map[string]any{{"name": "smoke", "type": "end2end"}}
			arr := rolloutArrival("burst-arr", "canary", phase, deployedAt, finalizedAt, packs)
			_ = unstructured.SetNestedSlice(arr.Object, []any{
				map[string]any{"name": "smoke", "status": phase},
			}, "status", "tests")
			c := newTestController(t, arr)

			// Fire 10 reconciles — every one observes the same rolled-in
			// spec (deployedAt unchanged), same status (unless mutated by
			// the previous reconcile). Legal transitions: first flips
			// terminal→Pending, second dispatches (Pending→Testing).
			// From there Testing stays Testing (no dispatcher configured
			// so the stub-finalize path only fires from handleTesting,
			// not reconcileOne without waiting for the poll interval).
			var phasesSeen []string
			for i := 0; i < 10; i++ {
				// Re-fetch the arrival each iteration so successive
				// reconciles see prior status mutations (as production
				// does via the informer cache).
				got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
					Get(context.Background(), arr.GetName(), metav1.GetOptions{})
				require.NoError(t, err)
				c.reconcileOne(context.Background(), got)
				after, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
					Get(context.Background(), arr.GetName(), metav1.GetOptions{})
				p, _, _ := unstructured.NestedString(after.Object, "status", "phase")
				phasesSeen = append(phasesSeen, p)
			}

			// Assertion — exactly ONE Pending state before Testing takes
			// over. If the rollout-retest gate mis-fired, we'd see
			// Testing→Pending→Testing→Pending flapping (or an N-retest
			// storm on any variant).
			pendingCount := 0
			testingCount := 0
			terminalPostRetest := 0
			transitionedThroughPending := false
			for _, p := range phasesSeen {
				switch p {
				case PhasePending:
					pendingCount++
					transitionedThroughPending = true
				case PhaseTesting:
					testingCount++
				case PhasePassed, PhaseFailed, PhaseTimeout:
					if transitionedThroughPending {
						terminalPostRetest++
					}
				}
			}
			// Must have transitioned through Pending at least once.
			assert.True(t, transitionedThroughPending, "expected at least one Pending state after rollout-restart trigger")
			// Pending is a one-shot checkpoint — as soon as handlePending
			// runs, phase moves to Testing. In-flight guard prevents
			// re-entry. So Pending should appear at most once.
			assert.LessOrEqual(t, pendingCount, 1,
				"rollout-restart must fire at most one retest per burst; saw %d Pending states in phases: %v", pendingCount, phasesSeen)
			// Testing should stay stable once entered (no ticker fires in
			// this test, so no stub-finalize).
			assert.GreaterOrEqual(t, testingCount, 1, "must reach Testing after Pending; phases: %v", phasesSeen)
		})
	}
}

// TestInvariant_SkippedNeverStaysStuckWithPacks — for any Skipped
// Arrival that has packs (a race where the watcher merge-patched packs
// AFTER the controller saw the empty spec), the controller must flip
// back to Pending. Otherwise the service would sit as Skipped forever
// even though it's actually testable.
func TestInvariant_SkippedNeverStaysStuckWithPacks(t *testing.T) {
	// Multiple pack shapes — invariant must hold whatever the packs look
	// like, so long as len(packs) > 0.
	packShapes := [][]map[string]any{
		{{"name": "smoke", "type": "end2end"}},
		{{"name": "smoke", "type": "end2end"}, {"name": "heavy", "type": "end2end-ui"}},
		{{"name": "contract", "type": "contract"}},
	}

	for i, packs := range packShapes {
		t.Run("pack-shape-"+time.Duration(i).String(), func(t *testing.T) {
			arr := newArrival("stuck-arr", "svc", "0.0.1", PhaseSkipped, packs)
			c := newTestController(t, arr)

			c.reconcileOne(context.Background(), arr)

			got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
				Get(context.Background(), arr.GetName(), metav1.GetOptions{})
			phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
			assert.Equal(t, PhasePending, phase, "Skipped with any non-empty packs shape must flip back to Pending")
		})
	}
}

// TestInvariant_HandlePendingIsIdempotent — the re-entry guard in
// handlePending must produce STABLE state under repeated firings from
// the informer cache. First call dispatches; every subsequent call is
// a no-op.
func TestInvariant_HandlePendingIsIdempotent(t *testing.T) {
	packs := []map[string]any{{"name": "smoke", "type": "end2end"}}
	arr := newArrival("idem-arr", "svc", "0.0.1", "", packs)
	c := newTestController(t, arr)

	// First call
	c.handlePending(context.Background(), arr)
	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	phase1, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	tests1, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")

	// Ten more re-entry calls
	for i := 0; i < 10; i++ {
		c.handlePending(context.Background(), arr)
	}
	got, _ = c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
		Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	phase2, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	tests2, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")

	assert.Equal(t, phase1, phase2, "handlePending must be idempotent — phase unchanged after N repeats")
	assert.Equal(t, len(tests1), len(tests2), "handlePending must be idempotent — status.tests unchanged")
}

// TestInvariant_FinalizeAlwaysClearsInFlight — for every terminal
// phase reached via finalize(), inFlight must not carry a stale entry.
// Otherwise a subsequent rollout-restart retest would short-circuit at
// handlePending's re-entry guard and never dispatch.
func TestInvariant_FinalizeAlwaysClearsInFlight(t *testing.T) {
	for _, phase := range []string{PhasePassed, PhaseFailed, PhaseTimeout} {
		t.Run(phase, func(t *testing.T) {
			arr := newArrival("finalize-arr", "svc", "0.0.1", PhaseTesting, nil)
			c := newTestController(t, arr)
			c.inFlight[arr.GetName()] = time.Now()

			c.finalize(context.Background(), arr, phase)

			c.mu.Lock()
			_, exists := c.inFlight[arr.GetName()]
			c.mu.Unlock()
			assert.False(t, exists, "phase %s: finalize must clear inFlight so retest can re-dispatch later", phase)
		})
	}
}
