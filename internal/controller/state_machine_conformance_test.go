// state_machine_conformance_test.go pins the Arrival lifecycle state
// machine as a single table-driven test. Every legal transition +
// every guarded (illegal) transition is enumerated; a regression in
// reconcileOne (or the helpers it drives) should tip exactly one row.
//
// Mirrors the intent of orchestrator-controller's
// state_machine_conformance_test.go — one row per (from, condition) →
// expected `to`, with a short reason column so failure output points
// straight at the invariant that broke rather than "assert phase=X".
//
// The controller here uses the client-go DYNAMIC (unstructured) client
// deliberately (Arrival is a CRD, no typed listers). So this test drives
// the SAME code path production hits — reconcileOne on an
// *unstructured.Unstructured backed by dynamicfake — rather than a typed
// controller-runtime reconciler.
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

// smokePack is the minimal test pack most rows here need — just enough
// to distinguish "packs configured" from "no packs".
func smokePack() []map[string]any {
	return []map[string]any{{"name": "smoke", "type": "end2end"}}
}

// buildArrival is a broader builder than newArrival — sets phase +
// optional deployedAt / finalizedAt / tests so a single test row can
// describe a fully-realised Arrival state.
func buildArrival(t *testing.T, phase, deployedAt, finalizedAt string, tests []any, packs []map[string]any) *unstructured.Unstructured {
	t.Helper()
	u := newArrival("conformance-arr", "canary", "0.0.29", phase, packs)
	if deployedAt != "" {
		_ = unstructured.SetNestedField(u.Object, deployedAt, "spec", "deployedAt")
	}
	if finalizedAt != "" {
		_ = unstructured.SetNestedField(u.Object, finalizedAt, "status", "finalizedAt")
	}
	if tests != nil {
		_ = unstructured.SetNestedSlice(u.Object, tests, "status", "tests")
	}
	return u
}

// TestStateMachineConformance is the canonical enumeration of every
// (from-phase, spec-condition, expected-to-phase) triple the controller
// honours. Guarded transitions include the "no change" rows — a phase
// that must be left alone under a given condition is just as much a
// conformance requirement as an active transition.
//
// Rows are grouped by from-phase so a failure summary reads as a
// coherent slice of the state machine.
func TestStateMachineConformance(t *testing.T) {
	now := time.Now().UTC()
	// pre-cooldown = deployedAt only a minute after finalizedAt; below
	// the 5-minute retestCooldown gate.
	preCooldownFinalizedAt := now.Add(-2 * time.Minute).Format(time.RFC3339)
	preCooldownDeployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339)
	// post-cooldown = deployedAt an hour after finalizedAt; well past
	// the cooldown, so a retest should fire.
	postCooldownFinalizedAt := now.Add(-1 * time.Hour).Format(time.RFC3339)
	postCooldownDeployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339)

	type row struct {
		name        string
		fromPhase   string
		packs       []map[string]any
		deployedAt  string
		finalizedAt string
		tests       []any
		wantPhase   string
		wantTests   int  // -1 = don't assert, 0 = assert empty, N>0 = assert N
		assertTests bool // whether to enforce wantTests at all
		wantCleared bool // whether status.tests must be cleared after reconcile
		reason      string
	}

	rows := []row{
		// ── From (empty / initial) ─────────────────────────────────────
		{
			name:      "empty→Testing when packs present",
			fromPhase: "", packs: smokePack(),
			wantPhase: PhaseTesting, wantTests: 1, assertTests: true,
			reason: "the initial reconcile of a freshly-created Arrival with test packs must transition into Testing",
		},
		{
			name:      "empty→Skipped when no packs",
			fromPhase: "", packs: nil,
			wantPhase: PhaseSkipped, wantTests: 0, assertTests: true,
			reason: "empty testPacks means the service is opted out; go straight to terminal Skipped",
		},

		// ── From Pending ───────────────────────────────────────────────
		{
			name:      "Pending→Testing when packs present",
			fromPhase: PhasePending, packs: smokePack(),
			wantPhase: PhaseTesting, wantTests: 1, assertTests: true,
			reason: "Pending is a re-entrant checkpoint; packs present → dispatch",
		},
		{
			name:      "Pending→Skipped when no packs",
			fromPhase: PhasePending, packs: nil,
			wantPhase: PhaseSkipped,
			reason:    "Pending with no packs is the same terminal-decision as the empty-phase case",
		},

		// ── From Skipped ───────────────────────────────────────────────
		{
			name:      "Skipped→Pending when testPacks appear",
			fromPhase: PhaseSkipped, packs: smokePack(),
			wantPhase: PhasePending,
			reason:    "watcher/controller race — controller saw pre-testPacks Arrival; flip back so handlePending runs",
		},
		{
			name:      "Skipped→Skipped (no-op) when still no packs",
			fromPhase: PhaseSkipped, packs: nil,
			wantPhase: PhaseSkipped,
			reason:    "Skipped is idempotent when nothing changed",
		},

		// ── From terminal (Passed/Failed/Timeout) — WITHIN cooldown ───
		{
			name:      "Passed stays Passed within cooldown",
			fromPhase: PhasePassed, packs: smokePack(),
			deployedAt: preCooldownDeployedAt, finalizedAt: preCooldownFinalizedAt,
			wantPhase: PhasePassed,
			reason:    "single rollout's replica-add burst shares a deployedAt; must not fire N retests within cooldown",
		},
		{
			name:      "Failed stays Failed within cooldown",
			fromPhase: PhaseFailed, packs: smokePack(),
			deployedAt: preCooldownDeployedAt, finalizedAt: preCooldownFinalizedAt,
			wantPhase: PhaseFailed,
			reason:    "same as Passed; terminal Failed also gated by cooldown",
		},
		{
			name:      "Timeout stays Timeout within cooldown",
			fromPhase: PhaseTimeout, packs: smokePack(),
			deployedAt: preCooldownDeployedAt, finalizedAt: preCooldownFinalizedAt,
			wantPhase: PhaseTimeout,
			reason:    "Timeout is terminal like Passed/Failed; cooldown gate applies uniformly",
		},

		// ── From terminal — POST cooldown (rollout-restart) ────────────
		{
			name:      "Passed→Pending after cooldown (rollout-restart)",
			fromPhase: PhasePassed, packs: smokePack(),
			deployedAt: postCooldownDeployedAt, finalizedAt: postCooldownFinalizedAt,
			tests:     []any{map[string]any{"name": "smoke", "status": "Passed"}},
			wantPhase: PhasePending, wantCleared: true,
			reason: "deployedAt after finalizedAt+5min → retest requested; also clears status.tests so handlePending re-dispatches",
		},
		{
			name:      "Failed→Pending after cooldown",
			fromPhase: PhaseFailed, packs: smokePack(),
			deployedAt: postCooldownDeployedAt, finalizedAt: postCooldownFinalizedAt,
			tests:     []any{map[string]any{"name": "smoke", "status": "Failed"}},
			wantPhase: PhasePending, wantCleared: true,
			reason: "same as Passed; failed terminal is retestable when a new deploy lands",
		},
		{
			name:      "Timeout→Pending after cooldown",
			fromPhase: PhaseTimeout, packs: smokePack(),
			deployedAt: postCooldownDeployedAt, finalizedAt: postCooldownFinalizedAt,
			tests:     []any{map[string]any{"name": "smoke", "status": "Timeout"}},
			wantPhase: PhasePending, wantCleared: true,
			reason: "Timeout is terminal and retestable — same gate",
		},

		// ── From terminal — missing/malformed timestamps ───────────────
		{
			name:      "Passed stays Passed when deployedAt/finalizedAt missing",
			fromPhase: PhasePassed, packs: smokePack(),
			wantPhase: PhasePassed,
			reason:    "no rollout signal without both timestamps; must not retest speculatively",
		},

		// ── From Testing — no direct reconcile transition ──────────────
		// handleTesting is exercised in its own tests (stub-mode
		// finalize + timeout + real-dispatch job status). reconcileOne
		// routes to handleTesting; the conformance requirement here is
		// only that reconcileOne PRESERVES phase=Testing when there's
		// no in-flight timer (that seeds a controller-restart replay).
		{
			name:      "Testing stays Testing on reconcileOne (replay path)",
			fromPhase: PhaseTesting, packs: smokePack(),
			tests:     []any{map[string]any{"name": "smoke", "status": "Running"}},
			wantPhase: PhaseTesting,
			reason:    "controller-restart: no inFlight timer; reconcileOne seeds one and leaves phase alone",
		},

		// ── Unknown phase — must be no-op ──────────────────────────────
		{
			name:      "Unknown phase left alone",
			fromPhase: "SomeUnknownPhase", packs: smokePack(),
			wantPhase: "SomeUnknownPhase",
			reason:    "belt-and-braces: an unforeseen future phase must never regress to an old one",
		},
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			arr := buildArrival(t, tc.fromPhase, tc.deployedAt, tc.finalizedAt, tc.tests, tc.packs)
			c := newTestController(t, arr)

			c.reconcileOne(context.Background(), arr)

			got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
				Get(context.Background(), arr.GetName(), metav1.GetOptions{})
			require.NoError(t, err)
			phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
			assert.Equal(t, tc.wantPhase, phase, tc.reason)

			if tc.assertTests {
				tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
				assert.Len(t, tests, tc.wantTests, "%s — status.tests count", tc.reason)
			}
			if tc.wantCleared {
				tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
				assert.Empty(t, tests, "%s — status.tests must be cleared for re-dispatch", tc.reason)
			}
		})
	}
}

// TestIllegalTransitions_NeverHappen guards the "walking back" and
// "phase-skipping" moves the controller MUST never make. Complements
// TestStateMachineConformance which pins the positive shape — this one
// asserts negative shape: from ANY-phase, without the specific
// preconditions each legal move requires, phase stays put.
func TestIllegalTransitions_NeverHappen(t *testing.T) {
	now := time.Now().UTC()
	staleFinalizedAt := now.Add(-1 * time.Hour).Format(time.RFC3339)
	freshDeployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339)

	// Cases below are shaped as "given a phase X and precondition Y that
	// looks retest-adjacent but is missing SOMETHING critical, phase X
	// stays." Each row explains what makes the input insufficient.
	rows := []struct {
		name      string
		fromPhase string
		wantPhase string // must equal fromPhase (no transition)
		mutate    func(u *unstructured.Unstructured)
		reason    string
	}{
		{
			name: "Passed never regresses to Testing without spec change",
			// A Passed Arrival with no timestamps + packs still there
			// must NOT flip to Testing (that would be a silent regression).
			fromPhase: PhasePassed, wantPhase: PhasePassed,
			mutate: func(u *unstructured.Unstructured) {
				// Ensure packs exist but no rollout signal.
				_ = unstructured.SetNestedSlice(u.Object, []any{
					map[string]any{"name": "smoke", "type": "end2end"},
				}, "spec", "testPacks")
			},
			reason: "packs present alone must not un-terminalise; needs a rollout-restart signal",
		},
		{
			name: "Failed never regresses to Passed",
			// Even with rollout-restart signal, the immediate transition
			// is Failed → Pending (which then re-dispatches). Never
			// Failed → Passed directly.
			fromPhase: PhaseFailed, wantPhase: PhasePending,
			mutate: func(u *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(u.Object, freshDeployedAt, "spec", "deployedAt")
				_ = unstructured.SetNestedField(u.Object, staleFinalizedAt, "status", "finalizedAt")
				_ = unstructured.SetNestedSlice(u.Object, []any{
					map[string]any{"name": "smoke", "type": "end2end"},
				}, "spec", "testPacks")
			},
			reason: "even legal retest never jumps straight to Passed — must transit Pending → Testing → terminal",
		},
		{
			name:      "Skipped never jumps directly to Passed",
			fromPhase: PhaseSkipped, wantPhase: PhasePending,
			mutate: func(u *unstructured.Unstructured) {
				// Late packs appearing → Skipped→Pending, NOT Skipped→Passed.
				_ = unstructured.SetNestedSlice(u.Object, []any{
					map[string]any{"name": "smoke", "type": "end2end"},
				}, "spec", "testPacks")
			},
			reason: "any Skipped→terminal jump would skip the actual test dispatch step",
		},
		{
			name:      "Testing never regresses to Pending without an explicit finalize+retest cycle",
			fromPhase: PhaseTesting, wantPhase: PhaseTesting,
			mutate: func(u *unstructured.Unstructured) {
				// Even with a rollout-restart-looking spec, an in-flight
				// Testing Arrival is left alone — mid-flight retest is
				// unsafe. The retest gate only applies to terminal phases.
				_ = unstructured.SetNestedField(u.Object, freshDeployedAt, "spec", "deployedAt")
				_ = unstructured.SetNestedSlice(u.Object, []any{
					map[string]any{"name": "smoke", "type": "end2end"},
				}, "spec", "testPacks")
			},
			reason: "Testing → Pending mid-flight would abandon a real Job; retest is only for terminal Arrivals",
		},
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			arr := newArrival("illegal-arr", "canary", "0.0.29", tc.fromPhase, nil)
			if tc.mutate != nil {
				tc.mutate(arr)
			}
			c := newTestController(t, arr)

			c.reconcileOne(context.Background(), arr)

			got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").
				Get(context.Background(), arr.GetName(), metav1.GetOptions{})
			require.NoError(t, err)
			phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
			assert.Equal(t, tc.wantPhase, phase, tc.reason)
		})
	}
}
