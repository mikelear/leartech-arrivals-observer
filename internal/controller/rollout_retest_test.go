package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// rolloutArrival builds an Arrival in a terminal phase with the given
// deployedAt + finalizedAt timestamps for testing the rollout-restart
// retest gate.
func rolloutArrival(name, service, phase, deployedAt, finalizedAt string, packs []map[string]any) *unstructured.Unstructured {
	u := newArrival(name, service, "0.0.29", phase, packs)
	if deployedAt != "" {
		_ = unstructured.SetNestedField(u.Object, deployedAt, "spec", "deployedAt")
	}
	if finalizedAt != "" {
		_ = unstructured.SetNestedField(u.Object, finalizedAt, "status", "finalizedAt")
	}
	return u
}

func TestShouldRetestOnRollout_NewDeploymentAfterCooldown(t *testing.T) {
	now := time.Now().UTC()
	finalizedAt := now.Add(-1 * time.Hour).Format(time.RFC3339)
	deployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339) // operator just ran kubectl rollout restart

	u := rolloutArrival("a", "canary", PhaseFailed, deployedAt, finalizedAt, nil)
	c := &Controller{}
	assert.True(t, c.shouldRetestOnRollout(u), "deployedAt 1h after finalizedAt — well past 5min cooldown")
}

func TestShouldRetestOnRollout_CoolDownNotMet(t *testing.T) {
	now := time.Now().UTC()
	finalizedAt := now.Add(-2 * time.Minute).Format(time.RFC3339)
	deployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339) // 1min apart — within cooldown

	u := rolloutArrival("a", "canary", PhaseFailed, deployedAt, finalizedAt, nil)
	c := &Controller{}
	assert.False(t, c.shouldRetestOnRollout(u), "deployedAt only 1min after finalizedAt — within 5min cooldown, ignore")
}

func TestShouldRetestOnRollout_DeployedAtBeforeFinalize(t *testing.T) {
	now := time.Now().UTC()
	deployedAt := now.Add(-1 * time.Hour).Format(time.RFC3339)
	finalizedAt := now.Add(-30 * time.Minute).Format(time.RFC3339)

	u := rolloutArrival("a", "canary", PhaseFailed, deployedAt, finalizedAt, nil)
	c := &Controller{}
	assert.False(t, c.shouldRetestOnRollout(u), "deployedAt before finalizedAt — normal post-test state, ignore")
}

func TestShouldRetestOnRollout_MissingTimestamps(t *testing.T) {
	c := &Controller{}
	for _, tc := range []struct {
		name        string
		deployedAt  string
		finalizedAt string
	}{
		{"both empty", "", ""},
		{"only deployedAt", "2026-05-18T10:00:00Z", ""},
		{"only finalizedAt", "", "2026-05-18T10:00:00Z"},
		{"deployedAt malformed", "not-a-timestamp", "2026-05-18T10:00:00Z"},
		{"finalizedAt malformed", "2026-05-18T10:00:00Z", "also-bad"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := rolloutArrival("a", "canary", PhaseFailed, tc.deployedAt, tc.finalizedAt, nil)
			assert.False(t, c.shouldRetestOnRollout(u), "missing/malformed timestamps must not trigger retest")
		})
	}
}

func TestReconcileOne_TerminalRolloutRestartFlipsToPending(t *testing.T) {
	now := time.Now().UTC()
	finalizedAt := now.Add(-1 * time.Hour).Format(time.RFC3339)
	deployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339)
	packs := []map[string]any{{"name": "smoke", "type": "smoke"}}

	arr := rolloutArrival("canary-0-0-29-jx-staging", "canary", PhaseFailed, deployedAt, finalizedAt, packs)
	// Seed status.tests so we can verify they get cleared.
	_ = unstructured.SetNestedSlice(arr.Object, []any{map[string]any{"name": "smoke", "status": "Failed"}}, "status", "tests")

	c := &Controller{
		cfg:      Config{Namespace: "jx-staging"},
		dynamic:  dynamicfake.NewSimpleDynamicClient(arrivalScheme(t), arr),
		inFlight: make(map[string]time.Time),
	}

	c.reconcileOne(context.Background(), arr)

	got, err := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	tests, _, _ := unstructured.NestedSlice(got.Object, "status", "tests")
	assert.Equal(t, PhasePending, phase, "terminal-phase Arrival with stale finalizedAt + new deployedAt must reset to Pending")
	assert.Empty(t, tests, "status.tests must be cleared so handlePending re-dispatches fresh")
}

func TestReconcileOne_TerminalWithinCooldown_NoChange(t *testing.T) {
	now := time.Now().UTC()
	finalizedAt := now.Add(-2 * time.Minute).Format(time.RFC3339)
	deployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339) // 1min after finalize — within cooldown
	packs := []map[string]any{{"name": "smoke", "type": "smoke"}}

	arr := rolloutArrival("canary-0-0-29-jx-staging", "canary", PhaseFailed, deployedAt, finalizedAt, packs)
	c := &Controller{
		cfg:      Config{Namespace: "jx-staging"},
		dynamic:  dynamicfake.NewSimpleDynamicClient(arrivalScheme(t), arr),
		inFlight: make(map[string]time.Time),
	}

	c.reconcileOne(context.Background(), arr)

	got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), arr.GetName(), metav1.GetOptions{})
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, PhaseFailed, phase, "within cooldown — terminal phase must NOT be flipped")
}

func TestReconcileOne_AllThreeTerminalPhasesTriggerRetest(t *testing.T) {
	now := time.Now().UTC()
	finalizedAt := now.Add(-1 * time.Hour).Format(time.RFC3339)
	deployedAt := now.Add(-1 * time.Minute).Format(time.RFC3339)

	for _, phase := range []string{PhasePassed, PhaseFailed, PhaseTimeout} {
		t.Run(phase, func(t *testing.T) {
			arr := rolloutArrival("a", "canary", phase, deployedAt, finalizedAt,
				[]map[string]any{{"name": "smoke", "type": "smoke"}})
			c := &Controller{
				cfg:      Config{Namespace: "jx-staging"},
				dynamic:  dynamicfake.NewSimpleDynamicClient(arrivalScheme(t), arr),
				inFlight: make(map[string]time.Time),
			}

			c.reconcileOne(context.Background(), arr)

			got, _ := c.dynamic.Resource(arrivalGVR).Namespace("jx-staging").Get(context.Background(), "a", metav1.GetOptions{})
			gotPhase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
			assert.Equal(t, PhasePending, gotPhase, "phase %s with rollout-restart must flip to Pending", phase)
		})
	}
}
