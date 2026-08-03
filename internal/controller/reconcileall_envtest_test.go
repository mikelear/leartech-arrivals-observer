//go:build integration

// reconcileall_envtest_test.go covers reconcileAll — the ticker-driven
// path that lists every Arrival in the informer cache and reconciles
// each in turn. Uses envtest so the dynamicinformer factory has a real
// apiserver to watch.
//
// Gated by the `integration` build tag: excluded from the default
// `go test ./...` build. Run via `make test-integration`.
package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic/dynamicinformer"
)

// TestEnvtest_ReconcileAll_ExercisesEveryCachedArrival — creates two
// Arrivals against a real apiserver, starts a dynamic informer, waits
// for cache sync, then calls reconcileAll. Both Arrivals must advance
// (one to Testing, one to Skipped).
func TestEnvtest_ReconcileAll_ExercisesEveryCachedArrival(t *testing.T) {
	_, _, dyn, ns := requireEnvtest(t)

	ctx := context.Background()

	// Two Arrivals — one with packs (→ Testing), one without (→ Skipped).
	withPacks := newArrivalCR(ns, "with-packs", "svc-a", "0.0.1",
		[]map[string]any{{"name": "smoke", "type": "end2end"}})
	noPacks := newArrivalCR(ns, "no-packs", "svc-b", "0.0.1", nil)

	_, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).Create(ctx, withPacks, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).Create(ctx, noPacks, metav1.CreateOptions{})
	require.NoError(t, err)

	// Build a Controller wired to a real dynamic informer factory.
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 0, ns, nil)
	c := &Controller{
		cfg:      Config{Namespace: ns, PollInterval: 100 * time.Millisecond, Timeout: 1 * time.Second},
		dynamic:  dyn,
		factory:  factory,
		inFlight: make(map[string]time.Time),
	}
	// Start the informer to seed the cache.
	inf := factory.ForResource(arrivalGVR).Informer()
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	if !waitCacheSync(inf, 2*time.Second) {
		t.Fatal("informer cache failed to sync within 2s")
	}

	c.reconcileAll(ctx)

	// Wait a tick for the patches to propagate.
	time.Sleep(50 * time.Millisecond)

	// Assert both moved.
	gotWith, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Get(ctx, "with-packs", metav1.GetOptions{})
	require.NoError(t, err)
	phaseWith, _, _ := unstructured.NestedString(gotWith.Object, "status", "phase")
	assert.Equal(t, PhaseTesting, phaseWith, "packs present → Testing")

	gotNo, err := dyn.Resource(arrivalGVRForEnvtest).Namespace(ns).
		Get(ctx, "no-packs", metav1.GetOptions{})
	require.NoError(t, err)
	phaseNo, _, _ := unstructured.NestedString(gotNo.Object, "status", "phase")
	assert.Equal(t, PhaseSkipped, phaseNo, "no packs → Skipped")
}

// waitCacheSync polls informer.HasSynced with a bounded timeout.
// Avoids pulling in the whole cache.WaitForCacheSync signature.
func waitCacheSync(inf interface{ HasSynced() bool }, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if inf.HasSynced() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return inf.HasSynced()
}
