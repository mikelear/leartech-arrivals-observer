// Package controller observes Arrival CRs and drives them through the
// dispatch lifecycle:
//
//	(no phase) → Pending  (initial — set by reconcile if missing)
//	Pending    → Skipped  (no testPacks defined for this service)
//	Pending    → Testing  (testPacks present; jobs would be dispatched)
//	Testing    → Passed | Failed | Timeout (terminal)
//
// Phase 2.7.2 first cut is STUB DISPATCH — Testing → Passed transitions
// happen on a fixed delay without any real K8s Job creation. The plumbing
// for status updates is end-to-end real (status subresource patches via
// the dynamic client), so 2.7.2b can swap the stub for real Job dispatch
// + result-store polling without touching the lifecycle state machine.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// Phase enum mirrors the CRD's status.phase enum.
const (
	PhasePending = "Pending"
	PhaseTesting = "Testing"
	PhasePassed  = "Passed"
	PhaseFailed  = "Failed"
	PhaseTimeout = "Timeout"
	PhaseSkipped = "Skipped"
)

var arrivalGVR = schema.GroupVersionResource{
	Group:    "qa.leartech.com",
	Version:  "v1alpha1",
	Resource: "arrivals",
}

// Config controls the controller's behaviour.
type Config struct {
	Namespace      string
	KubeConfigPath string
	ResyncPeriod   time.Duration

	// PollInterval is the cadence the controller re-evaluates Testing-phase
	// arrivals. In stub mode every Pending arrival becomes Passed after one
	// PollInterval; once 2.7.2b lands this is also the Job-poll cadence.
	PollInterval time.Duration

	// Timeout is the wall-clock per-Arrival ceiling. Testing arrivals older
	// than this are forced to PhaseTimeout regardless of test state.
	Timeout time.Duration
}

// Controller runs the Arrival lifecycle loop.
type Controller struct {
	cfg     Config
	dynamic dynamic.Interface
	factory dynamicinformer.DynamicSharedInformerFactory

	// inFlight tracks arrivals currently in stub-dispatch (Testing). Once
	// 2.7.2b real-Jobs land this becomes the per-arrival Job map.
	mu       sync.Mutex
	inFlight map[string]time.Time
}

// New constructs the Controller with K8s clients connected.
func New(_ context.Context, cfg Config) (*Controller, error) {
	restCfg, err := buildRestConfig(cfg.KubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("build rest config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, cfg.ResyncPeriod, cfg.Namespace, nil)
	return &Controller{
		cfg:      cfg,
		dynamic:  dyn,
		factory:  factory,
		inFlight: make(map[string]time.Time),
	}, nil
}

// Run starts the Arrival informer + the periodic reconcile until ctx is done.
func (c *Controller) Run(ctx context.Context) {
	informer := c.factory.ForResource(arrivalGVR).Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.reconcile(ctx, obj) },
		UpdateFunc: func(_, obj any) { c.reconcile(ctx, obj) },
	})
	if err != nil {
		log.Error().Err(err).Msg("install Arrival event handler")
		return
	}

	c.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		log.Error().Msg("Arrival informer cache failed to sync")
		return
	}
	log.Info().Str("namespace", c.cfg.Namespace).Msg("controller running — informer cache synced")

	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcileAll(ctx)
		}
	}
}

// reconcile is invoked on Add/Update events. Single-arrival entry point.
func (c *Controller) reconcile(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	c.reconcileOne(ctx, u)
}

// reconcileAll lists everything in the cache and reconciles each item. Used
// by the ticker to advance Testing-phase arrivals without waiting for the
// next event.
func (c *Controller) reconcileAll(ctx context.Context) {
	informer := c.factory.ForResource(arrivalGVR).Informer()
	for _, obj := range informer.GetStore().List() {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		c.reconcileOne(ctx, u)
	}
}

// reconcileOne drives a single Arrival forward by one step.
func (c *Controller) reconcileOne(ctx context.Context, u *unstructured.Unstructured) {
	name := u.GetName()
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")

	switch phase {
	case "", PhasePending:
		c.handlePending(ctx, u)
	case PhaseTesting:
		c.handleTesting(ctx, u, name)
	default:
		// terminal phases ignored
	}
}

// handlePending triages a fresh arrival: no testPacks → Skipped; otherwise
// → Testing + record dispatch start time.
func (c *Controller) handlePending(ctx context.Context, u *unstructured.Unstructured) {
	name := u.GetName()
	packs, _, _ := unstructured.NestedSlice(u.Object, "spec", "testPacks")
	stagingURL, _, _ := unstructured.NestedString(u.Object, "spec", "stagingUrl")

	if len(packs) == 0 {
		log.Info().Str("arrival", name).Msg("no test packs configured → Skipped")
		c.patchStatus(ctx, name, map[string]any{
			"phase":       PhaseSkipped,
			"finalizedAt": time.Now().UTC().Format(time.RFC3339),
		})
		return
	}

	tests := make([]any, 0, len(packs))
	for _, p := range packs {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		tests = append(tests, map[string]any{
			"name":       pm["name"],
			"type":       pm["type"],
			"status":     "Running",
			"startedAt":  time.Now().UTC().Format(time.RFC3339),
			"retryCount": int64(0),
		})
	}

	log.Info().
		Str("arrival", name).
		Str("stagingUrl", stagingURL).
		Int("packs", len(packs)).
		Msg("dispatching tests (stub) — Pending → Testing")

	c.mu.Lock()
	c.inFlight[name] = time.Now()
	c.mu.Unlock()

	c.patchStatus(ctx, name, map[string]any{
		"phase": PhaseTesting,
		"tests": tests,
	})
}

// handleTesting advances a Testing arrival. Stub behaviour: after one
// PollInterval, mark all tests Passed and finalize the arrival. Once
// 2.7.2b lands, this is replaced with real Job-status polling +
// result-store reads.
func (c *Controller) handleTesting(ctx context.Context, u *unstructured.Unstructured, name string) {
	c.mu.Lock()
	startedAt, dispatched := c.inFlight[name]
	c.mu.Unlock()
	if !dispatched {
		// Controller restart with arrivals already in Testing — replay timer.
		c.mu.Lock()
		c.inFlight[name] = time.Now()
		c.mu.Unlock()
		return
	}

	// Wall-clock timeout — force Timeout if we've been here too long.
	if time.Since(startedAt) > c.cfg.Timeout {
		log.Warn().Str("arrival", name).Dur("elapsed", time.Since(startedAt)).Msg("dispatch timeout")
		c.finalize(ctx, u, PhaseTimeout)
		return
	}

	// Stub: only finalize once we've held Testing for at least one PollInterval
	// (otherwise we'd flip Pending → Testing → Passed in a single tick, which
	// hides the state transition from observers).
	if time.Since(startedAt) < c.cfg.PollInterval {
		return
	}
	c.finalize(ctx, u, PhasePassed)
}

// finalize transitions an arrival to a terminal phase, marking each test
// terminal too.
func (c *Controller) finalize(ctx context.Context, u *unstructured.Unstructured, phase string) {
	name := u.GetName()
	now := time.Now().UTC().Format(time.RFC3339)

	tests, _, _ := unstructured.NestedSlice(u.Object, "status", "tests")
	patched := make([]any, 0, len(tests))
	for _, t := range tests {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		// Promote Running tests to whatever the arrival's terminal phase says.
		if tm["status"] == "Running" || tm["status"] == "Pending" {
			switch phase {
			case PhasePassed:
				tm["status"] = "Passed"
			case PhaseFailed:
				tm["status"] = "Failed"
			case PhaseTimeout:
				tm["status"] = "Timeout"
			}
			tm["completedAt"] = now
		}
		patched = append(patched, tm)
	}

	c.mu.Lock()
	delete(c.inFlight, name)
	c.mu.Unlock()

	c.patchStatus(ctx, name, map[string]any{
		"phase":       phase,
		"tests":       patched,
		"finalizedAt": now,
	})
	log.Info().Str("arrival", name).Str("phase", phase).Msg("arrival finalized")
}

// patchStatus issues a merge-patch against the /status subresource.
func (c *Controller) patchStatus(ctx context.Context, name string, status map[string]any) {
	body := map[string]any{"status": status}
	patch, err := json.Marshal(body)
	if err != nil {
		log.Error().Err(err).Str("arrival", name).Msg("marshal status patch")
		return
	}
	_, err = c.dynamic.Resource(arrivalGVR).
		Namespace(c.cfg.Namespace).
		Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil && !apierrors.IsNotFound(err) {
		log.Error().Err(err).Str("arrival", name).Msg("patch status")
	}
}

func buildRestConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
