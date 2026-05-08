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

	"github.com/mikelear/leartech-arrivals-observer/internal/dispatch"
	"github.com/mikelear/leartech-arrivals-observer/internal/forensics"
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

	// PollInterval is the controller's reconcile + Job-status-poll cadence.
	PollInterval time.Duration

	// Timeout is the wall-clock per-Arrival ceiling. Testing arrivals older
	// than this are forced to PhaseTimeout regardless of test state.
	Timeout time.Duration

	// Dispatch holds the Job-builder configuration (runner image, GCS
	// secret, result-store bucket, cluster id). Empty Dispatcher disables
	// real Job creation and the controller falls back to stub-finalize
	// (used in unit tests + first-cut deployments before runner image is
	// available).
	Dispatcher *dispatch.Dispatcher

	// Forensics is dispatched fire-and-forget when an Arrival reaches a
	// terminal Failed/Timeout phase. nil disables forensics entirely.
	Forensics *forensics.Dispatcher
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

// New constructs the Controller with K8s clients connected. The caller
// supplies cfg.Dispatcher (already constructed with its own kubernetes
// client). If Dispatcher is nil the controller falls back to stub
// finalize — useful for unit tests + early-deploy validation before the
// runner image is wired.
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
	case PhaseSkipped:
		// Recover from the watcher/controller race where the controller
		// saw a stale Arrival (no testPacks) before the new watcher had
		// merge-patched testPacks in. If spec.testPacks is now non-empty,
		// flip back to Pending so handlePending can dispatch.
		packs, _, _ := unstructured.NestedSlice(u.Object, "spec", "testPacks")
		if len(packs) > 0 {
			log.Info().Str("arrival", name).Msg("Skipped → Pending (testPacks now present)")
			c.patchStatus(ctx, name, map[string]any{"phase": PhasePending})
		}
	default:
		// other terminal phases ignored
	}
}

// handlePending triages a fresh arrival: no testPacks → Skipped; otherwise
// → dispatch Jobs + flip to Testing.
func (c *Controller) handlePending(ctx context.Context, u *unstructured.Unstructured) {
	name := u.GetName()
	packs, _, _ := unstructured.NestedSlice(u.Object, "spec", "testPacks")
	stagingURL, _, _ := unstructured.NestedString(u.Object, "spec", "stagingUrl")
	service, _, _ := unstructured.NestedString(u.Object, "spec", "service")
	version, _, _ := unstructured.NestedString(u.Object, "spec", "version")

	if len(packs) == 0 {
		log.Info().Str("arrival", name).Msg("no test packs configured → Skipped")
		c.patchStatus(ctx, name, map[string]any{
			"phase":       PhaseSkipped,
			"finalizedAt": time.Now().UTC().Format(time.RFC3339),
		})
		return
	}

	// Build Test list for the dispatcher.
	tests := make([]dispatch.Test, 0, len(packs))
	for _, p := range packs {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		tests = append(tests, dispatch.Test{
			PackName: asString(pm["name"]),
			PackType: asString(pm["type"]),
		})
	}

	// Dispatch Jobs (or stub-finalize if no dispatcher configured).
	var jobNames map[string]string
	if c.cfg.Dispatcher != nil {
		var err error
		jobNames, err = c.cfg.Dispatcher.Dispatch(ctx, dispatch.Args{
			ArrivalName: name,
			Namespace:   c.cfg.Namespace,
			Service:     service,
			Version:     version,
			StagingURL:  stagingURL,
		}, tests)
		if err != nil {
			log.Error().Err(err).Str("arrival", name).Msg("dispatch failed; arrival → Failed")
			c.finalize(ctx, u, PhaseFailed)
			return
		}
	}

	statusTests := make([]any, 0, len(tests))
	for _, t := range tests {
		entry := map[string]any{
			"name":       t.PackName,
			"type":       t.PackType,
			"status":     "Running",
			"startedAt":  time.Now().UTC().Format(time.RFC3339),
			"retryCount": int64(0),
		}
		if jn, ok := jobNames[t.PackName]; ok {
			entry["jobName"] = jn
		}
		statusTests = append(statusTests, entry)
	}

	log.Info().
		Str("arrival", name).
		Str("stagingUrl", stagingURL).
		Int("packs", len(tests)).
		Bool("realDispatch", c.cfg.Dispatcher != nil).
		Msg("dispatching tests — Pending → Testing")

	c.mu.Lock()
	c.inFlight[name] = time.Now()
	c.mu.Unlock()

	c.patchStatus(ctx, name, map[string]any{
		"phase": PhaseTesting,
		"tests": statusTests,
	})
}

// handleTesting advances a Testing arrival. Polls each test's Job status
// (real dispatch) or stub-finalizes after PollInterval (no Dispatcher).
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

	// No dispatcher: stub mode. After one PollInterval, mark all tests
	// Passed and finalize the arrival.
	if c.cfg.Dispatcher == nil {
		if time.Since(startedAt) < c.cfg.PollInterval {
			return
		}
		c.finalize(ctx, u, PhasePassed)
		return
	}

	// Real dispatch: poll each test's Job. Decide arrival phase from
	// per-test outcomes: any Running → still Testing; any Failed → Failed
	// (once all settle); else Passed.
	tests, _, _ := unstructured.NestedSlice(u.Object, "status", "tests")
	if len(tests) == 0 {
		return
	}
	updated := make([]any, 0, len(tests))
	allDone := true
	anyFailed := false
	for _, t := range tests {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		curStatus := asString(tm["status"])
		// Settled tests stay settled.
		if curStatus == "Passed" || curStatus == "Failed" || curStatus == "Timeout" {
			if curStatus == "Failed" {
				anyFailed = true
			}
			updated = append(updated, tm)
			continue
		}
		jobName := asString(tm["jobName"])
		if jobName == "" {
			updated = append(updated, tm)
			allDone = false
			continue
		}
		js, err := c.cfg.Dispatcher.GetStatus(ctx, c.cfg.Namespace, jobName)
		if err != nil {
			log.Warn().Err(err).Str("job", jobName).Msg("get job status failed")
			updated = append(updated, tm)
			allDone = false
			continue
		}
		switch js {
		case dispatch.JobPassed:
			tm["status"] = "Passed"
			tm["completedAt"] = time.Now().UTC().Format(time.RFC3339)
		case dispatch.JobFailed:
			tm["status"] = "Failed"
			tm["completedAt"] = time.Now().UTC().Format(time.RFC3339)
			anyFailed = true
		case dispatch.JobRunning, dispatch.JobUnknown:
			allDone = false
		}
		updated = append(updated, tm)
	}

	if !allDone {
		// Persist Running/partial test progress so kubectl observers see it.
		c.patchStatus(ctx, name, map[string]any{"tests": updated})
		return
	}

	phase := PhasePassed
	if anyFailed {
		phase = PhaseFailed
	}
	c.mu.Lock()
	delete(c.inFlight, name)
	c.mu.Unlock()
	c.patchStatus(ctx, name, map[string]any{
		"phase":       phase,
		"tests":       updated,
		"finalizedAt": time.Now().UTC().Format(time.RFC3339),
	})
	log.Info().Str("arrival", name).Str("phase", phase).Msg("arrival finalized (real dispatch)")

	// Fire-and-forget forensics on terminal Failed (or Timeout). Disabled
	// when no Forensics dispatcher is configured (Forensics nil) — same
	// graceful-degrade pattern as the test dispatcher.
	if (phase == PhaseFailed || phase == PhaseTimeout) && c.cfg.Forensics != nil {
		c.maybeDispatchForensics(ctx, u)
	}
}

// maybeDispatchForensics builds Args from the Arrival spec and creates
// a forensics Job. Fire-and-forget — errors are logged but don't affect
// the Arrival's terminal phase.
func (c *Controller) maybeDispatchForensics(ctx context.Context, u *unstructured.Unstructured) {
	name := u.GetName()
	service, _, _ := unstructured.NestedString(u.Object, "spec", "service")
	version, _, _ := unstructured.NestedString(u.Object, "spec", "version")
	deployedAt, _, _ := unstructured.NestedString(u.Object, "spec", "deployedAt")

	jobName, err := c.cfg.Forensics.Dispatch(ctx, forensics.Args{
		ArrivalName:      name,
		ArrivalNamespace: c.cfg.Namespace,
		Service:          service,
		Version:          version,
		// Phase 1 hardening: walk back through Arrivals (label
		// selector qa.leartech.com/service=<svc>) and pick the most
		// recent finalized one before this arrival's CreationTimestamp.
		// Spike: leave empty; the runner treats empty PreviousVersion
		// as "first deploy" — diff degrades to a per-endpoint listing
		// of the new window only, still useful as a baseline snapshot.
		PreviousVersion: "",
		DeployedAt:      deployedAt,
	})
	if err != nil {
		log.Warn().Err(err).Str("arrival", name).Msg("forensics dispatch failed (non-fatal)")
		return
	}
	if jobName == "" {
		return // disabled
	}
	// Record jobName on the arrival so kubectl observers can find the Job.
	c.patchStatus(ctx, name, map[string]any{
		"forensics": map[string]any{
			"jobName": jobName,
		},
	})
}

// asString safely extracts a string from an unstructured map field,
// tolerating typed string vs interface{} variations.
func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
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
