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
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
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
	"github.com/mikelear/leartech-arrivals-observer/internal/metrics"
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

// deploymentGVR identifies apps/v1 Deployments. Read via the dynamic
// client in waitForDeploymentRollout to gate test dispatch on the
// rolling update completing — see qa-architecture/tier-2-demo.md for
// the canary 0.0.21 incident that motivated this gate.
var deploymentGVR = schema.GroupVersionResource{
	Group:    "apps",
	Version:  "v1",
	Resource: "deployments",
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

	// RolloutTimeout caps how long handleNewArrival waits for the parent
	// Deployment's rolling update to complete before dispatching tests.
	// Without this gate, tests run during the K8s rolling-update window
	// and observe a mixed old+new pod response (see Tier-2 demo finding
	// in qa-architecture/tier-2-demo.md). Zero falls back to 5 minutes.
	RolloutTimeout time.Duration
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

// retestCooldown rate-limits the "terminal-phase retest on rolling
// restart" branch in reconcileOne. Multiple ReplicaSet events per
// rollout (one per replica) shouldn't each trigger an independent
// retest — the watcher upserts spec.deployedAt on every event. The
// cooldown gates the terminal→Pending flip on finalizedAt being at
// least this old, so a typical rolling restart re-tests exactly once
// after the rollout settles, not N times.
const retestCooldown = 5 * time.Minute

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
	case PhasePassed, PhaseFailed, PhaseTimeout:
		// Terminal-phase Arrivals are normally left alone. But when an
		// operator runs `kubectl rollout restart deploy/<svc>` (or any
		// rolling update of the same version) the watcher upserts
		// spec.deployedAt to the new ReplicaSet's creationTimestamp.
		// If that deployedAt is AFTER status.finalizedAt, the existing
		// terminal verdict is stale — the rerun is a deliberate
		// re-test request. Reset to Pending + clear status.tests so
		// handlePending re-dispatches.
		//
		// Gated by a 5-minute cooldown against finalizedAt so a single
		// rollout's burst of replica-add events doesn't trigger N
		// retests (only the first one fires; the rest see Phase=Pending
		// or Phase=Testing).
		if c.shouldRetestOnRollout(u) {
			log.Info().Str("arrival", name).Str("oldPhase", phase).Msg("rollout-restart detected — terminal → Pending (clearing tests for re-dispatch)")
			c.patchStatus(ctx, name, map[string]any{"phase": PhasePending, "tests": []any{}})
		}
	default:
		// unknown phase — leave alone
	}
}

// shouldRetestOnRollout returns true when an Arrival in a terminal
// phase has a deployedAt that's newer than finalizedAt by at least
// retestCooldown. Helper isolated for unit-testability.
func (c *Controller) shouldRetestOnRollout(u *unstructured.Unstructured) bool {
	deployedAtStr, _, _ := unstructured.NestedString(u.Object, "spec", "deployedAt")
	finalizedAtStr, _, _ := unstructured.NestedString(u.Object, "status", "finalizedAt")
	if deployedAtStr == "" || finalizedAtStr == "" {
		return false
	}
	deployedAt, err := time.Parse(time.RFC3339, deployedAtStr)
	if err != nil {
		return false
	}
	finalizedAt, err := time.Parse(time.RFC3339, finalizedAtStr)
	if err != nil {
		return false
	}
	// Trigger when deployedAt is newer than finalizedAt + cooldown.
	// The cooldown anchors on finalizedAt (not "now") so that:
	//  - a rollout that lands 30s after finalize doesn't fire (false
	//    positive — same release just settling)
	//  - a manual `kubectl rollout restart` minutes later DOES fire
	//  - a burst of replica events during a single restart all see
	//    the SAME deployedAt → only the first reconcile crosses the
	//    threshold, subsequent ones see phase=Pending and fall through
	return deployedAt.After(finalizedAt.Add(retestCooldown))
}

// handlePending triages a fresh arrival: no testPacks → Skipped; otherwise
// → dispatch Jobs + flip to Testing.
func (c *Controller) handlePending(ctx context.Context, u *unstructured.Unstructured) {
	name := u.GetName()
	packs, _, _ := unstructured.NestedSlice(u.Object, "spec", "testPacks")
	stagingURL, _, _ := unstructured.NestedString(u.Object, "spec", "stagingUrl")
	service, _, _ := unstructured.NestedString(u.Object, "spec", "service")
	version, _, _ := unstructured.NestedString(u.Object, "spec", "version")
	envSpec, _, _ := unstructured.NestedSlice(u.Object, "spec", "env")
	serviceResSpec, _, _ := unstructured.NestedMap(u.Object, "spec", "resources")

	if len(packs) == 0 {
		log.Info().
			Str("event", "arrival_skipped").
			Str("arrival", name).
			Str("service", service).
			Str("version", version).
			Str("phase", PhaseSkipped).
			Msg("no test packs configured → Skipped")
		metrics.RecordArrivalFinalized(PhaseSkipped, service)
		c.patchStatus(ctx, name, map[string]any{
			"phase":       PhaseSkipped,
			"finalizedAt": time.Now().UTC().Format(time.RFC3339),
		})
		return
	}

	// Re-entry guard. Dispatch is non-idempotent (creates a K8s Job).
	// The reconcile ticker reads the informer cache which is eventually-
	// consistent — a quick re-reconcile after Dispatch + patchStatus
	// (phase=Testing) can still observe Phase=Pending from a stale cache
	// read and call handlePending again. Without this guard, the second
	// call enters Dispatch → AlreadyExists → #144's delete-stale path →
	// Foreground propagation can't kill the still-running pod in 30s →
	// timeout → Arrival → Failed with Tests=[] (the very signature
	// 2026-05-18 17:08-17:09 dotnet-template@0.0.8 hit on both clusters).
	//
	// Atomic check+set: if we already kicked off dispatch for this
	// Arrival, return immediately. Successor reconciles after the phase
	// patch propagates will route via the phase switch to handleTesting
	// anyway. inFlight is cleared by finalize (line 470 / 647), so a
	// genuine re-dispatch after terminal→Pending (rollout-restart #143)
	// proceeds normally.
	c.mu.Lock()
	if _, dispatched := c.inFlight[name]; dispatched {
		c.mu.Unlock()
		log.Debug().Str("arrival", name).Msg("handlePending: already dispatched (informer-cache lag); skipping re-entry")
		return
	}
	c.inFlight[name] = time.Now()
	c.mu.Unlock()

	// Build Test list for the dispatcher. Per-pack Resources + Env
	// decoded here (both optional — nil / empty when unset on the pack).
	// Decode failures don't block dispatch; the pack falls back to the
	// service / global defaults via resolveResources' precedence chain.
	tests := make([]dispatch.Test, 0, len(packs))
	for _, p := range packs {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		t := dispatch.Test{
			PackName: asString(pm["name"]),
			PackType: asString(pm["type"]),
		}
		if resMap, ok := pm["resources"].(map[string]any); ok && len(resMap) > 0 {
			t.Resources = decodeResources(resMap)
		}
		if envRaw, ok := pm["env"].([]any); ok && len(envRaw) > 0 {
			t.Env = decodeEnvVars(envRaw)
		}
		tests = append(tests, t)
	}

	// Decode per-service env injection from spec.env (corev1.EnvVar
	// shape preserved through unstructured by encode-then-decode).
	// Failures here are logged + dropped — don't block dispatch on a
	// malformed env entry; tests may still pass with defaults.
	env := decodeEnvVars(envSpec)
	serviceRes := decodeResources(serviceResSpec)

	// Gate test dispatch on Deployment rollout completion. Without this
	// tests run during the K8s rolling-update window and observe a
	// mixed old+new pod response — Tier-2 demo on canary 0.0.21
	// (2026-05-14) captured a 5004ms /api/v1/example span from a
	// draining old pod even though the new pod responded in ~350ms.
	// Polls Deployment status until kubectl-rollout-status-equivalent
	// conditions hold (observedGeneration current, updated == spec,
	// available == spec, unavailable == 0), or times out. Missing
	// Deployment (non-Deployment-backed services) → skip the gate.
	if c.cfg.Dispatcher != nil {
		timeout := c.cfg.RolloutTimeout
		if timeout <= 0 {
			timeout = 5 * time.Minute
		}
		if err := c.waitForDeploymentRollout(ctx, c.cfg.Namespace, service, timeout); err != nil {
			log.Error().Err(err).Str("arrival", name).Str("deploy", service).Msg("rollout did not complete; arrival → Failed")
			c.finalize(ctx, u, PhaseFailed)
			return
		}
	}

	// Dispatch Jobs (or stub-finalize if no dispatcher configured).
	var jobNames map[string]string
	if c.cfg.Dispatcher != nil {
		var err error
		jobNames, err = c.cfg.Dispatcher.Dispatch(ctx, dispatch.Args{
			ArrivalName:      name,
			Namespace:        c.cfg.Namespace,
			Service:          service,
			Version:          version,
			StagingURL:       stagingURL,
			Env:              env,
			ServiceResources: serviceRes,
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
		// event=pack_dispatched — one record per pack so
		// `arrival="<x>" | event="pack_dispatched"` lists exactly
		// what was dispatched for this arrival.
		log.Info().
			Str("event", "pack_dispatched").
			Str("arrival", name).
			Str("service", service).
			Str("version", version).
			Str("pack", t.PackName).
			Str("packType", t.PackType).
			Msg("test pack dispatched")
	}

	// event=arrival_testing — the Pending→Testing transition itself.
	log.Info().
		Str("event", "arrival_testing").
		Str("arrival", name).
		Str("service", service).
		Str("version", version).
		Str("phase", PhaseTesting).
		Str("stagingUrl", stagingURL).
		Int("packs", len(tests)).
		Bool("realDispatch", c.cfg.Dispatcher != nil).
		Msg("dispatching tests — Pending → Testing")

	// Note: inFlight was set at the top of this function as the re-entry
	// guard. We don't re-set it here. cleared by finalize on terminal.

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
		service, _, _ := unstructured.NestedString(u.Object, "spec", "service")
		version, _, _ := unstructured.NestedString(u.Object, "spec", "version")
		log.Warn().
			Str("event", "arrival_timeout").
			Str("arrival", name).
			Str("service", service).
			Str("version", version).
			Str("phase", PhaseTimeout).
			Dur("elapsed", time.Since(startedAt)).
			Msg("dispatch timeout")
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
	service, _, _ := unstructured.NestedString(u.Object, "spec", "service")
	version, _, _ := unstructured.NestedString(u.Object, "spec", "version")

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
			recordPackResult(tm, service, "Passed")
		case dispatch.JobFailed:
			tm["status"] = "Failed"
			tm["completedAt"] = time.Now().UTC().Format(time.RFC3339)
			anyFailed = true
			recordPackResult(tm, service, "Failed")
			// OOM detection is best-effort: if the dispatcher exposes
			// per-pod reason, IsOOMReason gates the counter bump.
			// Absent detection returns "" and the branch no-ops.
			if reason := jobFailureReason(ctx, c.cfg.Dispatcher, c.cfg.Namespace, jobName); metrics.IsOOMReason(reason) {
				metrics.RecordJobOOM(service)
				log.Warn().
					Str("event", "pack_oom").
					Str("arrival", name).
					Str("service", service).
					Str("version", version).
					Str("pack", asString(tm["name"])).
					Str("job", jobName).
					Msg("pack Job OOMKilled")
			}
		case dispatch.JobRunning, dispatch.JobUnknown:
			allDone = false
		}
		// event=pack_result fires on any state transition (Running→terminal)
		// so consumers see the moment a pack settled, with its verdict.
		if newStatus := asString(tm["status"]); newStatus == "Passed" || newStatus == "Failed" {
			log.Info().
				Str("event", "pack_result").
				Str("arrival", name).
				Str("service", service).
				Str("version", version).
				Str("pack", asString(tm["name"])).
				Str("packStatus", newStatus).
				Str("job", jobName).
				Msg("pack settled")
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
	metrics.RecordArrivalFinalized(phase, service)
	// event=arrival_passed | arrival_failed — terminal record so a
	// single Loki query can count outcomes per service.
	terminalEvent := "arrival_passed"
	if phase == PhaseFailed {
		terminalEvent = "arrival_failed"
	}
	log.Info().
		Str("event", terminalEvent).
		Str("arrival", name).
		Str("service", service).
		Str("version", version).
		Str("phase", phase).
		Msg("arrival finalized (real dispatch)")

	// Fire-and-forget forensics on every terminal phase, Passed included.
	// The Tempo snapshot is valuable per-arrival regardless of outcome:
	// Failed/Timeout answers "why did this break?", Passed answers "did
	// anything quietly degrade?" — the latter is what gate-cli's Layer 1
	// duration-regression check needs to drill into.
	// Disabled when Forensics is nil (same graceful-degrade pattern as
	// the test dispatcher).
	if c.cfg.Forensics != nil {
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

	prev := c.findPreviousVersion(ctx, service, version, u.GetCreationTimestamp().Time)
	if prev != "" {
		log.Info().Str("arrival", name).Str("previousVersion", prev).Msg("forensics will compare against previous deployment")
	}

	jobName, err := c.cfg.Forensics.Dispatch(ctx, forensics.Args{
		ArrivalName:      name,
		ArrivalNamespace: c.cfg.Namespace,
		Service:          service,
		Version:          version,
		PreviousVersion:  prev,
		DeployedAt:       deployedAt,
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

// decodeEnvVars converts an unstructured-shaped env slice (from CR
// spec.env) back into typed []corev1.EnvVar. Round-trips via JSON
// because that's the only reliable way to thread the optional
// valueFrom.secretKeyRef shape through unstructured.NestedSlice.
// Returns nil on any decode error — caller continues without
// per-service env injection rather than blocking the dispatch.
func decodeEnvVars(raw []any) []corev1.EnvVar {
	if len(raw) == 0 {
		return nil
	}
	bytes, err := json.Marshal(raw)
	if err != nil {
		log.Warn().Err(err).Msg("encode spec.env for re-decode")
		return nil
	}
	var out []corev1.EnvVar
	if err := json.Unmarshal(bytes, &out); err != nil {
		log.Warn().Err(err).Msg("decode spec.env into []corev1.EnvVar")
		return nil
	}
	return out
}

// decodeResources converts an unstructured-shaped resources map
// (from CR spec.resources or spec.testPacks[].resources) back into a
// typed *corev1.ResourceRequirements. Pointer return so callers can
// distinguish "unset at this rung" from "explicitly set to empty" and
// defer to the next rung in the precedence chain. JSON round-trip is
// required because Quantity is stringly-typed in the unstructured
// map (e.g. "512Mi") but needs a Quantity type on the way out.
//
// Returns nil on empty input or any decode error — caller falls back
// to whichever precedence rung has a value.
func decodeResources(raw map[string]any) *corev1.ResourceRequirements {
	if len(raw) == 0 {
		return nil
	}
	bytes, err := json.Marshal(raw)
	if err != nil {
		log.Warn().Err(err).Msg("encode spec.resources for re-decode")
		return nil
	}
	var out corev1.ResourceRequirements
	if err := json.Unmarshal(bytes, &out); err != nil {
		log.Warn().Err(err).Msg("decode spec.resources into corev1.ResourceRequirements")
		return nil
	}
	if len(out.Requests) == 0 && len(out.Limits) == 0 {
		return nil
	}
	return &out
}

// asString safely extracts a string from an unstructured map field,
// tolerating typed string vs interface{} variations.
func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// findPreviousVersion walks back through Arrivals matching service to
// find the most recent finalized one (Passed, Failed, Timeout — NOT
// Skipped, since those reflect "no testPacks" not real previous-deploy
// signal) before the given currentArrival's creation time. Returns its
// spec.version, or empty string if no prior deployment found (treated by
// the forensics runner as "first deploy" → single-window snapshot).
//
// Best-effort: errors are logged but never block the dispatch path. The
// label selector + status.finalizedAt filter is fast — there are
// typically <50 Arrivals per service in jx-staging.
func (c *Controller) findPreviousVersion(ctx context.Context, service, currentVersion string, currentCreated time.Time) string {
	if service == "" {
		return ""
	}
	selector := fmt.Sprintf("qa.leartech.com/service=%s", service)
	list, err := c.dynamic.Resource(arrivalGVR).
		Namespace(c.cfg.Namespace).
		List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		log.Debug().Err(err).Str("service", service).Msg("findPreviousVersion: list failed")
		return ""
	}
	var (
		bestVer   string
		bestFinal time.Time
	)
	for _, item := range list.Items {
		ver, _, _ := unstructured.NestedString(item.Object, "spec", "version")
		if ver == "" || ver == currentVersion {
			continue
		}
		// Skip current Arrival's own ancestors (same name) — created at
		// or after the current one. Use creationTimestamp ordering.
		if !item.GetCreationTimestamp().Time.Before(currentCreated) {
			continue
		}
		// Prefer arrivals that reached a real terminal phase. Skipped
		// arrivals don't represent actual test runs against the version.
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase != PhasePassed && phase != PhaseFailed && phase != PhaseTimeout {
			continue
		}
		fAt, _, _ := unstructured.NestedString(item.Object, "status", "finalizedAt")
		if fAt == "" {
			continue
		}
		ft, err := time.Parse(time.RFC3339, fAt)
		if err != nil {
			continue
		}
		if ft.After(bestFinal) {
			bestFinal = ft
			bestVer = ver
		}
	}
	return bestVer
}

// finalize transitions an arrival to a terminal phase, marking each test
// terminal too.
func (c *Controller) finalize(ctx context.Context, u *unstructured.Unstructured, phase string) {
	name := u.GetName()
	service, _, _ := unstructured.NestedString(u.Object, "spec", "service")
	version, _, _ := unstructured.NestedString(u.Object, "spec", "version")
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
			// One pack_result per promoted pack — matches the shape emitted
			// from handleTesting's polling loop for real-dispatch mode.
			recordPackResult(tm, service, asString(tm["status"]))
			log.Info().
				Str("event", "pack_result").
				Str("arrival", name).
				Str("service", service).
				Str("version", version).
				Str("pack", asString(tm["name"])).
				Str("packStatus", asString(tm["status"])).
				Msg("pack settled (finalize)")
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
	metrics.RecordArrivalFinalized(phase, service)
	// Map phase to Loki-friendly event name so a single query can grep
	// all terminal transitions across arrivals.
	terminalEvent := "arrival_" + strings.ToLower(phase)
	log.Info().
		Str("event", terminalEvent).
		Str("arrival", name).
		Str("service", service).
		Str("version", version).
		Str("phase", phase).
		Msg("arrival finalized")
}

// recordPackResult records a pack's terminal status: increments the
// PackResult counter and, if the pack has both startedAt + completedAt,
// records the wall-clock pack duration histogram observation. The pack
// map is the *status* map (name, startedAt, completedAt, status, jobName).
//
// Kept as a package function (not a *Controller method) so tests that
// don't spin up a full controller can drive it directly. Nil-safe on
// missing keys — a malformed pack skips the histogram but still records
// the counter.
func recordPackResult(pack map[string]any, service, status string) {
	metrics.RecordPackResult(status, service)
	packName := asString(pack["name"])
	if packName == "" || service == "" {
		return
	}
	startedAt := asString(pack["startedAt"])
	completedAt := asString(pack["completedAt"])
	if startedAt == "" || completedAt == "" {
		return
	}
	start, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return
	}
	end, err := time.Parse(time.RFC3339, completedAt)
	if err != nil {
		return
	}
	if end.Before(start) {
		return
	}
	metrics.ObservePackDuration(service, packName, end.Sub(start).Seconds())
}

// jobFailureReason returns the pack Job's underlying pod-termination
// reason if the dispatcher exposes one — e.g. "OOMKilled". Returns ""
// on any lookup failure or when the dispatcher doesn't implement the
// optional reader (nil-safe).
//
// Extracted here (not on *Dispatcher) so the controller doesn't grow a
// hard dep on a new dispatch method; the type-assertion lets the
// dispatcher evolve independently.
func jobFailureReason(ctx context.Context, d *dispatch.Dispatcher, namespace, jobName string) string {
	if d == nil {
		return ""
	}
	type reasonReader interface {
		GetFailureReason(ctx context.Context, namespace, jobName string) (string, error)
	}
	rr, ok := any(d).(reasonReader)
	if !ok {
		return ""
	}
	reason, err := rr.GetFailureReason(ctx, namespace, jobName)
	if err != nil {
		log.Debug().Err(err).Str("job", jobName).Msg("get job failure reason failed")
		return ""
	}
	return reason
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

// rolloutPollInterval is the cadence at which waitForDeploymentRollout
// re-checks Deployment status. 5s is short enough to react quickly to
// rollout completion (typical rolling update is 30-60s) without
// hammering the API server.
const rolloutPollInterval = 5 * time.Second

// waitForDeploymentRollout polls the named Deployment until its rolling
// update has fully completed (only new-version pods serving), or until
// timeout elapses. Returns nil on success.
//
// Mirrors kubectl rollout status semantics — all four conditions hold:
//   - observedGeneration  >= generation     (controller saw the spec)
//   - updatedReplicas     == spec.replicas  (all new pods exist)
//   - availableReplicas   == spec.replicas  (all new pods Ready)
//   - unavailableReplicas == 0
//
// Without this gate, observer dispatches tests during the rolling-
// update window where the K8s Service load-balances across BOTH old
// and new pods. Tests measure mixed behavior, polluting results.json
// + Tempo trace data — Tier-2 demo on canary 0.0.21 captured a 5004ms
// /api/v1/example span from a draining old pod while the new pod
// responded in ~350ms.
//
// Graceful when Deployment doesn't exist (non-Deployment-backed
// services like StatefulSet-based services): logs + returns nil so
// dispatch proceeds. The gate is a best-effort guard, not a hard
// invariant.
func (c *Controller) waitForDeploymentRollout(ctx context.Context, namespace, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		d, err := c.dynamic.Resource(deploymentGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			log.Warn().Str("namespace", namespace).Str("deploy", name).Msg("Deployment not found; assuming non-Deployment-backed service, proceeding without rollout gate")
			return nil
		}
		if err != nil {
			return fmt.Errorf("get deployment %s/%s: %w", namespace, name, err)
		}
		if isDeploymentRolledOut(d) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %s waiting for deployment %s/%s to roll out", timeout, namespace, name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rolloutPollInterval):
		}
	}
}

// isDeploymentRolledOut returns true when all four kubectl-rollout-
// status conditions hold for the given Deployment.
func isDeploymentRolledOut(d *unstructured.Unstructured) bool {
	specReplicas, _, _ := unstructured.NestedInt64(d.Object, "spec", "replicas")
	if specReplicas == 0 {
		// Deployments without explicit spec.replicas default to 1 in K8s
		// but unstructured leaves it as missing. Treat as 1 to match the
		// API server's defaulting behavior.
		specReplicas = 1
	}

	generation := d.GetGeneration()
	observedGen, _, _ := unstructured.NestedInt64(d.Object, "status", "observedGeneration")
	if observedGen < generation {
		return false
	}

	updated, _, _ := unstructured.NestedInt64(d.Object, "status", "updatedReplicas")
	available, _, _ := unstructured.NestedInt64(d.Object, "status", "availableReplicas")
	unavailable, _, _ := unstructured.NestedInt64(d.Object, "status", "unavailableReplicas")

	return updated == specReplicas &&
		available == specReplicas &&
		unavailable == 0
}
