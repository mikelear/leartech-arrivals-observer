package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// deployScheme registers apps/v1 Deployment + DeploymentList with a
// runtime.Scheme so dynamicfake's tracker can resolve Get/List calls.
func deployScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	gvk := deploymentGVR.GroupVersion().WithKind("Deployment")
	s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(deploymentGVR.GroupVersion().WithKind("DeploymentList"), &unstructured.UnstructuredList{})
	return s
}

// buildDeploy is a thin helper that builds an Unstructured Deployment
// with the spec.replicas / status fields the rollout gate reads.
func buildDeploy(name, ns string, generation int64, specReplicas int64, observedGen, updated, available, unavailable int64) *unstructured.Unstructured {
	d := &unstructured.Unstructured{}
	d.SetGroupVersionKind(deploymentGVR.GroupVersion().WithKind("Deployment"))
	d.SetName(name)
	d.SetNamespace(ns)
	d.SetGeneration(generation)
	_ = unstructured.SetNestedField(d.Object, specReplicas, "spec", "replicas")
	_ = unstructured.SetNestedField(d.Object, observedGen, "status", "observedGeneration")
	_ = unstructured.SetNestedField(d.Object, updated, "status", "updatedReplicas")
	_ = unstructured.SetNestedField(d.Object, available, "status", "availableReplicas")
	_ = unstructured.SetNestedField(d.Object, unavailable, "status", "unavailableReplicas")
	return d
}

func TestIsDeploymentRolledOut_HappyPath(t *testing.T) {
	// All four conditions hold — rollout complete.
	d := buildDeploy("canary", "jx-staging", 3, 2, 3, 2, 2, 0)
	if !isDeploymentRolledOut(d) {
		t.Error("fully-available deployment must report rolled-out")
	}
}

func TestIsDeploymentRolledOut_ObservedGenerationStale(t *testing.T) {
	// Controller hasn't seen the latest spec yet.
	d := buildDeploy("canary", "jx-staging", 5, 2, 4, 2, 2, 0)
	if isDeploymentRolledOut(d) {
		t.Error("observedGeneration < generation must return false")
	}
}

func TestIsDeploymentRolledOut_UpdatedReplicasShort(t *testing.T) {
	// Some pods still on the old image.
	d := buildDeploy("canary", "jx-staging", 3, 2, 3, 1, 2, 0)
	if isDeploymentRolledOut(d) {
		t.Error("updatedReplicas < spec.replicas must return false")
	}
}

func TestIsDeploymentRolledOut_AvailableReplicasShort(t *testing.T) {
	// New pods exist but haven't passed readiness/minReadySeconds.
	d := buildDeploy("canary", "jx-staging", 3, 2, 3, 2, 1, 0)
	if isDeploymentRolledOut(d) {
		t.Error("availableReplicas < spec.replicas must return false")
	}
}

func TestIsDeploymentRolledOut_HasUnavailable(t *testing.T) {
	// Old pods still draining.
	d := buildDeploy("canary", "jx-staging", 3, 2, 3, 2, 2, 1)
	if isDeploymentRolledOut(d) {
		t.Error("unavailableReplicas > 0 must return false")
	}
}

func TestIsDeploymentRolledOut_DefaultsReplicasToOne(t *testing.T) {
	// spec.replicas unset on Unstructured (NestedInt64 returns 0) — must
	// default to 1 to match K8s API server defaulting behavior.
	d := &unstructured.Unstructured{}
	d.SetGroupVersionKind(deploymentGVR.GroupVersion().WithKind("Deployment"))
	d.SetName("canary")
	d.SetNamespace("jx-staging")
	d.SetGeneration(1)
	_ = unstructured.SetNestedField(d.Object, int64(1), "status", "observedGeneration")
	_ = unstructured.SetNestedField(d.Object, int64(1), "status", "updatedReplicas")
	_ = unstructured.SetNestedField(d.Object, int64(1), "status", "availableReplicas")
	_ = unstructured.SetNestedField(d.Object, int64(0), "status", "unavailableReplicas")

	if !isDeploymentRolledOut(d) {
		t.Error("missing spec.replicas should default to 1 and pass when 1 updated/available")
	}
}

func TestWaitForDeploymentRollout_AlreadyRolledOut(t *testing.T) {
	d := buildDeploy("canary", "jx-staging", 3, 2, 3, 2, 2, 0)
	dyn := dynamicfake.NewSimpleDynamicClient(deployScheme(), d)
	c := &Controller{dynamic: dyn}

	err := c.waitForDeploymentRollout(context.Background(), "jx-staging", "canary", 5*time.Second)
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}
}

func TestWaitForDeploymentRollout_MissingDeployment(t *testing.T) {
	// Non-Deployment-backed service: gate skipped gracefully.
	dyn := dynamicfake.NewSimpleDynamicClient(deployScheme())
	c := &Controller{dynamic: dyn}

	err := c.waitForDeploymentRollout(context.Background(), "jx-staging", "ghost", 5*time.Second)
	if err != nil {
		t.Errorf("missing deployment must not error — got %v", err)
	}
}

func TestWaitForDeploymentRollout_TimesOut(t *testing.T) {
	// Deployment exists but never reaches rolled-out state.
	d := buildDeploy("canary", "jx-staging", 3, 2, 3, 1, 2, 0) // updated < spec
	dyn := dynamicfake.NewSimpleDynamicClient(deployScheme(), d)
	c := &Controller{dynamic: dyn}

	// 100ms timeout is well below rolloutPollInterval (5s); first Get
	// returns non-rolled-out, deadline immediately passes, returns error.
	err := c.waitForDeploymentRollout(context.Background(), "jx-staging", "canary", 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("error should mention timeout: %v", err)
	}
}

func TestWaitForDeploymentRollout_ContextCancelled(t *testing.T) {
	d := buildDeploy("canary", "jx-staging", 3, 2, 3, 1, 2, 0) // never ready
	dyn := dynamicfake.NewSimpleDynamicClient(deployScheme(), d)
	c := &Controller{dynamic: dyn}

	// 30s timeout — much longer than ctx cancel. Cancel ctx after 50ms
	// to force the select on time.After(rolloutPollInterval) to lose
	// to ctx.Done(). Note: function first does Get + isRolledOut check
	// + deadline check, then enters the select. So if first iteration
	// is fast enough, ctx cancel happens during the sleep.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := c.waitForDeploymentRollout(ctx, "jx-staging", "canary", 30*time.Second)
	if err == nil {
		t.Fatal("expected context-cancel error")
	}
}
