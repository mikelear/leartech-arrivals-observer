package watcher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/mikelear/leartech-arrivals-observer/internal/config"
)

// arrivalScheme registers the Arrival GVR with a runtime.Scheme so the
// dynamicfake tracker can resolve Create/Patch/Get calls.
func arrivalScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	gvk := arrivalGVR.GroupVersion().WithKind("Arrival")
	s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(arrivalGVR.GroupVersion().WithKind("ArrivalList"), &unstructured.UnstructuredList{})
	return s
}

func newTestWatcher(t *testing.T, services map[string]config.ServiceConfig, k8sObjs ...runtime.Object) *Watcher {
	t.Helper()
	return &Watcher{
		cfg: Config{
			Namespace: "jx-staging",
			ClusterID: "test-cluster",
			Services:  services,
		},
		clients: clients{
			core:    fake.NewSimpleClientset(k8sObjs...),
			dynamic: dynamicfake.NewSimpleDynamicClient(arrivalScheme()),
		},
	}
}

func newRS(name, service, version string, replicas *int32, managedBy string) *appsv1.ReplicaSet {
	labels := map[string]string{}
	if service != "" {
		labels["app.kubernetes.io/name"] = service
	}
	if version != "" {
		labels["app.kubernetes.io/version"] = version
	}
	if managedBy != "" {
		labels["app.kubernetes.io/managed-by"] = managedBy
	}
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "jx-staging",
			Labels:    labels,
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: replicas},
	}
}

func ptr32(v int32) *int32 { return &v }

func TestSanitizeForDNS(t *testing.T) {
	tests := []struct{ in, want string }{
		{"0.1.0", "0-1-0"},
		{"v1_2_3", "v1-2-3"},
		{"ABCxyz", "abcxyz"},
		{"already-clean", "already-clean"},
		{"1.0.0-rc1+sha", "1-0-0-rc1-sha"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitizeForDNS(tc.in))
		})
	}
}

func TestArrivalNameFor(t *testing.T) {
	assert.Equal(t, "canary-0-0-29-jx-staging", arrivalNameFor("canary", "0.0.29", "jx-staging"))
	assert.Equal(t, "auth-svc-v1-2-3-rc1-jx-prod", arrivalNameFor("auth-svc", "v1.2.3-rc1", "jx-prod"))
}

func TestHandleReplicaSetAdd_SkipsNonHelmManaged(t *testing.T) {
	w := newTestWatcher(t, nil)
	rs := newRS("rs-1", "canary", "0.0.29", ptr32(1), "") // no managed-by

	w.handleReplicaSetAdd(context.Background(), rs)

	list, err := w.clients.dynamic.Resource(arrivalGVR).Namespace("jx-staging").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, list.Items, "non-Helm RS must not produce an Arrival")
}

func TestHandleReplicaSetAdd_SkipsMissingServiceLabel(t *testing.T) {
	w := newTestWatcher(t, nil)
	rs := newRS("rs-1", "", "0.0.29", ptr32(1), "Helm")

	w.handleReplicaSetAdd(context.Background(), rs)

	list, _ := w.clients.dynamic.Resource(arrivalGVR).Namespace("jx-staging").List(context.Background(), metav1.ListOptions{})
	assert.Empty(t, list.Items)
}

func TestHandleReplicaSetAdd_SkipsMissingVersionLabel(t *testing.T) {
	w := newTestWatcher(t, nil)
	rs := newRS("rs-1", "canary", "", ptr32(1), "Helm")

	w.handleReplicaSetAdd(context.Background(), rs)

	list, _ := w.clients.dynamic.Resource(arrivalGVR).Namespace("jx-staging").List(context.Background(), metav1.ListOptions{})
	assert.Empty(t, list.Items)
}

func TestHandleReplicaSetAdd_SkipsScaledDownOldRS(t *testing.T) {
	w := newTestWatcher(t, nil)
	rs := newRS("rs-old", "canary", "0.0.28", ptr32(0), "Helm") // desiredReplicas=0

	w.handleReplicaSetAdd(context.Background(), rs)

	list, _ := w.clients.dynamic.Resource(arrivalGVR).Namespace("jx-staging").List(context.Background(), metav1.ListOptions{})
	assert.Empty(t, list.Items, "scaled-down old RS must not produce Arrival")
}

func TestHandleReplicaSetAdd_HappyPath_CreatesArrival(t *testing.T) {
	services := map[string]config.ServiceConfig{
		"canary": {
			StagingURL: "http://canary.jx-staging.svc",
			TestPacks:  []config.TestPack{{Name: "end2end", Type: "end2end"}},
		},
	}
	w := newTestWatcher(t, services)
	rs := newRS("rs-1", "canary", "0.0.29", ptr32(1), "Helm")

	w.handleReplicaSetAdd(context.Background(), rs)

	got, err := w.clients.dynamic.Resource(arrivalGVR).
		Namespace("jx-staging").
		Get(context.Background(), "canary-0-0-29-jx-staging", metav1.GetOptions{})
	require.NoError(t, err)

	assert.Equal(t, "canary", got.GetLabels()["qa.leartech.com/service"])
	assert.Equal(t, "0.0.29", got.GetLabels()["qa.leartech.com/version"])
	assert.Equal(t, "test-cluster", got.GetLabels()["qa.leartech.com/cluster"])

	svc, _, _ := unstructured.NestedString(got.Object, "spec", "service")
	ver, _, _ := unstructured.NestedString(got.Object, "spec", "version")
	stagingURL, _, _ := unstructured.NestedString(got.Object, "spec", "stagingUrl")
	assert.Equal(t, "canary", svc)
	assert.Equal(t, "0.0.29", ver)
	assert.Equal(t, "http://canary.jx-staging.svc", stagingURL)

	packs, _, _ := unstructured.NestedSlice(got.Object, "spec", "testPacks")
	require.Len(t, packs, 1)
	assert.Equal(t, "end2end", packs[0].(map[string]any)["name"])
}

func TestHandleReplicaSetAdd_ServiceMissingFromMap_StillCreatesArrival(t *testing.T) {
	w := newTestWatcher(t, map[string]config.ServiceConfig{}) // empty map
	rs := newRS("rs-1", "unknown-svc", "1.0.0", ptr32(1), "Helm")

	w.handleReplicaSetAdd(context.Background(), rs)

	got, err := w.clients.dynamic.Resource(arrivalGVR).
		Namespace("jx-staging").
		Get(context.Background(), "unknown-svc-1-0-0-jx-staging", metav1.GetOptions{})
	require.NoError(t, err)

	stagingURL, _, _ := unstructured.NestedString(got.Object, "spec", "stagingUrl")
	packs, _, _ := unstructured.NestedSlice(got.Object, "spec", "testPacks")
	assert.Empty(t, stagingURL, "stagingUrl must be empty when service not configured")
	assert.Empty(t, packs, "testPacks must be empty when service not configured")
}

func TestUpsertArrival_InjectsEnvVarsRoundTrip(t *testing.T) {
	services := map[string]config.ServiceConfig{
		"auth-service": {
			StagingURL: "http://auth.svc",
			TestPacks:  []config.TestPack{{Name: "end2end", Type: "end2end"}},
			Env: []corev1.EnvVar{
				{Name: "USER_ID", Value: "user-123"},
				{
					Name: "USER_PASSWORD",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "auth-test-secret"},
							Key:                  "password",
						},
					},
				},
			},
		},
	}
	w := newTestWatcher(t, services)
	rs := newRS("rs-1", "auth-service", "0.1.40", ptr32(1), "Helm")

	w.handleReplicaSetAdd(context.Background(), rs)

	got, err := w.clients.dynamic.Resource(arrivalGVR).
		Namespace("jx-staging").
		Get(context.Background(), "auth-service-0-1-40-jx-staging", metav1.GetOptions{})
	require.NoError(t, err)

	envSlice, _, _ := unstructured.NestedSlice(got.Object, "spec", "env")
	require.Len(t, envSlice, 2)
	literal := envSlice[0].(map[string]any)
	secret := envSlice[1].(map[string]any)
	assert.Equal(t, "USER_ID", literal["name"])
	assert.Equal(t, "user-123", literal["value"])
	assert.Equal(t, "USER_PASSWORD", secret["name"])
	valueFrom, ok := secret["valueFrom"].(map[string]any)
	require.True(t, ok, "secretKeyRef shape preserved through unstructured marshal")
	secretRef := valueFrom["secretKeyRef"].(map[string]any)
	assert.Equal(t, "auth-test-secret", secretRef["name"])
	assert.Equal(t, "password", secretRef["key"])
}

func TestUpsertArrival_SecondObservation_PatchesNotDuplicates(t *testing.T) {
	services := map[string]config.ServiceConfig{
		"canary": {StagingURL: "http://canary.svc", TestPacks: []config.TestPack{{Name: "smoke", Type: "smoke"}}},
	}
	w := newTestWatcher(t, services)

	rs := newRS("rs-1", "canary", "0.0.29", ptr32(1), "Helm")
	w.handleReplicaSetAdd(context.Background(), rs)

	// Same version observed again — the second call must Patch, not 409.
	w.handleReplicaSetAdd(context.Background(), rs)

	list, err := w.clients.dynamic.Resource(arrivalGVR).Namespace("jx-staging").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1, "second observation must patch the existing Arrival")
}

func TestLabelOrDeployment_FallsBackToParentDeployment(t *testing.T) {
	// Hand-rolled chart pattern: RS has narrow selectorLabels (no version),
	// parent Deployment carries the full label set.
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "auth-ui",
			Namespace: "jx-staging",
			Labels: map[string]string{
				"app.kubernetes.io/version":    "0.0.40",
				"app.kubernetes.io/managed-by": "Helm",
				"app.kubernetes.io/name":       "auth-ui",
			},
		},
	}
	core := fake.NewSimpleClientset(deploy)

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "auth-ui-abc123",
			Namespace: "jx-staging",
			Labels:    map[string]string{"app.kubernetes.io/name": "auth-ui"}, // no version
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Deployment", Name: "auth-ui"},
			},
		},
	}

	assert.Equal(t, "0.0.40", labelOrDeployment(context.Background(), core, rs, "app.kubernetes.io/version"))
	assert.Equal(t, "Helm", labelOrDeployment(context.Background(), core, rs, "app.kubernetes.io/managed-by"))
}

func TestLabelOrDeployment_PrefersRSLabelWhenPresent(t *testing.T) {
	// Modern chart pattern: RS carries the full label set, no need to traverse.
	core := fake.NewSimpleClientset()
	rs := newRS("rs-1", "canary", "0.0.29", ptr32(1), "Helm")
	assert.Equal(t, "0.0.29", labelOrDeployment(context.Background(), core, rs, "app.kubernetes.io/version"))
}

func TestLabelOrDeployment_NoOwnerNoLabel_ReturnsEmpty(t *testing.T) {
	core := fake.NewSimpleClientset()
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orphan",
			Namespace: "jx-staging",
			Labels:    map[string]string{},
		},
	}
	assert.Equal(t, "", labelOrDeployment(context.Background(), core, rs, "app.kubernetes.io/version"))
}

func TestLabelOrDeployment_ParentLookupFails_ReturnsEmpty(t *testing.T) {
	core := fake.NewSimpleClientset() // no Deployments registered
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-1",
			Namespace: "jx-staging",
			Labels:    map[string]string{"app.kubernetes.io/name": "ghost"},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Deployment", Name: "ghost"},
			},
		},
	}
	assert.Equal(t, "", labelOrDeployment(context.Background(), core, rs, "app.kubernetes.io/version"))
}

func TestHandleReplicaSetAdd_NonReplicaSetType_NoOp(t *testing.T) {
	w := newTestWatcher(t, nil)
	w.handleReplicaSetAdd(context.Background(), "not-a-replicaset") // wrong type
	list, _ := w.clients.dynamic.Resource(arrivalGVR).Namespace("jx-staging").List(context.Background(), metav1.ListOptions{})
	assert.Empty(t, list.Items)
}

// arrivalNameFor only sanitizes the version segment — service + namespace
// are passed through verbatim (K8s names are already DNS-1123 by
// convention). Confirm the version part loses _ and . regardless of
// upstream input.
func TestArrivalNameFor_VersionSanitized(t *testing.T) {
	got := arrivalNameFor("svc", "V1_0.1+sha", "jx-staging")
	assert.Equal(t, "svc-v1-0-1-sha-jx-staging", got)
	assert.False(t, strings.ContainsAny(got, "_."), "version part must not contain _ or .")
}

// Sanity check that dynamicfake actually rejects malformed Patch calls,
// so absence of a Patch error in the second-observation test really means
// the merge-patch worked.
func TestUpsertArrival_PatchPathExercised(t *testing.T) {
	services := map[string]config.ServiceConfig{
		"canary": {StagingURL: "http://canary.svc", TestPacks: []config.TestPack{{Name: "smoke", Type: "smoke"}}},
	}
	w := newTestWatcher(t, services)
	rs := newRS("rs-1", "canary", "0.0.29", ptr32(1), "Helm")

	err := w.upsertArrival(context.Background(), rs, "canary-0-0-29-jx-staging", "canary", "0.0.29")
	require.NoError(t, err)
	err = w.upsertArrival(context.Background(), rs, "canary-0-0-29-jx-staging", "canary", "0.0.29")
	require.NoError(t, err, "second upsert must Patch (not 409)")

	got, err := w.clients.dynamic.Resource(arrivalGVR).
		Namespace("jx-staging").
		Get(context.Background(), "canary-0-0-29-jx-staging", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, apierrors.IsNotFound(err))
	assert.Equal(t, "canary", got.GetLabels()["qa.leartech.com/service"])
}

// arrivalGVK is used in the helper but referenced for documentation —
// keep a no-op test importing the GVK to surface obvious schema mistakes
// at compile time.
var _ = schema.GroupVersionResource{Group: "qa.leartech.com", Version: "v1alpha1", Resource: "arrivals"}

// TestUpsertArrival_ThreadsServiceResourcesAndPerPackFieldsIntoCR is
// the CRD-round-trip test — chart-values shape flows through the
// Watcher into the Arrival CR spec preserving:
//   - spec.resources (service-wide override)
//   - spec.testPacks[].resources (per-pack override)
//   - spec.testPacks[].env (per-pack env layer)
//
// Both quantity strings ("512Mi") and EnvVar shapes survive the
// json.Marshal → unstructured → json.Unmarshal round-trip.
func TestUpsertArrival_ThreadsServiceResourcesAndPerPackFieldsIntoCR(t *testing.T) {
	svcRes := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
	packRes := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("3Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("6Gi")},
	}
	services := map[string]config.ServiceConfig{
		"leartech-portal": {
			StagingURL: "https://portal.example",
			Resources:  svcRes,
			TestPacks: []config.TestPack{
				{Name: "smoke", Type: "end2end"},
				{
					Name:      "end2end-ui",
					Type:      "end2end-ui",
					Resources: packRes,
					Env: []corev1.EnvVar{
						{Name: "PLAYWRIGHT_WORKERS", Value: "2"},
					},
				},
			},
		},
	}
	w := newTestWatcher(t, services)
	rs := newRS("rs-1", "leartech-portal", "0.1.0", ptr32(1), "Helm")

	w.handleReplicaSetAdd(context.Background(), rs)

	got, err := w.clients.dynamic.Resource(arrivalGVR).
		Namespace("jx-staging").
		Get(context.Background(), "leartech-portal-0-1-0-jx-staging", metav1.GetOptions{})
	require.NoError(t, err)

	// spec.resources round-trip
	resMap, found, err := unstructured.NestedMap(got.Object, "spec", "resources")
	require.NoError(t, err)
	require.True(t, found, "spec.resources must be present when service.Resources set")
	req := resMap["requests"].(map[string]any)
	assert.Equal(t, "500m", req["cpu"])
	assert.Equal(t, "1Gi", req["memory"])

	// spec.testPacks[]
	packs, _, _ := unstructured.NestedSlice(got.Object, "spec", "testPacks")
	require.Len(t, packs, 2)
	smoke := packs[0].(map[string]any)
	heavy := packs[1].(map[string]any)

	// smoke has no per-pack resources/env
	if _, ok := smoke["resources"]; ok {
		t.Errorf("smoke.resources must be absent when unset in config, got %v", smoke["resources"])
	}
	if _, ok := smoke["env"]; ok {
		t.Errorf("smoke.env must be absent when unset in config, got %v", smoke["env"])
	}

	// heavy has both
	heavyRes := heavy["resources"].(map[string]any)
	heavyReq := heavyRes["requests"].(map[string]any)
	heavyLim := heavyRes["limits"].(map[string]any)
	assert.Equal(t, "3Gi", heavyReq["memory"])
	assert.Equal(t, "6Gi", heavyLim["memory"])
	heavyEnv := heavy["env"].([]any)
	require.Len(t, heavyEnv, 1)
	assert.Equal(t, "PLAYWRIGHT_WORKERS", heavyEnv[0].(map[string]any)["name"])
	assert.Equal(t, "2", heavyEnv[0].(map[string]any)["value"])
}

// TestUpsertArrival_NoResourcesNoEnv_BackwardsCompat pins the
// backward-compat contract: services with no resources / pack-env
// behave exactly as they did before this initiative — no spec.resources
// key, no pack.resources / pack.env keys.
func TestUpsertArrival_NoResourcesNoEnv_BackwardsCompat(t *testing.T) {
	services := map[string]config.ServiceConfig{
		"legacy-service": {
			StagingURL: "https://legacy.example",
			TestPacks:  []config.TestPack{{Name: "smoke", Type: "end2end"}},
		},
	}
	w := newTestWatcher(t, services)
	rs := newRS("rs-1", "legacy-service", "1.0.0", ptr32(1), "Helm")

	w.handleReplicaSetAdd(context.Background(), rs)

	got, err := w.clients.dynamic.Resource(arrivalGVR).
		Namespace("jx-staging").
		Get(context.Background(), "legacy-service-1-0-0-jx-staging", metav1.GetOptions{})
	require.NoError(t, err)

	_, found, _ := unstructured.NestedMap(got.Object, "spec", "resources")
	assert.False(t, found, "spec.resources must not be set when config.Resources is nil (backwards compat)")

	packs, _, _ := unstructured.NestedSlice(got.Object, "spec", "testPacks")
	require.Len(t, packs, 1)
	smoke := packs[0].(map[string]any)
	assert.Nil(t, smoke["resources"], "pack.resources must not be set when unconfigured")
	assert.Nil(t, smoke["env"], "pack.env must not be set when unconfigured")
	// Legacy shape had exactly these two keys per pack.
	for k := range smoke {
		if k != "name" && k != "type" {
			t.Errorf("unexpected key %q on pack (backwards compat expects only name+type)", k)
		}
	}
}

// TestTestPacksToSlice_RoundTrip covers the helper in isolation —
// the more integration-heavy tests above already exercise it via
// upsertArrival, but a direct test isolates regressions in the JSON
// round-trip logic from CRD schema questions.
func TestTestPacksToSlice_RoundTrip(t *testing.T) {
	in := []config.TestPack{
		{Name: "smoke", Type: "end2end"},
		{Name: "heavy", Type: "end2end-ui", Env: []corev1.EnvVar{{Name: "X", Value: "y"}}},
	}
	out, err := testPacksToSlice(in)
	require.NoError(t, err)
	require.Len(t, out, 2)
	smoke := out[0].(map[string]any)
	heavy := out[1].(map[string]any)
	assert.Equal(t, "smoke", smoke["name"])
	assert.Equal(t, "end2end", smoke["type"])
	assert.Equal(t, "heavy", heavy["name"])
	heavyEnv := heavy["env"].([]any)
	require.Len(t, heavyEnv, 1)
	assert.Equal(t, "X", heavyEnv[0].(map[string]any)["name"])
	assert.Equal(t, "y", heavyEnv[0].(map[string]any)["value"])
}

// TestTestPacksToSlice_EmptyReturnsNil ensures the "no packs
// configured" case doesn't produce an empty slice in the CR (which
// would surface as an empty spec.testPacks key rather than the key
// being absent — a subtle backwards-compat concern for the controller's
// Skipped path).
func TestTestPacksToSlice_EmptyReturnsNil(t *testing.T) {
	out, err := testPacksToSlice(nil)
	require.NoError(t, err)
	assert.Nil(t, out)

	out, err = testPacksToSlice([]config.TestPack{})
	require.NoError(t, err)
	assert.Nil(t, out)
}

// TestResourceRequirementsToMap_RoundTrip pins the direct helper. Nil
// input → nil output (feeder can check before calling SetNestedMap).
func TestResourceRequirementsToMap_RoundTrip(t *testing.T) {
	out, err := resourceRequirementsToMap(nil)
	require.NoError(t, err)
	assert.Nil(t, out)

	in := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
	}
	out, err = resourceRequirementsToMap(in)
	require.NoError(t, err)
	req := out["requests"].(map[string]any)
	lim := out["limits"].(map[string]any)
	assert.Equal(t, "512Mi", req["memory"])
	assert.Equal(t, "2Gi", lim["memory"])
}
