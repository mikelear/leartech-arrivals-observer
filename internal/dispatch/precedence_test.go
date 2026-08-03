package dispatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// resReqs is a tiny constructor for tests that want a ResourceRequirements
// with just a memory request set — enough to make each rung distinct.
func resReqs(memoryReq string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(memoryReq)},
	}
}

// TestResolveResources_Precedence locks the pack > service > global >
// defensive-fallback contract. Each rung is a distinct memory request
// so we can inspect which one buildJob picked. Empty (all-zero) values
// at a rung MUST be treated as "unset" and fall through — otherwise a
// service that explicitly set resources: {} would silently override
// the global.
func TestResolveResources_Precedence(t *testing.T) {
	pack := resReqs("4Gi")
	service := resReqs("2Gi")
	global := resReqs("512Mi")

	cases := []struct {
		name    string
		pack    *corev1.ResourceRequirements
		service *corev1.ResourceRequirements
		global  corev1.ResourceRequirements
		wantMem string
		reason  string
	}{
		{"pack set wins over everything", &pack, &service, global, "4Gi", "heavy pack Playwright memory bump must beat service/global defaults"},
		{"pack nil → service wins over global", nil, &service, global, "2Gi", "service-wide override applies when no pack override"},
		{"pack + service nil → global wins", nil, nil, global, "512Mi", "chart-side default applies when neither pack nor service override"},
		{"empty pack pointer falls through to service", &corev1.ResourceRequirements{}, &service, global, "2Gi", "explicit-but-empty resource block at pack MUST NOT silently override lower rungs"},
		{"empty pack + empty service pointer falls through to global", &corev1.ResourceRequirements{}, &corev1.ResourceRequirements{}, global, "512Mi", "both explicit-empty must defer to global"},
		{"everything empty → defensive default 512Mi", nil, nil, corev1.ResourceRequirements{}, "512Mi", "runtime falls back to hardcoded 512Mi request when Helm didn't set anything"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveResources(tc.pack, tc.service, tc.global)
			assert.Equal(t, tc.wantMem, got.Requests.Memory().String(), tc.reason)
		})
	}
}

// TestResolveResources_DefensiveDefaultAllZeros ensures the hardcoded
// fallback IS what buildJob sends when literally nothing is set. This
// protects the unit-test path where dispatch.Config's Resources is
// empty (and no per-service / per-pack override arrives from a CR).
func TestResolveResources_DefensiveDefaultAllZeros(t *testing.T) {
	got := resolveResources(nil, nil, corev1.ResourceRequirements{})
	assert.Equal(t, "250m", got.Requests.Cpu().String())
	assert.Equal(t, "512Mi", got.Requests.Memory().String())
	assert.Equal(t, "1500m", got.Limits.Cpu().String())
	assert.Equal(t, "2Gi", got.Limits.Memory().String())
}

// TestResolveEnv_Layering_StandardServicePack pins the layering order
// standard → service → pack (last wins for name collisions per K8s
// semantics). The concrete "last wins" behaviour is a K8s Pod spec
// property, not something buildJob emulates locally — resolveEnv's
// job is just to APPEND in the right order so K8s sees the
// last-desired value last.
func TestResolveEnv_Layering_StandardServicePack(t *testing.T) {
	standard := []corev1.EnvVar{
		{Name: "STAGING_URL", Value: "https://x"},
		{Name: "SHARED_KEY", Value: "from-standard"},
	}
	service := []corev1.EnvVar{
		{Name: "USER_EMAIL", Value: "user@example.com"},
		{Name: "SHARED_KEY", Value: "from-service"}, // service overrides standard
	}
	pack := []corev1.EnvVar{
		{Name: "PLAYWRIGHT_WORKERS", Value: "2"},
		{Name: "SHARED_KEY", Value: "from-pack"}, // pack overrides service (last wins)
	}

	got := resolveEnv(standard, service, pack)

	// Must have every entry from every layer, in order.
	require.Len(t, got, len(standard)+len(service)+len(pack))
	assert.Equal(t, "STAGING_URL", got[0].Name)
	assert.Equal(t, "SHARED_KEY", got[1].Name)
	assert.Equal(t, "from-standard", got[1].Value)
	assert.Equal(t, "USER_EMAIL", got[2].Name)
	assert.Equal(t, "SHARED_KEY", got[3].Name)
	assert.Equal(t, "from-service", got[3].Value)
	assert.Equal(t, "PLAYWRIGHT_WORKERS", got[4].Name)
	assert.Equal(t, "SHARED_KEY", got[5].Name)
	assert.Equal(t, "from-pack", got[5].Value, "pack layer must be last so K8s uses its value for SHARED_KEY")

	// Confirm the LAST occurrence of SHARED_KEY (what K8s uses) is
	// from-pack. This is the invariant that lets "last-wins" work.
	var lastSharedValue string
	for _, e := range got {
		if e.Name == "SHARED_KEY" {
			lastSharedValue = e.Value
		}
	}
	assert.Equal(t, "from-pack", lastSharedValue, "K8s pod semantics: last-name-wins → pack layer must be resolved as the effective value")
}

// TestResolveEnv_NilAndEmptyLayers_Skipped covers the boring but
// important edge — three nil layers must produce an empty (not nil)
// slice so the caller can safely append.
func TestResolveEnv_NilAndEmptyLayers_Skipped(t *testing.T) {
	got := resolveEnv(nil, nil, nil)
	assert.Empty(t, got)
	got = resolveEnv([]corev1.EnvVar{{Name: "A"}}, nil, []corev1.EnvVar{{Name: "B"}})
	require.Len(t, got, 2)
	assert.Equal(t, "A", got[0].Name)
	assert.Equal(t, "B", got[1].Name)
}

// TestBuildJob_UsesPackResourcesOverServiceOverGlobal exercises the
// full buildJob path with a Dispatcher whose global Resources is set,
// an Args.ServiceResources override, and a Test.Resources per-pack
// override — the pod's Container.Resources MUST reflect the per-pack
// values (not the service or global).
func TestBuildJob_UsesPackResourcesOverServiceOverGlobal(t *testing.T) {
	d := newTestDispatcher() // global request cpu=100m per the helper
	svcRes := resReqs("2Gi")
	packRes := resReqs("6Gi")

	job, err := d.buildJob(
		Args{
			ArrivalName:      "canary-0-0-1-jx-staging",
			Namespace:        "jx-staging",
			Service:          "canary",
			Version:          "0.0.1",
			StagingURL:       "https://x",
			ServiceResources: &svcRes,
		},
		Test{PackName: "heavy", PackType: "end2end-ui", Resources: &packRes},
		"ar-heavy",
	)
	require.NoError(t, err)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers)
	got := job.Spec.Template.Spec.Containers[0].Resources
	assert.Equal(t, "6Gi", got.Requests.Memory().String(), "per-pack Resources must win at Job-build time")
}

// TestBuildJob_UsesServiceResources_WhenPackUnset pins the middle rung
// of the precedence chain: pack unset (nil) + service set → service
// wins over global.
func TestBuildJob_UsesServiceResources_WhenPackUnset(t *testing.T) {
	d := newTestDispatcher()
	svcRes := resReqs("2Gi")

	job, err := d.buildJob(
		Args{ArrivalName: "a", Namespace: "jx-staging", Service: "svc", Version: "0.0.1", StagingURL: "https://x", ServiceResources: &svcRes},
		Test{PackName: "smoke", PackType: "end2end"}, // no Resources
		"ar-smoke",
	)
	require.NoError(t, err)
	got := job.Spec.Template.Spec.Containers[0].Resources
	assert.Equal(t, "2Gi", got.Requests.Memory().String())
}

// TestBuildJob_UsesGlobalResources_WhenPackAndServiceUnset locks the
// bottom rung — neither the pack nor the service set Resources, so
// the Job spec receives dispatch.Config.Resources (the observer's
// global default).
func TestBuildJob_UsesGlobalResources_WhenPackAndServiceUnset(t *testing.T) {
	d := newTestDispatcher() // helper sets global cpu req 100m, limit cpu 500m
	job, err := d.buildJob(
		Args{ArrivalName: "a", Namespace: "jx-staging", Service: "svc", Version: "0.0.1", StagingURL: "https://x"},
		Test{PackName: "smoke", PackType: "end2end"},
		"ar-smoke",
	)
	require.NoError(t, err)
	got := job.Spec.Template.Spec.Containers[0].Resources
	assert.Equal(t, "100m", got.Requests.Cpu().String(), "must fall through to Config.Resources (100m cpu req)")
}

// TestBuildJob_EnvLayeringOrder_StandardServicePack asserts the
// concrete env-slice on the resulting Container carries entries in the
// documented order — standard first, service second, pack last. Names
// unique per layer so we can pin the boundary without relying on
// specific standard-env count.
func TestBuildJob_EnvLayeringOrder_StandardServicePack(t *testing.T) {
	d := newTestDispatcher()
	job, err := d.buildJob(
		Args{
			ArrivalName: "a", Namespace: "jx-staging", Service: "svc", Version: "0.0.1", StagingURL: "https://x",
			Env: []corev1.EnvVar{{Name: "SERVICE_LAYER_MARKER", Value: "svc"}},
		},
		Test{
			PackName: "smoke", PackType: "end2end",
			Env: []corev1.EnvVar{{Name: "PACK_LAYER_MARKER", Value: "pack"}},
		},
		"ar-smoke",
	)
	require.NoError(t, err)
	env := job.Spec.Template.Spec.Containers[0].Env

	// Locate positions of the standard STAGING_URL, service marker,
	// pack marker and confirm strict ordering.
	pos := func(name string) int {
		for i, e := range env {
			if e.Name == name {
				return i
			}
		}
		return -1
	}
	stagingPos := pos("STAGING_URL")
	svcPos := pos("SERVICE_LAYER_MARKER")
	packPos := pos("PACK_LAYER_MARKER")
	require.Greater(t, stagingPos, -1, "STAGING_URL must be present (from standard layer)")
	require.Greater(t, svcPos, -1, "SERVICE_LAYER_MARKER must be present")
	require.Greater(t, packPos, -1, "PACK_LAYER_MARKER must be present")
	assert.Less(t, stagingPos, svcPos, "standard env must appear before service env")
	assert.Less(t, svcPos, packPos, "service env must appear before pack env (last wins)")
}

// TestBuildJob_EnvLastWinsOnCollision_PackOverridesService checks the
// K8s pod semantics of "last name wins" — when service and pack both
// define SAME_KEY, buildJob's slice ordering must place the pack copy
// last so K8s resolves the pod env to the pack value.
func TestBuildJob_EnvLastWinsOnCollision_PackOverridesService(t *testing.T) {
	d := newTestDispatcher()
	job, err := d.buildJob(
		Args{
			ArrivalName: "a", Namespace: "jx-staging", Service: "svc", Version: "0.0.1", StagingURL: "https://x",
			Env: []corev1.EnvVar{{Name: "PLAYWRIGHT_WORKERS", Value: "8"}},
		},
		Test{
			PackName: "heavy", PackType: "end2end-ui",
			Env: []corev1.EnvVar{{Name: "PLAYWRIGHT_WORKERS", Value: "2"}},
		},
		"ar-heavy",
	)
	require.NoError(t, err)
	env := job.Spec.Template.Spec.Containers[0].Env
	var last string
	for _, e := range env {
		if e.Name == "PLAYWRIGHT_WORKERS" {
			last = e.Value
		}
	}
	assert.Equal(t, "2", last, "pack env for PLAYWRIGHT_WORKERS must be the last occurrence → K8s uses 2, not the service's 8")
}

// TestDispatch_UsesPerPackResources_EndToEnd exercises the full
// Dispatcher.Dispatch entrypoint (not just buildJob) with a per-pack
// Resources override and confirms the CREATED Job has those Resources.
// Catches wiring regressions between Dispatch → buildJob and the K8s
// Job spec.
func TestDispatch_UsesPerPackResources_EndToEnd(t *testing.T) {
	d := newTestDispatcher()
	pack := resReqs("6Gi")
	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName: "canary-0-0-1-jx-staging",
		Namespace:   "jx-staging",
		Service:     "canary",
		Version:     "0.0.1",
		StagingURL:  "https://x",
	}, []Test{{PackName: "heavy", PackType: "end2end-ui", Resources: &pack}})
	require.NoError(t, err)
	require.NotEmpty(t, got)

	job, err := d.clients.BatchV1().Jobs("jx-staging").Get(context.Background(), got["heavy"], metav1.GetOptions{})
	require.NoError(t, err)
	resources := job.Spec.Template.Spec.Containers[0].Resources
	assert.Equal(t, "6Gi", resources.Requests.Memory().String())
}

// TestIsResourcesEmpty covers the small helper directly — used inside
// resolveResources; a wrong implementation would silently break the
// precedence chain, so pin it explicitly.
func TestIsResourcesEmpty(t *testing.T) {
	assert.True(t, isResourcesEmpty(corev1.ResourceRequirements{}))
	assert.False(t, isResourcesEmpty(resReqs("1Gi")))
	assert.False(t, isResourcesEmpty(corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}))
}
