// property_test.go carries the dispatch-layer property tests — the
// invariants that must hold for ANY combination of pack/service/global
// resource + env layers.
//
// Complements precedence_test.go which pins specific tabulated cases.
// This file enumerates the full 2^3 product of (pack-set, service-set,
// global-set) for resources and the 2^3 product of (standard, service,
// pack) for env, and asserts the invariant on every point in the
// space. A regression that only manifests on one specific combo of
// nil/set rungs would light up here even if the tabulated case list
// missed it.
package dispatch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestProperty_ResolveResourcesPrecedence enumerates every combination
// of (pack set/unset, service set/unset, global set/unset) with each
// rung producing a distinct memory request. The invariant: the
// resolved output equals the highest-priority set rung, falling
// through to the defensive default only when all rungs are empty.
func TestProperty_ResolveResourcesPrecedence(t *testing.T) {
	packVal := resReqs("4Gi")
	serviceVal := resReqs("2Gi")
	globalVal := resReqs("1Gi")
	defensiveMem := "512Mi" // hardcoded in resolveResources

	// Each pack/service is either nil, explicit-empty, or set-to-value.
	// Empty-pointer must fall through same as nil.
	packChoices := []struct {
		name string
		ptr  *corev1.ResourceRequirements
	}{
		{"nil", nil},
		{"empty-ptr", &corev1.ResourceRequirements{}},
		{"set-4Gi", &packVal},
	}
	serviceChoices := []struct {
		name string
		ptr  *corev1.ResourceRequirements
	}{
		{"nil", nil},
		{"empty-ptr", &corev1.ResourceRequirements{}},
		{"set-2Gi", &serviceVal},
	}
	globalChoices := []struct {
		name string
		val  corev1.ResourceRequirements
	}{
		{"empty", corev1.ResourceRequirements{}},
		{"set-1Gi", globalVal},
	}

	for _, p := range packChoices {
		for _, s := range serviceChoices {
			for _, g := range globalChoices {
				name := fmt.Sprintf("pack=%s/svc=%s/glo=%s", p.name, s.name, g.name)
				t.Run(name, func(t *testing.T) {
					got := resolveResources(p.ptr, s.ptr, g.val)
					// Derive the expected memory value from the precedence rule.
					var wantMem string
					switch {
					case p.name == "set-4Gi":
						wantMem = "4Gi"
					case s.name == "set-2Gi":
						wantMem = "2Gi"
					case g.name == "set-1Gi":
						wantMem = "1Gi"
					default:
						wantMem = defensiveMem
					}
					assert.Equal(t, wantMem, got.Requests.Memory().String(),
						"precedence: pack>service>global>defensive — combo %s", name)
				})
			}
		}
	}
}

// TestProperty_ResolveEnvLayerOrdering asserts that resolveEnv produces
// output whose LAST occurrence of any duplicate name comes from the
// last non-nil layer. This is the invariant that makes K8s pod-env
// "last-wins" produce the desired precedence.
//
// Enumerates every subset of {standard, service, pack} providing a
// value for SHARED_KEY, and asserts the last-occurrence rule holds.
func TestProperty_ResolveEnvLayerOrdering(t *testing.T) {
	// Bit i indicates whether layer i contributes SHARED_KEY.
	for bits := 0; bits < 8; bits++ {
		hasStandard := bits&1 != 0
		hasService := bits&2 != 0
		hasPack := bits&4 != 0

		var standard, service, pack []corev1.EnvVar
		if hasStandard {
			standard = []corev1.EnvVar{{Name: "SHARED_KEY", Value: "standard"}}
		}
		if hasService {
			service = []corev1.EnvVar{{Name: "SHARED_KEY", Value: "service"}}
		}
		if hasPack {
			pack = []corev1.EnvVar{{Name: "SHARED_KEY", Value: "pack"}}
		}
		name := fmt.Sprintf("standard=%v/service=%v/pack=%v", hasStandard, hasService, hasPack)

		t.Run(name, func(t *testing.T) {
			got := resolveEnv(standard, service, pack)
			// Determine expected last occurrence — highest-priority
			// (later) layer that had SHARED_KEY.
			var wantLast string
			switch {
			case hasPack:
				wantLast = "pack"
			case hasService:
				wantLast = "service"
			case hasStandard:
				wantLast = "standard"
			default:
				wantLast = "" // no layer contributed; last is unset
			}

			var lastValue string
			for _, e := range got {
				if e.Name == "SHARED_KEY" {
					lastValue = e.Value
				}
			}
			assert.Equal(t, wantLast, lastValue,
				"last-name-wins invariant: with %s, K8s must resolve SHARED_KEY to %q", name, wantLast)
		})
	}
}

// TestProperty_BuildJobResourcesMatchesResolvedPrecedence walks the same
// pack/service/global product but through the FULL buildJob path (not
// just resolveResources in isolation), asserting the actual Job's
// Container.Resources equals what the isolated helper computes. Catches
// wiring regressions between resolveResources and buildJob's rendering.
func TestProperty_BuildJobResourcesMatchesResolvedPrecedence(t *testing.T) {
	packRes := resReqs("6Gi")
	serviceRes := resReqs("3Gi")

	// Just three interesting combos to keep test count reasonable — the
	// tabulated resolve tests above cover the rest.
	cases := []struct {
		name    string
		pack    *corev1.ResourceRequirements
		service *corev1.ResourceRequirements
		// The invariant tested here is the RUNG that wins, expressed as
		// the memory OR CPU request signature that only that rung sets.
		// Pack sets memory=6Gi; service sets memory=3Gi; global
		// (newTestDispatcher) sets cpu=100m + no memory — so a Container
		// with memory=0 + cpu=100m indicates the global rung won.
		wantMemory string
		wantCPU    string
	}{
		{"pack wins", &packRes, &serviceRes, "6Gi", "0"},
		{"service wins (pack unset)", nil, &serviceRes, "3Gi", "0"},
		{"global wins (both unset) — cpu=100m from newTestDispatcher", nil, nil, "0", "100m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDispatcher()
			job, err := d.buildJob(
				Args{ArrivalName: "a", Namespace: "jx-staging", Service: "svc",
					Version: "0.0.1", StagingURL: "https://x",
					ServiceResources: tc.service},
				Test{PackName: "p", PackType: "end2end", Resources: tc.pack},
				"ar-p",
			)
			if err != nil {
				t.Fatalf("buildJob: %v", err)
			}
			got := job.Spec.Template.Spec.Containers[0].Resources
			assert.Equal(t, tc.wantMemory, got.Requests.Memory().String(),
				"buildJob must apply resolveResources' precedence to Container.Resources (memory)")
			assert.Equal(t, tc.wantCPU, got.Requests.Cpu().String(),
				"buildJob must apply resolveResources' precedence to Container.Resources (cpu)")
		})
	}
}

// TestProperty_EnvLayeringPreservedInBuiltJob — every combination of
// (service-env, pack-env) presence produces a Container.Env whose
// last-occurrence semantics matches resolveEnv's contract. Reads only
// SHARED_KEY (which the standard layer never sets, so nil counts).
func TestProperty_EnvLayeringPreservedInBuiltJob(t *testing.T) {
	svcEnv := []corev1.EnvVar{{Name: "SHARED_KEY", Value: "service"}}
	packEnv := []corev1.EnvVar{{Name: "SHARED_KEY", Value: "pack"}}
	cases := []struct {
		name     string
		svc, p   []corev1.EnvVar
		wantLast string
	}{
		{"both set — pack wins (last)", svcEnv, packEnv, "pack"},
		{"only service set", svcEnv, nil, "service"},
		{"only pack set", nil, packEnv, "pack"},
		{"neither set — SHARED_KEY not present at all", nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDispatcher()
			job, err := d.buildJob(
				Args{ArrivalName: "a", Namespace: "jx-staging", Service: "svc",
					Version: "0.0.1", StagingURL: "https://x", Env: tc.svc},
				Test{PackName: "p", PackType: "end2end", Env: tc.p},
				"ar-p",
			)
			if err != nil {
				t.Fatalf("buildJob: %v", err)
			}
			env := job.Spec.Template.Spec.Containers[0].Env
			var lastValue string
			for _, e := range env {
				if e.Name == "SHARED_KEY" {
					lastValue = e.Value
				}
			}
			assert.Equal(t, tc.wantLast, lastValue, "K8s last-name-wins semantics for SHARED_KEY")
		})
	}
}

// TestProperty_JobNameStaysBelow63CharLimit — for every plausible
// arrival/pack input, the produced Job name must respect K8s' 63-char
// DNS label limit. Enumerates a range of arrival lengths + a range of
// pack lengths crossing the truncation boundary.
func TestProperty_JobNameStaysBelow63CharLimit(t *testing.T) {
	pack := "p" // vary this too? end2end / end2end-ui are realistic
	for _, arrivalLen := range []int{5, 30, 50, 63, 100, 200} {
		arr := make([]byte, arrivalLen)
		for i := range arr {
			arr[i] = 'a'
		}
		got := jobNameFor(string(arr), pack)
		if len(got) > 63 {
			t.Errorf("arrivalLen=%d → jobNameFor produced %d-char name (limit 63): %q", arrivalLen, len(got), got)
		}
	}
	// And with a normal arrival name but pack lengths crossing the
	// 63-char boundary.
	arr := "canary-0-0-29-jx-staging"
	for _, packLen := range []int{1, 10, 20, 30, 50, 60, 100} {
		p := make([]byte, packLen)
		for i := range p {
			p[i] = 'p'
		}
		got := jobNameFor(arr, string(p))
		if len(got) > 63 {
			t.Errorf("packLen=%d → jobNameFor produced %d-char name (limit 63): %q", packLen, len(got), got)
		}
	}
}

// Compile-time reference so the resource package isn't dropped if a
// future edit removes the last direct use.
var _ = resource.MustParse
