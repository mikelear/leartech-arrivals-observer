package controller

// status.tests must never be empty for an Arrival that declared test packs.
//
// `tests: []` is read downstream — by qa-gate, and by humans — as "the packs
// were declared but produced nothing". That reading points at the declaration
// as the culprit, and the obvious fix is to delete the testPacks entry. On
// 2026-09-08 that inference was wrong for five services at once: their suites
// had all PASSED, and the empty list only meant Dispatch returned early.
//
// So the invariant is not "tests is populated when things go well". It is:
// if packs were declared, status.tests names every one of them and says what
// happened to it, INCLUDING on the failure path.

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mikelear/leartech-arrivals-observer/internal/dispatch"
)

const crdPath = "../../charts/leartech-arrivals-observer/templates/crd-arrival.yaml"

func TestDispatchEvidence_NamesEveryDeclaredPack_EvenWhenNoneDispatched(t *testing.T) {
	tests := []dispatch.Test{
		{PackName: "end2end", PackType: "end2end"},
		{PackName: "end2end-ui", PackType: "end2end-ui"},
	}

	got := dispatchEvidence(tests, nil) // nothing dispatched at all

	require.Len(t, got, 2,
		"an Arrival that declared 2 packs must record 2 entries even when dispatch "+
			"failed outright — an empty list is what made five passing services look "+
			"like they had dishonest testPacks declarations")
	for _, e := range got {
		m := e.(map[string]any)
		assert.NotEmpty(t, m["name"])
		assert.Equal(t, "Pending", m["status"], "a pack with no Job has not started")
		assert.NotContains(t, m, "jobName", "there is no Job to point at")
	}
}

func TestDispatchEvidence_MarksDispatchedPacksRunningAndNamesTheirJobs(t *testing.T) {
	tests := []dispatch.Test{
		{PackName: "end2end", PackType: "end2end"},
		{PackName: "end2end-ui", PackType: "end2end-ui"},
	}

	got := dispatchEvidence(tests, map[string]string{"end2end": "ar-svc-end2end"})

	require.Len(t, got, 2)
	first := got[0].(map[string]any)
	second := got[1].(map[string]any)

	assert.Equal(t, "Running", first["status"])
	assert.Equal(t, "ar-svc-end2end", first["jobName"],
		"the controller polls status.tests[].jobName, so a dispatched pack must carry it")
	assert.Equal(t, "Pending", second["status"],
		"the pack that never got a Job must be distinguishable from the one that did")
}

// THE TRAP. status values are pinned by an enum in the CRD. A value outside it
// makes the apiserver reject the whole status patch — which leaves tests=[],
// the exact state this evidence exists to prevent. A first cut of this code
// used "NotDispatched" and would have silently failed that way in-cluster.
func TestEveryStatusDispatchEvidenceCanEmit_IsAcceptedByTheCRD(t *testing.T) {
	raw, err := os.ReadFile(crdPath)
	require.NoError(t, err, "cannot read the CRD; this test cannot validate anything")

	allowed := testStatusEnum(t, string(raw))
	require.NotEmpty(t, allowed, "found no tests[].status enum in the CRD — the parse is broken, not the code")
	require.Contains(t, allowed, "Running", "anchor: the enum must at least allow Running")

	// Every branch of dispatchEvidence, exercised.
	emitted := map[string]bool{}
	for _, e := range dispatchEvidence(
		[]dispatch.Test{{PackName: "a", PackType: "end2end"}, {PackName: "b", PackType: "end2end"}},
		map[string]string{"a": "ar-a"},
	) {
		emitted[e.(map[string]any)["status"].(string)] = true
	}
	require.Len(t, emitted, 2, "both branches should have been exercised")

	for s := range emitted {
		assert.Contains(t, allowed, s,
			"dispatchEvidence emits status=%q but the CRD's tests[].status enum is %v. "+
				"The apiserver would reject the status patch and the Arrival would keep "+
				"tests=[] — the very failure this records against.", s, allowed)
	}
}

// testStatusEnum pulls the enum list for tests[].status out of the CRD. The
// file has two identically-named `status:` keys (the Arrival phase and the
// per-test status), so take the enum from the LAST one, which is the per-test
// property under status.tests.items.
func testStatusEnum(t *testing.T, crd string) []string {
	t.Helper()
	var last string
	for _, line := range strings.Split(crd, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "enum:") && strings.Contains(trimmed, "Running") {
			last = trimmed
		}
	}
	if last == "" {
		return nil
	}
	last = last[strings.Index(last, "[")+1 : strings.LastIndex(last, "]")]
	var out []string
	for _, v := range strings.Split(last, ",") {
		out = append(out, strings.TrimSpace(v))
	}
	return out
}
