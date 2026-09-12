package dispatch

// THE 2026-09-08 WEDGE — a passing suite must never report a failing Arrival.
//
// What happened: our estate-wide auth rollout restarted every Deployment, which
// triggered the Observer's rollout-restart retest path (#143). Re-dispatch hit
// AlreadyExists on each prior Job and called deleteJobAndWait. Those deletions
// never completed — twenty Jobs across both clusters sat in Terminating with no
// Pods left and `foregroundDeletion` still set, because `spec.template` had
// drifted from what the apiserver now validates, so EVERY update to them was
// rejected as immutable: ours, and the garbage collector's finalizer removal.
//
// Unremovable and unrecreatable, they failed every re-dispatch. The controller
// returned before writing status.tests, so five services whose end2end suites
// had all PASSED (succeeded=1) reported Arrival.phase=Failed with tests=[], and
// qa-gate blocked every GitOps PR on both clusters for four days.
//
// The dangerous part was not the outage but the diagnosis: `tests=[]` reads as
// "declared a pack that produced nothing", and the fix that suggests is to
// delete the testPacks declaration. That would have permanently silenced five
// working test suites to hide an infrastructure bug.
//
// Each property below is a matched pair — an unremovable job is only meaningful
// against a removable one, or every assertion would pass on a Dispatch that
// simply never deleted anything.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	wedgedArrival = "leartech-qa-canary-0-0-39-jx-staging"
	wedgedPack    = "end2end"
	wedgedNS      = "jx-staging"
)

func wedgedJobName() string { return jobNameFor(wedgedArrival, wedgedPack) }

// existingJob builds a Job already holding the name this dispatch wants.
func existingJob(succeeded, active int32, terminating bool) *batchv1.Job {
	j := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: wedgedJobName(), Namespace: wedgedNS},
		Status:     batchv1.JobStatus{Succeeded: succeeded, Active: active},
	}
	if terminating {
		ts := metav1.NewTime(time.Now().Add(-4 * 24 * time.Hour))
		j.DeletionTimestamp = &ts
		j.Finalizers = []string{"foregroundDeletion"}
	}
	return j
}

// fakeOf exposes the reactor chain of the Dispatcher's fake clientset.
func fakeOf(d *Dispatcher) *fake.Clientset { return d.clients.(*fake.Clientset) }

func dispatchOne(t *testing.T, d *Dispatcher) (map[string]string, error) {
	t.Helper()
	return d.Dispatch(context.Background(), Args{
		ArrivalName: wedgedArrival,
		Namespace:   wedgedNS,
		Service:     "leartech-qa-canary",
		Version:     "0.0.39",
		StagingURL:  "https://leartech-qa-canary-jx-staging.jx.leartech.com",
	}, []Test{{PackName: wedgedPack, PackType: "end2end"}})
}

// ── the regression ──────────────────────────────────────────────────────────

// THE ONE THAT MATTERS. Succeeded suite, Job cannot be removed or recreated.
// Dispatch must surface the verdict, not an error.
func TestUnremovableJobThatSucceeded_IsAdopted_NotReportedAsFailure(t *testing.T) {
	d := newTestDispatcher(existingJob(1, 0, true))
	d.deleteWait, d.forceClearWait = 20*time.Millisecond, 20*time.Millisecond
	wedgeDeletes(d)

	got, err := dispatchOne(t, d)

	require.NoError(t, err,
		"a suite that PASSED (succeeded=1) was reported as a dispatch failure because "+
			"its Job could not be deleted. The controller turns this into "+
			"Arrival.phase=Failed with tests=[], which is what blocked every GitOps PR "+
			"for four days and nearly got five working testPacks deleted.")
	assert.Equal(t, wedgedJobName(), got[wedgedPack],
		"the adopted job name must be returned so the controller records it on "+
			"status.tests[].jobName and polls it for the real verdict")
}

// The control. Same unremovable Job, but its suite did NOT succeed — there is
// no verdict to adopt, so failing is correct. Without this, the test above
// would pass on a Dispatch that swallowed every error unconditionally.
func TestUnremovableJobThatDidNotSucceed_StillFails(t *testing.T) {
	d := newTestDispatcher(existingJob(0, 0, true))
	d.deleteWait, d.forceClearWait = 20*time.Millisecond, 20*time.Millisecond
	wedgeDeletes(d)

	_, err := dispatchOne(t, d)

	require.Error(t, err,
		"an unremovable Job with no successful run has no verdict to adopt; "+
			"reporting success here would invent a result")
}

// ── adoption vs replacement ─────────────────────────────────────────────────

// A running suite must never be deleted out from under itself.
func TestJobStillRunning_IsAdoptedRatherThanDeleted(t *testing.T) {
	d := newTestDispatcher(existingJob(0, 1, false))
	d.deleteWait = 20 * time.Millisecond

	got, err := dispatchOne(t, d)

	require.NoError(t, err)
	assert.Equal(t, wedgedJobName(), got[wedgedPack])

	live, gerr := d.clients.BatchV1().Jobs(wedgedNS).Get(context.Background(), wedgedJobName(), metav1.GetOptions{})
	require.NoError(t, gerr, "the in-flight job was deleted; adoption means leaving it alone")
	assert.EqualValues(t, 1, live.Status.Active, "the adopted job should be untouched")
}

// The paired case that keeps #143 honest: when the old Job CAN be removed, a
// re-dispatch must produce a genuinely fresh run rather than recycling the old
// verdict. Adoption is a fallback for the unremovable case, not the new default.
func TestTerminalRemovableJob_IsDeletedAndRecreated_ForAFreshRun(t *testing.T) {
	d := newTestDispatcher(existingJob(1, 0, false))
	d.deleteWait = time.Second

	got, err := dispatchOne(t, d)
	require.NoError(t, err)
	assert.Equal(t, wedgedJobName(), got[wedgedPack])

	live, gerr := d.clients.BatchV1().Jobs(wedgedNS).Get(context.Background(), wedgedJobName(), metav1.GetOptions{})
	require.NoError(t, gerr)
	assert.EqualValues(t, 0, live.Status.Succeeded,
		"a removable prior run must be replaced by a fresh Job, not adopted — "+
			"otherwise a rollout-restart retest silently returns the pre-restart verdict")
}

// ── force-clear safety ──────────────────────────────────────────────────────

// Force-clearing a finalizer is only safe once nothing is left to cascade to.
// Doing it to a Job with live Pods would orphan a running suite — the mirror
// image of the bug this all exists to fix.
func TestForceClear_RefusesWhilePodsAreStillActive(t *testing.T) {
	d := newTestDispatcher(existingJob(0, 2, true))
	d.forceClearWait = 20 * time.Millisecond

	err := d.clearWedgedFinalizers(context.Background(), wedgedNS, wedgedJobName())

	require.Error(t, err, "force-clearing a finalizer while pods are Active would orphan them")
	assert.Contains(t, err.Error(), "active pod")
}

// And the matching accept: no active pods, finalizer present → cleared.
func TestForceClear_RemovesTheFinalizerWhenNothingIsLeftToCascadeTo(t *testing.T) {
	d := newTestDispatcher(existingJob(1, 0, true))
	d.forceClearWait = 2 * time.Second
	patched := unwedgeOnFinalizerClear(d)

	err := d.clearWedgedFinalizers(context.Background(), wedgedNS, wedgedJobName())

	require.NoError(t, err)
	assert.True(t, *patched,
		"the finalizer was never patched away — the wait must have exited via some "+
			"other route, so this proves nothing about force-clearing")
}

// unwedgeOnFinalizerClear emulates the one apiserver behaviour the fake
// clientset omits: an object already carrying a deletionTimestamp disappears
// the moment its last finalizer is removed. Without this the fake keeps the
// Job forever and the post-clear wait can never succeed.
//
// Returns a flag recording whether the patch was actually issued, so the test
// can tell "cleared successfully" from "never tried".
func unwedgeOnFinalizerClear(d *Dispatcher) *bool {
	seen := false
	cs := fakeOf(d)
	cs.PrependReactor("patch", "jobs", func(a k8stesting.Action) (bool, runtime.Object, error) {
		seen = true
		gvr := schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
		_ = cs.Tracker().Delete(gvr, wedgedNS, wedgedJobName())
		return true, nil, nil
	})
	return &seen
}

// ── partial dispatch ────────────────────────────────────────────────────────

// Dispatch must return the packs it DID create even when a later pack fails.
// Returning nil discarded them, which is why an Arrival could end Failed with
// status.tests=[] while a real Job ran on to completion in the background.
func TestPartialDispatch_ReturnsTheJobsAlreadyCreated(t *testing.T) {
	d := newTestDispatcher(existingJob(0, 0, true))
	d.deleteWait, d.forceClearWait = 20*time.Millisecond, 20*time.Millisecond
	wedgeDeletes(d)

	got, err := d.Dispatch(context.Background(), Args{
		ArrivalName: wedgedArrival,
		Namespace:   wedgedNS,
		Service:     "leartech-qa-canary",
		Version:     "0.0.39",
		StagingURL:  "https://leartech-qa-canary-jx-staging.jx.leartech.com",
	}, []Test{
		{PackName: "smoke", PackType: "end2end"},
		{PackName: wedgedPack, PackType: "end2end"},
	})

	require.Error(t, err, "the wedged pack must still surface an error")
	assert.Contains(t, got, "smoke",
		"the Job created for 'smoke' exists in the cluster and is running; dropping it "+
			"from the returned map is what left status.tests empty while the suite ran")
}

// ── anchors ─────────────────────────────────────────────────────────────────

// Guards the fake: if seeding stopped producing a name collision, every test
// above would pass by never entering the AlreadyExists path at all.
func TestTheHarnessActuallyCollides(t *testing.T) {
	d := newTestDispatcher(existingJob(1, 0, false))
	_, err := d.clients.BatchV1().Jobs(wedgedNS).Create(
		context.Background(),
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: wedgedJobName(), Namespace: wedgedNS}},
		metav1.CreateOptions{},
	)
	require.True(t, apierrors.IsAlreadyExists(err),
		"the seeded job must collide with the dispatched name, or these tests prove nothing")
}

// wedgeDeletes makes Delete a no-op and Patch fail exactly as the apiserver did
// on 2026-09-08: `spec.template: field is immutable`.
func wedgeDeletes(d *Dispatcher) {
	cs := fakeOf(d)
	cs.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil // accepted, and nothing disappears
	})
	cs.PrependReactor("patch", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInvalid(
			schema.GroupKind{Group: "batch", Kind: "Job"}, wedgedJobName(), nil)
	})
}
