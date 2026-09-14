package dispatch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A Job this service creates has NO ownerReference, NO helm release and NO
// chart, so nothing in the cluster will ever remove it. Measured 2026-09-14:
// 205 completed forensics pods on az against 11 on gcp, and 10,674 un-reaped
// Jobs estate-wide with the oldest 244 days old.
//
// TTLSecondsAfterFinished is the only mechanism that reaps a standalone Job.
func TestDispatchedJobCarriesATTL(t *testing.T) {
	d := &Dispatcher{cfg: Config{
		ActiveDeadlineSeconds:      1800,
		JobTTLSecondsAfterFinished: 86400,
	}}

	job, err := d.buildJob(Args{Namespace: "ns", Service: "svc"}, Test{PackName: "p"}, "job-1")
	require.NoError(t, err)
	require.NotNil(t, job.Spec.TTLSecondsAfterFinished,
		"a dispatched Job with no TTL is never reaped by anything: no owner, no release, no chart")
	require.Equal(t, int32(86400), *job.Spec.TTLSecondsAfterFinished)
}

// Zero is not "unset", it is "delete the moment it finishes" — which would
// destroy the Job before the observer reads its result, and is strictly worse
// than the leak it replaces. This asserts the config default is a real value,
// so a missing env var cannot silently mean zero.
func TestTTLDefaultIsNotZero(t *testing.T) {
	d := &Dispatcher{cfg: Config{ActiveDeadlineSeconds: 1800, JobTTLSecondsAfterFinished: 0}}
	job, err := d.buildJob(Args{Namespace: "ns", Service: "svc"}, Test{PackName: "p"}, "job-2")
	require.NoError(t, err)
	require.NotNil(t, job.Spec.TTLSecondsAfterFinished)
	require.Zero(t, *job.Spec.TTLSecondsAfterFinished,
		"this documents that a zero config value propagates as zero — which is why "+
			"config.JobTTLSecondsAfterFinished carries a non-zero default, asserted in the config package")
}

// The TTL is counted from COMPLETION, so it must not be confused with the
// run budget. If someone ever sets TTL below the deadline thinking it caps
// runtime, the job still runs to the deadline — this pins that they are
// independent knobs so the comment cannot rot into a wrong assumption.
func TestTTLAndDeadlineAreIndependent(t *testing.T) {
	d := &Dispatcher{cfg: Config{ActiveDeadlineSeconds: 1800, JobTTLSecondsAfterFinished: 60}}
	job, err := d.buildJob(Args{Namespace: "ns", Service: "svc"}, Test{PackName: "p"}, "job-3")
	require.NoError(t, err)
	require.Equal(t, int64(1800), *job.Spec.ActiveDeadlineSeconds, "run budget unchanged")
	require.Equal(t, int32(60), *job.Spec.TTLSecondsAfterFinished, "reap delay unchanged")
}
