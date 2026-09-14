package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The dispatcher propagates whatever this value is, including zero — and a
// TTLSecondsAfterFinished of 0 means "delete the moment it finishes", which
// would destroy a Job before the observer reads its result. That is strictly
// worse than the leak it replaces, so the DEFAULT is the thing worth pinning:
// a missing env var must never mean zero.
func TestJobTTLDefaultIsSaneWhenUnset(t *testing.T) {
	require.NoError(t, os.Unsetenv("JOB_TTL_SECONDS_AFTER_FINISHED"))

	c, err := Load()
	require.NoError(t, err)

	require.NotZero(t, c.JobTTLSecondsAfterFinished,
		"a zero TTL deletes a Job the instant it finishes, before its result is read")

	// The run budget is the floor worth checking against. A TTL shorter than
	// the time a job may take is not wrong — TTL counts from completion — but
	// a TTL under a few minutes leaves no window to read a failed run at all.
	require.GreaterOrEqual(t, c.JobTTLSecondsAfterFinished, int32(300),
		"under five minutes there is no practical chance to read a failed run's logs")
}

// Operators must still be able to tune it, including down, for a cluster
// under storage pressure.
func TestJobTTLIsOverridable(t *testing.T) {
	t.Setenv("JOB_TTL_SECONDS_AFTER_FINISHED", "600")

	c, err := Load()
	require.NoError(t, err)
	require.Equal(t, int32(600), c.JobTTLSecondsAfterFinished)
}
