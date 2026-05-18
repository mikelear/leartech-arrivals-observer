package leader

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRunLeaderElection_AcquiresAndRunsFn(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fired := make(chan struct{}, 1)
	go func() {
		_ = RunLeaderElection(ctx, Config{
			LeaseName:     "test-lease",
			Namespace:     "default",
			Identity:      "pod-a",
			Client:        cs,
			LeaseDuration: 1 * time.Second,
			RenewDeadline: 500 * time.Millisecond,
			RetryPeriod:   100 * time.Millisecond,
		}, func(_ context.Context) {
			fired <- struct{}{}
		})
	}()

	select {
	case <-fired:
		// good — single candidate became leader and ran fn
	case <-time.After(3 * time.Second):
		t.Fatal("OnStartedLeading callback never fired within 3s")
	}
}

func TestRunLeaderElection_MissingFieldsReturnError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"missing LeaseName", Config{Namespace: "default", Client: cs}},
		{"missing Namespace", Config{LeaseName: "x", Client: cs}},
		{"missing Client", Config{LeaseName: "x", Namespace: "default"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := RunLeaderElection(context.Background(), tc.cfg, func(_ context.Context) {})
			require.Error(t, err)
		})
	}
}

func TestIdentityFromEnv_UsesHostname(t *testing.T) {
	t.Setenv("HOSTNAME", "observer-7b8d4857f7-292h9")
	assert.Equal(t, "observer-7b8d4857f7-292h9", identityFromEnv())
}

func TestIdentityFromEnv_FallbackWhenUnset(t *testing.T) {
	_ = os.Unsetenv("HOSTNAME")
	id := identityFromEnv()
	assert.True(t, len(id) > 0, "fallback identity must be non-empty")
	assert.NotEqual(t, "unknown-0", id, "fallback must include a timestamp suffix")
}
