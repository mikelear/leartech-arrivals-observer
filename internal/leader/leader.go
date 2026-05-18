// Package leader wraps k8s.io/client-go's leaderelection in a small
// helper. Multi-replica observer pods race on dispatch (#13): both see
// every informer event and both call Dispatch, with the loser
// prematurely finalizing the Arrival when its delete+recreate of the
// already-existing Job fails. Standard fix is leader election via
// coordination.k8s.io/Lease.
package leader

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Config controls Lease cadence + identity.
type Config struct {
	// LeaseName is the coordination.k8s.io/Lease object name. One per
	// controller binary; all replicas race for the same Lease.
	LeaseName string
	// Namespace is where the Lease lives — same namespace as the pods.
	Namespace string
	// Identity uniquely names this candidate. Pod name (HOSTNAME env)
	// is the standard choice; falls back to a random suffix if unset.
	Identity string
	// Client is a kubernetes clientset with permission to get/create/
	// update Leases in Namespace.
	Client kubernetes.Interface

	// Tuning — defaults match controller-runtime / kube-controller-manager.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// RunLeaderElection blocks, running fn when this pod acquires the
// Lease. fn must respect its ctx — when leadership is lost, ctx is
// cancelled so fn unwinds. RunLeaderElection returns when its outer
// ctx is cancelled.
func RunLeaderElection(ctx context.Context, cfg Config, fn func(ctx context.Context)) error {
	if cfg.LeaseName == "" {
		return fmt.Errorf("leader: LeaseName required")
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("leader: Namespace required")
	}
	if cfg.Client == nil {
		return fmt.Errorf("leader: Client required")
	}
	if cfg.Identity == "" {
		cfg.Identity = identityFromEnv()
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 15 * time.Second
	}
	if cfg.RenewDeadline == 0 {
		cfg.RenewDeadline = 10 * time.Second
	}
	if cfg.RetryPeriod == 0 {
		cfg.RetryPeriod = 2 * time.Second
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaseName,
			Namespace: cfg.Namespace,
		},
		Client: cfg.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.Identity,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   cfg.LeaseDuration,
		RenewDeadline:   cfg.RenewDeadline,
		RetryPeriod:     cfg.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				log.Info().Str("identity", cfg.Identity).Str("lease", cfg.LeaseName).Msg("acquired leadership — starting controllers")
				fn(leaderCtx)
			},
			OnStoppedLeading: func() {
				log.Info().Str("identity", cfg.Identity).Msg("lost leadership — controllers stopped")
			},
			OnNewLeader: func(newLeader string) {
				if newLeader == cfg.Identity {
					return
				}
				log.Info().Str("leader", newLeader).Msg("observing new leader")
			},
		},
	})
	return nil
}

func identityFromEnv() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	return fmt.Sprintf("unknown-%d", time.Now().UnixNano())
}
