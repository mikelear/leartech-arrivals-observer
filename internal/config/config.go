// Package config loads 12-factor configuration via envconfig.
// Per golden-standard: no config files baked into the image.
package config

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

// Config is the envconfig-populated runtime configuration.
type Config struct {
	Port      string `envconfig:"PORT" default:"8080"`
	ClusterID string `envconfig:"CLUSTER_ID" default:""`

	// WatchNamespace is the K8s namespace whose ReplicaSets the observer
	// watches. Default jx-staging — same namespace where post-deploy tests
	// would run. Future: support multiple namespaces (jx-preproduction etc).
	WatchNamespace string `envconfig:"WATCH_NAMESPACE" default:"jx-staging"`

	// KubeConfigPath optionally points at a kubeconfig file for out-of-cluster
	// runs (debug-pod local invocations). Empty in production — uses
	// in-cluster ServiceAccount via rest.InClusterConfig().
	KubeConfigPath string `envconfig:"KUBECONFIG"`
}

// Load reads env and returns a populated Config.
func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, fmt.Errorf("envconfig: %w", err)
	}
	return &c, nil
}
