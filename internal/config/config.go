// Package config loads 12-factor configuration via envconfig.
// Per golden-standard: no config files baked into the image.
package config

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// TestPack is one entry in services[<name>].testPacks — name + type pair
// that the dispatcher will create a Job for.
type TestPack struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ServiceConfig is the per-service dispatch configuration that the watcher
// embeds into Arrival.spec at ReplicaSet event time.
type ServiceConfig struct {
	StagingURL string     `json:"stagingUrl"`
	TestPacks  []TestPack `json:"testPacks"`
}

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

	// ServicesJSON is the JSON-encoded per-service dispatch config. Sourced
	// from the chart's services map (configmap.yaml renders it). Parsed via
	// LoadServices().
	ServicesJSON string `envconfig:"SERVICES_JSON" default:"{}"`

	// Dispatch tunables — parsed by Load() into typed values.
	DispatchTimeoutMinutes      int    `envconfig:"DISPATCH_TIMEOUT_MINUTES" default:"30"`
	DispatchPollIntervalSeconds int    `envconfig:"DISPATCH_POLL_INTERVAL_SECONDS" default:"30"`
	DispatchRunnerImage         string `envconfig:"DISPATCH_RUNNER_IMAGE"`
	DispatchResultStoreBucket   string `envconfig:"DISPATCH_RESULT_STORE_BUCKET"`
	DispatchGCSKeySecret        string `envconfig:"DISPATCH_GCS_KEY_SECRET" default:"test-artifacts-gcs-key"`
}

// Load reads env and returns a populated Config.
func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, fmt.Errorf("envconfig: %w", err)
	}
	return &c, nil
}

// LoadServices parses ServicesJSON into a service-name → ServiceConfig map.
// Returns an empty map when ServicesJSON is empty/unset — no error, since a
// no-services deployment is a valid mode (records all Arrivals as Skipped).
func (c *Config) LoadServices() (map[string]ServiceConfig, error) {
	if c.ServicesJSON == "" {
		return map[string]ServiceConfig{}, nil
	}
	var m map[string]ServiceConfig
	if err := json.Unmarshal([]byte(c.ServicesJSON), &m); err != nil {
		return nil, fmt.Errorf("parse SERVICES_JSON: %w", err)
	}
	return m, nil
}

// DispatchTimeout returns the per-Arrival wall-clock timeout as a duration.
func (c *Config) DispatchTimeout() time.Duration {
	return time.Duration(c.DispatchTimeoutMinutes) * time.Minute
}

// DispatchPollInterval returns the controller's poll cadence.
func (c *Config) DispatchPollInterval() time.Duration {
	return time.Duration(c.DispatchPollIntervalSeconds) * time.Second
}
