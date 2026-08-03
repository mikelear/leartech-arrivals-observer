// Package config loads 12-factor configuration via envconfig.
// Per golden-standard: no config files baked into the image.
package config

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
	corev1 "k8s.io/api/core/v1"
)

// TestPack is one entry in services[<name>].testPacks — name + type pair
// that the dispatcher will create a Job for.
//
// Resources (optional, pointer so we can distinguish "unset" from
// "explicit empty") overrides the service-level + global default when
// dispatching this pack — e.g. a heavy Playwright end2end-ui pack that
// spawns multiple Chromium workers can ask for more memory + CPU
// without inflating the request for lighter smoke packs on the same
// service.
//
// Env (optional) layers on top of the service-level Env. Merge order in
// the dispatched Job is: standard env → service.Env → pack.Env (last
// wins for name collisions per K8s semantics). Useful for pack-specific
// tuning (e.g. PLAYWRIGHT_WORKERS=1 on the heavy pack) without
// polluting every other pack's env.
type TestPack struct {
	Name      string                       `json:"name"`
	Type      string                       `json:"type"`
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	Env       []corev1.EnvVar              `json:"env,omitempty"`
}

// ServiceConfig is the per-service dispatch configuration that the watcher
// embeds into Arrival.spec at ReplicaSet event time.
//
// Env injects per-service env vars into the dispatched Job — same shape
// as corev1.EnvVar so chart values can use either literal {name,value}
// or secret-backed {name, valueFrom: {secretKeyRef: ...}}. Used to
// thread per-cluster config into test specs (e.g. HYDRA_ADMIN_URL,
// USER_EMAIL/PASSWORD from a k8s Secret) without rebuilding the test
// runner image. Tests read via process.env.<NAME>.
//
// Resources (optional, pointer) overrides the observer's global
// DISPATCH_RESOURCES_JSON default for every pack dispatched under this
// service. Precedence at Job-build time: pack.Resources > service.Resources
// > global default (dispatch.Config.Resources) > defensive hardcoded
// fallback. Set this when an entire service (all its packs) needs
// more room than the observer's global default provides.
type ServiceConfig struct {
	StagingURL string                       `json:"stagingUrl"`
	TestPacks  []TestPack                   `json:"testPacks"`
	Env        []corev1.EnvVar              `json:"env,omitempty"`
	Resources  *corev1.ResourceRequirements `json:"resources,omitempty"`
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

	// Repo discovery for the runner clone step.
	DispatchRepoHost string `envconfig:"DISPATCH_REPO_HOST" default:"github.com"`
	DispatchRepoOrg  string `envconfig:"DISPATCH_REPO_ORG" default:"mikelear"`
	// DispatchRefFallbacksRaw is the |-delimited list of refspec
	// fallback templates. Each template is rendered with .Version + .Cluster
	// when a Job is built. See dispatch.RenderRefFallbacks.
	DispatchRefFallbacksRaw string `envconfig:"DISPATCH_REF_FALLBACKS" default:"v{{.Version}}-{{.Cluster}}|v{{.Version}}|{{.Version}}|main"`

	// Health-probe params — passed straight through to the runner Job env.
	DispatchHealthEndpoint         string `envconfig:"DISPATCH_HEALTH_ENDPOINT" default:"/health/live"`
	DispatchHealthTimeoutSeconds   int    `envconfig:"DISPATCH_HEALTH_TIMEOUT_SECONDS" default:"600"`
	DispatchHealthCurlSeconds      int    `envconfig:"DISPATCH_HEALTH_CURL_SECONDS" default:"5"`
	DispatchHealthPollSeconds      int    `envconfig:"DISPATCH_HEALTH_POLL_SECONDS" default:"5"`
	DispatchHealthSuccessThreshold int    `envconfig:"DISPATCH_HEALTH_SUCCESS_THRESHOLD" default:"3"`

	// Per-Job resources, JSON-encoded corev1.ResourceRequirements.
	// {requests:{cpu,memory},limits:{cpu,memory}}.
	DispatchResourcesJSON string `envconfig:"DISPATCH_RESOURCES_JSON" default:"{}"`

	// GitHub auth — SecretKeyRef the dispatched Job uses to clone private repos.
	DispatchGitSecretName string `envconfig:"DISPATCH_GIT_SECRET_NAME" default:"tekton-git"`
	DispatchGitSecretKey  string `envconfig:"DISPATCH_GIT_SECRET_KEY" default:"password"`

	// Path templates — CONTRACT with leartech-gate's reader. Substituted
	// at controller-side via Go text/template (.Cluster .Namespace
	// .Service .Version .Pack); pre-rendered prefix passed to Job env.
	PathsPostDeployTemplate string `envconfig:"PATHS_POST_DEPLOY_TEMPLATE" default:"results/v1/post-deploy/{{.Cluster}}/{{.Namespace}}/{{.Service}}/{{.Version}}/{{.Pack}}"`
	PathsForensicsTemplate  string `envconfig:"PATHS_FORENSICS_TEMPLATE" default:"forensics/v1/{{.Cluster}}/{{.Namespace}}/{{.Service}}/{{.Version}}"`

	// Forensics — span-diff Job dispatched on Arrival.phase=Failed.
	ForensicsEnabled               bool    `envconfig:"FORENSICS_ENABLED" default:"true"`
	ForensicsRunnerImage           string  `envconfig:"FORENSICS_RUNNER_IMAGE"`
	ForensicsTempoBaseURL          string  `envconfig:"FORENSICS_TEMPO_BASE_URL" default:"http://tempo.jx-observability:3200"`
	ForensicsWindowMinutes         int     `envconfig:"FORENSICS_WINDOW_MINUTES" default:"5"`
	ForensicsLatencyRatio          float64 `envconfig:"FORENSICS_LATENCY_RATIO" default:"1.5"`
	ForensicsErrorRateDelta        float64 `envconfig:"FORENSICS_ERROR_RATE_DELTA" default:"0.05"`
	ForensicsContextTimeoutMinutes int     `envconfig:"FORENSICS_CONTEXT_TIMEOUT_MINUTES" default:"5"`
	// ForensicsMinBaselineSamples gates latency-regression flagging — a
	// baseline endpoint with fewer than this many samples is treated as
	// statistically meaningless. Default 3 protects against single-curl
	// noise. Low-traffic services (e.g. canary smoke that hits a path
	// exactly once) can lower to 1 to opt out of the guard.
	ForensicsMinBaselineSamples int `envconfig:"FORENSICS_MIN_BASELINE_SAMPLES" default:"3"`

	// Issue-opening lifecycle inside the forensics-runner (runner #9).
	// When true the runner opens / updates / closes GitHub Issues on
	// the service repo for latency or error-rate regressions. Default
	// false — flip per-cluster via chart values after smoke-test.
	ForensicsEnableIssueCreation bool   `envconfig:"FORENSICS_ENABLE_ISSUE_CREATION" default:"false"`
	ForensicsIssueRepoOwner      string `envconfig:"FORENSICS_ISSUE_REPO_OWNER" default:"mikelear"`
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
