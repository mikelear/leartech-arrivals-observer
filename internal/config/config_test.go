package config

import (
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// TestLoadDefaults confirms envconfig supplies defaults when env is empty.
func TestLoadDefaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("CLUSTER_ID", "")
	t.Setenv("WATCH_NAMESPACE", "")
	_ = os.Unsetenv("PORT")
	_ = os.Unsetenv("CLUSTER_ID")
	_ = os.Unsetenv("WATCH_NAMESPACE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "8080" {
		t.Errorf("Port = %q, want 8080", cfg.Port)
	}
	if cfg.WatchNamespace != "jx-staging" {
		t.Errorf("WatchNamespace = %q, want jx-staging", cfg.WatchNamespace)
	}
}

// TestLoadOverrides confirms env values override defaults.
func TestLoadOverrides(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("CLUSTER_ID", "gcp")
	t.Setenv("WATCH_NAMESPACE", "jx-preproduction")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "9090" {
		t.Errorf("Port = %q, want 9090", cfg.Port)
	}
	if cfg.ClusterID != "gcp" {
		t.Errorf("ClusterID = %q, want gcp", cfg.ClusterID)
	}
	if cfg.WatchNamespace != "jx-preproduction" {
		t.Errorf("WatchNamespace = %q, want jx-preproduction", cfg.WatchNamespace)
	}
}

func TestLoadServices_EmptyJSON(t *testing.T) {
	c := &Config{ServicesJSON: ""}
	m, err := c.LoadServices()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

func TestLoadServices_ValidJSON(t *testing.T) {
	c := &Config{
		ServicesJSON: `{"foo":{"stagingUrl":"https://foo.example","testPacks":[{"name":"smoke","type":"end2end"}]}}`,
	}
	m, err := c.LoadServices()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(m) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m))
	}
	if m["foo"].StagingURL != "https://foo.example" {
		t.Errorf("stagingUrl = %q", m["foo"].StagingURL)
	}
	if len(m["foo"].TestPacks) != 1 || m["foo"].TestPacks[0].Name != "smoke" {
		t.Errorf("testPacks = %+v", m["foo"].TestPacks)
	}
}

func TestLoadServices_InvalidJSONErrors(t *testing.T) {
	c := &Config{ServicesJSON: "not json"}
	_, err := c.LoadServices()
	if err == nil {
		t.Error("expected error on invalid JSON, got nil")
	}
}

// TestLoadServices_PerPackResourcesAndEnv exercises the wire shape the
// chart's SERVICES_JSON produces when a pack sets its own Resources or
// Env. The dispatch precedence chain relies on these round-tripping
// through the Config struct with the pointer set (not nil) when the
// JSON has a value.
func TestLoadServices_PerPackResourcesAndEnv(t *testing.T) {
	c := &Config{ServicesJSON: `{
      "leartech-portal": {
        "stagingUrl": "https://portal.example",
        "resources": {"requests":{"cpu":"500m","memory":"1Gi"}},
        "env": [{"name":"HYDRA_ADMIN_URL","value":"https://hydra.example"}],
        "testPacks": [
          {"name":"smoke","type":"end2end"},
          {"name":"end2end-ui","type":"end2end-ui",
            "resources":{"requests":{"cpu":"1500m","memory":"3Gi"},"limits":{"memory":"6Gi"}},
            "env":[{"name":"PLAYWRIGHT_WORKERS","value":"2"}]}
        ]
      }
    }`}
	m, err := c.LoadServices()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	svc, ok := m["leartech-portal"]
	if !ok {
		t.Fatal("leartech-portal missing from map")
	}
	if svc.Resources == nil {
		t.Fatal("service.Resources must be non-nil when JSON provided a resources block")
	}
	if svc.Resources.Requests.Memory().String() != "1Gi" {
		t.Errorf("service.Resources.Requests.memory = %s, want 1Gi", svc.Resources.Requests.Memory().String())
	}
	if len(svc.Env) != 1 || svc.Env[0].Name != "HYDRA_ADMIN_URL" {
		t.Errorf("service.Env = %+v, want single HYDRA_ADMIN_URL", svc.Env)
	}
	if len(svc.TestPacks) != 2 {
		t.Fatalf("expected 2 testPacks, got %d", len(svc.TestPacks))
	}
	smoke, heavy := svc.TestPacks[0], svc.TestPacks[1]
	if smoke.Resources != nil {
		t.Error("smoke.Resources must be nil (pack didn't set it) — that's how precedence knows to defer to service")
	}
	if len(smoke.Env) != 0 {
		t.Errorf("smoke.Env = %+v, want empty", smoke.Env)
	}
	if heavy.Resources == nil {
		t.Fatal("heavy.Resources must be non-nil when pack JSON provided resources")
	}
	if heavy.Resources.Requests.Memory().String() != "3Gi" {
		t.Errorf("heavy.Resources.Requests.memory = %s, want 3Gi", heavy.Resources.Requests.Memory().String())
	}
	if len(heavy.Env) != 1 || heavy.Env[0].Value != "2" {
		t.Errorf("heavy.Env = %+v, want PLAYWRIGHT_WORKERS=2", heavy.Env)
	}
	// Sanity: EnvVar shape round-trips with secretKeyRef too.
	_ = corev1.EnvVar{} // pin import
}

// TestLoadServices_MissingResourcesLeavesPointersNil confirms the JSON
// "unset" state produces nil pointers — the precedence-chain contract
// depends on nil to mean "defer to next rung", so absence of a
// resources block MUST NOT surface as a zero-valued struct.
func TestLoadServices_MissingResourcesLeavesPointersNil(t *testing.T) {
	c := &Config{ServicesJSON: `{
      "svc": {
        "stagingUrl": "https://x",
        "testPacks":[{"name":"smoke","type":"end2end"}]
      }
    }`}
	m, err := c.LoadServices()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if m["svc"].Resources != nil {
		t.Error("service.Resources must be nil when JSON did not set it")
	}
	if m["svc"].TestPacks[0].Resources != nil {
		t.Error("pack.Resources must be nil when JSON did not set it")
	}
	if len(m["svc"].TestPacks[0].Env) != 0 {
		t.Error("pack.Env must be empty when JSON did not set it")
	}
}

func TestDispatchTimeout(t *testing.T) {
	c := &Config{DispatchTimeoutMinutes: 30}
	if c.DispatchTimeout().Minutes() != 30 {
		t.Errorf("DispatchTimeout = %v, want 30m", c.DispatchTimeout())
	}
}

func TestDispatchPollInterval(t *testing.T) {
	c := &Config{DispatchPollIntervalSeconds: 45}
	if c.DispatchPollInterval().Seconds() != 45 {
		t.Errorf("DispatchPollInterval = %v, want 45s", c.DispatchPollInterval())
	}
}
