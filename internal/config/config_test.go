package config

import (
	"os"
	"testing"
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
