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
	os.Unsetenv("PORT")
	os.Unsetenv("CLUSTER_ID")
	os.Unsetenv("WATCH_NAMESPACE")

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
