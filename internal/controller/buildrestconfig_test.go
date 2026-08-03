package controller

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildRestConfig_FromKubeconfigPath — exercises the fallback used
// by out-of-cluster debug runs. Mirrors the watcher's equivalent test
// so both entry points into the controller are covered.
func TestBuildRestConfig_FromKubeconfigPath(t *testing.T) {
	dir := t.TempDir()
	kcPath := filepath.Join(dir, "kubeconfig.yaml")
	err := os.WriteFile(kcPath, []byte(`
apiVersion: v1
kind: Config
current-context: test
clusters:
- name: test
  cluster:
    server: https://kubernetes.default:443
contexts:
- name: test
  context:
    cluster: test
    user: test
users:
- name: test
  user:
    token: fake-token
`), 0o600)
	require.NoError(t, err)

	cfg, err := buildRestConfig(kcPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "https://kubernetes.default:443", cfg.Host)
}

// TestBuildRestConfig_InCluster_FailsGracefully covers the branch
// where KUBECONFIG is unset AND rest.InClusterConfig() can't find the
// SA files. The controller boot logs the error and exits — production
// paths always have the SA mount.
func TestBuildRestConfig_InCluster_FailsGracefully(t *testing.T) {
	saved := os.Getenv("KUBERNETES_SERVICE_HOST")
	_ = os.Unsetenv("KUBERNETES_SERVICE_HOST")
	defer func() {
		if saved != "" {
			_ = os.Setenv("KUBERNETES_SERVICE_HOST", saved)
		}
	}()

	_, err := buildRestConfig("")
	assert.Error(t, err)
}
