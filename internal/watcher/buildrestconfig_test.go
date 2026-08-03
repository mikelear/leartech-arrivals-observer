package watcher

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildRestConfig_FromKubeconfigPath — the out-of-cluster fallback
// path. Write a minimal kubeconfig to a temp file and confirm
// buildRestConfig loads it.
func TestBuildRestConfig_FromKubeconfigPath(t *testing.T) {
	dir := t.TempDir()
	kcPath := filepath.Join(dir, "kubeconfig.yaml")
	// Minimal valid kubeconfig — clientcmd needs cluster+user+context.
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
	require.NoError(t, err, "buildRestConfig with an explicit kubeconfig path must succeed")
	require.NotNil(t, cfg)
	assert.Equal(t, "https://kubernetes.default:443", cfg.Host)
}

// TestBuildRestConfig_InCluster_FailsGracefully — with no kubeconfig
// path AND no in-cluster env (no service-account files), the fallback
// should return an error rather than panic. Real production runs have
// the ServiceAccount volume mounted so the branch differs; this test
// covers the failure branch that surfaces during laptop `go run`.
func TestBuildRestConfig_InCluster_FailsGracefully(t *testing.T) {
	// Ensure envtest / test env doesn't leak in-cluster hints.
	saved := os.Getenv("KUBERNETES_SERVICE_HOST")
	_ = os.Unsetenv("KUBERNETES_SERVICE_HOST")
	defer func() {
		if saved != "" {
			_ = os.Setenv("KUBERNETES_SERVICE_HOST", saved)
		}
	}()

	_, err := buildRestConfig("")
	assert.Error(t, err, "in-cluster config outside a cluster must return an error, not panic")
}
