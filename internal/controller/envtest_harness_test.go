// envtest_harness_test.go bootstraps a high-fidelity K8s test
// environment for controller tests that want a REAL apiserver + etcd
// (no kubelet). Complements the fast dynamicfake-backed unit tests in
// controller_test.go — those cover the state-machine logic; this
// harness catches the classes of bugs fakes can't (patch semantics
// nobody re-implements identically, CRD schema validation, status
// subresource semantics, resource-version conflicts).
//
// GATED — the harness is only exercised when the envtest binaries are
// installed AND KUBEBUILDER_ASSETS points at them. Without those,
// `go test` skips this file's tests cleanly so a laptop-without-assets
// run stays green. The catalog go-test task installs envtest via the
// setup-envtest tool, so CI exercises this path.
//
// Design mirrors orchestrator-controller's envtest bootstrap in intent:
// one *envtest.Environment shared across tests via TestMain, with
// per-test namespace isolation so each test operates on its own
// jx-envtest-<N> namespace.
package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// envtestCtx holds the shared envtest environment + REST config. Set
// by envtestSetup, read by tests that require a live apiserver.
var (
	envtestREST     *rest.Config
	envtestSkip     bool
	envtestSkipMsg  string
	envtestNsCount  atomic.Int32 // per-test namespace suffix
	envtestStopFunc func() error
)

// envtestAssetsAvailable is true iff the KUBEBUILDER_ASSETS env var is
// set (or the fallback path exists at /usr/local/kubebuilder/bin) AND
// contains at least the etcd + kube-apiserver binaries envtest boots.
// Mirrors the skip-guard pattern from
// orchestrator-controller/internal/controller/envtest_test.go: keep
// laptop `go test ./...` green when nobody installed the assets, but
// run for real in CI where they're guaranteed.
func envtestAssetsAvailable() (string, bool) {
	if p := os.Getenv("KUBEBUILDER_ASSETS"); p != "" {
		if _, err := os.Stat(filepath.Join(p, "etcd")); err == nil {
			if _, err := os.Stat(filepath.Join(p, "kube-apiserver")); err == nil {
				return p, true
			}
		}
	}
	// Fallback location the setup-envtest tool defaults to.
	fallback := "/usr/local/kubebuilder/bin"
	if _, err := os.Stat(filepath.Join(fallback, "etcd")); err == nil {
		if _, err := os.Stat(filepath.Join(fallback, "kube-apiserver")); err == nil {
			return fallback, true
		}
	}
	return "", false
}

// TestMain boots the envtest environment once for all tests in this
// package (so etcd + kube-apiserver don't start N times). If envtest
// assets aren't installed, sets envtestSkip so individual tests can
// t.Skip with a clear reason and the rest of the package still runs.
func TestMain(m *testing.M) {
	assetsDir, ok := envtestAssetsAvailable()
	if !ok {
		envtestSkip = true
		envtestSkipMsg = "envtest assets (etcd + kube-apiserver) not found — set KUBEBUILDER_ASSETS or install to /usr/local/kubebuilder/bin; skipping envtest-backed cases"
		os.Exit(m.Run())
		return
	}

	// Load the Arrival CRD from the chart so envtest installs it on
	// startup — the controller reconciles Arrivals so the CRD must
	// exist. Chart-file path is repo-root/charts/.../crd-arrival.yaml.
	// Template is Helm-gated but the base object is standard CRD YAML
	// so we can render it minimally with a synthetic if-branch pass.
	crdBytes := readChartCRD()

	env := &envtest.Environment{
		BinaryAssetsDirectory: assetsDir,
		CRDInstallOptions: envtest.CRDInstallOptions{
			CRDs: parseCRDs(crdBytes),
		},
		// Longer than default so a slow start (CI cold cache) doesn't
		// flake the whole package.
		ControlPlaneStartTimeout: 60 * time.Second,
	}

	restCfg, err := env.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest.Start: %v\n", err)
		os.Exit(2)
	}
	envtestREST = restCfg
	envtestStopFunc = env.Stop

	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

// readChartCRD returns the Helm-rendered form of the chart's CRD file
// — the file has one `{{- if .Values.crd.install }}` gate which we
// pre-strip so we don't drag in the whole Helm engine just for this
// bootstrap. If the chart layout changes so the CRD needs Helm
// templating for real, replace this with a chartrender.Render call.
func readChartCRD() []byte {
	path := filepath.Join("..", "..", "charts", "leartech-arrivals-observer", "templates", "crd-arrival.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		panic("read chart CRD: " + err.Error())
	}
	// Strip the two lines that gate the CRD: `{{- if .Values.crd.install }}`
	// and `{{- end }}`. Everything between is standard CRD YAML.
	// Also strip the `metadata.labels` block that would reference a
	// template helper — envtest doesn't need it and it's not standard YAML.
	out := stripHelmDirectives(b)
	return out
}

// stripHelmDirectives removes lines containing `{{ ... }}` from a YAML
// file that only uses Helm directives to gate top-level presence — as
// crd-arrival.yaml does. NOT a general Helm renderer.
func stripHelmDirectives(b []byte) []byte {
	// Fast path — split, filter, rejoin.
	lines := splitLines(b)
	out := make([]byte, 0, len(b))
	inHelmDirective := false
	for _, l := range lines {
		s := string(l)
		if containsHelmBraces(s) {
			// If the line is only `{{ ... }}` we drop it; if it's
			// e.g. `name: {{ .foo }}` we couldn't render safely — but
			// crd-arrival.yaml only uses gate directives at the top
			// level.
			inHelmDirective = true
			_ = inHelmDirective
			continue
		}
		out = append(out, l...)
		out = append(out, '\n')
	}
	return out
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

func containsHelmBraces(s string) bool {
	// Detect Helm-style {{ or }}. This is a conservative check — any
	// line containing either token is dropped. The chart's CRD file
	// uses these only in gate lines.
	for i := 0; i < len(s)-1; i++ {
		if s[i] == '{' && s[i+1] == '{' {
			return true
		}
		if s[i] == '}' && s[i+1] == '}' {
			return true
		}
	}
	return false
}

// parseCRDs decodes the CRD YAML into a slice of CustomResourceDefinition
// values ready for envtest to install. Uses the k8s runtime's
// YAML-to-typed decoder so quirks like missing apiVersion get flagged.
func parseCRDs(b []byte) []*apiextv1.CustomResourceDefinition {
	scheme := runtime.NewScheme()
	_ = apiextv1.AddToScheme(scheme)
	dec := yaml.NewDecodingSerializer(nil)
	_ = dec // unused but retained for potential future multi-doc splitting

	// Single-doc for now.
	crd := &apiextv1.CustomResourceDefinition{}
	if err := decodeYAML(b, crd); err != nil {
		panic("decode CRD YAML: " + err.Error())
	}
	return []*apiextv1.CustomResourceDefinition{crd}
}

// decodeYAML uses sigs yaml decoder via runtime encode path. We keep
// this thin because envtest's own CRDInstallOptions.Paths accepts YAML
// files directly, which we could switch to if this proves fragile.
func decodeYAML(b []byte, out *apiextv1.CustomResourceDefinition) error {
	// Manually decode with k8s.io/apimachinery/pkg/util/yaml — a bit
	// heavy but no interpreter quirks.
	return yamlToStruct(b, out)
}

// requireEnvtest skips the test if envtest isn't available and returns
// a REST config + clients otherwise. Tests must call this as the
// first line so they cooperatively bail early.
func requireEnvtest(t *testing.T) (*rest.Config, kubernetes.Interface, dynamic.Interface, string) {
	t.Helper()
	if envtestSkip {
		t.Skipf("envtest disabled: %s", envtestSkipMsg)
	}
	cs, err := kubernetes.NewForConfig(envtestREST)
	if err != nil {
		t.Fatalf("kubernetes.NewForConfig: %v", err)
	}
	dyn, err := dynamic.NewForConfig(envtestREST)
	if err != nil {
		t.Fatalf("dynamic.NewForConfig: %v", err)
	}
	// Create an isolated namespace for this test so parallel tests
	// don't step on each other's Arrivals.
	nsIndex := envtestNsCount.Add(1)
	ns := fmt.Sprintf("jx-envtest-%03d", nsIndex)
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	_, err = cs.CoreV1().Namespaces().Create(context.Background(), nsObj, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", ns, err)
	}
	t.Cleanup(func() {
		_ = cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
	})
	return envtestREST, cs, dyn, ns
}

// arrivalGVRForEnvtest returns the Arrival GVR. Kept close to the
// tests to avoid an import from the package-under-test's constants.
var arrivalGVRForEnvtest = schema.GroupVersionResource{
	Group:    "qa.leartech.com",
	Version:  "v1alpha1",
	Resource: "arrivals",
}

// newArrivalCR builds an Arrival Unstructured suitable for envtest
// Create. Fields match the CRD's required set (service, version,
// replicaSet, deployedAt) plus any packs the caller wants.
func newArrivalCR(ns, name, service, version string, packs []map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   arrivalGVRForEnvtest.Group,
		Version: arrivalGVRForEnvtest.Version,
		Kind:    "Arrival",
	})
	u.SetName(name)
	u.SetNamespace(ns)
	_ = unstructured.SetNestedField(u.Object, service, "spec", "service")
	_ = unstructured.SetNestedField(u.Object, version, "spec", "version")
	_ = unstructured.SetNestedField(u.Object, "test-rs", "spec", "replicaSet")
	_ = unstructured.SetNestedField(u.Object, time.Now().UTC().Format(time.RFC3339), "spec", "deployedAt")
	if len(packs) > 0 {
		raw := make([]any, len(packs))
		for i, p := range packs {
			raw[i] = p
		}
		_ = unstructured.SetNestedSlice(u.Object, raw, "spec", "testPacks")
	}
	return u
}
