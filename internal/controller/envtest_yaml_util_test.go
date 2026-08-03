//go:build integration

// envtest_yaml_util_test.go — small helper factored out to isolate the
// yaml-decode step so the harness file can focus on the harness.
//
// Gated by the `integration` build tag: helper is only used by the
// envtest_harness_test.go bootstrap, which is itself under the same
// tag. Excluded from the default `go test ./...` build.
package controller

import (
	"fmt"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// yamlToStruct decodes the given YAML into a typed CRD. Uses the
// buffered-YAML reader so trailing newlines / unknown fields don't
// break the parse. Errors are wrapped with a caller-friendly prefix so
// TestMain's panic message points at the underlying cause.
func yamlToStruct(b []byte, out *apiextv1.CustomResourceDefinition) error {
	// yaml.Unmarshal handles JSON+YAML seamlessly and preserves the
	// full apiextv1 schema (including the openAPIV3Schema block).
	if err := yaml.Unmarshal(b, out); err != nil {
		return fmt.Errorf("yaml.Unmarshal CRD: %w", err)
	}
	if out.Kind != "CustomResourceDefinition" {
		return fmt.Errorf("decoded YAML is not a CRD (got Kind=%q)", out.Kind)
	}
	return nil
}
