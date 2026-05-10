package watcher

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// TestEnvVarsToSlice locks the round-trip contract for per-service
// env injection: literal {name, value} and {name, valueFrom:
// {secretKeyRef: …}} both survive the unstructured.Unstructured
// encode/decode cycle.
func TestEnvVarsToSlice_LiteralAndSecretRef(t *testing.T) {
	in := []corev1.EnvVar{
		{Name: "USER_ID", Value: "user-test-001"},
		{Name: "HYDRA_ADMIN_URL", Value: "http://hydra:4445"},
		{
			Name: "USER_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "auth-service-test-user"},
					Key:                  "password",
				},
			},
		},
	}
	out, err := envVarsToSlice(in)
	if err != nil {
		t.Fatalf("envVarsToSlice: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(out))
	}

	// Round-trip back through JSON to verify shape preserved.
	rawBytes, _ := json.Marshal(out)
	var roundTrip []corev1.EnvVar
	if err := json.Unmarshal(rawBytes, &roundTrip); err != nil {
		t.Fatalf("decode round-trip: %v", err)
	}
	if roundTrip[0].Name != "USER_ID" || roundTrip[0].Value != "user-test-001" {
		t.Errorf("literal #0 mangled: %+v", roundTrip[0])
	}
	if roundTrip[2].ValueFrom == nil || roundTrip[2].ValueFrom.SecretKeyRef == nil {
		t.Fatalf("secretKeyRef lost in round-trip: %+v", roundTrip[2])
	}
	if roundTrip[2].ValueFrom.SecretKeyRef.Name != "auth-service-test-user" {
		t.Errorf("secret name mangled: %s", roundTrip[2].ValueFrom.SecretKeyRef.Name)
	}
	if roundTrip[2].ValueFrom.SecretKeyRef.Key != "password" {
		t.Errorf("secret key mangled: %s", roundTrip[2].ValueFrom.SecretKeyRef.Key)
	}
}

func TestEnvVarsToSlice_Empty(t *testing.T) {
	out, err := envVarsToSlice(nil)
	if err != nil || out != nil {
		t.Errorf("nil input should give nil/nil, got (%v, %v)", out, err)
	}
}
