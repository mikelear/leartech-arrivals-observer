// Package dispatch creates K8s Jobs that execute a single test pack
// against an arrival's stagingUrl. Each Job:
//
//  1. Clones <repoHost>/<repoOrg>/<service> at the highest-priority
//     refspec from the configured fallback chain
//  2. Waits for STAGING_URL/<healthEndpoint> to return 200 N times
//  3. Runs the test pack (end2end → bash run.sh, end2end-ui → playwright)
//  4. Uploads results.json + Playwright artifacts to the result-store
//     under the path template configured in chart values.paths
//  5. Exits 0 on success, non-zero on test failure or runner crash
//
// Path layout (CONTRACT — must match leartech-gate's reader):
//
//	gs://<bucket>/results/v1/post-deploy/<cluster>/<namespace>/<service>/<version>/<pack>/
package dispatch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"github.com/rs/zerolog/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Config controls dispatch behaviour. Fields populated from chart values
// via the observer's ConfigMap (see internal/config/config.go).
type Config struct {
	RunnerImage           string
	ResultStoreBucket     string
	GCSKeySecret          string
	ClusterID             string
	ActiveDeadlineSeconds int64

	// Repo discovery
	RepoHost             string
	RepoOrg              string
	RefFallbackTemplates []string // each is a Go text/template

	// Health probe
	HealthEndpoint         string
	HealthTimeoutSeconds   int
	HealthCurlSeconds      int
	HealthPollSeconds      int
	HealthSuccessThreshold int

	// Resources for the dispatched Job pod.
	Resources corev1.ResourceRequirements

	// Git auth secret refs.
	GitSecretName string
	GitSecretKey  string

	// PostDeployPathTemplate is the result-store path template (Go
	// text/template). Rendered per (cluster, namespace, service, version,
	// pack) at Dispatch-time and passed as RESULT_STORE_PATH_PREFIX env.
	PostDeployPathTemplate string
}

// Test describes one test-pack to dispatch.
type Test struct {
	PackName string // e.g. "end2end-ui"
	PackType string // e.g. "end2end-ui"
}

// Args bundles the per-arrival fields needed to render a Job.
type Args struct {
	ArrivalName string
	Namespace   string
	Service     string
	Version     string
	StagingURL  string

	// Env carries per-service env injection from chart values
	// (services.<name>.env), threaded via the Arrival CR spec.env.
	// Appended to the Job's env after the standard set; literal
	// values + secretKeyRef both supported (corev1.EnvVar shape).
	Env []corev1.EnvVar
}

// Dispatcher creates Jobs.
type Dispatcher struct {
	cfg     Config
	clients kubernetes.Interface
}

// New constructs a Dispatcher.
func New(cfg Config, clients kubernetes.Interface) *Dispatcher {
	return &Dispatcher{cfg: cfg, clients: clients}
}

// Dispatch creates one Job per Test. Returns a map test-pack-name → job-name
// for the controller to record on Arrival.status.tests[].jobName.
func (d *Dispatcher) Dispatch(ctx context.Context, args Args, tests []Test) (map[string]string, error) {
	out := make(map[string]string, len(tests))
	for _, t := range tests {
		jobName := jobNameFor(args.ArrivalName, t.PackName)
		job, err := d.buildJob(args, t, jobName)
		if err != nil {
			return nil, fmt.Errorf("build job %s: %w", jobName, err)
		}
		_, err = d.clients.BatchV1().Jobs(args.Namespace).Create(ctx, job, metav1.CreateOptions{})
		switch {
		case err == nil:
			log.Info().
				Str("arrival", args.ArrivalName).
				Str("job", jobName).
				Str("pack", t.PackName).
				Msg("dispatched test job")
		case apierrors.IsAlreadyExists(err):
			log.Info().Str("job", jobName).Msg("job already exists; reusing")
		default:
			return nil, fmt.Errorf("create job %s: %w", jobName, err)
		}
		out[t.PackName] = jobName
	}
	return out, nil
}

// pathVars is the substitution context for the post-deploy path template.
type pathVars struct {
	Cluster   string
	Namespace string
	Service   string
	Version   string
	Pack      string
}

// refVars is the substitution context for refspec fallback templates.
// Pack/Service intentionally not exposed — refs are version+cluster-only.
type refVars struct {
	Version string
	Cluster string
}

// buildJob constructs the Job spec. The container runs an inline bash
// script that does clone+wait+test+upload — keeps the runner image
// generic and the per-pack logic visible in the Arrival CR's namespace.
func (d *Dispatcher) buildJob(args Args, t Test, jobName string) (*batchv1.Job, error) {
	const backoff int32 = 0 // no auto-retry; arrivals-observer manages retries
	deadline := d.cfg.ActiveDeadlineSeconds

	// Render path prefix once per Job — the runner script reads
	// RESULT_STORE_PATH_PREFIX directly without any shell substitution.
	prefix, err := renderTemplate(d.cfg.PostDeployPathTemplate, pathVars{
		Cluster:   d.cfg.ClusterID,
		Namespace: args.Namespace,
		Service:   args.Service,
		Version:   args.Version,
		Pack:      t.PackName,
	})
	if err != nil {
		return nil, fmt.Errorf("render postDeployPathTemplate: %w", err)
	}

	// Render refspec fallbacks — space-joined for the script's `for`-loop.
	refs, err := renderRefFallbacks(d.cfg.RefFallbackTemplates, refVars{
		Version: args.Version,
		Cluster: d.cfg.ClusterID,
	})
	if err != nil {
		return nil, fmt.Errorf("render refFallbacks: %w", err)
	}

	standardEnv := []corev1.EnvVar{
		{Name: "STAGING_URL", Value: args.StagingURL},
		{Name: "SERVICE", Value: args.Service},
		{Name: "VERSION", Value: args.Version},
		{Name: "TEST_PACK", Value: t.PackName},
		{Name: "TEST_PACK_TYPE", Value: t.PackType},
		{Name: "RESULT_STORE_BUCKET", Value: d.cfg.ResultStoreBucket},
		{Name: "RESULT_STORE_PATH_PREFIX", Value: prefix},
		{Name: "CLUSTER_ID", Value: d.cfg.ClusterID},
		{Name: "NAMESPACE", Value: args.Namespace},
		{Name: "ARRIVAL_NAME", Value: args.ArrivalName},
		{Name: "REPO_HOST", Value: d.cfg.RepoHost},
		{Name: "REPO_ORG", Value: d.cfg.RepoOrg},
		{Name: "REF_FALLBACKS", Value: strings.Join(refs, " ")},
		{Name: "HEALTH_ENDPOINT", Value: d.cfg.HealthEndpoint},
		{Name: "HEALTH_TIMEOUT_SECONDS", Value: fmt.Sprintf("%d", d.cfg.HealthTimeoutSeconds)},
		{Name: "HEALTH_CURL_SECONDS", Value: fmt.Sprintf("%d", d.cfg.HealthCurlSeconds)},
		{Name: "HEALTH_POLL_SECONDS", Value: fmt.Sprintf("%d", d.cfg.HealthPollSeconds)},
		{Name: "HEALTH_SUCCESS_THRESHOLD", Value: fmt.Sprintf("%d", d.cfg.HealthSuccessThreshold)},
		{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/run/secrets/test-artifacts/key.json"},
		{
			Name: "GIT_TOKEN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: d.cfg.GitSecretName},
					Key:                  d.cfg.GitSecretKey,
					Optional:             ptrBool(true),
				},
			},
		},
	}
	envVars := make([]corev1.EnvVar, 0, len(standardEnv)+len(args.Env))
	envVars = append(envVars, standardEnv...)
	// Append per-service env (HYDRA_ADMIN_URL, USER_EMAIL, etc.) so
	// test specs can read them via process.env.X. Append AFTER the
	// standard set so per-service overrides effectively layer on top
	// (last value wins for duplicates per K8s semantics).
	envVars = append(envVars, args.Env...)

	resources := d.cfg.Resources
	if len(resources.Requests) == 0 && len(resources.Limits) == 0 {
		// Defensive default — chart should always set these but if running
		// outside Helm (e.g. unit tests) we still want sane values.
		resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1500m"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: args.Namespace,
			Labels: map[string]string{
				"qa.leartech.com/arrival":      args.ArrivalName,
				"qa.leartech.com/service":      args.Service,
				"qa.leartech.com/version":      sanitizeLabel(args.Version),
				"qa.leartech.com/test-pack":    t.PackName,
				"app.kubernetes.io/managed-by": "leartech-arrivals-observer",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptrInt32(backoff),
			ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"qa.leartech.com/arrival":   args.ArrivalName,
						"qa.leartech.com/test-pack": t.PackName,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{{
						Name: "gcs-key",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: d.cfg.GCSKeySecret,
								Optional:   ptrBool(true),
							},
						},
					}},
					Containers: []corev1.Container{{
						Name:  "runner",
						Image: d.cfg.RunnerImage,
						Env:   envVars,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "gcs-key",
							MountPath: "/var/run/secrets/test-artifacts",
							ReadOnly:  true,
						}},
						Resources: resources,
						Command:   []string{"bash", "-c", runnerScript},
					}},
				},
			},
		},
	}, nil
}

// renderTemplate runs a Go text/template against the given data.
// Returns "" if the template is empty (nothing to render).
func renderTemplate(tmpl string, data any) (string, error) {
	if tmpl == "" {
		return "", nil
	}
	t, err := template.New("").Option("missingkey=error").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute: %w", err)
	}
	return buf.String(), nil
}

// renderRefFallbacks renders each template in the list against the given
// version/cluster context. Empty entries are dropped.
func renderRefFallbacks(templates []string, vars refVars) ([]string, error) {
	out := make([]string, 0, len(templates))
	for _, tmpl := range templates {
		s, err := renderTemplate(tmpl, vars)
		if err != nil {
			return nil, fmt.Errorf("ref %q: %w", tmpl, err)
		}
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// ParseResources unmarshals a chart-rendered JSON string into a
// corev1.ResourceRequirements. Empty input → empty struct (controller
// will fall back to dispatch.go's defensive defaults).
func ParseResources(jsonStr string) (corev1.ResourceRequirements, error) {
	var out corev1.ResourceRequirements
	if jsonStr == "" || jsonStr == "{}" {
		return out, nil
	}
	if err := json.Unmarshal([]byte(jsonStr), &out); err != nil {
		return out, fmt.Errorf("parse resources JSON: %w", err)
	}
	return out, nil
}

// runnerScript is the inline bash that runs inside the Job container.
// Reads env, clones, waits, runs the test pack, uploads results.
const runnerScript = `set -euo pipefail
echo "==> arrival=$ARRIVAL_NAME service=$SERVICE version=$VERSION pack=$TEST_PACK type=$TEST_PACK_TYPE"
echo "==> stagingUrl=$STAGING_URL cluster=$CLUSTER_ID namespace=$NAMESPACE bucket=$RESULT_STORE_BUCKET"
echo "==> resultPathPrefix=$RESULT_STORE_PATH_PREFIX"

WORK=/tmp/work
mkdir -p "$WORK" && cd "$WORK"

# Most service repos are PRIVATE; embed the GitHub token in the clone
# URL when available. Fall back to anonymous for public repos. Without
# auth, private-repo clones get HTTP 401 which git reports as
# "branch not found" — masks the real failure.
if [ -n "${GIT_TOKEN:-}" ]; then
  REPO_URL="https://x-access-token:${GIT_TOKEN}@${REPO_HOST}/${REPO_ORG}/${SERVICE}.git"
else
  REPO_URL="https://${REPO_HOST}/${REPO_ORG}/${SERVICE}.git"
  echo "::WARN GIT_TOKEN not set; private-repo clones will fail with 401"
fi

# REF_FALLBACKS is a space-separated list rendered by the controller from
# chart values.dispatch.refFallbacks. Try most-specific refspec first.
clone_with_fallback() {
  for ref in $REF_FALLBACKS; do
    [ -z "$ref" ] && continue
    echo "==> trying clone --branch=$ref"
    if git clone --depth=1 --branch "$ref" "$REPO_URL" repo 2>/dev/null; then
      echo "==> cloned at ref=$ref"
      return 0
    fi
  done
  echo "::FATAL no matching ref found (tried: $REF_FALLBACKS)"
  return 1
}
clone_with_fallback
cd repo

# Health-probe the staging URL — N consecutive successes within
# HEALTH_TIMEOUT_SECONDS. Each curl times out after HEALTH_CURL_SECONDS.
END=$(( $(date +%s) + HEALTH_TIMEOUT_SECONDS ))
HEALTH=0
while [ "$(date +%s)" -lt "$END" ]; do
  if curl -sSf -o /dev/null -m "$HEALTH_CURL_SECONDS" "${STAGING_URL}${HEALTH_ENDPOINT}"; then
    HEALTH=$((HEALTH+1))
    [ "$HEALTH" -ge "$HEALTH_SUCCESS_THRESHOLD" ] && break
  else
    HEALTH=0
  fi
  sleep "$HEALTH_POLL_SECONDS"
done
[ "$HEALTH" -ge "$HEALTH_SUCCESS_THRESHOLD" ] || { echo "::FATAL stagingUrl health failed"; exit 1; }
echo "==> staging healthy"

cd "$TEST_PACK"
TEST_EXIT=0
case "$TEST_PACK_TYPE" in
  end2end)
    [ -x run.sh ] || { echo "::FATAL no executable run.sh in $TEST_PACK"; exit 1; }
    PREVIEW_URL="$STAGING_URL" bash run.sh || TEST_EXIT=$?
    ;;
  end2end-ui)
    npm ci --no-audit --no-fund || npm install --no-audit --no-fund
    # Trace/screenshot/video are config-only in Playwright (no CLI flag);
    # service playwright.config.ts already enables them in 'use'. Add the
    # html reporter alongside list so we get a browsable report uploaded.
    PREVIEW_URL="$STAGING_URL" \
      npx playwright test \
        --reporter=list,html \
        --trace=on \
        || TEST_EXIT=$?
    ;;
  *)
    echo "::FATAL unsupported TEST_PACK_TYPE=$TEST_PACK_TYPE"; exit 1
    ;;
esac

# Auth gcloud once for all uploads.
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" 2>/dev/null || \
  echo "::WARN gcloud auth failed (key-file missing?); uploads may fail"

# Upload destination: pre-rendered template prefix from controller
# (CONTRACT — must match leartech-gate's reader).
DEST_PREFIX="gs://${RESULT_STORE_BUCKET}/${RESULT_STORE_PATH_PREFIX}"

if [ -f results.json ]; then
  echo "==> uploading results.json to ${DEST_PREFIX}/results.json"
  gsutil cp results.json "${DEST_PREFIX}/results.json" || \
    echo "::WARN results.json upload failed"
fi

# Playwright artifacts — trace.zip + screenshots + videos + HTML report.
if [ -d "test-results" ]; then
  echo "==> uploading Playwright artifacts (test-results/) to ${DEST_PREFIX}/test-results/"
  gsutil -m cp -r test-results "${DEST_PREFIX}/" 2>&1 | tail -3 || \
    echo "::WARN test-results upload failed"
fi
if [ -d "playwright-report" ]; then
  echo "==> uploading Playwright HTML report to ${DEST_PREFIX}/playwright-report/"
  gsutil -m cp -r playwright-report "${DEST_PREFIX}/" 2>&1 | tail -3 || \
    echo "::WARN playwright-report upload failed"
fi

# Contract translation: catalog-shared run.sh exits 0 regardless of
# individual test outcomes (so the catalog's PR-time task uploads
# artifacts then reads results.json.success). Our K8s Job-based
# dispatcher needs the OPPOSITE — the Job's exit code MUST reflect
# actual test pass/fail so arrivals-observer's controller marks
# Arrival.phase correctly (which gates the qa-gate verdict downstream
# and triggers forensics-runner on Failed). Read results.json here,
# AFTER uploads but BEFORE final exit, and override TEST_EXIT when
# success=false. Only relevant for end2end (Playwright end2end-ui
# already propagates real exit codes via npx playwright test).
# Verified 2026-05-11: without this translation, canary 0.0.7's
# deliberate-fail test pack reached Arrival.phase=Passed despite
# results.json.success=false.
if [ "$TEST_PACK_TYPE" = "end2end" ] && [ "$TEST_EXIT" -eq 0 ] && [ -f results.json ]; then
  REPORTED_SUCCESS=""
  if command -v jq >/dev/null 2>&1; then
    # Read .success directly — do NOT use the jq alternative operator
    # (.success // true) because it returns true when .success is the
    # literal value false (falsy). On missing key or malformed JSON,
    # jq output is empty/null; || true swallows the exit code under
    # set -e (jq exits non-zero on parse errors).
    REPORTED_SUCCESS=$(jq -r '.success' results.json 2>/dev/null || true)
  else
    # Grep fallback for runner images without jq. Matches "success": false
    # with optional whitespace.
    if grep -qE '"success"[[:space:]]*:[[:space:]]*false' results.json; then
      REPORTED_SUCCESS="false"
    fi
  fi
  if [ "$REPORTED_SUCCESS" = "false" ]; then
    echo "==> results.json.success=false — overriding TEST_EXIT 0 → 1 so Arrival.phase=Failed"
    TEST_EXIT=1
  fi
fi

echo "==> done; test exit code=${TEST_EXIT}"
exit $TEST_EXIT
`

// jobNameFor builds the K8s Job name for an arrival × pack pair.
//
// K8s Pods derived from Jobs are named "<job-name>-<random-suffix>"
// (~6 chars). Pod names must be DNS-1123 LABELS — max 63 chars — so the
// Job name itself is effectively capped at 63 to leave room. K8s
// enforces this at create time.
//
// Naïve truncation (name[:63]) chops mid-string and can lose the pack
// suffix entirely — e.g. "ar-leartech-angular-service-template-0-0-13-
// jx-staging-end2end-ui" cuts to "...jx-stagi" with no pack name, and
// the controller's job-status polling can't find the Job back.
//
// Strategy: short names pass through unchanged. Long names get a
// deterministic 8-char SHA-256 hash of the arrival name, then the pack
// suffix concatenated verbatim — pack stays visible for `kubectl get
// jobs | grep <pack>` and the hash is stable across observer restarts
// so the status-poll keeps working.
func jobNameFor(arrivalName, pack string) string {
	arr := strings.ToLower(strings.ReplaceAll(arrivalName, "_", "-"))
	p := strings.ToLower(strings.ReplaceAll(pack, "_", "-"))
	name := fmt.Sprintf("ar-%s-%s", arr, p)
	if len(name) <= 63 {
		return name
	}

	h := sha256.Sum256([]byte(arr))
	hashPart := hex.EncodeToString(h[:4]) // 8 chars; collision risk negligible per service
	// Budget: "ar-" + 8 + "-" + len(p) = 12 + len(p). If pack alone exceeds
	// 51 chars, truncate the pack too (extreme edge — real pack names are
	// "end2end", "smoke", "end2end-ui" etc.).
	const maxPack = 51
	if len(p) > maxPack {
		p = strings.TrimRight(p[:maxPack], "-")
	}
	return fmt.Sprintf("ar-%s-%s", hashPart, p)
}

func sanitizeLabel(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '.', c == '_':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c-'A'+'a')
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

func ptrInt32(v int32) *int32 { return &v }
func ptrBool(v bool) *bool    { return &v }

// JobStatus summarises a Job's phase for the controller.
type JobStatus int

// JobStatus enum values. JobUnknown covers both NotFound and "no terminal
// signal yet" — controller treats it as "keep polling".
const (
	JobUnknown JobStatus = iota
	JobRunning
	JobPassed
	JobFailed
)

// GetStatus reads a Job and returns its summarised state.
func (d *Dispatcher) GetStatus(ctx context.Context, namespace, jobName string) (JobStatus, error) {
	job, err := d.clients.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return JobUnknown, nil
		}
		return JobUnknown, err
	}
	if job.Status.Succeeded > 0 {
		return JobPassed, nil
	}
	if job.Status.Failed > 0 {
		return JobFailed, nil
	}
	return JobRunning, nil
}
