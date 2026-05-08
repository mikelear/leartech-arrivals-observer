// Package dispatch creates K8s Jobs that execute a single test pack
// against an arrival's stagingUrl. Each Job:
//
//  1. Clones github.com/mikelear/<service> at tag v<version>
//  2. Waits for STAGING_URL/health/live to return 200
//  3. Runs the test pack (end2end → bash run.sh, end2end-ui → playwright)
//  4. Uploads results.json to gs://<bucket>/results/v1/post-deploy/
//     <service>/<version>/<cluster>/<pack>/results.json
//  5. Exits 0 on success, 1 on test failure or runner crash
//
// The controller polls Job.status to drive the Arrival lifecycle. The
// Job's pass/fail is the source of truth for now; a future result-store
// reader can layer on top to capture per-test details from results.json.
package dispatch

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Config controls dispatch behaviour.
type Config struct {
	// RunnerImage is the default container image (overridable per test
	// pack — defined in values.services.<name>.testPacks[].runnerImage,
	// not yet wired through; using default for 2.7.2b first cut).
	RunnerImage string

	// ResultStoreBucket — gs://<bucket>/results/v1/post-deploy/...
	ResultStoreBucket string

	// GCSKeySecret holds the JSON service-account key for GCS upload.
	// Must exist in the same namespace as the dispatched Job.
	GCSKeySecret string

	// ClusterID — recorded into the result-store path.
	ClusterID string

	// ActiveDeadlineSeconds caps the Job's runtime. Aligned to the
	// controller's per-Arrival Timeout so the Job can't outlive the
	// Arrival.
	ActiveDeadlineSeconds int64
}

// Test describes one test-pack to dispatch.
type Test struct {
	PackName string // e.g. "smoke"
	PackType string // e.g. "end2end" or "end2end-ui"
}

// Args bundles the per-arrival fields needed to render a Job.
type Args struct {
	ArrivalName string
	Namespace   string
	Service     string
	Version     string
	StagingURL  string
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
		job := d.buildJob(args, t, jobName)
		_, err := d.clients.BatchV1().Jobs(args.Namespace).Create(ctx, job, metav1.CreateOptions{})
		switch {
		case err == nil:
			log.Info().
				Str("arrival", args.ArrivalName).
				Str("job", jobName).
				Str("pack", t.PackName).
				Msg("dispatched test job")
		case apierrors.IsAlreadyExists(err):
			// Idempotent — controller restarted mid-flight, Job still there.
			log.Info().Str("job", jobName).Msg("job already exists; reusing")
		default:
			return nil, fmt.Errorf("create job %s: %w", jobName, err)
		}
		out[t.PackName] = jobName
	}
	return out, nil
}

// buildJob constructs the Job spec. The container runs an inline bash
// script that does clone+wait+test+upload — keeps the runner image
// generic and the per-pack logic visible in the Arrival CR's owning
// namespace.
func (d *Dispatcher) buildJob(args Args, t Test, jobName string) *batchv1.Job {
	const backoff int32 = 0 // no auto-retry; arrivals-observer manages retries
	deadline := d.cfg.ActiveDeadlineSeconds

	envVars := []corev1.EnvVar{
		{Name: "STAGING_URL", Value: args.StagingURL},
		{Name: "SERVICE", Value: args.Service},
		{Name: "VERSION", Value: args.Version},
		{Name: "TEST_PACK", Value: t.PackName},
		{Name: "TEST_PACK_TYPE", Value: t.PackType},
		{Name: "RESULT_STORE_BUCKET", Value: d.cfg.ResultStoreBucket},
		{Name: "CLUSTER_ID", Value: d.cfg.ClusterID},
		{Name: "ARRIVAL_NAME", Value: args.ArrivalName},
		{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/run/secrets/test-artifacts/key.json"},
		// GitHub auth — most service repos are PRIVATE; without a token
		// the runner script's `git clone` returns 401 and reports it as
		// "branch not found" (silent for caller). Use the same secret
		// Tekton tasks use (`tekton-git/password`); optional so
		// public-repo tests still work without it.
		{
			Name: "GIT_TOKEN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "tekton-git"},
					Key:                  "password",
					Optional:             ptrBool(true),
				},
			},
		},
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: args.Namespace,
			Labels: map[string]string{
				"qa.leartech.com/arrival":   args.ArrivalName,
				"qa.leartech.com/service":   args.Service,
				"qa.leartech.com/version":   sanitizeLabel(args.Version),
				"qa.leartech.com/test-pack": t.PackName,
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
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("250m"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1500m"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
						},
						Command: []string{"bash", "-c", runnerScript},
					}},
				},
			},
		},
	}
}

// runnerScript is the inline bash that runs inside the Job container.
// Reads env, clones, waits, runs the test pack, uploads results.
const runnerScript = `set -euo pipefail
echo "==> arrival=$ARRIVAL_NAME service=$SERVICE version=$VERSION pack=$TEST_PACK type=$TEST_PACK_TYPE"
echo "==> stagingUrl=$STAGING_URL cluster=$CLUSTER_ID bucket=$RESULT_STORE_BUCKET"

WORK=/tmp/work
mkdir -p "$WORK" && cd "$WORK"

# Most service repos are PRIVATE; embed the GitHub token in the clone
# URL when available. Fall back to anonymous for public repos like
# leartech-qa-canary. Without auth, private-repo clones get HTTP 401
# which git reports as "branch not found" — masks the real failure.
if [ -n "${GIT_TOKEN:-}" ]; then
  REPO_URL="https://x-access-token:${GIT_TOKEN}@github.com/mikelear/${SERVICE}.git"
else
  REPO_URL="https://github.com/mikelear/${SERVICE}.git"
  echo "::WARN GIT_TOKEN not set; private-repo clones will fail with 401"
fi

# Per-service git tag schemes vary: most services tag as v<version>; the
# JX-release multi-cluster pattern tags as v<version>-<cluster> (canary).
# Some don't tag at all (chart-only releases). Try the most-specific
# refspec first and fall through to less-specific ones, then main.
clone_with_fallback() {
  for ref in "v${VERSION}-${CLUSTER_ID}" "v${VERSION}" "${VERSION}" "main"; do
    [ -z "$ref" ] && continue
    echo "==> trying clone --branch=$ref"
    if git clone --depth=1 --branch "$ref" "$REPO_URL" repo 2>/dev/null; then
      echo "==> cloned at ref=$ref"
      return 0
    fi
  done
  echo "::FATAL no matching ref found (tried v<ver>-<cluster>, v<ver>, <ver>, main)"
  return 1
}
clone_with_fallback
cd repo

# Wait up to 10min for stagingUrl health.
END=$(( $(date +%s) + 600 ))
HEALTH=0
while [ "$(date +%s)" -lt "$END" ]; do
  if curl -sSf -o /dev/null -m 5 "${STAGING_URL}/health/live"; then
    HEALTH=$((HEALTH+1))
    [ "$HEALTH" -ge 3 ] && break
  else
    HEALTH=0
  fi
  sleep 5
done
[ "$HEALTH" -ge 3 ] || { echo "::FATAL stagingUrl health failed"; exit 1; }
echo "==> staging healthy"

cd "$TEST_PACK"
case "$TEST_PACK_TYPE" in
  end2end)
    [ -x run.sh ] || { echo "::FATAL no executable run.sh in $TEST_PACK"; exit 1; }
    PREVIEW_URL="$STAGING_URL" bash run.sh
    ;;
  end2end-ui)
    npm ci --no-audit --no-fund || npm install --no-audit --no-fund
    PREVIEW_URL="$STAGING_URL" npx playwright test --reporter=list
    ;;
  *)
    echo "::FATAL unsupported TEST_PACK_TYPE=$TEST_PACK_TYPE"; exit 1
    ;;
esac

# Read results.json (test runner contract). Required for end2end; for
# end2end-ui Playwright a follow-on step would synthesize it from the
# JUnit reporter — out of scope for 2.7.2b first cut.
if [ ! -f results.json ]; then
  echo "::ERROR no results.json produced (test runner contract violation)"
  exit 1
fi

# Upload to gs://<bucket>/results/v1/post-deploy/<service>/<version>/<cluster>/<pack>/results.json
DEST="gs://${RESULT_STORE_BUCKET}/results/v1/post-deploy/${SERVICE}/${VERSION}/${CLUSTER_ID}/${TEST_PACK}/results.json"
echo "==> uploading results to ${DEST}"
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" 2>/dev/null || \
  echo "::WARN gcloud auth failed (key-file missing?); upload may fail"
gsutil cp results.json "$DEST" || {
  echo "::ERROR gsutil cp failed; result will be Job-status-only"
  # Don't fail the Job on upload error — the Job's exit code is still
  # the source of truth. Result-store reader is best-effort.
}
echo "==> done"
`

func jobNameFor(arrivalName, pack string) string {
	// K8s names are limited to 63 chars; truncate aggressively.
	name := fmt.Sprintf("ar-%s-%s", arrivalName, pack)
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ToLower(name)
	if len(name) > 63 {
		name = name[:63]
	}
	return name
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
