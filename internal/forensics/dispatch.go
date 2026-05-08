// Package forensics dispatches a one-shot K8s Job that runs the
// leartech-forensics-runner image. The runner queries Tempo for two
// deployment windows (deployed_at ± windowMinutes), computes an
// endpoint-level diff, uploads diff.json to GCS, and patches
// Arrival.status.forensics with a summary.
//
// The controller calls this fire-and-forget when an Arrival reaches
// phase=Failed (or Timeout). Forensics is best-effort — if the Job
// can't be created or the runner crashes, the Arrival's terminal
// phase is preserved; only the .status.forensics field stays empty.
package forensics

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

// Config controls forensics Job creation.
type Config struct {
	Enabled       bool
	RunnerImage   string
	TempoBaseURL  string
	WindowMinutes int
	// GCSKeySecret + ResultStoreBucket reused from the dispatch path —
	// same shape, same secret, same upload semantics.
	GCSKeySecret      string
	ResultStoreBucket string
	ClusterID         string
}

// Args bundles the per-Arrival fields needed to render a forensics Job.
type Args struct {
	ArrivalName      string
	ArrivalNamespace string
	Service          string
	Version          string
	PreviousVersion  string // empty = first deploy of this service
	DeployedAt       string // RFC3339, copied from Arrival.spec.deployedAt
}

// Dispatcher creates the forensics Job.
type Dispatcher struct {
	cfg     Config
	clients kubernetes.Interface
}

// New constructs a Dispatcher.
func New(cfg Config, clients kubernetes.Interface) *Dispatcher {
	return &Dispatcher{cfg: cfg, clients: clients}
}

// Dispatch creates a forensics Job for the given Arrival. Returns the
// Job name (for recording in Arrival.status.forensics.jobName) or "" if
// the dispatcher is disabled / the runner image is unset.
func (d *Dispatcher) Dispatch(ctx context.Context, args Args) (string, error) {
	if !d.cfg.Enabled || d.cfg.RunnerImage == "" {
		return "", nil
	}
	jobName := jobNameFor(args.ArrivalName)
	job := d.buildJob(args, jobName)
	_, err := d.clients.BatchV1().Jobs(args.ArrivalNamespace).Create(ctx, job, metav1.CreateOptions{})
	switch {
	case err == nil:
		log.Info().
			Str("arrival", args.ArrivalName).
			Str("job", jobName).
			Msg("dispatched forensics job")
	case apierrors.IsAlreadyExists(err):
		log.Info().Str("job", jobName).Msg("forensics job already exists; reusing")
	default:
		return "", fmt.Errorf("create forensics job %s: %w", jobName, err)
	}
	return jobName, nil
}

func (d *Dispatcher) buildJob(args Args, jobName string) *batchv1.Job {
	const backoff int32 = 0
	deadline := int64(15 * 60) // 15min wall-clock — Tempo queries should be fast

	envVars := []corev1.EnvVar{
		{Name: "SERVICE", Value: args.Service},
		{Name: "VERSION", Value: args.Version},
		{Name: "PREVIOUS_VERSION", Value: args.PreviousVersion},
		{Name: "DEPLOYED_AT", Value: args.DeployedAt},
		{Name: "CLUSTER_ID", Value: d.cfg.ClusterID},
		{Name: "ARRIVAL_NAME", Value: args.ArrivalName},
		{Name: "ARRIVAL_NAMESPACE", Value: args.ArrivalNamespace},
		{Name: "TEMPO_BASE_URL", Value: d.cfg.TempoBaseURL},
		{Name: "RESULT_STORE_BUCKET", Value: d.cfg.ResultStoreBucket},
		{Name: "WINDOW_MINUTES", Value: fmt.Sprintf("%d", d.cfg.WindowMinutes)},
		{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/run/secrets/test-artifacts/key.json"},
		// gcloud writes its config to $HOME/.config/gcloud by default.
		// Runner pod is non-root (UID 1001) without a writable $HOME, so
		// gcloud auth fails with "Could not create directory [/.config/
		// gcloud/configurations]". Override CLOUDSDK_CONFIG to a writable
		// path; this also keeps gcloud state out of the Job's tmp lifetime.
		{Name: "CLOUDSDK_CONFIG", Value: "/tmp/gcloud"},
		{Name: "HOME", Value: "/tmp"},
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: args.ArrivalNamespace,
			Labels: map[string]string{
				"qa.leartech.com/arrival":      args.ArrivalName,
				"qa.leartech.com/service":      args.Service,
				"qa.leartech.com/job-kind":     "forensics",
				"app.kubernetes.io/managed-by": "leartech-arrivals-observer",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptrInt32(backoff),
			ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"qa.leartech.com/arrival":  args.ArrivalName,
						"qa.leartech.com/job-kind": "forensics",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: "leartech-arrivals-observer",
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
						Name:  "forensics",
						Image: d.cfg.RunnerImage,
						Env:   envVars,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "gcs-key",
							MountPath: "/var/run/secrets/test-artifacts",
							ReadOnly:  true,
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					}},
				},
			},
		},
	}
}

func jobNameFor(arrivalName string) string {
	name := fmt.Sprintf("forensics-%s", arrivalName)
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ToLower(name)
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

func ptrInt32(v int32) *int32 { return &v }
func ptrBool(v bool) *bool    { return &v }
