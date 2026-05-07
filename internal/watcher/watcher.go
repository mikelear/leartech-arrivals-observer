// Package watcher observes K8s ReplicaSet "Added" events in the configured
// namespace and creates Arrival CRs (qa.leartech.com/v1alpha1) for each new
// service version it sees.
//
// Phase 2.7.1 scope:
//   - In-cluster client setup via rest.InClusterConfig()
//   - SharedInformer over ReplicaSets in cfg.Namespace
//   - On Add: idempotent Arrival CR upsert keyed by <service>-<version>-<ns>
//   - Filter: ReplicaSets with both app.kubernetes.io/name + version labels
//
// Out of scope (next session 2.7.2):
//   - Test dispatch via K8s Job
//   - Result polling + Arrival.status updates
//   - Redis lock for cross-replica dedup (today: apiserver-side dedup via
//     CR name uniqueness — Create returns AlreadyExists, we Update spec only)
package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/mikelear/leartech-arrivals-observer/internal/config"
)

// Config controls the watcher's behaviour.
type Config struct {
	// Namespace to watch ReplicaSets in (e.g. jx-staging).
	Namespace string

	// ClusterID is recorded as a label on every Arrival CR so consumers can
	// correlate arrivals across clusters.
	ClusterID string

	// KubeConfigPath optionally selects a kubeconfig file for out-of-cluster
	// runs. Empty = use rest.InClusterConfig().
	KubeConfigPath string

	// ResyncPeriod for the informer. 0 disables resync — fine for spike;
	// bump if event drops are observed.
	ResyncPeriod time.Duration

	// Services is the per-service dispatch config map. The watcher embeds
	// stagingUrl + testPacks from this map into each Arrival's spec at
	// ReplicaSet event time. Services not in the map produce Arrivals with
	// empty stagingUrl + testPacks → controller marks them Skipped.
	Services map[string]config.ServiceConfig
}

// Watcher is the running ReplicaSet observer.
type Watcher struct {
	cfg     Config
	clients clients
	factory informers.SharedInformerFactory
}

type clients struct {
	core    kubernetes.Interface
	dynamic dynamic.Interface
}

// arrivalGVR is the dynamic client target for Arrival CRs.
var arrivalGVR = schema.GroupVersionResource{
	Group:    "qa.leartech.com",
	Version:  "v1alpha1",
	Resource: "arrivals",
}

// New constructs a Watcher with K8s clients connected.
func New(_ context.Context, cfg Config) (*Watcher, error) {
	restCfg, err := buildRestConfig(cfg.KubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("build rest config: %w", err)
	}

	core, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}

	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		core,
		cfg.ResyncPeriod,
		informers.WithNamespace(cfg.Namespace),
	)

	return &Watcher{cfg: cfg, clients: clients{core: core, dynamic: dyn}, factory: factory}, nil
}

// Run starts the informer + handles ReplicaSet Added events until ctx is done.
func (w *Watcher) Run(ctx context.Context) {
	rsInformer := w.factory.Apps().V1().ReplicaSets().Informer()
	_, err := rsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			w.handleReplicaSetAdd(ctx, obj)
		},
		// Update event ignored — only new versions trigger Arrivals.
		// Delete event ignored — Arrival history persists for the gate.
	})
	if err != nil {
		log.Error().Err(err).Msg("install ReplicaSet event handler")
		return
	}

	w.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), rsInformer.HasSynced) {
		log.Error().Msg("ReplicaSet informer cache failed to sync")
		return
	}
	log.Info().Str("namespace", w.cfg.Namespace).Msg("watcher running — informer cache synced")
	<-ctx.Done()
}

// handleReplicaSetAdd is invoked for every ReplicaSet observed (initial cache
// fill + subsequent live Add events). Idempotent CR upsert.
func (w *Watcher) handleReplicaSetAdd(ctx context.Context, obj any) {
	rs, ok := obj.(*appsv1.ReplicaSet)
	if !ok {
		return
	}

	// Filter: only Helm-managed Deployments. Skips Jenkins one-off pods,
	// system DaemonSets, etc. Tightens noise floor on jx-staging.
	if rs.Labels["app.kubernetes.io/managed-by"] != "Helm" {
		return
	}

	service := rs.Labels["app.kubernetes.io/name"]
	version := rs.Labels["app.kubernetes.io/version"]
	if service == "" || version == "" {
		return
	}

	// Skip ReplicaSets with desired-replicas == 0 (scaled-down old versions
	// after a rolling update). The current ReplicaSet's spec.replicas matches
	// the parent Deployment's desired count.
	if rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 {
		return
	}

	arrivalName := arrivalNameFor(service, version, w.cfg.Namespace)
	if err := w.upsertArrival(ctx, rs, arrivalName, service, version); err != nil {
		log.Error().Err(err).
			Str("rs", rs.Name).
			Str("service", service).
			Str("version", version).
			Msg("upsert arrival failed")
		return
	}
	log.Info().
		Str("arrival", arrivalName).
		Str("rs", rs.Name).
		Str("service", service).
		Str("version", version).
		Msg("arrival recorded")
}

// upsertArrival creates an Arrival CR if absent, idempotent on existing.
// Spec.deployedAt updates to now() on every observation so a rolling restart
// of the same version refreshes the timestamp without changing identity.
//
// stagingUrl + testPacks come from the services map (chart values). Services
// not in the map land with empty stagingUrl + testPacks; the controller
// marks those Skipped. Spec is rewritten on every observation so chart-side
// services map edits propagate without a rolling restart.
func (w *Watcher) upsertArrival(ctx context.Context, rs *appsv1.ReplicaSet, arrivalName, service, version string) error {
	svcCfg, hasCfg := w.cfg.Services[service]

	cr := &unstructured.Unstructured{}
	cr.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   arrivalGVR.Group,
		Version: arrivalGVR.Version,
		Kind:    "Arrival",
	})
	cr.SetName(arrivalName)
	cr.SetNamespace(w.cfg.Namespace)
	cr.SetLabels(map[string]string{
		"qa.leartech.com/service": service,
		"qa.leartech.com/version": version,
		"qa.leartech.com/cluster": w.cfg.ClusterID,
	})

	deployedAt := rs.CreationTimestamp.Format(time.RFC3339)
	if rs.CreationTimestamp.IsZero() {
		deployedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if err := unstructured.SetNestedField(cr.Object, service, "spec", "service"); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(cr.Object, version, "spec", "version"); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(cr.Object, rs.Name, "spec", "replicaSet"); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(cr.Object, deployedAt, "spec", "deployedAt"); err != nil {
		return err
	}
	if hasCfg && svcCfg.StagingURL != "" {
		if err := unstructured.SetNestedField(cr.Object, svcCfg.StagingURL, "spec", "stagingUrl"); err != nil {
			return err
		}
	}
	if hasCfg && len(svcCfg.TestPacks) > 0 {
		packs := make([]any, 0, len(svcCfg.TestPacks))
		for _, p := range svcCfg.TestPacks {
			packs = append(packs, map[string]any{"name": p.Name, "type": p.Type})
		}
		if err := unstructured.SetNestedSlice(cr.Object, packs, "spec", "testPacks"); err != nil {
			return err
		}
	}

	_, err := w.clients.dynamic.Resource(arrivalGVR).
		Namespace(w.cfg.Namespace).
		Create(ctx, cr, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	// Already exists → merge-patch the dispatch-relevant spec fields.
	// Status is on its own subresource so this never clobbers controller writes.
	patchObj := map[string]any{
		"spec": map[string]any{
			"deployedAt": deployedAt,
			"replicaSet": rs.Name,
		},
	}
	if hasCfg && svcCfg.StagingURL != "" {
		patchObj["spec"].(map[string]any)["stagingUrl"] = svcCfg.StagingURL
	}
	if hasCfg && len(svcCfg.TestPacks) > 0 {
		packs := make([]map[string]any, 0, len(svcCfg.TestPacks))
		for _, p := range svcCfg.TestPacks {
			packs = append(packs, map[string]any{"name": p.Name, "type": p.Type})
		}
		patchObj["spec"].(map[string]any)["testPacks"] = packs
	}
	patch, err := json.Marshal(patchObj)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	_, err = w.clients.dynamic.Resource(arrivalGVR).
		Namespace(w.cfg.Namespace).
		Patch(ctx, arrivalName, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// arrivalNameFor produces a deterministic Arrival CR name. Underscores +
// dots in version strings get replaced with `-` per RFC 1123 label rules
// (CRs follow DNS-1123 subdomain).
func arrivalNameFor(service, version, namespace string) string {
	return fmt.Sprintf("%s-%s-%s", service, sanitizeForDNS(version), namespace)
}

func sanitizeForDNS(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c-'A'+'a')
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

// buildRestConfig prefers in-cluster, falls back to kubeconfig file for
// out-of-cluster debug runs.
func buildRestConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

