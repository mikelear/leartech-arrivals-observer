# leartech-arrivals-observer — Claude Context

Golden Go HTTP service template. Clone-and-rename starter for new
Leartech Go services, fully wired into a Jenkins X / Lighthouse
multi-cluster CI/CD chain.

## Bootstrap a new service with Claude

Open Claude Code (or any capable coding agent) in a fresh clone of this
repo and feed it this prompt:

> Rename this template from `leartech-arrivals-observer` to
> `leartech-<my-service>`. Touch `go.mod`, `Makefile`, `README.md`,
> `charts/**/*.yaml`, `preview/helmfile.yaml.gotmpl`,
> `.lighthouse/jenkins-x/*.yaml`, `cmd/server/main.go`'s swagger
> annotations, and `.golangci.yml`'s `goimports.local-prefixes`.
> Then seed a `v0.0.1` git tag and update `renovate.json`'s
> `matchPackageNames` if the go-runtime base image needs tracking.
> Leave the `.lighthouse/jenkins-x/*` files' `uses:` references
> pointing at `mikelear/leartech-pipeline-catalog` unchanged — those
> are shared.

Claude should be able to complete the rename in one pass by grepping
for `leartech-arrivals-observer` and substituting. For Go, also update
the module path in every `.go` file's `github.com/mikelear/leartech-arrivals-observer/...` imports.

After rename, run:

```bash
go mod tidy
git tag -fa v0.0.1 -m "bootstrap: seed for jx-release-version"
git push origin main v0.0.1
```

Register the repo on each Lighthouse cluster's gitops source-config
(see **Cluster prerequisites** below), push a trivial PR, and the
full 11-check presubmit chain fires.

## Repo layout

| Path | Purpose |
|---|---|
| `cmd/server/main.go` | Entry point with swaggo annotations (`@title`, `@version`, `@securityDefinitions`) |
| `internal/{config,handlers,middleware,db}/` | Standard service internals — gin router, health handlers, auth middleware, pgx pool |
| `docs/` | swag-generated OpenAPI spec (regenerated on `make swag`) — served at `/openapi.json` |
| `migrations/` | goose SQL migrations run as Helm post-install Job |
| `Dockerfile` | Multi-stage: `ghcr.io/mikelear/leartech-go-runtime` build → `gcr.io/distroless/static-debian12:nonroot` runtime |
| `Makefile` | Slim — `swag`, `lint`, `build`, `test`, `test-coverage` (6 targets total) |
| `.golangci.yml` | Thin override — base config is fetched from catalog at CI time |
| `charts/leartech-arrivals-observer/` | Helm chart with `leartech-helm-library` dependency |
| `preview/` | Per-PR preview helmfile (env-templated URLs) |
| `end2end/run.sh` + `end2end/01-smoke.sh` | Smoke tests run by shared end2end Tekton task |
| `.lighthouse/jenkins-x/` | 11-trigger presubmit + release suite (thin `uses:` wrappers) |
| `renovate.json` | Dependency bump automation (patch auto-merge, leartech-go-runtime auto-merge) |

## Pipeline triggers

All 11 checks below are thin wrappers over
[mikelear/leartech-pipeline-catalog](https://github.com/mikelear/leartech-pipeline-catalog)
tasks. Zero pipeline logic lives in this repo.

| Check | Catalog source |
|---|---|
| `pr` | `tasks/go/pullrequest.yaml` — go build + kaniko + jx-preview |
| `lint` | `tasks/go-lint/pullrequest.yaml` — golangci-lint with centralised base config (yq-merged at CI time) |
| `test` | `tasks/go-test/pullrequest.yaml` — `go test -race -coverprofile` + coverage sticky comment |
| `govulncheck` | `tasks/govulncheck/pullrequest.yaml` — Go dep vulnerability scan |
| `security-scan` | `tasks/security-scan/pullrequest.yaml` — gitleaks + semgrep + grype |
| `image-scan` | `tasks/security-scan/image-scan.yaml` |
| `dynamic-scan` | `tasks/security-scan/dynamic/pullrequest.yaml` — nmap + egress-isolation |
| `ai-review` | `tasks/ai-review/pullrequest.yaml` — multi-LLM code review |
| `ai-feedback` | `tasks/ai-review/feedback.yaml` — comment-triggered on `/ai-feedback` |
| `end2end` | `tasks/end2end/pullrequest.yaml` — runs this repo's `end2end/run.sh` |
| `release` (postsubmit) | `tasks/go/release.yaml` — cluster-suffixed tag, cosign, helm-release, jx-promote |

## Runtime base

Build stage: `ghcr.io/mikelear/leartech-go-runtime:X` — pre-installs
`git`, `make`, `swag`, `ca-certificates` on `golang:1.25-alpine`.
Runtime stage: `gcr.io/distroless/static-debian12:nonroot` — no shell,
no package manager, uid 65532.

**Distroless nonroot CWD is `/home/nonroot`**, not `/`. Use absolute
paths in `c.File(...)` and similar — a relative path like
`"docs/swagger.json"` resolves against `/home/nonroot/...` and misses.
The Dockerfile does `COPY --chown=65532:65532 /docs /docs` so nonroot
can read the generated OpenAPI spec.

## Database

If your service needs Postgres, **don't bundle a Bitnami chart**. Use the shared CNPG cluster.

**Per-PR-preview** (helmfile in your service's `preview/`): keep the existing Bitnami sub-release pattern — previews are ephemeral, isolation matters more than consolidation.

**Staging + production** (helmfile in `jx-build-cluster-{gsm,akv}/helmfiles/jx-staging/`):

1. In your chart's `templates/postgres-database.yaml`, gate a CNPG `Database` CR on `.Values.postgresql.useSharedCluster`:

   ```yaml
   {{- if .Values.postgresql.useSharedCluster }}
   apiVersion: postgresql.cnpg.io/v1
   kind: Database
   metadata:
     name: {{ .Release.Name }}-<service>
   spec:
     cluster:
       name: {{ .Values.postgresql.sharedCluster.name }}  # leartech-staging
     name: <service>
     owner: <service>
   {{- end }}
   ```

2. In your `deployment.yaml`, env-inject the DSN from CNPG's generated secret (`<cluster>-<owner>-app`, key `uri`):

   ```yaml
   - name: DATABASE_URL
     valueFrom:
       secretKeyRef:
         name: {{ .Values.postgresql.sharedCluster.name }}-<service>-app
         key: uri
   ```

3. In the staging helmfile overlay (`configs/<your-service>.yaml` in the gitops repo), set `postgresql.useSharedCluster: true`.

**Why**: prevents the per-namespace Bitnami sprawl pattern. One operator, one backup target, one Postgres version to upgrade. Schema migrations stay in your service repo (run via Job or `bootstrap.initdb.postImportApplicationSQL`).

Reference implementation: `mikelear/leartech-auth-service@feat/postgres-cnpg-cutover`.

## Tracing

OTLP traces emit by default to the cluster's Tempo (`tempo.jx-observability:4318`). Service name + version + cluster tag are set as resource attributes. The `internal/tracing` package handles initialisation; the `otelgin` middleware on the gin router gives every HTTP request a span.

To opt out for a specific deployment: `tracing.enabled: false` in the chart values — sets `OTEL_TRACES_DISABLED=1` in the container env (no silent retry-spam).

## Cluster prerequisites

Before pipelines fire on a new clone:

1. **Register** the repo in each Lighthouse cluster's gitops source-config:
   `.jx/gitops/source-config.yaml` → add `- name: leartech-<my-service>` under the `mikelear/` group.
2. **`jx-cluster-config` ConfigMap** in each cluster's `jx` namespace with `CLUSTER_ID: gcp` or `CLUSTER_ID: az` so release pipelines know which cluster-suffixed tag to push.
3. **Container registry access**: Kaniko pushes to `$PUSH_CONTAINER_REGISTRY/$DOCKER_REGISTRY_ORG/<app>:$VERSION`. tekton-bot needs push creds.
4. **`ghcr.io` visibility**: on first publish, the container package is private by default — flip to Public so Kaniko on other clusters can pull it.
5. **`cosign-keys` secret** in `jx` namespace holding `cosign.key` for image signing.

## Release mechanics

- `jx-release-version --previous-version from-tag > VERSION` with custom cluster-suffix logic in `tasks/release/next-version.yaml` — prevents GCP/Azure races on parallel releases.
- Bootstrap: seed `v0.0.1` before the first automated release.
- Git tag is cluster-suffixed: `v0.1.0-gcp` / `v0.1.0-az`.
- Image tag is plain `$VERSION` (per-cluster registries don't race).
- Cosign signs the cluster-registry image (+ ghcr.io tag).
- `jx promote` opens the auto-PR on each cluster's gitops repo.
- Codegen task (sibling to `from-build-pack`, `runAfter`) generates
  client SDKs in angular/typescript/go/python. AZ publishes,
  GCP skips via the catalog's single-publisher gate. This template's
  release exercises the full chain on every push, acting as the catalog's
  canary — if codegen breaks, the template's release fails before any
  consumer notices.

## Codegen / OpenAPI spec

- `cmd/server/main.go` carries the `@title` / `@version` / `@description`
  / `@license` / `@securityDefinitions` annotations (use single-space
  separator: `// @title ...`, NOT tab — swag silently drops tab-separated
  annotations and the spec ships with empty `info{}`).
- `internal/handlers/*.go` carry per-endpoint annotations (`@Summary`,
  `@Tags`, `@Router`, `@Success`, etc.).
- Run `make swag` after changing annotations and commit the regenerated
  `docs/{docs.go,swagger.json,swagger.yaml}` — the codegen task can't
  see the build pod's regenerated copy (separate git-clone).
- Published packages (visible in your fork after first release):
  `@<org>/<repo>-{angular,typescript}` on npm GitHub Packages,
  `<org>/leartech-{go,py}-packages/<repo>` mono-repo subdirs.

## Iteration mechanics

Commit directly to `main` on this repo — it's a template, there's no
prod to break. Exercise pipeline changes via PRs opened **from a
consumer repo** that clones this template.

## Dependencies

- [`mikelear/leartech-pipeline-catalog`](https://github.com/mikelear/leartech-pipeline-catalog) — Tekton task catalog
- [`mikelear/leartech-dockerfiles`](https://github.com/mikelear/leartech-dockerfiles) — `leartech-go-runtime` base image source
- `ghcr.io/mikelear/leartech-go-runtime` — build-stage base (cosign-signed, weekly rebuild)
- `leartech-go-common` — shared auth middleware, slog setup, pgx pool builder
- `leartech-helm-library` — shared chart helpers (published to each cluster's OCI chart registry)
- Jenkins X / Lighthouse + Tekton installed on each target cluster

## Running the chain elsewhere

If you're forking this to a new org, the bill-of-materials is:

1. Fork or rebuild `leartech-dockerfiles` (for `leartech-go-runtime`)
2. Fork `leartech-pipeline-catalog` and adjust `uses:` references in this template's `.lighthouse/jenkins-x/*.yaml` to point at your fork
3. Fork `leartech-helm-library` and `leartech-go-common` (adjust references)
4. Update every `mikelear/...` reference in this template to `<your-org>/...`
5. Ensure Jenkins X is installed on your cluster(s) with Lighthouse + Tekton
