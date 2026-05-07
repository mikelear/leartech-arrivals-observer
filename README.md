# leartech-arrivals-observer

Golden Go service template. Clone, rename, replace the example handler, ship.

Every rule in `~/leartech/hub/shared-rules/golden-service-standard.md` is satisfied by this repo out of the box. The README of each cloned service should replace this description with the actual service's purpose.

## What's in the box

| Area | What | Where |
|---|---|---|
| HTTP | gin + zerolog + graceful shutdown | `cmd/server/main.go` |
| Health | `/health/live`, `/health/ready` (pings Postgres), unauthenticated | `internal/handlers/health.go` |
| Metrics | `/metrics` (Prometheus), scoped via NetworkPolicy | `internal/handlers/metrics.go` |
| OpenAPI | swaggo annotations → `docs/swagger.json` → swgui at `/docs`, raw spec at `/openapi.json` | `cmd/server/main.go` + handlers |
| Auth | `leartech-go-common/pkg/auth` bearer middleware on all `/api/v1/*` routes | `internal/middleware/auth.go` |
| Database | pgx pool (Postgres-only per golden std); goose migrations as Helm post-install job | `internal/db/pgx.go`, `charts/**/migrations-job.yaml`, `migrations/*.sql` |
| Config | envconfig (12-factor) | `internal/config/config.go` |
| Chart | uses `leartech-helm-library` for labels/securityContext/probes — consistent spine | `charts/leartech-arrivals-observer/` |
| Preview | Per-PR helmfile with env-templated URLs (no hardcoded cluster registries) | `preview/` |
| end2end | Smoke script wired into the shared end2end Tekton task | `end2end/` |
| Pipelines | PR: build+lint+test+coverage+vulnscan+security-scan+image-scan+dynamic-scan+ai-review+preview. Release: cosign-signed + cluster-suffixed tag + `jx promote` | `.lighthouse/jenkins-x/` |

## Cloning into a new service

```bash
# 1. Create a new repo from the template
gh repo create mikelear/<new-service-name> --private

# 2. Clone this template locally, rename, push
git clone --depth=1 https://github.com/mikelear/leartech-arrivals-observer.git <new-service-name>
cd <new-service-name>
rm -rf .git && git init && git branch -m main

# 3. Rename the module path (this is the one-shot manual step)
OLD=github.com/mikelear/leartech-arrivals-observer
NEW=github.com/mikelear/<new-service-name>
find . -type f \( -name '*.go' -o -name 'go.mod' -o -name 'Makefile' -o -name '*.yaml' -o -name '*.md' \) \
  -exec sed -i.bak "s|${OLD}|${NEW}|g" {} \; -exec rm -f {}.bak \;

# 4. Seed the first git tag (bootstrap for jx-release-version)
git add -A && git commit -m "feat: clone from leartech-arrivals-observer"
git tag v0.0.1
git remote add origin https://github.com/mikelear/<new-service-name>.git
git push -u origin main
git push origin v0.0.1

# 5. Register with jx — see ~/leartech/hub/CLAUDE.md § "Registering a new
#    repo with JX" (add to source-config.yaml in gitops repos, don't jx import)
```

## Local development

Six `make` targets — nothing else hidden. swag + golangci-lint are
auto-installed on first run of the corresponding target.

```bash
# Regenerate OpenAPI spec after editing handler annotations
make swag

# Lint (fetches leartech-pipeline-catalog base + merges with local overrides)
make lint

# Build
make build
./bin/server   # run — needs DATABASE_URL + AUTH_* env for full mode

# Tests
make test
make test-coverage

# Local DB + migrations
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/leartech_go_service_template?sslmode=disable'
go install github.com/pressly/goose/v3/cmd/goose@latest
goose -dir migrations postgres "${DATABASE_URL}" up
```

## Release mechanics

Per `~/leartech/hub/shared-rules/conventions.md` § Golden release pattern:

- `jx-release-version --previous-version from-tag` (NOT `--tag` — would race on both clusters)
- Git tag is cluster-suffixed: `v0.1.0-gcp` / `v0.1.0-az`
- Image tag is plain `$VERSION` (per-cluster registries don't race)
- `jx promote` opens an auto-PR against each cluster's gitops repo
- Cosign signs the image against the verifier-trusted key

`CLUSTER_ID` comes from the cluster-wide `jx-cluster-config` ConfigMap. Seed `v0.0.1` unsuffixed before the first automated release (it's in the clone instructions above).

## References

- `~/leartech/hub/shared-rules/golden-service-standard.md` — the architecture decisions this template satisfies
- `~/leartech/hub/shared-rules/conventions.md` — CI/pipeline rules
- `~/leartech/leartech-go-common` — auth middleware, logger, httptools
- `~/leartech/leartech-helm-library` — chart spine (labels, securityContext, probes)
