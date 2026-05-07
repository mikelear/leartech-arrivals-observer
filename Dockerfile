# Golden Dockerfile — distroless/static runtime, non-root, no shell.
# Build stage uses leartech-go-runtime (cosign-signed, weekly rebuild)
# which pre-installs git, make, ca-certificates, tzdata, and swag.
# Version pinned: Renovate bumps it on each new leartech-go-runtime release.

# ---- build stage ----
FROM ghcr.io/mikelear/leartech-go-runtime:0.28.0 AS build

# Dependency layer — cached unless go.mod/go.sum change
COPY go.mod go.sum ./
RUN go mod download

COPY . .


# VERSION is baked into main.version for the /health/live payload.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/server \
    ./cmd/server

# ---- runtime stage ----
# distroless/static:nonroot — no shell, no package manager, uid 65532
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/server /server
# --chown=65532:65532 makes the generated OpenAPI spec readable under any
# kernel/filesystem policy that double-checks ownership beyond the 0644
# world-read bits (some nodes reject root-owned files in distroless/nonroot
# containers with 403 via http.ServeFile).

EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/server"]
