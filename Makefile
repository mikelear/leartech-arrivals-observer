.PHONY: all pre-push lint lint-config lint-check fmt vet tidy tidy-check \
        build test test-verbose test-coverage envtest-assets vuln secrets clean help diagnose

VERSION             ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GOLANGCI_BASE_URL   := https://raw.githubusercontent.com/mikelear/leartech-pipeline-catalog/main/go/.golangci.base.yml
GITLEAKS_VERSION       := v8.18.4
GOVULNCHECK_VERSION    := v1.1.4   # pinned to match CI (catalog tasks/govulncheck/pullrequest.yaml). v1.2.0+ requires go 1.25; bump alongside fleet-wide go.mod upgrade.
GOIMPORTS_VERSION      := latest
GOLANGCI_LINT_VERSION  := v2.11.4   # pinned to match CI (catalog tasks/go-lint/pullrequest.yaml uses image golangci/golangci-lint:v2.11.4)
GO                  := go
MODULE              := github.com/mikelear/leartech-arrivals-observer

# envtest — real kube-apiserver + etcd fixture used by the higher-fidelity
# controller reconcile tests (state_machine_conformance_test.go et al.).
# Tests SKIP cleanly when KUBEBUILDER_ASSETS is unset OR the binaries
# aren't present, so `make test` on a fresh laptop still runs — but for
# maximum coverage + parity with CI, `make envtest-assets` installs the
# control-plane bundle via setup-envtest. See internal/controller/
# envtest_harness_test.go for the skip-guard semantics.
ENVTEST_K8S_VERSION    := 1.30.x
SETUP_ENVTEST_VERSION  := latest

# arrivals-observer is a K8s controller, not an HTTP API — no swagger/swag target.
# Only /health/live, /health/ready, /metrics are exposed (no annotated routes).

all: fmt build test lint   ## Format, build, test, lint

pre-push: fmt vet tidy-check build test lint vuln secrets   ## Tier-1 gates that MUST pass before pushing

lint-config:   ## Fetch + merge base golangci config from pipeline-catalog
	@command -v yq >/dev/null || { echo "yq required (brew install yq)"; exit 1; }
	@curl -fsSL -o .golangci.base.yml $(GOLANGCI_BASE_URL)
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' .golangci.base.yml .golangci.yml > .golangci.merged.yml

lint: lint-config   ## Run golangci-lint
	@# Self-bootstrap: install golangci-lint v2 to GOPATH/bin if not present
	@# there with v2.x. Use the explicit path to avoid PATH-ordering issues
	@# (e.g. brew-installed v1.x shadowing the GOPATH/bin v2.x).
	@GOLANGCI_BIN=$$($(GO) env GOPATH)/bin/golangci-lint; \
	if [ ! -x "$$GOLANGCI_BIN" ] || ! $$GOLANGCI_BIN --version 2>/dev/null | grep -qE 'version 2\.'; then \
		echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION) to $$GOLANGCI_BIN..."; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$($(GO) env GOPATH)/bin $(GOLANGCI_LINT_VERSION); \
	fi; \
	$$GOLANGCI_BIN run --config .golangci.merged.yml ./...

lint-check: lint   ## Alias of lint (idempotent — no auto-fix here)

fmt:   ## Format Go code (gofmt + goimports)
	$(GO) fmt ./...
	@command -v goimports >/dev/null || $(GO) install golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)
	goimports -w -local $(MODULE) .

vet:   ## Run go vet
	$(GO) vet ./...

tidy:   ## Tidy and verify modules
	$(GO) mod tidy
	$(GO) mod verify

tidy-check:   ## Verify go.mod/go.sum are tidy (CI-mode — no writes)
	@cp go.mod go.mod.bak; cp go.sum go.sum.bak
	@$(GO) mod tidy
	@if ! diff -q go.mod go.mod.bak >/dev/null 2>&1 || ! diff -q go.sum go.sum.bak >/dev/null 2>&1; then \
		mv go.mod.bak go.mod; mv go.sum.bak go.sum; \
		echo "FAIL: go.mod/go.sum are not tidy. Run 'make tidy' and commit."; exit 1; \
	fi
	@rm -f go.mod.bak go.sum.bak
	@echo "PASS: go.mod/go.sum are tidy"

build:   ## Build the binary
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/server ./cmd/server

envtest-assets:   ## Install setup-envtest + fetch etcd/kube-apiserver bundle
	@# One-shot bootstrap for the envtest-backed higher-fidelity reconcile
	@# tests. Skips a re-install when setup-envtest is already present.
	@# Exports KUBEBUILDER_ASSETS for the caller's shell so the envtest
	@# harness picks up the binaries on the next `make test`.
	@SETUP_ENVTEST=$$($(GO) env GOPATH)/bin/setup-envtest; \
	if [ ! -x "$$SETUP_ENVTEST" ]; then \
		echo "Installing sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION)..."; \
		$(GO) install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION); \
	fi; \
	KUBEBUILDER_ASSETS=$$("$$SETUP_ENVTEST" use $(ENVTEST_K8S_VERSION) -p path); \
	echo "envtest binaries ready at: $$KUBEBUILDER_ASSETS"; \
	echo "export KUBEBUILDER_ASSETS=\"$$KUBEBUILDER_ASSETS\""

# Auto-wiring for test / test-coverage: if setup-envtest exists AND
# KUBEBUILDER_ASSETS is not already set by the caller, populate it so
# the envtest-backed cases run rather than skip. Zero-cost when
# setup-envtest isn't installed (empty variable, tests skip).
KUBEBUILDER_ASSETS_AUTO := $(shell \
	SETUP_ENVTEST=$$($(GO) env GOPATH)/bin/setup-envtest; \
	if [ -x "$$SETUP_ENVTEST" ] && [ -z "$$KUBEBUILDER_ASSETS" ]; then \
		"$$SETUP_ENVTEST" use $(ENVTEST_K8S_VERSION) -p path 2>/dev/null || true; \
	fi \
)

test:   ## Run unit tests (envtest cases run if setup-envtest is installed; otherwise skip cleanly)
	@KUBEBUILDER_ASSETS="$${KUBEBUILDER_ASSETS:-$(KUBEBUILDER_ASSETS_AUTO)}" \
		$(GO) test ./... -v -count=1 -race

test-verbose:   ## Run tests with verbose output (alias of test for now)
	$(GO) test --tags=unit -v -failfast -count=1 ./...

test-coverage:   ## Run tests with coverage threshold check
	@# One-way RATCHET floor: 70.0. Raised (from the historical 60.0) once
	@# the envtest + state-machine-conformance + chart-render tests landed
	@# (2026-08 test-harness-parity initiative). Never lower this — the
	@# safety property is that a code change accompanied by dropping
	@# coverage should FAIL the gate rather than silently regress.
	@# The pipeline task (.lighthouse/jenkins-x/test.yaml) mirrors this
	@# value via COVERAGE_THRESHOLD; keep them in sync when raising.
	@KUBEBUILDER_ASSETS="$${KUBEBUILDER_ASSETS:-$(KUBEBUILDER_ASSETS_AUTO)}" \
		$(GO) test ./... -v -count=1 -race -coverprofile=cover.out
	@TOTAL=$$($(GO) tool cover -func=cover.out | awk '/^total:/ {print $$3}' | sed 's/%//'); \
	THRESHOLD=70.0; \
	echo "coverage: $$TOTAL% (threshold: $$THRESHOLD%)"; \
	awk -v t="$$TOTAL" -v th="$$THRESHOLD" 'BEGIN { exit !(t < th) }' && echo "FAIL: below threshold" && exit 1 || true; \
	echo "PASS"

vuln:   ## Scan for known Go vulnerabilities (govulncheck)
	@command -v govulncheck >/dev/null || $(GO) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	govulncheck ./...

secrets:   ## Scan for committed secrets (gitleaks)
	@command -v gitleaks >/dev/null || { \
		echo "Installing gitleaks $(GITLEAKS_VERSION)..."; \
		GOOS=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
		GOARCH=$$(uname -m | sed 's/x86_64/x64/;s/aarch64/arm64/'); \
		curl -sSfL "https://github.com/gitleaks/gitleaks/releases/download/$(GITLEAKS_VERSION)/gitleaks_$${GITLEAKS_VERSION#v}_$${GOOS}_$${GOARCH}.tar.gz" | \
			tar -xz -C /tmp gitleaks && mv /tmp/gitleaks $$($(GO) env GOPATH)/bin/gitleaks; \
	}
	gitleaks detect --source . --no-banner --redact

clean:   ## Clean build artifacts
	rm -rf bin/ cover.out .golangci.base.yml .golangci.merged.yml *.bak

diagnose:   ## Show which Tekton presubmit checks are covered locally vs need cluster
	@echo "Tekton presubmit checks for this repo (PR-time):"
	@echo "─────────────────────────────────────────────────"
	@for f in .lighthouse/jenkins-x/*.yaml .lighthouse/jenkins-x/*/*.yaml; do \
		[ -f "$$f" ] || continue; \
		case "$$f" in *triggers.yaml|*release.yaml|*pullrequest.yaml) continue;; esac; \
		name=$$(echo "$$f" | sed 's|.lighthouse/jenkins-x/||; s|.yaml$$||'); \
		case $$name in \
			lint) covered="✓ \033[32mmake lint\033[0m";; \
			test|test-coverage) covered="✓ \033[32mmake test\033[0m";; \
			govulncheck) covered="✓ \033[32mmake vuln\033[0m";; \
			end2end) covered="✗ \033[33mTier 3 — needs preview cluster\033[0m";; \
			end2end-ui) covered="✗ \033[33mTier 3 — Playwright needs preview cluster\033[0m";; \
			ai-review*) covered="✗ \033[33mTier 3 — LLM-against-deployed-preview\033[0m";; \
			security-scan/dynamic*) covered="✗ \033[33mTier 3 — DAST needs running app\033[0m";; \
			security-scan/image*) covered="✗ \033[33mTier 3 — needs built image\033[0m";; \
			security-scan*) covered="◐ \033[33mpartial — gitleaks via 'make secrets'; SAST needs cluster\033[0m";; \
			*) covered="? \033[31munknown — extend Makefile diagnose mapping\033[0m";; \
		esac; \
		printf "  %-30s %b\n" "$$name" "$$covered"; \
	done
	@echo ""
	@echo "Legend: ✓ covered by 'make pre-push'   ◐ partial   ✗ requires Tekton cluster   ? mapping needs update"
	@echo ""
	@echo "Run 'make pre-push' before pushing to catch all locally-covered failures."

help:   ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
