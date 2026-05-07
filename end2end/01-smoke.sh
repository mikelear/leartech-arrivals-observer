#!/usr/bin/env bash
# 01-smoke.sh — golden-standard smoke test for the preview deploy.
#
# Exercises every public unauthenticated endpoint with GET and HEAD
# (since we explicitly register HEAD alongside GET — see cmd/server/main.go).
# Fails (exit 1) on the first unexpected status.
#
# Invoked by end2end/run.sh which captures exit status and last-3-lines
# of output for the PR sticky comment.

set -eo pipefail
: "${PREVIEW_URL:?PREVIEW_URL must be set by the end2end task}"

check() {
  local method="$1" path="$2" want="$3"
  local code
  # Capture the HTTP code via stdout; don't use `|| echo` because curl
  # may exit non-zero AFTER writing a valid code (partial transfer on
  # HEAD, etc.) which would concatenate "200" + "000" = "200000".
  code=$(curl -sS -o /dev/null -w '%{http_code}' -X "$method" -m 10 "${PREVIEW_URL}${path}" 2>/dev/null || true)
  [ -z "$code" ] && code="000"
  if [ "$code" = "$want" ]; then
    printf '[smoke] %-4s %-20s %s\n' "$method" "$path" "HTTP $code ✓"
  else
    printf '[smoke] %-4s %-20s %s\n' "$method" "$path" "HTTP $code (want $want) ✗"
    return 1
  fi
}

check GET  /health/live   200
check HEAD /health/live   200
check GET  /health/ready  200
check HEAD /health/ready  200
check GET  /openapi.json  200
check HEAD /openapi.json  200
check GET  /docs          200
check HEAD /docs          200

echo "[smoke] all 8 checks passed"
