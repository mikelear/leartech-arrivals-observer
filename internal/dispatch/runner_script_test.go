package dispatch

import (
	"strings"
	"testing"
)

// TestRunnerScript_EmitsStructuredLogAndUploadsLogsJsonl asserts the
// pack-runner script emits the durability primitives the initiative
// requires:
//
//	log_json — JSON emitter with time/level/event/service/pack/msg
//	tee -a "$LOG_PATH" — every echo also lands in the local log file
//	logs.jsonl upload beside results.json in the result-store bucket
//
// These live inside a bash string constant so any refactor drops one
// silently. Pin them here.
func TestRunnerScript_EmitsStructuredLogAndUploadsLogsJsonl(t *testing.T) {
	required := []string{
		// The JSON-log helper function name.
		"log_json() {",
		// The path used in-container to accumulate the log.
		"LOG_PATH=/tmp/logs.jsonl",
		// The runner-start structured event.
		`log_json info pack_runner_start`,
		// The runner-done event, emitted before the upload so the log
		// captures its own finalisation record.
		`log_json info pack_runner_done`,
		// The exec redirection that captures the rest of the script.
		`exec > >(tee -a "$LOG_PATH") 2>&1`,
		// The upload path that mirrors results.json' shape.
		`gsutil cp "$LOG_PATH" "${DEST_PREFIX}/logs.jsonl"`,
	}
	for _, sub := range required {
		if !strings.Contains(runnerScript, sub) {
			t.Errorf("runnerScript missing required piece %q", sub)
		}
	}
}

// TestRunnerScript_LogJsonHelperStampsFullFieldSet — the emitted JSON
// must include every field the observer's Loki pivot relies on:
// time/level/event/service/version/pack/arrival/cluster/msg.
// Regression-guard against a partial rename that would break the
// Loki `| json | arrival="…" | event=~"pack_.*"` query.
func TestRunnerScript_LogJsonHelperStampsFullFieldSet(t *testing.T) {
	required := []string{
		`"time":"%s"`,
		`"level":"%s"`,
		`"event":"%s"`,
		`"service":"%s"`,
		`"version":"%s"`,
		`"pack":"%s"`,
		`"arrival":"%s"`,
		`"cluster":"%s"`,
		`"msg":"%s"`,
	}
	for _, field := range required {
		if !strings.Contains(runnerScript, field) {
			t.Errorf("runnerScript log_json missing field placeholder %q — Loki query would break", field)
		}
	}
}
