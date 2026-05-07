{{/*
Resource request/limit defaults.

Intentionally conservative — overcommit-safe on small clusters but correctly
sized so services aren't permanently throttled. Override per service via
.Values.resources.

Per cluster policy: NO CPU limit by default (limits cause throttling under
bursty load; requests alone drive scheduling + fair-share). Memory limit IS
set so a leaking service gets OOMKilled rather than taking down its node.

Usage:
  containers:
  - name: app
    resources:
      {{- include "leartech.resources" . | nindent 6 }}
*/}}

{{- define "leartech.resources" -}}
{{- if .Values.resources -}}
{{- toYaml .Values.resources -}}
{{- else -}}
requests:
  cpu: 100m
  memory: 128Mi
limits:
  memory: 512Mi
{{- end -}}
{{- end -}}
