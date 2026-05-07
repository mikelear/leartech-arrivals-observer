{{/*
Liveness / readiness / startup probe defaults.

Services expose `/health/live` and `/health/ready` on the `http` named port per
golden-service-standard.md § "The every backend service must ship contract".

Defaults are intentionally conservative — tune per service via
.Values.probes.<liveness|readiness|startup>.<field>.

Usage:
  containers:
  - name: app
    ports:
    - name: http
      containerPort: 8080
    livenessProbe:
      {{- include "leartech.livenessProbe" . | nindent 6 }}
    readinessProbe:
      {{- include "leartech.readinessProbe" . | nindent 6 }}
    startupProbe:
      {{- include "leartech.startupProbe" . | nindent 6 }}
*/}}

{{- define "leartech.livenessProbe" -}}
httpGet:
  path: {{ .Values.probes.liveness.path | default "/health/live" }}
  port: {{ .Values.probes.liveness.port | default "http" }}
initialDelaySeconds: {{ .Values.probes.liveness.initialDelaySeconds | default 10 }}
periodSeconds: {{ .Values.probes.liveness.periodSeconds | default 10 }}
timeoutSeconds: {{ .Values.probes.liveness.timeoutSeconds | default 3 }}
failureThreshold: {{ .Values.probes.liveness.failureThreshold | default 3 }}
{{- end -}}

{{- define "leartech.readinessProbe" -}}
httpGet:
  path: {{ .Values.probes.readiness.path | default "/health/ready" }}
  port: {{ .Values.probes.readiness.port | default "http" }}
initialDelaySeconds: {{ .Values.probes.readiness.initialDelaySeconds | default 5 }}
periodSeconds: {{ .Values.probes.readiness.periodSeconds | default 5 }}
timeoutSeconds: {{ .Values.probes.readiness.timeoutSeconds | default 3 }}
failureThreshold: {{ .Values.probes.readiness.failureThreshold | default 3 }}
{{- end -}}

{{- define "leartech.startupProbe" -}}
httpGet:
  path: {{ .Values.probes.startup.path | default "/health/live" }}
  port: {{ .Values.probes.startup.port | default "http" }}
initialDelaySeconds: {{ .Values.probes.startup.initialDelaySeconds | default 0 }}
periodSeconds: {{ .Values.probes.startup.periodSeconds | default 5 }}
timeoutSeconds: {{ .Values.probes.startup.timeoutSeconds | default 3 }}
failureThreshold: {{ .Values.probes.startup.failureThreshold | default 30 }}
{{- end -}}
