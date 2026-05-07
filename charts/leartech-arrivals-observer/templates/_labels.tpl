{{/*
Standard Kubernetes recommended labels + selector labels.

All leartech workloads get `app.kubernetes.io/part-of: leartech` so cluster-wide
queries like `kubectl get pods -l app.kubernetes.io/part-of=leartech` work out
of the box.

Usage:
  metadata:
    labels:
      {{- include "leartech.labels" . | nindent 4 }}

  selector:
    matchLabels:
      {{- include "leartech.selectorLabels" . | nindent 6 }}
*/}}

{{- define "leartech.labels" -}}
helm.sh/chart: {{ include "leartech.chart" . }}
{{ include "leartech.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: leartech
{{- end -}}

{{- define "leartech.selectorLabels" -}}
app.kubernetes.io/name: {{ include "leartech.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
