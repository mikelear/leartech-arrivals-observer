{{/*
Standard name helpers.

Usage:
  {{ include "leartech.name" . }}      # the short name of the chart
  {{ include "leartech.fullname" . }}  # release-qualified name, kube-DNS-safe (≤63)
  {{ include "leartech.chart" . }}     # chart name + version, for the helm.sh/chart label
*/}}

{{- define "leartech.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "leartech.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "leartech.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "leartech.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "leartech.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
