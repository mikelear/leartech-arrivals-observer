{{/*
Pod-level and container-level securityContext defaults aligned with the Kyverno
baseline profile (no privilege escalation, non-root, read-only root FS, drop ALL).

Services that genuinely need to write to the root FS must mount an emptyDir.

Usage:
  spec:
    template:
      spec:
        securityContext:
          {{- include "leartech.podSecurityContext" . | nindent 10 }}
        containers:
        - name: app
          securityContext:
            {{- include "leartech.containerSecurityContext" . | nindent 12 }}
*/}}

{{- define "leartech.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 1000
runAsGroup: 1000
fsGroup: 1000
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "leartech.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 1000
capabilities:
  drop:
  - ALL
{{- end -}}
