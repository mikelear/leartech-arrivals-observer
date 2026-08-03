{{/*
Auth personas — reusable env-bundle helper for authed UI test packs.

Any test spec running against a service that requires a signed-in user
needs one or both of these personas plumbed via env vars. Rather than
copy-pasting the four secretKeyRef entries into every service's
services.<name>.env block in values.yaml, this helper emits the
standard bundle. Consumers include it inline:

  services:
    leartech-auth-ui:
      testPacks:
        - { name: end2end-ui, type: end2end-ui }
      env:
        {{- include "leartech.authPersonasEnvVars" . | nindent 8 }}

or per-pack:

  services:
    leartech-portal:
      testPacks:
        - name: end2end-ui
          type: end2end-ui
          env:
            {{- include "leartech.authPersonasEnvVars" . | nindent 12 }}

The `auth-service-test-users` secret is provisioned in each cluster's
jx-staging namespace by leartech-auth-service's chart (per the shared
services-map convention). All four refs are marked `optional: true` so
a preview namespace without the secret still spawns the runner pod
(env vars just resolve to empty; test specs that skip on empty
USER_* handle this gracefully).

USER_* → standard authed user persona (login flows, dashboard).
PLATFORM_ADMIN_* → elevated persona for admin-only screens.

If your service needs a different secret name / key layout, don't
extend this helper — just inline the raw env block in your values.yaml
overlay. This helper is intentionally opinionated for the auth-service
convention; more personas can grow additional helpers alongside it
(leartech.dealerPersonasEnvVars, leartech.brokerPersonasEnvVars, …).

For a documented services-map snippet without using this helper, see
the "Reusable auth-personas services-map snippet" example block below
the `services:` key in this chart's values.yaml.
*/}}
{{- define "leartech.authPersonasEnvVars" -}}
- name: USER_EMAIL
  valueFrom:
    secretKeyRef:
      name: auth-service-test-users
      key: user_email
      optional: true
- name: USER_PASSWORD
  valueFrom:
    secretKeyRef:
      name: auth-service-test-users
      key: user_password
      optional: true
- name: PLATFORM_ADMIN_EMAIL
  valueFrom:
    secretKeyRef:
      name: auth-service-test-users
      key: platform_admin_email
      optional: true
- name: PLATFORM_ADMIN_PASSWORD
  valueFrom:
    secretKeyRef:
      name: auth-service-test-users
      key: platform_admin_password
      optional: true
{{- end -}}
