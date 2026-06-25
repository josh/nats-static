{{/*
Expand the name of the chart.
*/}}
{{- define "nats-static.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name (release name + chart name unless overridden).
*/}}
{{- define "nats-static.fullname" -}}
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

{{/*
Chart name and version, used as the helm.sh/chart label.
*/}}
{{- define "nats-static.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Selector labels (stable across upgrades — used in Deployment selector & Pod labels).
*/}}
{{- define "nats-static.selectorLabels" -}}
app.kubernetes.io/name: {{ include "nats-static.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "nats-static.labels" -}}
helm.sh/chart: {{ include "nats-static.chart" . }}
{{ include "nats-static.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
ServiceAccount name.
*/}}
{{- define "nats-static.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "nats-static.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Container image reference. Tag defaults to .Chart.AppVersion.
Explicit image.tag overrides pass through.
*/}}
{{- define "nats-static.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Default Secret name backing the NATS auth methods. Falls back to the chart
fullname when a method's secret.name is left empty, so the chart renders without
overrides.
*/}}
{{- define "nats-static.defaultSecretName" -}}
{{- printf "%s-secrets" (include "nats-static.fullname" .) -}}
{{- end -}}

{{/*
The app's NATS auth method: token | user | creds | nkey | none. Determined by
which secrets.nats method block is populated (the connection is anonymous when
none is).
*/}}
{{- define "nats-static.natsMethod" -}}
{{- $n := .Values.secrets.nats -}}
{{- if $n.token.token -}}token
{{- else if or $n.user.user $n.user.password -}}user
{{- else if $n.creds.file -}}creds
{{- else if $n.nkey.seed -}}nkey
{{- else -}}none
{{- end -}}
{{- end -}}

{{/*
Count of populated secrets.nats method blocks (for validation).
*/}}
{{- define "nats-static.natsMethodCount" -}}
{{- $n := .Values.secrets.nats -}}
{{- $c := 0 -}}
{{- if $n.token.token -}}{{- $c = add1 $c -}}{{- end -}}
{{- if or $n.user.user $n.user.password -}}{{- $c = add1 $c -}}{{- end -}}
{{- if $n.creds.file -}}{{- $c = add1 $c -}}{{- end -}}
{{- if $n.nkey.seed -}}{{- $c = add1 $c -}}{{- end -}}
{{- $c -}}
{{- end -}}

{{/*
Count of populated secrets.nack method blocks (for validation / mirror logic).
*/}}
{{- define "nats-static.nackMethodCount" -}}
{{- $n := .Values.secrets.nack -}}
{{- $c := 0 -}}
{{- if $n.token.token -}}{{- $c = add1 $c -}}{{- end -}}
{{- if or $n.user.user $n.user.password -}}{{- $c = add1 $c -}}{{- end -}}
{{- if $n.creds.file -}}{{- $c = add1 $c -}}{{- end -}}
{{- if $n.nkey.seed -}}{{- $c = add1 $c -}}{{- end -}}
{{- $c -}}
{{- end -}}

{{/*
True when a NATS auth method is configured (so the Pod needs a secrets volume).
*/}}
{{- define "nats-static.hasSecrets" -}}
{{- $method := include "nats-static.natsMethod" . | trim -}}
{{- if ne $method "none" -}}true{{- end -}}
{{- end -}}

{{/*
The effective Secret name backing the app's NATS auth (the active method's
secret.name, defaulting to the chart fullname Secret).
*/}}
{{- define "nats-static.natsSecretName" -}}
{{- $method := include "nats-static.natsMethod" . | trim -}}
{{- if ne $method "none" -}}
{{- $b := index .Values.secrets.nats $method -}}
{{- $b.secret.name | default (include "nats-static.defaultSecretName" .) -}}
{{- end -}}
{{- end -}}

{{/*
The NACK Account CRD auth method: a populated secrets.nack block, otherwise it
mirrors the app's NATS method.
*/}}
{{- define "nats-static.accountMethod" -}}
{{- $a := .Values.secrets.nack -}}
{{- if $a.token.token -}}token
{{- else if or $a.user.user $a.user.password -}}user
{{- else if $a.creds.file -}}creds
{{- else if $a.nkey.seed -}}nkey
{{- else -}}{{- include "nats-static.natsMethod" . | trim -}}
{{- end -}}
{{- end -}}

{{/*
The Account CRD auth block (spec.token / spec.user / spec.creds / spec.nkey).
Uses secrets.nack when populated, otherwise mirrors secrets.nats verbatim.
Emits nothing for an anonymous (none) method.
*/}}
{{- define "nats-static.accountUser" -}}
{{- $defaultName := include "nats-static.defaultSecretName" . -}}
{{- $method := include "nats-static.accountMethod" . | trim -}}
{{- $nackCount := include "nats-static.nackMethodCount" . | int -}}
{{- if ne $method "none" -}}
{{- $b := dict -}}
{{- $name := "" -}}
{{- if gt $nackCount 0 -}}
{{- $b = index .Values.secrets.nack $method -}}
{{- $name = $b.secret.name | default (index .Values.secrets.nats $method).secret.name | default $defaultName -}}
{{- else -}}
{{- $b = index .Values.secrets.nats $method -}}
{{- $name = $b.secret.name | default $defaultName -}}
{{- end -}}
{{- if eq $method "token" -}}
token:
  secret:
    name: {{ $name | quote }}
  token: {{ $b.token | quote }}
{{- else if eq $method "user" -}}
user:
  secret:
    name: {{ $name | quote }}
  user: {{ $b.user | quote }}
  password: {{ $b.password | quote }}
{{- else if eq $method "creds" -}}
creds:
  secret:
    name: {{ $name | quote }}
  file: {{ $b.file | quote }}
{{- else if eq $method "nkey" -}}
nkey:
  secret:
    name: {{ $name | quote }}
  seed: {{ $b.seed | quote }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Fail-fast validation of the secrets / nack configuration. Included from
configmap.yaml so any render path triggers it.
*/}}
{{- define "nats-static.validate" -}}
{{- if gt (include "nats-static.natsMethodCount" . | int) 1 -}}
{{- fail "secrets.nats: set at most one auth method (token, user, creds, or nkey)" -}}
{{- end -}}
{{- $u := .Values.secrets.nats.user -}}
{{- if and (or $u.user $u.password) (not (and $u.user $u.password)) -}}
{{- fail "secrets.nats.user: set both the user and password keys" -}}
{{- end -}}
{{- $nackCount := include "nats-static.nackMethodCount" . | int -}}
{{- if gt $nackCount 1 -}}
{{- fail "secrets.nack: set at most one auth method (token, user, creds, or nkey)" -}}
{{- end -}}
{{- $nu := .Values.secrets.nack.user -}}
{{- if and (or $nu.user $nu.password) (not (and $nu.user $nu.password)) -}}
{{- fail "secrets.nack.user: set both the user and password keys" -}}
{{- end -}}
{{- if and .Values.nack.account.name (not .Values.nack.enabled) -}}
{{- fail "nack.account.name is set but nack.enabled is false; enable NACK or clear the account name" -}}
{{- end -}}
{{- if and (gt $nackCount 0) (not .Values.nack.account.name) -}}
{{- fail "secrets.nack is populated but nack.account.name is empty; set nack.account.name or clear secrets.nack" -}}
{{- end -}}
{{- end -}}

{{/*
Rendered config.json content. Merges the chart-owned auth file paths into the
user's .Values.config block based on the selected secrets.nats method.
*/}}
{{- define "nats-static.configJson" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $method := include "nats-static.natsMethod" . | trim -}}
{{- if eq $method "token" -}}
{{- $_ := set $cfg.nats "token_file" "/etc/nats-static/secrets/nats.token" -}}
{{- else if eq $method "user" -}}
{{- $_ := set $cfg.nats "user_file" "/etc/nats-static/secrets/nats.user" -}}
{{- $_ := set $cfg.nats "password_file" "/etc/nats-static/secrets/nats.password" -}}
{{- else if eq $method "creds" -}}
{{- $_ := set $cfg.nats "creds_file" "/etc/nats-static/secrets/nats.creds" -}}
{{- else if eq $method "nkey" -}}
{{- $_ := set $cfg.nats "nkey_seed_file" "/etc/nats-static/secrets/nats.nk" -}}
{{- end -}}
{{- $cfg | toJson -}}
{{- end -}}
