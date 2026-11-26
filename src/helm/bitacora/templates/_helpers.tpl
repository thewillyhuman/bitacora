{{/*
Named templates shared by the manifests of this chart: names, labels, images,
the TOML that both daemons have in common, and the settings of a pod that both
workloads have in common.
*/}}

{{/* The chart name, or what replaces it. */}}
{{- define "bitacora.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
The prefix of every resource name: the release name and the chart name, or the
release name alone when it already contains the chart name. Cut at 52
characters so that the longest suffix the chart adds ("-controller") and the
ordinal of a pod still fit in the 63 characters of a DNS label.
*/}}
{{- define "bitacora.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 52 | trimSuffix "-" }}
{{- else }}
{{- $name := include "bitacora.name" . }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 52 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 52 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* The labels that select the pods of this release; they never change. */}}
{{- define "bitacora.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bitacora.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* The labels of every resource of this release. */}}
{{- define "bitacora.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "bitacora.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* The name of the ServiceAccount the pods run as. */}}
{{- define "bitacora.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "bitacora.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The image of one program. Call it with a list: the root context and the name
of the program.
*/}}
{{- define "bitacora.image" -}}
{{- $root := index . 0 -}}
{{- $program := index . 1 -}}
{{- printf "%s/%s:%s" $root.Values.image.repository $program (default $root.Chart.AppVersion $root.Values.image.tag) }}
{{- end }}

{{/*
The [cluster] and [log] tables of a daemon's configuration, which the replica
and the controller share. Strings are written with toJson: a JSON string is
also a TOML string. Credentials are never written here; see
"bitacora.objectStoreEnvironment".
*/}}
{{- define "bitacora.clusterConfig" -}}
{{- if not .Values.etcd.endpoints }}
{{- fail "etcd.endpoints must name at least one etcd endpoint" }}
{{- end -}}
[cluster]
etcd_endpoints = {{ .Values.etcd.endpoints | toJson }}
etcd_key_prefix = {{ .Values.etcd.keyPrefix | toJson }}
{{- if eq .Values.objectStore.kind "s3" }}

[cluster.object_store]
kind = "s3"
bucket = {{ required "objectStore.s3.bucket is required when objectStore.kind is s3" .Values.objectStore.s3.bucket | toJson }}
region = {{ .Values.objectStore.s3.region | toJson }}
{{- with .Values.objectStore.s3.endpoint }}
endpoint = {{ . | toJson }}
{{- end }}
key_prefix = {{ .Values.objectStore.s3.keyPrefix | toJson }}
force_path_style = {{ .Values.objectStore.s3.forcePathStyle }}
allow_http = {{ .Values.objectStore.s3.allowHttp }}
{{- else if ne .Values.objectStore.kind "none" }}
{{- fail (printf "objectStore.kind must be none or s3, not %s" .Values.objectStore.kind) }}
{{- end }}

[log]
level = {{ .Values.log.level | toJson }}
format = {{ .Values.log.format | toJson }}
{{- end }}

{{/*
The environment variables that carry the credentials of the object store from
the operator's Secret to a daemon. Empty when there is no such Secret.
*/}}
{{- define "bitacora.objectStoreEnvironment" -}}
{{- if and (eq .Values.objectStore.kind "s3") .Values.objectStore.s3.credentials.existingSecret -}}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.objectStore.s3.credentials.existingSecret }}
      key: {{ .Values.objectStore.s3.credentials.accessKeyIdKey }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.objectStore.s3.credentials.existingSecret }}
      key: {{ .Values.objectStore.s3.credentials.secretAccessKeyKey }}
{{- end }}
{{- end }}

{{/*
The security settings of a pod. The images run as user 65532; fsGroup gives
that user the volume of a replica, and the change policy keeps a restart from
walking every file of a volume that already has the right owner.
*/}}
{{- define "bitacora.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
fsGroup: 65532
fsGroupChangePolicy: OnRootMismatch
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/* The security settings of a container: nothing it does not need. */}}
{{- define "bitacora.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
{{- end }}

{{/*
The probes of a daemon, which both serve on their metrics port. Starting may
take long when a replica has many segments to open, so the startup probe
waits five minutes before the liveness probe takes over.
*/}}
{{- define "bitacora.probes" -}}
startupProbe:
  httpGet:
    path: /healthz
    port: metrics
  periodSeconds: 5
  failureThreshold: 60
livenessProbe:
  httpGet:
    path: /healthz
    port: metrics
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /readyz
    port: metrics
  periodSeconds: 5
  failureThreshold: 2
{{- end }}
