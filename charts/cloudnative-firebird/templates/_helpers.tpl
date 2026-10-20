{{/*
The names are fixed (the operator's own Lease, webhook Service and certificate Secret carry them),
so one release per cluster; the namespace is the release's.
*/}}
{{- define "cloudnative-firebird.labels" -}}
app.kubernetes.io/name: cloudnative-firebird
app.kubernetes.io/component: operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "cloudnative-firebird.selectorLabels" -}}
app.kubernetes.io/name: cloudnative-firebird
app.kubernetes.io/component: operator
{{- end }}

{{- define "cloudnative-firebird.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}
