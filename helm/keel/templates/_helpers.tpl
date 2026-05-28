{{/*
Expand the name of the chart.
*/}}
{{- define "keel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "keel.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "keel.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "keel.labels" -}}
helm.sh/chart: {{ include "keel.chart" . }}
{{ include "keel.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "keel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "keel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "keel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "keel.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Effective configuration format ("ini" or "yaml"). Honours the explicit
`.Values.configFormat` if set; otherwise picks "yaml" when the structured
`.Values.configYaml` map is provided, falling back to "ini".
*/}}
{{- define "keel.configFormat" -}}
{{- if .Values.configFormat -}}
{{- .Values.configFormat | lower -}}
{{- else if .Values.configYaml -}}
yaml
{{- else -}}
ini
{{- end -}}
{{- end }}

{{/*
Basename of the config file inside the ConfigMap and the container.
*/}}
{{- define "keel.configFilename" -}}
{{- if eq (include "keel.configFormat" .) "yaml" -}}
keel.yaml
{{- else -}}
keel.ini
{{- end -}}
{{- end }}

{{/*
Absolute path of the config file inside the container.
*/}}
{{- define "keel.configPath" -}}
/etc/keel/{{ include "keel.configFilename" . }}
{{- end }}
