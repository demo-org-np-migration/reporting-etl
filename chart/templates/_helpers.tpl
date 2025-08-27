{{- define "reporting-etl.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/part-of: cauri
cauri.io/team: platform
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
