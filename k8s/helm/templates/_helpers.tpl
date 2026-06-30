{{- define "flowrulz.namespace" -}}
{{- .Values.global.namespace | default "flowrulz" -}}
{{- end -}}

{{- define "flowrulz.image" -}}
{{- $registry := .Values.global.imageRegistry -}}
{{- $repo := .Values.flowrulz.image.repository -}}
{{- $tag := .Values.flowrulz.image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end -}}

{{- define "sim.image" -}}
{{- $registry := .Values.global.imageRegistry -}}
{{- $repo := .Values.sim.image.repository -}}
{{- $tag := .Values.sim.image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end -}}
