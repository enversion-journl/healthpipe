# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# Dynamic Dagster workspace template (deployment artifact).
#
# Rendered at deploy time into a workspace.yaml listing the available gRPC
# code-server locations: a primary code server plus an entry for each additional
# code location discovered at runtime via your orchestrator's service discovery.
#
# The local devcontainer does NOT use this file — it loads code directly via
# workspace.yaml. The block below uses a templating/service-discovery syntax;
# adapt it to your own deployment platform.

load_from:
  # Primary code server (always present)
  - grpc_server:
      host:
        env: DAGSTER_CODE_SERVER_HOST
      port:
        env: DAGSTER_CODE_SERVER_PORT
      location_name: "pipelines"

{{ range services }}
{{ if .Tags | contains "dagster-code-location" }}
{{ if .Tags | contains "feature-branch" }}
  # Additional code location: {{ .Name }}
  - grpc_server:
      host: "{{ .Address }}"
      port: {{ .Port }}
      location_name: "{{ .Name | replaceAll "dagster-code-server-" "" }}"
{{ end }}
{{ end }}
{{ end }}
