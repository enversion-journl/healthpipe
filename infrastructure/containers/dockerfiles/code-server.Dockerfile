# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# Dagster Code Server Dockerfile
# This container runs the gRPC code server that serves user code definitions
# to the daemon and webserver. Redeploy this container to update code without
# interrupting running pipelines.
#
# This Dockerfile uses a pre-built base layer (code-server-base) that contains
# system dependencies and Python packages. Only the user code is copied here,
# making builds much faster for code-only changes.
#
# Base layer: built from code-server-base.Dockerfile and published to your own
# container registry. CI/CD supplies the full reference via
#   --build-arg BASE_IMAGE=<registry>/<namespace>/dagster-code-server-base:latest
# The default below is an unqualified placeholder for local/standalone builds.

ARG BASE_IMAGE=dagster-code-server-base:latest
FROM ${BASE_IMAGE}

# Build timestamp to bust cache (set by CI/CD)
ARG BUILD_TIMESTAMP=unknown
RUN echo "Build timestamp: ${BUILD_TIMESTAMP}"

# Copy user code (dagster_etl directory with all packages)
COPY dagster_etl/ /opt/dagster/app/dagster_etl/

# Set PYTHONPATH to include the dagster_etl directory for proper module imports
ENV PYTHONPATH=/opt/dagster/app/dagster_etl

# Expose gRPC port
EXPOSE 4000

# Run dagster gRPC server
# -h: host to bind to
# -p: port to listen on
# -m: module name containing Definitions
# -d: working directory
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "definitions", "-d", "/opt/dagster/app/dagster_etl"]
