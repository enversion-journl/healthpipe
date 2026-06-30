# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# Dagster Code Server Base Dockerfile
# This is the base layer containing system dependencies and Python packages.
# This layer rarely changes and should only be rebuilt when dependencies change.
# The code-server.Dockerfile builds on top of this base layer.
#
# CI builds this and publishes it to your own container registry, then builds
# code-server.Dockerfile with --build-arg BASE_IMAGE=<that image>. Example:
#   docker buildx build \
#     --platform linux/amd64 \
#     -f infrastructure/containers/dockerfiles/code-server-base.Dockerfile \
#     -t <registry>/<namespace>/dagster-code-server-base:latest \
#     --push \
#     .

FROM python:3.12-slim-bookworm

# Install system dependencies needed for SQL tooling, Oracle, and builds
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gnupg \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
    odbcinst && \
    rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /opt/dagster/app

# Copy pyproject.toml and install dependencies
COPY pyproject.toml /opt/dagster/app/pyproject.toml

# Install dependencies (using pip directly — uv segfaults under QEMU amd64 emulation on ARM hosts)
RUN pip install --no-cache-dir ".[dev]"

# Verify dagster CLI is available
RUN which dagster && dagster --version
