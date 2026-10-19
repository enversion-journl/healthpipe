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

FROM --platform=$BUILDPLATFORM python:3.12-slim-bookworm AS lock
RUN pip install --no-cache-dir uv==0.12.19
WORKDIR /src
COPY pyproject.toml uv.lock ./
RUN uv export --locked --extra dev --no-emit-project --no-emit-package pip \
    --format requirements-txt -o /requirements.txt

FROM python:3.12-slim-bookworm

# System dependencies.
#
# There is NO msodbcsql18 / unixodbc install here, and that is deliberate. Every
# database client in pyproject.toml carries its own driver:
#   mssql-python  bundles Microsoft's DDBC driver inside ddbc_bindings*.so
#   pymssql       bundles FreeTDS in its wheel
#   oracledb      thin mode, pure Python — no Oracle Instant Client
#   psycopg[binary]  bundles libpq
# Nothing in this repo imports pyodbc, and no connection string names an ODBC
# driver (etl/core/resources.py builds SERVER=/DATABASE=/UID=/PWD= strings that
# mssql-python resolves itself), so the system ODBC driver manager was never on
# any code path.
#
# libltdl7 and libgssapi-krb5-2 ARE required: mssql-python dlopens both at
# connect time. Without them every query fails with "DDBC Error: Failed to load
# the driver" — verified on python:3.12-slim-bookworm/amd64, where adding just
# those two turns that error into a plain "[Microsoft]TCP Provider" network
# error, i.e. the driver loads.
#
# Dropping the Microsoft apt repo is what lets this image build on gitlab.com
# SHARED runners. They cannot reach packages.microsoft.com — the curl there died
# with "Recv failure: Connection reset by peer" after 126s (job 16167332804),
# which failed build:base:qa and skipped build:qa with it, so release/1.0.75
# never got an image at all. gnupg and apt-transport-https went with the repo;
# they existed only to add and trust it.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libltdl7 \
    libgssapi-krb5-2 && \
    rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /opt/dagster/app

# Copy pyproject.toml and install dependencies
COPY pyproject.toml /opt/dagster/app/pyproject.toml
COPY --from=lock /requirements.txt /tmp/requirements.txt

# Install dependencies (using pip directly — uv segfaults under QEMU amd64 emulation on ARM hosts)
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt \
    && pip install --no-cache-dir --no-deps .

# Verify dagster CLI is available
RUN which dagster && dagster --version
