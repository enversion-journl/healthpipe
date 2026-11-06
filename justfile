# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

uv_run := "uv run --locked --extra dev"
etl_path := justfile_directory() / "etl"

[private]
default:
    @{{ just_executable() }} --list

# Install the locked dependencies with the dev extra and the pre-commit hooks.
install:
    uv sync --locked --extra dev
    {{ uv_run }} pre-commit install

# Run the pre-commit hooks on the files that changed since FROM_REF and check the license headers.
lint from_ref="origin/main":
    SKIP=gitleaks {{ uv_run }} pre-commit run --from-ref {{ from_ref }} --to-ref HEAD --show-diff-on-failure
    {{ uv_run }} python etl/scripts/license_headers.py --check

# Format the Python code with Black and apply the Ruff fixes.
format:
    {{ uv_run }} black .
    {{ uv_run }} ruff check --fix .

# Run the unit tests with coverage.
test *args:
    cd etl && {{ uv_run }} python -m pytest tests/unit/ --cov=. --cov-report=term-missing {{ args }}

# Run the integration tests against the devcontainer databases.
test-integration *args:
    cd etl && USE_REAL_DB_FOR_TESTS=true {{ uv_run }} python -m pytest tests/integration/ {{ args }}

# Run mypy --strict on etl/utils and mypy on the etl tree.
typecheck:
    {{ uv_run }} mypy --strict etl/utils
    -{{ uv_run }} mypy etl

# Load the Dagster code location.
validate:
    PYTHONPATH="{{ etl_path }}" {{ uv_run }} dagster definitions validate

# Start the Dagster web UI on http://localhost:3000.
dev:
    cd etl && PYTHONPATH="{{ etl_path }}" {{ uv_run }} dagster dev -m definitions

# Load the example source and run the ILS user dimension from DSI to DSO (devcontainer only).
example:
    {{ uv_run }} python examples/example_source/load_example_source.py
    sed -i "s/^ILS_PROD_MSSQL_DATABASE=.*/ILS_PROD_MSSQL_DATABASE=example_source/; s/^MSSQL_LINKED_SERVER_DB=.*/MSSQL_LINKED_SERVER_DB=staging/" .env
    PYTHONPATH="{{ etl_path }}" {{ uv_run }} dagster asset materialize -m definitions --select "*staging_infra_erp,staging_infra_dfm,*dso_infra_erp,dso_infra_linked_server"
    PYTHONPATH="{{ etl_path }}" {{ uv_run }} dagster asset materialize -m definitions --select "ext_ils_zz_oper,ext_ils_sgh,arc_ils_zz_oper,arc_ils_sgh,erp/stg_dim_ILSBruger,map_ILSBruger,dim_ILSBruger,dso_sync_dim_ilsbruger"
