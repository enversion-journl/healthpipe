# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Migration assets and jobs.

Two reusable assets that execute SQL migration scripts from
dagster_etl/migrations/staging/ and dagster_etl/migrations/dso/.
Each has a companion job with a config pane for the script filename.

Folder structure:
  migrations/
    staging/   -- scripts targeting the staging/DSA database
    dso/       -- scripts targeting the DSO/reporting database
"""

import re
from pathlib import Path

from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    RunConfig,
    asset,
    define_asset_job,
)

MIGRATIONS_DIR = Path(__file__).parent


class MigrationConfig(Config):
    script: str


def _run_migration(
    context: AssetExecutionContext,
    resource: MSSQLResource,
    target: str,
    script: str,
) -> MaterializeResult:
    """Execute a SQL migration script, splitting on GO batch separators."""

    migration_file = MIGRATIONS_DIR / target / script
    if not migration_file.exists():
        raise FileNotFoundError(
            f"Migration script not found: {script} " f"(looked in {MIGRATIONS_DIR / target})"
        )

    sql_text = migration_file.read_text(encoding="utf-8")

    batches = re.split(r"^\s*GO\s*$", sql_text, flags=re.MULTILINE | re.IGNORECASE)
    batches = [b.strip() for b in batches if b.strip()]

    context.log.info(f"Migration script: {target}/{script} ({len(batches)} batches)")

    executed = 0
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for i, batch in enumerate(batches, 1):
                prints = re.findall(r"PRINT\s+'([^']+)'", batch)
                for msg in prints:
                    context.log.info(f"[batch {i}] {msg}")

                cursor.execute(batch)
                conn.commit()
                executed += 1

            context.log.info(f"Migration complete: {executed}/{len(batches)} batches executed")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "migration_file": f"{target}/{script}",
            "batches_total": len(batches),
            "batches_executed": executed,
        }
    )


# ============================================================================
# Staging migration
# ============================================================================


@asset(
    key="run_migration_staging",
    group_name="erp_migrations",
    compute_kind="mssql",
    tags={"domain": "erp", "type": "migration", "target": "staging"},
    description=(
        "Run a SQL migration script from dagster_etl/migrations/staging/. "
        "Use the 'run_migration_staging_job' job for an easy launch UI."
    ),
)
def run_migration_staging(
    context: AssetExecutionContext,
    config: MigrationConfig,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Execute a SQL migration script against the staging database."""
    return _run_migration(context, mssql_target_staging, "staging", config.script)


run_migration_staging_job = define_asset_job(
    name="run_migration_staging_job",
    selection="run_migration_staging",
    config=RunConfig(ops={"run_migration_staging": MigrationConfig(script="<file_name.sql>")}),
    description=(
        "Run a SQL migration script against the STAGING database. "
        "Set the 'script' config to the filename in dagster_etl/migrations/staging/."
    ),
)


# ============================================================================
# DSO migration
# ============================================================================


@asset(
    key="run_migration_dso",
    group_name="erp_migrations",
    compute_kind="mssql",
    tags={"domain": "erp", "type": "migration", "target": "dso"},
    description=(
        "Run a SQL migration script from dagster_etl/migrations/dso/. "
        "Use the 'run_migration_dso_job' job for an easy launch UI."
    ),
)
def run_migration_dso(
    context: AssetExecutionContext,
    config: MigrationConfig,
    mssql_target_dso: MSSQLResource,
) -> MaterializeResult:
    """Execute a SQL migration script against the DSO database."""
    return _run_migration(context, mssql_target_dso, "dso", config.script)


run_migration_dso_job = define_asset_job(
    name="run_migration_dso_job",
    selection="run_migration_dso",
    config=RunConfig(ops={"run_migration_dso": MigrationConfig(script="<file_name.sql>")}),
    description=(
        "Run a SQL migration script against the DSO database. "
        "Set the 'script' config to the filename in dagster_etl/migrations/dso/."
    ),
)
