# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
One-time migration: DSO meta.SyncWatermarks → staging meta.DFMState (scope='dso').

Checkpoint strings are produced with CONVERT(..., 121) on SQL Server so they match
DfmManager.set_checkpoint / query_source_checkpoint (yyyy-mm-dd hh:mi:ss.mmm).

Cutover step-back: each migrated value is DATEADD(DAY, -1, last_sync_timestamp)
relative to that table's own last successful sync. Legacy SyncWatermarks store DSO
wall-clock stamped *after* the sync (inclusive >=); DFM stores staging data MAX
(exclusive >). A verbatim copy would skip mid-sync staging rows and the boundary
second once. One day of idempotent re-MERGE is cheap insurance vs a full table MERGE.

Note: SyncWatermarks.last_sync_timestamp is DATETIME2(0), so there is no fractional
precision to lose from the legacy store — style 121 emits .000 for those values.
Source dw_timestamp_* columns used by ongoing DSO delta are also typically
DATETIME2(0); MERGE on PK remains idempotent if a boundary second is reprocessed.

The SFI datapoint store's watermark (sfi_datapoint.refresh_daily) becomes its checkpoint
under scope='sfi_store', three hours back: DSO stamps written in Danish local time lie up to
two hours ahead of UTC.

Also fills existing DFMState rows that still have checkpoint_value NULL (auto-seeded
by start_dfm_run if a DSO sync ran before the migrator). Rows with a non-NULL
checkpoint are left untouched.

Preferred cutover (Dagster UI or CLI):

    # Job (Launchpad)
    migrate_dso_sync_watermarks_to_dfm_job

    # Or materialize asset
    migrate_dso_sync_watermarks_to_dfm

Optional CLI fallback (from etl/ with same DB env as Dagster resources):

    python -m DSO.load.migrate_sync_watermarks_to_dfm

    MSSQL_TARGET_DSO_*       — read [meta].[SyncWatermarks] on DSO
    MSSQL_TARGET_STAGING_*   — write [meta].[DFMState] on staging
      (falls back to MSSQL_SOURCE_STAGING_* if TARGET staging password unset)

Other sfi_datapoint.* keys are not copied. Idempotent. Prefer running once per environment
cutover (DEV → TEST → PROD) *before* the first watermark DSO sync on the new
code so those runs stay delta rather than full MERGE; NULL-seed fill still
recovers if ordering slips.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import NamedTuple

import dagster as dg
from core.dfm_manager import SCHEMA_TO_TARGET_TYPE
from core.resources import MSSQLResource
from DSO.load.refresh_sfi_datapoint import STORE_DFM_SCHEMA, STORE_DFM_SCOPE, STORE_DFM_TABLE

logger = logging.getLogger(__name__)

SYNC_WATERMARKS_TABLE = "[meta].[SyncWatermarks]"
DFM_STATE_TABLE = "[meta].[DFMState]"
DSO_SCOPE = "dso"
SFI_SCHEMA = "sfi_datapoint"
SFI_STORE_WATERMARK = "refresh_daily"
SFI_STORE_STEP_BACK_HOURS = 3


class MigrationStats(NamedTuple):
    """Per-cutover outcome for SyncWatermarks → DFMState."""

    inserted: int
    updated: int
    skipped: int

    @property
    def applied(self) -> int:
        return self.inserted + self.updated


def _target_type_for_schema(schema: str) -> str:
    if schema in SCHEMA_TO_TARGET_TYPE:
        return SCHEMA_TO_TARGET_TYPE[schema]
    return schema


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def mssql_from_env(prefix: str) -> MSSQLResource:
    """Build MSSQLResource from ``{prefix}_HOST/PORT/DB/USER/PASSWORD``."""
    return MSSQLResource(
        host=os.environ.get(f"{prefix}_HOST", "localhost"),
        port=int(os.environ.get(f"{prefix}_PORT", "1433")),
        database=os.environ.get(f"{prefix}_DB", "DW"),
        user=os.environ.get(f"{prefix}_USER", "sa"),
        password=_require_env(f"{prefix}_PASSWORD"),
    )


def _staging_prefix_for_cli() -> str:
    """Prefer TARGET staging (matches the Dagster asset); fall back to SOURCE."""
    if os.environ.get("MSSQL_TARGET_STAGING_PASSWORD", "").strip():
        return "MSSQL_TARGET_STAGING"
    return "MSSQL_SOURCE_STAGING"


def migrate_sync_watermarks_to_dfm(
    mssql_dso: MSSQLResource,
    mssql_staging: MSSQLResource,
    context: dg.AssetExecutionContext | None = None,
) -> MigrationStats:
    """Copy SyncWatermarks into DFMState (scope 'dso', and the SFI store under 'sfi_store');
    fill NULL seeds; skip set checkpoints."""
    log = context.log if context is not None else logger

    select_sql = f"""
        SELECT
            N'{DSO_SCOPE}' AS [scope],
            [schema_name],
            [table_name],
            CONVERT(
                NVARCHAR(50),
                DATEADD(DAY, -1, [last_sync_timestamp]),
                121
            ) AS [checkpoint_value]
        FROM {SYNC_WATERMARKS_TABLE}
        WHERE [schema_name] <> ?
        UNION ALL
        SELECT
            N'{STORE_DFM_SCOPE}',
            N'{STORE_DFM_SCHEMA}',
            N'{STORE_DFM_TABLE}',
            CONVERT(
                NVARCHAR(50),
                DATEADD(HOUR, -{SFI_STORE_STEP_BACK_HOURS}, [last_sync_timestamp]),
                121
            )
        FROM {SYNC_WATERMARKS_TABLE}
        WHERE [schema_name] = ? AND [table_name] = ?
    """

    with mssql_dso.get_connection() as dso_conn:
        cursor = dso_conn.cursor()
        try:
            cursor.execute(select_sql, (SFI_SCHEMA, SFI_SCHEMA, SFI_STORE_WATERMARK))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        log.info("No SyncWatermarks rows to migrate.")
        return MigrationStats(inserted=0, updated=0, skipped=0)

    merge_sql = f"""
        MERGE {DFM_STATE_TABLE} AS tgt
        USING (
            SELECT ? AS [scope], ? AS [target_type], ? AS [schema_name],
                   ? AS [table_name], ? AS [checkpoint_value]
        ) AS src
        ON  tgt.[scope]       = src.[scope]
        AND tgt.[target_type] = src.[target_type]
        AND tgt.[schema_name] = src.[schema_name]
        AND tgt.[table_name]  = src.[table_name]
        WHEN MATCHED AND tgt.[checkpoint_value] IS NULL THEN
            UPDATE SET
                tgt.[checkpoint_value] = src.[checkpoint_value],
                tgt.[last_mode]        = 'delta'
        WHEN NOT MATCHED THEN
            INSERT ([scope], [target_type], [schema_name], [table_name],
                    [checkpoint_value], [last_mode])
            VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
                    src.[checkpoint_value], 'delta')
        OUTPUT $action;
    """

    inserted = 0
    updated = 0
    with mssql_staging.get_connection() as stg_conn:
        cursor = stg_conn.cursor()
        try:
            for scope, schema_name, table_name, checkpoint_value in rows:
                target_type = _target_type_for_schema(schema_name)
                cursor.execute(
                    merge_sql,
                    (scope, target_type, schema_name, table_name, checkpoint_value),
                )
                action_rows = cursor.fetchall()
                for action_row in action_rows:
                    action = (action_row[0] or "").upper()
                    if action == "INSERT":
                        inserted += 1
                    elif action == "UPDATE":
                        updated += 1
            stg_conn.commit()
        finally:
            cursor.close()

    skipped = len(rows) - inserted - updated
    stats = MigrationStats(inserted=inserted, updated=updated, skipped=skipped)
    log.info(
        "SyncWatermarks → DFMState: inserted=%s updated_null=%s "
        "skipped_existing=%s (source_rows=%s).",
        stats.inserted,
        stats.updated,
        stats.skipped,
        len(rows),
    )
    return stats


def main(argv: list[str] | None = None) -> int:
    """CLI entry: build resources from env and run the migration."""
    del argv  # reserved for future flags
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    staging_prefix = _staging_prefix_for_cli()
    mssql_dso = mssql_from_env("MSSQL_TARGET_DSO")
    mssql_staging = mssql_from_env(staging_prefix)

    logger.info(
        "Migrating SyncWatermarks from DSO %s@%s/%s → staging DFMState %s@%s/%s (%s)",
        mssql_dso.user,
        mssql_dso.host,
        mssql_dso.database,
        mssql_staging.user,
        mssql_staging.host,
        mssql_staging.database,
        staging_prefix,
    )
    stats = migrate_sync_watermarks_to_dfm(mssql_dso, mssql_staging)
    logger.info(
        "Done. inserted=%s updated_null=%s skipped_existing=%s (scope='dso').",
        stats.inserted,
        stats.updated,
        stats.skipped,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
