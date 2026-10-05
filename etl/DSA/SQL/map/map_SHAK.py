# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map table asset for the SHAK dimension.

Populates map.SHAK with clean_code_SHAK -> dw_ek_SHAK mappings (IDENTITY-assigned).
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.staging.stg_SHAK import stg_SHAK


@dg.asset(
    name="map_SHAK",
    deps=["staging_infra_erp", stg_SHAK],
    group_name="dwh_shak",
    compute_kind="mssql",
    description="Populate map.SHAK with clean_code_SHAK -> dw_ek_SHAK mappings",
)
def map_SHAK(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_SHAK: list[dict],
) -> dg.MaterializeResult:
    """Populate map.SHAK with clean_code_SHAK -> dw_ek_SHAK mappings."""
    entries = [(entry["code"], entry["code"]) for entry in stg_SHAK]
    context.log.info(f"Processing {len(entries)} entries for map.SHAK")

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            is_mssql_python = type(conn).__module__.startswith("mssql_python")
            placeholder = "?" if is_mssql_python else "%s"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            cursor.execute(
                """
                CREATE TABLE #staging_shak_map (
                    clean_code_SHAK VARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL,
                    bk_code VARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL
                )
                """
            )

            insert_sql = (
                f"INSERT INTO #staging_shak_map (clean_code_SHAK, bk_code) "
                f"VALUES ({placeholder}, {placeholder})"
            )

            batch_size = 1000
            for i in range(0, len(entries), batch_size):
                cursor.executemany(insert_sql, entries[i : i + batch_size])

            cursor.execute(
                """
                INSERT INTO [map].[SHAK] (
                    clean_code_SHAK, bk_code, dw_timestamp_insert
                )
                SELECT
                    source.clean_code_SHAK,
                    source.bk_code,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #staging_shak_map source
                WHERE NOT EXISTS (
                    SELECT 1 FROM [map].[SHAK] m
                    WHERE m.clean_code_SHAK = source.clean_code_SHAK
                );
                """
            )
            inserted = cursor.rowcount
            conn.commit()
        finally:
            cursor.close()

    context.log.info(f"map.SHAK: {inserted} new entries inserted")
    return dg.MaterializeResult(
        metadata={
            "new_rows": inserted,
            "total_staged": len(entries),
            "table": "map.SHAK",
            "natural_key": "clean_code_SHAK",
        }
    )
