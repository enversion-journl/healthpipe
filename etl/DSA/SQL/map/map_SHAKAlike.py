# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map table asset for the SHAK-a-like dimension.

Populates map.SHAKAlike with clean_code_SHAKAlike -> dw_ek_SHAKAlike mappings.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.staging.stg_SHAKAlike import stg_SHAKAlike


@dg.asset(
    name="map_SHAKAlike",
    deps=["staging_infra_erp", stg_SHAKAlike],
    group_name="dwh_shakalike",
    compute_kind="mssql",
    description="Populate map.SHAKAlike with clean_code_SHAKAlike -> dw_ek_SHAKAlike",
)
def map_SHAKAlike(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_SHAKAlike: list[dict],
) -> dg.MaterializeResult:
    entries = [(entry["code"], entry["code"]) for entry in stg_SHAKAlike]
    context.log.info(f"Processing {len(entries)} entries for map.SHAKAlike")

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            is_mssql_python = type(conn).__module__.startswith("mssql_python")
            placeholder = "?" if is_mssql_python else "%s"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            cursor.execute(
                """
                CREATE TABLE #staging_shakalike_map (
                    clean_code_SHAKAlike VARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL,
                    bk_code VARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL
                )
                """
            )

            insert_sql = (
                f"INSERT INTO #staging_shakalike_map (clean_code_SHAKAlike, bk_code) "
                f"VALUES ({placeholder}, {placeholder})"
            )

            batch_size = 1000
            for i in range(0, len(entries), batch_size):
                cursor.executemany(insert_sql, entries[i : i + batch_size])

            cursor.execute(
                """
                INSERT INTO [map].[SHAKAlike] (
                    clean_code_SHAKAlike, bk_code, dw_timestamp_insert
                )
                SELECT
                    source.clean_code_SHAKAlike,
                    source.bk_code,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #staging_shakalike_map source
                WHERE NOT EXISTS (
                    SELECT 1 FROM [map].[SHAKAlike] m
                    WHERE m.clean_code_SHAKAlike = source.clean_code_SHAKAlike
                );
                """
            )
            inserted = cursor.rowcount
            conn.commit()
        finally:
            cursor.close()

    context.log.info(f"map.SHAKAlike: {inserted} new entries inserted")
    return dg.MaterializeResult(
        metadata={
            "new_rows": inserted,
            "total_staged": len(entries),
            "table": "map.SHAKAlike",
            "natural_key": "clean_code_SHAKAlike",
        }
    )
