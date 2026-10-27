# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map table asset for Okosted dimension.

Populates map.Okosted with clean_code_Okosted -> DW_EK mappings.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.staging.stg_Okosted import stg_Okosted


@dg.asset(
    name="map_Okosted",
    deps=["staging_infra_erp", stg_Okosted],
    group_name="dwh_okosted",
    compute_kind="mssql",
    description="Populate map.Okosted with clean_code_Okosted -> DW_EK mappings",
)
def map_Okosted(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_Okosted: list[dict],
) -> dg.MaterializeResult:
    """Populate map.Okosted with clean_code_Okosted -> DW_EK mappings."""

    entries = [
        (
            entry["code"],
            entry["code"],
        )
        for entry in stg_Okosted
    ]

    context.log.info(f"Processing {len(entries)} entries for map.Okosted")

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            is_mssql_python = type(conn).__module__.startswith("mssql_python")
            placeholder = "?" if is_mssql_python else "%s"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            cursor.execute(
                """
                CREATE TABLE #staging_okosted_map (
                    clean_code_Okosted VARCHAR(120) COLLATE DATABASE_DEFAULT NOT NULL,
                    bk_code VARCHAR(120) COLLATE DATABASE_DEFAULT NOT NULL
                )
            """
            )

            batch_size = 1000
            total_inserted = 0

            insert_sql = (
                f"INSERT INTO #staging_okosted_map (clean_code_Okosted, bk_code) "
                f"VALUES ({placeholder}, {placeholder})"
            )

            for i in range(0, len(entries), batch_size):
                batch = entries[i : i + batch_size]
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)

            context.log.info(f"Staged all {total_inserted} entries in temp table")

            cursor.execute(
                """
                INSERT INTO [map].[Okosted] (
                    clean_code_Okosted, bk_code, dw_timestamp_insert
                )
                SELECT
                    source.clean_code_Okosted,
                    source.bk_code,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #staging_okosted_map source
                WHERE NOT EXISTS (
                    SELECT 1 FROM [map].[Okosted] m
                    WHERE m.clean_code_Okosted = source.clean_code_Okosted
                );
            """
            )

            inserted = cursor.rowcount

            conn.commit()
        finally:
            cursor.close()

    context.log.info(f"map.Okosted: {inserted} new entries inserted")

    return dg.MaterializeResult(
        metadata={
            "new_rows": inserted,
            "total_staged": len(entries),
            "table": "map.Okosted",
            "natural_key": "clean_code_Okosted",
        }
    )
