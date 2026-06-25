# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map table asset for Okosted dimension.

Populates map.Okosted with CleanCode_Okosted -> DW_EK mappings.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.staging.stg_okosted import stg_okosted


@dg.asset(
    name="map_okosted",
    deps=["staging_infra_erp", stg_okosted],
    group_name="dwh_okosted",
    compute_kind="mssql",
    description="Populate map.Okosted with CleanCode_Okosted -> DW_EK mappings",
)
def map_okosted(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_okosted: list[dict],
) -> dg.MaterializeResult:
    """Populate map.Okosted with CleanCode_Okosted -> DW_EK mappings."""

    entries = [
        (
            entry["code"],
            entry["code"],
        )
        for entry in stg_okosted
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
                    CleanCode_Okosted VARCHAR(100) NOT NULL,
                    bk_Code VARCHAR(50) NOT NULL
                )
            """
            )

            batch_size = 1000
            total_inserted = 0

            insert_sql = (
                f"INSERT INTO #staging_okosted_map (CleanCode_Okosted, bk_Code) "
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
                    CleanCode_Okosted, bk_Code, DW_Timestamp_Insert
                )
                SELECT
                    source.CleanCode_Okosted,
                    source.bk_Code,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #staging_okosted_map source
                WHERE NOT EXISTS (
                    SELECT 1 FROM [map].[Okosted] m
                    WHERE m.CleanCode_Okosted = source.CleanCode_Okosted
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
            "natural_key": "CleanCode_Okosted",
        }
    )
