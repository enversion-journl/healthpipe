# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map table asset for Organisation dimension (SOR only).

Populates map.Organisation with CleanCode -> DW_EK mappings.
CleanCode format: 'SOR_' + SORId
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.staging.stg_organisation_sor import stg_organisation_sor


@dg.asset(
    name="map_organisation",
    deps=["staging_infra_organisation", stg_organisation_sor],
    group_name="dwh_organisation",
    compute_kind="mssql",
    description="Populate map.Organisation with CleanCode -> DW_EK mappings (SOR only)",
)
def map_organisation(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_organisation_sor: list[dict],
) -> dg.MaterializeResult:
    """Populate map.Organisation with CleanCode -> DW_EK mappings."""

    entries = [
        (
            f"SOR_{entry['SORId']}",
            entry["SORId"],
        )
        for entry in stg_organisation_sor
    ]

    context.log.info(f"Processing {len(entries)} entries for map.Organisation")

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            is_mssql_python = type(conn).__module__.startswith("mssql_python")
            placeholder = "?" if is_mssql_python else "%s"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            cursor.execute(
                """
                CREATE TABLE #staging_org (
                    CleanCode VARCHAR(100) NOT NULL,
                    Code VARCHAR(50) NOT NULL
                )
            """
            )

            batch_size = 1000
            total_inserted = 0

            insert_sql = (
                f"INSERT INTO #staging_org (CleanCode, Code) "
                f"VALUES ({placeholder}, {placeholder})"
            )

            for i in range(0, len(entries), batch_size):
                batch = entries[i : i + batch_size]
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)

                if total_inserted % 10000 == 0:
                    context.log.info(f"Staged {total_inserted}/{len(entries)} entries")

            context.log.info(f"Staged all {total_inserted} entries in temp table")

            cursor.execute(
                """
                MERGE INTO [map].[Organisation] AS target
                USING #staging_org AS source
                ON target.CleanCode = source.CleanCode
                WHEN MATCHED THEN
                    UPDATE SET
                        Code = source.Code,
                        DW_Timestamp_Update = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (CleanCode, Code,
                            DW_Timestamp_Insert, DW_Timestamp_Update)
                    VALUES (source.CleanCode, source.Code,
                            GETDATE(), GETDATE())
                OUTPUT $action;
            """
            )

            merge_results = cursor.fetchall()
            inserted = sum(1 for r in merge_results if r[0] == "INSERT")
            updated = sum(1 for r in merge_results if r[0] == "UPDATE")

            conn.commit()
        finally:
            cursor.close()

    context.log.info(f"map.Organisation: {inserted} inserted, {updated} updated")

    return dg.MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": len(entries),
            "method": "bulk_insert_with_merge",
        }
    )
