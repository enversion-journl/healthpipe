# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Vare

Assign DW_EK to Vare via IDENTITY.
Logic from: erp_dims_and_facts.yml (canonical natural key: CleanCode_Vare)

Uses the unified dimension staging table:
- stg.dim_vare: Combined MySupply + ILS vare data with vnr relationship column
- Stores bk_Kreditor and bk_LeverandoerVarenummer as traceability components.

DOCUMENTED EXCEPTION (per NON_FHIR guide §4 "Map table loads"):
This map uses an UPDATE statement in addition to INSERT-only logic.
The ``vnr`` (ILS product number) column is updated on existing map rows
when ILS staging provides a new or changed value. This is necessary because
vnr is a cross-system link used by stg_fct_vareudlevering_ils to resolve
Vare CleanCodes, and it may arrive after the initial map row is created
from MySupply data.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_vare"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK to Vare via IDENTITY using canonical natural key CleanCode_Vare",
)
def map_vare(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Vare combinations and update vnr from unified staging."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Vare map update from unified stg.dim_vare...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_vare]")
            stg_count = cursor.fetchone()[0]
            context.log.info(f"Unified stg.dim_vare has {stg_count} rows")

            cursor.execute(
                """
            INSERT INTO [map].[Vare] (
                bk_Kreditor,
                bk_LeverandoerVarenummer,
                CleanCode_Vare,
                vnr,
                DW_Timestamp_Insert
            )
            SELECT
                v.bk_Kreditor,
                v.bk_LeverandoerVarenummer,
                v.CleanCode_Vare,
                v.vnr,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_vare] v
            WHERE NOT EXISTS (
                SELECT 1
                FROM [map].[Vare] m
                WHERE m.CleanCode_Vare = v.CleanCode_Vare
            );
        """
            )

            new_count = cursor.rowcount
            context.log.info(f"Inserted {new_count} new Vare entries")

            # This implementation deviates from the general logic in mapUpd_* assets.
            # We update the vnr column directly on the map.Vare table, rather than using a MERGE statement in dimUpd_vare.py.
            # This is because the vnr column is not a natural key, and we need to update it directly when it
            # is updated from ILS staging table.

            cursor.execute(
                """
            UPDATE m
            SET m.vnr = v.vnr,
                m.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_vare] v
            INNER JOIN [map].[Vare] m
                ON m.CleanCode_Vare = v.CleanCode_Vare
            WHERE v.vnr IS NOT NULL
              AND (m.vnr IS NULL OR m.vnr <> v.vnr);
        """
            )

            vnr_updated = cursor.rowcount
            context.log.info(f"Updated vnr on {vnr_updated} existing Vare entries")

            if new_count > 0:
                cursor.execute(
                    """
                SELECT TOP 5 bk_Kreditor, bk_LeverandoerVarenummer, vnr
                FROM map.Vare
                ORDER BY DW_Timestamp_Insert DESC
            """
                )
                recent_inserts = cursor.fetchall()
                context.log.info(
                    f"Sample new entries: {[(r[0], r[1], r[2]) for r in recent_inserts]}"
                )

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM map.Vare")
            total_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(vnr) FROM map.Vare WHERE vnr IS NOT NULL")
            vnr_count = cursor.fetchone()[0]

            context.log.info(
                f"Map update complete: {new_count} new, {vnr_updated} vnr updated, "
                f"{total_count} total entries ({vnr_count} with vnr)"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "vnr_updated": vnr_updated,
            "total_rows": total_count,
            "rows_with_vnr": vnr_count,
            "table": "map.Vare",
            "natural_key": "CleanCode_Vare",
            "source_table": "stg.dim_vare",
        }
    )
