# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Rekvirent

Populate dim.Rekvirent from stg.dim_rekvirent with resolved surrogate keys.
Logic: Type 1 SCD - overwrite all attributes on each run.

Resolves:
- DW_EK_Rekvirent from map.Rekvirent
- DW_EK_Rekvirent_Parent from map.Rekvirent (self-join on ParentCode)
- DW_EK_Organisation from ref.Organisation (via CodeSOR -> sor_code)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_rekvirent"]),
        AssetKey(["stg_dim_rekvirent"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "module": "rekvirent", "scd": "type1"},
    description="Populate dim.Rekvirent (Type 1 SCD) with resolved DW_EK keys",
)
def dim_rekvirent(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: resolve EKs and MERGE into dim.Rekvirent."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Building temp table with resolved EKs...")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_rekvirent] stg
            LEFT JOIN [map].[Rekvirent] m
                ON m.CleanCode_Rekvirent = stg.CleanCode_Rekvirent
            WHERE m.DW_EK_Rekvirent IS NULL
        """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                raise ValueError(
                    f"dim_rekvirent: {unmapped_rows} staging rows have no match in map.Rekvirent. "
                    "Ensure map_rekvirent runs before dim_rekvirent and staging CleanCodes are valid."
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Rekvirent_Resolved;

            SELECT
                m.DW_EK_Rekvirent,
                stg.Code,
                stg.Display,
                stg.ParentCode,
                stg.[Level],
                stg.EntityType,
                stg.SGH,
                stg.RGRP,
                stg.RTYP,
                stg.RNR,
                stg.CodeSOR,
                stg.SORRelationship,
                COALESCE(m_parent.DW_EK_Rekvirent, -1) AS DW_EK_Rekvirent_Parent,
                COALESCE(m_org.DW_EK_Organisation, -1) AS DW_EK_Organisation
            INTO #Rekvirent_Resolved
            FROM [stg].[dim_rekvirent] stg
            LEFT JOIN [map].[Rekvirent] m
                ON m.CleanCode_Rekvirent = stg.CleanCode_Rekvirent
            LEFT JOIN [map].[Rekvirent] m_parent
                ON m_parent.CleanCode_Rekvirent = stg.ParentCode
            LEFT JOIN [ref].[Organisation] m_org
                ON m_org.sor_code = stg.CodeSOR
                AND m_org.DW_IsDeleted = 0
                AND stg.CodeSOR IS NOT NULL
        """
            )

            cursor.execute("SELECT COUNT(*) FROM #Rekvirent_Resolved")
            resolved_count = cursor.fetchone()[0]
            context.log.info(f"Resolved {resolved_count} rows for dim.Rekvirent")

            cursor.execute(
                """
            MERGE INTO [dim].[Rekvirent] AS target
            USING #Rekvirent_Resolved AS source
            ON target.DW_EK_Rekvirent = source.DW_EK_Rekvirent
            WHEN MATCHED THEN
                UPDATE SET
                    Code = source.Code,
                    Display = source.Display,
                    ParentCode = source.ParentCode,
                    [Level] = source.[Level],
                    EntityType = source.EntityType,
                    SGH = source.SGH,
                    RGRP = source.RGRP,
                    RTYP = source.RTYP,
                    RNR = source.RNR,
                    CodeSOR = source.CodeSOR,
                    SORRelationship = source.SORRelationship,
                    DW_EK_Organisation = source.DW_EK_Organisation,
                    DW_EK_Rekvirent_Parent = source.DW_EK_Rekvirent_Parent,
                    DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Rekvirent, Code, Display, ParentCode, [Level],
                    EntityType, SGH, RGRP, RTYP, RNR,
                    CodeSOR, SORRelationship,
                    DW_EK_Organisation, DW_EK_Rekvirent_Parent,
                    DW_Timestamp_Insert, DW_Timestamp_Update
                )
                VALUES (
                    source.DW_EK_Rekvirent, source.Code, source.Display,
                    source.ParentCode, source.[Level],
                    source.EntityType, source.SGH, source.RGRP, source.RTYP, source.RNR,
                    source.CodeSOR, source.SORRelationship,
                    source.DW_EK_Organisation, source.DW_EK_Rekvirent_Parent,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                )
            OUTPUT $action;
        """
            )

            merge_results = cursor.fetchall()
            inserted = sum(1 for r in merge_results if r[0] == "INSERT")
            updated = sum(1 for r in merge_results if r[0] == "UPDATE")

            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Rekvirent_Resolved")

            cursor.execute("UPDATE STATISTICS [dim].[Rekvirent]")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN DW_EK_Organisation <> -1 THEN 1 ELSE 0 END) AS has_org,
                SUM(CASE WHEN DW_EK_Rekvirent_Parent <> -1 THEN 1 ELSE 0 END) AS has_parent
            FROM [dim].[Rekvirent]
            WHERE DW_EK_Rekvirent <> -1
        """
            )
            stats = cursor.fetchone()
            total = stats[0]
            has_org = stats[1]
            has_parent = stats[2]
        finally:
            cursor.close()

    context.log.info(
        f"dim.Rekvirent: {inserted} inserted, {updated} updated, {total} total "
        f"({has_org} with Organization FK, {has_parent} with Parent FK)"
    )

    return MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": total,
            "has_organization_fk": has_org,
            "has_parent_fk": has_parent,
            "table": "dim.Rekvirent",
            "scd_type": "Type 1",
        }
    )
