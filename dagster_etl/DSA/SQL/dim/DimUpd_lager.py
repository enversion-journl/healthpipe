# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Lager

Populate dim.Lager from stg.dim_lager with resolved surrogate keys.
Logic: Type 1 SCD - overwrite all attributes on each run.

Resolves:
- DW_EK_Lager from map.Lager
- DW_EK_Lager_Parent from map.Lager (self-join on ParentCode)
- DW_EK_Organisation from ref.Organisation (via CodeSOR -> sor_code)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_lager"]),
        AssetKey(["stg_dim_lager"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description="Populate dim.Lager (Type 1 SCD) with resolved DW_EK keys",
)
def dim_lager(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: resolve EKs and MERGE into dim.Lager."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Building temp table with resolved EKs...")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_lager] stg
            LEFT JOIN [map].[Lager] m
                ON m.CleanCode_Lager = stg.CleanCode_Lager
            WHERE m.DW_EK_Lager IS NULL
        """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                raise ValueError(
                    f"dim_lager: {unmapped_rows} staging rows have no match in map.Lager. "
                    "Ensure map_lager runs before dim_lager and staging CleanCodes are valid."
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Lager_Resolved;

            SELECT
                m.DW_EK_Lager,
                stg.Code,
                stg.Display,
                stg.ParentCode,
                stg.[Level],
                stg.EntityType,
                stg.SGH,
                stg.LGR,
                stg.Adr,
                stg.Land,
                stg.Postdi,
                stg.Tlf,
                stg.CodeSOR,
                COALESCE(m_parent.DW_EK_Lager, -1) AS DW_EK_Lager_Parent,
                COALESCE(m_org.DW_EK_Organisation, -1) AS DW_EK_Organisation
            INTO #Lager_Resolved
            FROM [stg].[dim_lager] stg
            LEFT JOIN [map].[Lager] m
                ON m.CleanCode_Lager = stg.CleanCode_Lager
            LEFT JOIN [map].[Lager] m_parent
                ON m_parent.CleanCode_Lager = stg.ParentCode
            LEFT JOIN [ref].[Organisation] m_org
                ON m_org.sor_code = stg.CodeSOR
                AND m_org.DW_IsDeleted = 0
                AND stg.CodeSOR IS NOT NULL
        """
            )

            cursor.execute("SELECT COUNT(*) FROM #Lager_Resolved")
            resolved_count = cursor.fetchone()[0]
            context.log.info(f"Resolved {resolved_count} rows for dim.Lager")

            cursor.execute(
                """
            MERGE INTO [dim].[Lager] AS target
            USING #Lager_Resolved AS source
            ON target.DW_EK_Lager = source.DW_EK_Lager
            WHEN MATCHED THEN
                UPDATE SET
                    Code = source.Code,
                    Display = source.Display,
                    ParentCode = source.ParentCode,
                    [Level] = source.[Level],
                    EntityType = source.EntityType,
                    SGH = source.SGH,
                    LGR = source.LGR,
                    Adr = source.Adr,
                    Land = source.Land,
                    Postdi = source.Postdi,
                    Tlf = source.Tlf,
                    CodeSOR = source.CodeSOR,
                    DW_EK_Organisation = source.DW_EK_Organisation,
                    DW_EK_Lager_Parent = source.DW_EK_Lager_Parent,
                    DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Lager, Code, Display, ParentCode, [Level],
                    EntityType, SGH, LGR, Adr, Land, Postdi, Tlf,
                    CodeSOR, DW_EK_Organisation, DW_EK_Lager_Parent,
                    DW_Timestamp_Insert, DW_Timestamp_Update
                )
                VALUES (
                    source.DW_EK_Lager, source.Code, source.Display,
                    source.ParentCode, source.[Level],
                    source.EntityType, source.SGH, source.LGR,
                    source.Adr, source.Land, source.Postdi, source.Tlf,
                    source.CodeSOR,
                    source.DW_EK_Organisation, source.DW_EK_Lager_Parent,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                )
            OUTPUT $action;
        """
            )

            merge_results = cursor.fetchall()
            inserted = sum(1 for r in merge_results if r[0] == "INSERT")
            updated = sum(1 for r in merge_results if r[0] == "UPDATE")

            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Lager_Resolved")

            cursor.execute("UPDATE STATISTICS [dim].[Lager]")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN DW_EK_Organisation <> -1 THEN 1 ELSE 0 END) AS has_org,
                SUM(CASE WHEN DW_EK_Lager_Parent <> -1 THEN 1 ELSE 0 END) AS has_parent
            FROM [dim].[Lager]
            WHERE DW_EK_Lager <> -1
        """
            )
            stats = cursor.fetchone()
            total = stats[0]
            has_org = stats[1]
            has_parent = stats[2]
        finally:
            cursor.close()

    context.log.info(
        f"dim.Lager: {inserted} inserted, {updated} updated, {total} total "
        f"({has_org} with Organization FK, {has_parent} with Parent FK)"
    )

    return MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": total,
            "has_organization_fk": has_org,
            "has_parent_fk": has_parent,
            "table": "dim.Lager",
            "scd_type": "Type 1",
        }
    )
