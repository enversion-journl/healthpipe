# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladde from SydAX (Dimension Layer)

Combines two sources to produce dim.Finanskladde input:
- ext.finanskladdelinje_enriched: Finanskladde header columns denormalized at line
  grain (ensures consistency with dim.Finanskladdelinje and fct.Finanskladdelinje
  which flow from the same enriched extraction).
- ext.finanskladde_sydax: 1:1 source extract (catches Finanskladder that have no
  associated lines yet, which would otherwise be missing from the enriched output).

Deduplicate by CleanCode_Finanskladde, keeping the most recent DW_LoadTimestamp.

Grain: ref_LedgerjournaltableId via CleanCode_Finanskladde
Business key: bk_ref_LedgerjournaltableId
Partition columns (upstream): CreatedOn, Modified
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_finanskladde_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["ext_finanskladdelinje_enriched_sydax_daily"]),
        AssetKey(["ext_finanskladdelinje_enriched_sydax_monthly"]),
        AssetKey(["ext_finanskladdelinje_enriched_sydax_full_load"]),
        AssetKey(["ext_finanskladde_sydax_daily"]),
        AssetKey(["ext_finanskladde_sydax_monthly"]),
        AssetKey(["ext_finanskladde_sydax_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "finanskladde",
    },
    description=(
        "Combine Finanskladde header from ext.finanskladdelinje_enriched (in sync "
        "with dim.Finanskladdelinje + fct.Finanskladdelinje) and ext.finanskladde_sydax "
        "(catches orphan Finanskladder without lines), deduplicated by CleanCode_Finanskladde."
    ),
)
def stg_dim_finanskladde_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Filter + deduplicate SydAX finanskladde dimension data into stg.dim_finanskladde_sydax."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(
                "Creating stg.dim_finanskladde_sydax from ext.finanskladdelinje_enriched "
                "UNION ext.finanskladde_sydax (deduplicated by CleanCode_Finanskladde)..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_finanskladde_sydax];")

            cursor.execute(
                """
                WITH from_enriched AS (
                    SELECT
                        [ref_LedgerjournaltableId] AS bk_ref_LedgerjournaltableId,
                        'FINANSKLADDE_' + LTRIM(RTRIM(
                            CAST([ref_LedgerjournaltableId] AS NVARCHAR(100))
                        )) AS CleanCode_Finanskladde,
                        [Kladdenummer],
                        [Kladdetekst],
                        [Kladdenavn],
                        [ErLukket],
                        [DataAreaId],
                        [Maskinnummer_Kladde] AS Maskinnummer,
                        [Kladde_CreatedOn]    AS CreatedOn,
                        [Kladde_Modified]     AS Modified,
                        [DW_LoadTimestamp]
                    FROM [ext].[finanskladdelinje_enriched]
                    WHERE [ref_LedgerjournaltableId] IS NOT NULL
                ),
                from_source AS (
                    SELECT
                        [ref_LedgerjournaltableId] AS bk_ref_LedgerjournaltableId,
                        'FINANSKLADDE_' + LTRIM(RTRIM(
                            CAST([ref_LedgerjournaltableId] AS NVARCHAR(100))
                        )) AS CleanCode_Finanskladde,
                        [Kladdenummer],
                        [Kladdetekst],
                        [Kladdenavn],
                        [ErLukket],
                        [DataAreaId],
                        [Maskinnummer],
                        [CreatedOn],
                        [Modified],
                        [DW_LoadTimestamp]
                    FROM [ext].[finanskladde_sydax]
                    WHERE [Kildesystem] = 'SydAX'
                      AND [ref_LedgerjournaltableId] IS NOT NULL
                ),
                combined AS (
                    SELECT * FROM from_enriched
                    UNION ALL
                    SELECT * FROM from_source
                )
                SELECT
                    CleanCode_Finanskladde,
                    bk_ref_LedgerjournaltableId,
                    Kladdenummer,
                    Kladdetekst,
                    Kladdenavn,
                    ErLukket,
                    DataAreaId,
                    Maskinnummer,
                    CreatedOn,
                    Modified
                INTO [stg].[dim_finanskladde_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Finanskladde
                            ORDER BY DW_LoadTimestamp DESC
                        ) AS rn
                    FROM combined
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_finanskladde_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_finanskladde_sydax_CleanCode
                ON [stg].[dim_finanskladde_sydax](CleanCode_Finanskladde);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Kladdenavn) AS has_kladdenavn,
                    COUNT(Kladdetekst) AS has_kladdetekst
                FROM [stg].[dim_finanskladde_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with Kladdenavn, "
                f"{stats[2]}/{stats[0]} with Kladdetekst"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_finanskladde_sydax",
            "target_table": "stg.dim_finanskladde_sydax",
            "row_count": row_count,
            "source_tables": "ext.finanskladdelinje_enriched + ext.finanskladde_sydax",
            "deduplication": "ROW_NUMBER() on CleanCode_Finanskladde, ORDER BY DW_LoadTimestamp DESC",
            "grain": "ref_LedgerjournaltableId via CleanCode_Finanskladde",
            "has_kladdenavn": stats[1],
            "has_kladdetekst": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
