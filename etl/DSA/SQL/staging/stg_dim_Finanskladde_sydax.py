# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladde from SydAX (Dimension Layer)

Combines two archive-based sources to produce dim.Finanskladde input:
- stg.pre_Finanskladdelinje_sydax: header columns denormalized at line grain
  (finanskladder that have lines, already enriched by Task 1's pre_stg asset).
- arc.sydax_finanskladde: full header archive (catches finanskladder that have
  no associated lines yet / orphan headers).  Deduplicated to the current version
  per FinanskladdeId using ROW_NUMBER on EndDateTime DESC, then
  StartDateTime/dw_timestamp_update tiebreakers (matching the legacy enrichment's
  EndDateTime-based version selection; the arc tables carry NO dw_is_deleted
  column and EndDateTime is NOT NULL with a far-future sentinel on open rows).

Deduplicate by clean_code_Finanskladde, keeping the most recent dw_timestamp_update.

Grain: ref_LedgerjournaltableId via clean_code_Finanskladde
Business key: bk_ref_ledgerjournaltable_id
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_Finanskladde_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Finanskladdelinje_sydax"]),
        "arc_sydax_finanskladde",
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "finanskladde",
    },
    description=(
        "Combine Finanskladde header from stg.pre_Finanskladdelinje_sydax (in sync "
        "with dim.Finanskladdelinje + fct.Finanskladdelinje) and arc.sydax_finanskladde "
        "(latest version per FinanskladdeId, catches orphan headers without lines), "
        "deduplicated by clean_code_Finanskladde."
    ),
)
def stg_dim_Finanskladde_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Filter + deduplicate SydAX finanskladde dimension data into stg.dim_Finanskladde_sydax."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(
                "Creating stg.dim_Finanskladde_sydax from stg.pre_Finanskladdelinje_sydax "
                "UNION ALL arc.sydax_finanskladde (deduplicated by clean_code_Finanskladde)..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Finanskladde_sydax];")

            cursor.execute(
                """
                WITH from_lines AS (
                    -- Finanskladder that have lines (header columns already enriched
                    -- by pre_stg_Finanskladdelinje_sydax from arc.sydax_finanskladde).
                    SELECT
                        [bk_ref_ledgerjournaltable_id],
                        [clean_code_Finanskladde],
                        [kladdenummer],
                        [kladdetekst],
                        [kladdenavn],
                        [er_lukket],
                        [data_area_id],
                        [Maskinnummer_Kladde]  AS maskinnummer,
                        [Kladde_CreatedOn]     AS CreatedOn,
                        [Kladde_Modified]      AS Modified,
                        [dw_timestamp_update]
                    FROM [stg].[pre_Finanskladdelinje_sydax]
                    WHERE [bk_ref_ledgerjournaltable_id] IS NOT NULL
                ),
                from_header AS (
                    -- All finanskladde headers from the archive, incl. orphans.
                    -- Arc tables have NO dw_is_deleted — dedup to the current version
                    -- per FinanskladdeId using EndDateTime DESC (legacy enrichment's
                    -- version selector), with StartDateTime / dw_timestamp_update
                    -- as deterministic tiebreakers.
                    SELECT
                        [ref_LedgerjournaltableId] AS bk_ref_ledgerjournaltable_id,
                        'FINANSKLADDE_' + LTRIM(RTRIM(
                            CAST([ref_LedgerjournaltableId] AS NVARCHAR(100))
                        )) AS clean_code_Finanskladde,
                        [Kladdenummer] AS kladdenummer,
                        [Kladdetekst] AS kladdetekst,
                        [Kladdenavn] AS kladdenavn,
                        [ErLukket] AS er_lukket,
                        [DataAreaId] AS data_area_id,
                        [Maskinnummer] AS maskinnummer,
                        [CreatedOn],
                        [Modified],
                        [dw_timestamp_update]
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY [FinanskladdeId]
                                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                               ) AS _rn
                        FROM [arc].[sydax_finanskladde]
                        WHERE [Kildesystem] = 'SydAX'
                          AND [ref_LedgerjournaltableId] IS NOT NULL
                    ) arc_dedup
                    WHERE _rn = 1
                ),
                combined AS (
                    SELECT * FROM from_lines
                    UNION ALL
                    SELECT * FROM from_header
                )
                SELECT
                    clean_code_Finanskladde,
                    bk_ref_ledgerjournaltable_id,
                    kladdenummer,
                    kladdetekst,
                    kladdenavn,
                    er_lukket,
                    data_area_id,
                    maskinnummer,
                    CreatedOn,
                    Modified
                INTO [stg].[dim_Finanskladde_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY clean_code_Finanskladde
                            ORDER BY dw_timestamp_update DESC
                        ) AS rn
                    FROM combined
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_Finanskladde_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_finanskladde_sydax_CleanCode
                ON [stg].[dim_Finanskladde_sydax](clean_code_Finanskladde);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(kladdenavn) AS has_kladdenavn,
                    COUNT(kladdetekst) AS has_kladdetekst
                FROM [stg].[dim_Finanskladde_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with kladdenavn, "
                f"{stats[2]}/{stats[0]} with kladdetekst"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Finanskladde_sydax",
            "target_table": "stg.dim_Finanskladde_sydax",
            "row_count": row_count,
            "source_tables": "stg.pre_Finanskladdelinje_sydax + arc.sydax_finanskladde",
            "deduplication": "ROW_NUMBER() on clean_code_Finanskladde, ORDER BY dw_timestamp_update DESC",
            "grain": "ref_LedgerjournaltableId via clean_code_Finanskladde",
            "has_kladdenavn": stats[1],
            "has_kladdetekst": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
