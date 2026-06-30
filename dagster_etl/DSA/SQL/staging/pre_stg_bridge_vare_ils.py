# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Bridge Vare from ILS (Pre-staging Layer)

Consolidates all ILS bridge tables (laftdstam, lgrvare, indkfak, laftd, pmdm)
into a single (CVR_CleanCode, Leverandoer_Varenummer_CleanCode) mapping table.
Foundation for stg_dim_vare unified staging.
"""

from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    RetryPolicy,
    asset,
)


@asset(
    key=AssetKey(["erp", "pre_stg_bridge_vare_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["ext_laftdstam_ils_daily"]),
        AssetKey(["ext_laftd_ils_daily"]),
        AssetKey(["ext_lgrvare_ils_daily"]),
        AssetKey(["ext_indkfak_ils_daily"]),
        AssetKey(["ext_pmdm_ils_daily"]),
        # Monthly partitioned ILS assets
        AssetKey(["ext_laftdstam_ils_monthly"]),
        AssetKey(["ext_laftd_ils_monthly"]),
        AssetKey(["ext_lgrvare_ils_monthly"]),
        AssetKey(["ext_indkfak_ils_monthly"]),
        AssetKey(["ext_pmdm_ils_monthly"]),
        # Full-load ILS assets
        AssetKey(["ext_lev_ils"]),
        AssetKey(["ext_vare_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "ils",
        "entity": "bridge_vare",
    },
    description="Consolidated ILS bridge table for vare mapping. UNIONs laftdstam, lgrvare, indkfak, laftd, pmdm with CVR resolution via deterministic lev current-state mapping.",
)
def pre_stg_bridge_vare_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Consolidated bridge mapping from all ILS sources.

    UNIONs bridge tables with priority-based deduplication:
    1. laftdstam (agreement catalog - highest authority)
    2. lgrvare (warehouse catalog - broadest supplier coverage)
    3. indkfak (historical invoices - fresh data, major suppliers)
    4. laftd (agreement details)
    5. pmdm (product master data)

    JOINs a deterministic lev_current dataset for CVR_CleanCode resolution
    and vare_ils for product attributes.

    Deduplication policy:
    1. Prefer rows with required relationship completeness (lnr, vnr, Vare_CleanCode)
    2. Then apply bridge priority (1..5)
    3. Then latest DW_LoadTimestamp
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating consolidated bridge table stg.pre_bridge_vare_ils...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[pre_bridge_vare_ils];")

            cursor.execute(
                """
                ;WITH lev_current AS (
                    SELECT
                        lnr,
                        CVR_CleanCode
                    FROM (
                        SELECT
                            lev.lnr,
                            CASE
                                WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
                                THEN 'CVR_' + CASE WHEN REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') LIKE 'DK[0-9]%' THEN SUBSTRING(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''), 3, LEN(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''))) ELSE REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') END
                                ELSE NULL
                            END AS CVR_CleanCode,
                            ROW_NUMBER() OVER (
                                PARTITION BY lev.lnr
                                ORDER BY
                                    CASE WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> '' THEN 0 ELSE 1 END,
                                    COALESCE(lev.pro2modified, lev.pro2created, lev.DW_LoadTimestamp) DESC,
                                    lev.DW_LoadTimestamp DESC
                            ) AS rn
                        FROM [ext].[lev_ils] lev
                        WHERE lev.lnr IS NOT NULL
                    ) ranked
                    WHERE rn = 1
                      AND CVR_CleanCode IS NOT NULL
                      AND CVR_CleanCode <> ''
                ),
                all_bridges AS (
                    -- Priority 1: laftdstam (agreement catalog)
                    SELECT
                        lev.CVR_CleanCode,
                        CASE
                            WHEN s.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(s.[lvnr##1])) <> ''
                            THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(s.[lvnr##1], ' ', ''))))
                            ELSE NULL
                        END AS Leverandoer_Varenummer_CleanCode,
                        s.vnr,
                        CONCAT('VARE_', LTRIM(RTRIM(s.vnr))) AS Vare_CleanCode,
                        s.lnr,
                        'laftdstam' AS bridge_source,
                        1 AS bridge_priority,
                        s.DW_LoadTimestamp
                    FROM [ext].[laftdstam_ils] s
                    INNER JOIN lev_current lev ON lev.lnr = s.lnr
                    WHERE s.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(s.[lvnr##1])) <> ''

                    UNION ALL

                    -- Priority 2: lgrvare (warehouse catalog)
                    SELECT
                        lev.CVR_CleanCode,
                        CASE
                            WHEN lg.lvnr IS NOT NULL AND LTRIM(RTRIM(lg.lvnr)) <> ''
                            THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(lg.lvnr, ' ', ''))))
                            ELSE NULL
                        END AS Leverandoer_Varenummer_CleanCode,
                        lg.vnr,
                        CASE
                            WHEN lg.vnr IS NOT NULL AND LTRIM(RTRIM(CAST(lg.vnr AS NVARCHAR(100)))) <> ''
                            THEN 'VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(lg.vnr AS NVARCHAR(100)), ' ', ''))))
                            ELSE NULL
                        END AS Vare_CleanCode,
                        lg.lnr,
                        'lgrvare' AS bridge_source,
                        2 AS bridge_priority,
                        lg.DW_LoadTimestamp
                    FROM [ext].[lgrvare_ils] lg
                    INNER JOIN lev_current lev ON lev.lnr = lg.lnr
                    WHERE lg.lvnr IS NOT NULL AND LTRIM(RTRIM(lg.lvnr)) <> ''

                    UNION ALL

                    -- Priority 3: indkfak (historical invoices)
                    SELECT
                        lev.CVR_CleanCode,
                        CASE
                            WHEN ik.levvarenr IS NOT NULL AND LTRIM(RTRIM(ik.levvarenr)) <> ''
                            THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(ik.levvarenr, ' ', ''))))
                            ELSE NULL
                        END AS Leverandoer_Varenummer_CleanCode,
                        ik.vnr,
                        CASE
                            WHEN ik.vnr IS NOT NULL AND LTRIM(RTRIM(CAST(ik.vnr AS NVARCHAR(100)))) <> ''
                            THEN 'VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(ik.vnr AS NVARCHAR(100)), ' ', ''))))
                            ELSE NULL
                        END AS Vare_CleanCode,
                        ik.lnr,
                        'indkfak' AS bridge_source,
                        3 AS bridge_priority,
                        ik.DW_LoadTimestamp
                    FROM [ext].[indkfak_ils] ik
                    INNER JOIN lev_current lev ON lev.lnr = ik.lnr
                    WHERE ik.levvarenr IS NOT NULL AND LTRIM(RTRIM(ik.levvarenr)) <> ''
                      AND ik.levvarenr <> '-'
                      AND LEN(LTRIM(RTRIM(ik.levvarenr))) >= 3
                      AND ik.levvarenr LIKE '%[a-zA-Z0-9]%'
                      AND ik.vnr IS NOT NULL

                    UNION ALL

                    -- Priority 4: laftd (agreement details)
                    SELECT
                        lev.CVR_CleanCode,
                        -- Strip internal spaces to match the other LEVVARE branches
                        -- (lines ~112/132/156/200). Without REPLACE this laftd branch
                        -- produced 'LEVVARE_WL 0B 8T..' vs 'LEVVARE_WL0B8TA4..' elsewhere,
                        -- splitting map.Vare into two surrogates for the same item.
                        CONCAT('LEVVARE_', UPPER(LTRIM(RTRIM(REPLACE(ld.[lvnr##1], ' ', ''))))) AS Leverandoer_Varenummer_CleanCode,
                        ld.vnr,
                        CONCAT('VARE_', LTRIM(RTRIM(ld.vnr))) AS Vare_CleanCode,
                        ld.lnr,
                        'laftd' AS bridge_source,
                        4 AS bridge_priority,
                        ld.DW_LoadTimestamp
                    FROM [ext].[laftd_ils] ld
                    INNER JOIN lev_current lev ON lev.lnr = ld.lnr
                    WHERE ld.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(ld.[lvnr##1])) <> ''

                    UNION ALL

                    -- Priority 5: pmdm (product master data)
                    SELECT
                        lev.CVR_CleanCode,
                        CASE
                            WHEN p.lvnr IS NOT NULL AND LTRIM(RTRIM(p.lvnr)) <> ''
                            THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(p.lvnr, ' ', ''))))
                            ELSE NULL
                        END AS Leverandoer_Varenummer_CleanCode,
                        p.vnr,
                        CASE
                            WHEN p.vnr IS NOT NULL AND LTRIM(RTRIM(CAST(p.vnr AS NVARCHAR(100)))) <> ''
                            THEN 'VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(p.vnr AS NVARCHAR(100)), ' ', ''))))
                            ELSE NULL
                        END AS Vare_CleanCode,
                        p.lnr,
                        'pmdm' AS bridge_source,
                        5 AS bridge_priority,
                        p.DW_LoadTimestamp
                    FROM [ext].[pmdm_ils] p
                    INNER JOIN lev_current lev ON lev.lnr = p.lnr
                    WHERE p.lvnr IS NOT NULL AND LTRIM(RTRIM(p.lvnr)) <> ''
                ),
                ranked_bridges AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CVR_CleanCode, Leverandoer_Varenummer_CleanCode
                            ORDER BY
                                CASE WHEN lnr IS NOT NULL THEN 0 ELSE 1 END,
                                CASE WHEN vnr IS NOT NULL THEN 0 ELSE 1 END,
                                CASE WHEN Vare_CleanCode IS NOT NULL AND Vare_CleanCode <> '' THEN 0 ELSE 1 END,
                                bridge_priority ASC,
                                DW_LoadTimestamp DESC
                        ) AS rn
                    FROM all_bridges
                )
                SELECT
                    bridge.CVR_CleanCode AS CleanCode_Kreditor,
                    bridge.Leverandoer_Varenummer_CleanCode AS CleanCode_LeverandoerVarenummer,
                    bridge.vnr,
                    bridge.Vare_CleanCode AS CleanCode_Vare,
                    bridge.bridge_source,
                    bridge.bridge_priority,
                    bridge.lnr,
                    vare.navn AS vare_navn,
                    vare.faelvnr,
                    vare.ean_vnr,
                    vare.basisenh,
                    vare.vareart,
                    vare.varegrp,
                    vare.varetype,
                    vare.varestr,
                    vare.navn_conc,
                    bridge.DW_LoadTimestamp
                INTO [stg].[pre_bridge_vare_ils]
                FROM ranked_bridges bridge

                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT
                            v.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY v.vnr
                                ORDER BY v.aktiv DESC, v.oprdto DESC
                            ) AS rn
                        FROM [ext].[vare_ils] v
                    ) ranked
                    WHERE rn = 1
                ) vare ON vare.vnr = bridge.vnr

                WHERE bridge.rn = 1
                  AND bridge.Leverandoer_Varenummer_CleanCode IS NOT NULL
                  AND bridge.Leverandoer_Varenummer_CleanCode <> '';
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.pre_bridge_vare_ils with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_pre_bridge_vare_ils_PK
                ON [stg].[pre_bridge_vare_ils](CleanCode_Kreditor, CleanCode_LeverandoerVarenummer);
            """
            )

            conn.commit()

            cursor.execute(
                """
                SELECT
                    bridge_source,
                    COUNT(*) AS cnt
                FROM [stg].[pre_bridge_vare_ils]
                GROUP BY bridge_source
                ORDER BY COUNT(*) DESC
            """
            )
            breakdown = {row[0]: row[1] for row in cursor.fetchall()}
            context.log.info(f"Bridge source breakdown: {breakdown}")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.pre_bridge_vare_ils",
            "row_count": row_count,
            "source_tables": "laftdstam_ils + lgrvare_ils + indkfak_ils + laftd_ils + pmdm_ils + lev_ils + vare_ils",
            "grain": "CVR_CleanCode + Leverandoer_Varenummer_CleanCode (deduplicated by completeness + bridge priority)",
            "dedup_policy": "completeness-first (lnr/vnr/Vare_CleanCode), then bridge_priority, then DW_LoadTimestamp",
            "bridge_breakdown": breakdown,
            "layer": "stg_fct (Layer 0)",
        }
    )
