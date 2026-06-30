# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vareudlevering from ILS (Fact Layer)

Select columns needed for fct_vareudlevering from pre_stg_vareudlevering_ils.
Intermediate staging table for the fact table load.

Grain: one row per delivery line, identified by `prrowid` from dbo.udlev.

All type conversions (date keys, measures, combined datetimes) are performed
in this layer. Vare relationship resolution (CVR + LEVVARE/vnr) is also
pre-resolved here so the downstream fact asset only needs simple
CleanCode -> DW_EK lookups via map tables.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_fct_vareudlevering_ils"]),
    group_name="staging_vareudlevering",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_vareudlevering_ils"]),
        AssetKey(["arc_ils_lev"]),
        "map_vare",
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "ils",
        "entity": "vareudlevering",
    },
    description=(
        "Select columns needed for fct_vareudlevering from pre_stg_vareudlevering_ils "
        "with precomputed date keys and pre-resolved Resolved_Vare_CleanCode"
    ),
)
def stg_fct_vareudlevering_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Staging for ILS vareudlevering fact table.

    Performs all type conversions (date keys, measures) and pre-resolves
    Vare relationship to Resolved_Vare_CleanCode so the fact layer
    stays focused on simple CleanCode -> DW_EK map lookups + MERGE.

    Vare resolution strategy (two paths, COALESCE priority):
      1. Primary: lnr -> arc.ils_lev.se_nr -> CVR_CleanCode + LEVVARE_{lvnr_1}
         -> constructs CleanCode_Vare matching map.Vare PK format
      2. Fallback: CVR_CleanCode + vnr -> MIN(CleanCode_Vare) from map.Vare
         grouped by (bk_Kreditor, vnr) to guarantee uniqueness
    """

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "fct_vareudlevering_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0
    stats = (0, 0, 0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:

                context.log.info("Creating staging table stg.fct_vareudlevering_ils...")

                cursor.execute("DROP TABLE IF EXISTS [stg].[fct_vareudlevering_ils];")

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
                                        CASE
                                            WHEN lev.se_nr IS NOT NULL
                                             AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> '' THEN 0
                                            ELSE 1
                                        END,
                                        COALESCE(
                                            lev.pro2modified,
                                            lev.pro2created,
                                            lev.DW_Timestamp_Update
                                        ) DESC,
                                        lev.DW_Timestamp_Update DESC
                                ) AS rn
                            FROM [arc].[ils_lev] lev
                            WHERE lev.lnr IS NOT NULL
                        ) ranked
                        WHERE rn = 1
                          AND CVR_CleanCode IS NOT NULL
                          AND CVR_CleanCode <> ''
                    ),
                    vare_vnr_fallback AS (
                        SELECT
                            mv.bk_Kreditor,
                            UPPER(LTRIM(RTRIM(mv.vnr))) AS vnr_normalized,
                            MIN(mv.CleanCode_Vare) AS CleanCode_Vare
                        FROM [map].[Vare] mv
                        WHERE mv.vnr IS NOT NULL
                          AND LTRIM(RTRIM(mv.vnr)) <> ''
                        GROUP BY
                            mv.bk_Kreditor,
                            UPPER(LTRIM(RTRIM(mv.vnr)))
                    )
                    SELECT
                        -- CleanCodes for map lookups
                        src.CleanCode_Vareudlevering,
                        -- Business key
                        CAST(src.prrowid AS NVARCHAR(100)) AS bk_prrowid,
                        src.Lager_CleanCode,
                        src.Rekvirent_CleanCode,
                        src.Vare_CleanCode,

                        -- IlsVare CleanCode for map.IlsVare lookup
                        CASE WHEN src.vnr IS NOT NULL AND LTRIM(RTRIM(CAST(src.vnr AS NVARCHAR(100)))) <> ''
                            THEN CONCAT('ILSVARE_', UPPER(LTRIM(RTRIM(CAST(src.vnr AS NVARCHAR(100))))))
                            ELSE NULL
                        END AS IlsVare_CleanCode,

                        -- Pre-resolved Vare CleanCode matching map.Vare PK format
                        COALESCE(
                            CASE
                                WHEN lev.CVR_CleanCode IS NOT NULL
                                 AND src.lvnr_1 IS NOT NULL
                                 AND LTRIM(RTRIM(src.lvnr_1)) <> ''
                                THEN lev.CVR_CleanCode + '_LEVVARE_'
                                     + UPPER(LTRIM(RTRIM(src.lvnr_1)))
                            END,
                            vf.CleanCode_Vare
                        ) AS Resolved_Vare_CleanCode,

                        -- Measures (typed)
                        CAST(src.udlant AS DECIMAL(18,4)) AS Vareudlevering_Antal,
                        CAST(src.udlbel AS DECIMAL(19,4)) AS Vareudlevering_Beloeb,
                        CAST(src.indkbel AS DECIMAL(19,4)) AS Indkoeb_Beloeb,
                        CAST(src.rabatbel AS DECIMAL(19,4)) AS Rabat_Beloeb,
                        CAST(src.lagerantal AS DECIMAL(18,4)) AS Lager_Antal,

                        -- Date key conversions (4 dates, precomputed for fact layer)
                        COALESCE(
                            dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.udldto)), -1
                        ) AS DW_EK_Dato_Vareudlevering,
                        COALESCE(
                            dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.udlobdto)), -1
                        ) AS DW_EK_Dato_Udloeb,
                        COALESCE(
                            dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.postdto)), -1
                        ) AS DW_EK_Dato_Postering,
                        COALESCE(
                            dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.aflevdto)), -1
                        ) AS DW_EK_Dato_Aflevering,

                        -- Combined datetimes (date + time-in-seconds-from-midnight)
                        DATEADD(
                            SECOND, ISNULL(src.udlklk, 0), CAST(src.udldto AS DATETIME2)
                        ) AS Datotid_Vareudlevering,
                        CAST(src.udlobdto AS DATETIME2) AS Datotid_Udloeb,
                        CAST(src.postdto AS DATETIME2) AS Datotid_Postering,
                        DATEADD(
                            SECOND, ISNULL(src.aflevklk, 0), CAST(src.aflevdto AS DATETIME2)
                        ) AS Datotid_Aflevering,

                        DATEADD(
                            SECOND, ISNULL(src.udlklk, 0), CAST(src.udldto AS DATETIME2)
                        ) AS DW_haendelsestidspunkt

                    INTO [stg].[fct_vareudlevering_ils]
                    FROM [stg].[pre_vareudlevering_ils] src

                    LEFT JOIN lev_current lev
                        ON lev.lnr = src.lnr

                    LEFT JOIN vare_vnr_fallback vf
                        ON vf.bk_Kreditor = lev.CVR_CleanCode
                       AND src.vnr IS NOT NULL
                       AND LTRIM(RTRIM(src.vnr)) <> ''
                       AND vf.vnr_normalized = UPPER(LTRIM(RTRIM(src.vnr)));
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.fct_vareudlevering_ils with {row_count} rows")

                # Create clustered index on MERGE key
                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_fct_vareudlevering_ils_PK
                    ON [stg].[fct_vareudlevering_ils](CleanCode_Vareudlevering);
                """
                )

                # Create non-clustered indexes for map lookups
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_Lager
                    ON [stg].[fct_vareudlevering_ils](Lager_CleanCode);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_Vare
                    ON [stg].[fct_vareudlevering_ils](Resolved_Vare_CleanCode);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_Rekvirent
                    ON [stg].[fct_vareudlevering_ils](Rekvirent_CleanCode);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_IlsVare
                    ON [stg].[fct_vareudlevering_ils](IlsVare_CleanCode);
                """
                )

                context.log.info("Created indexes for map lookups")

                conn.commit()

                # Diagnostic
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(CleanCode_Vareudlevering) AS has_vareudlev_clean,
                        COUNT(Lager_CleanCode) AS has_lager_clean,
                        COUNT(Rekvirent_CleanCode) AS has_rekvirent_clean,
                        COUNT(Resolved_Vare_CleanCode) AS has_resolved_vare_clean
                    FROM [stg].[fct_vareudlevering_ils]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Vareudlevering, "
                    f"{stats[2]}/{stats[0]} with Lager_CleanCode, "
                    f"{stats[3]}/{stats[0]} with Rekvirent_CleanCode, "
                    f"{stats[4]}/{stats[0]} with Resolved_Vare_CleanCode"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.fct_vareudlevering_ils",
            "row_count": row_count,
            "source_table": "stg.pre_vareudlevering_ils",
            "grain": "prrowid (unique delivery line)",
            "has_vareudlev_clean": stats[1],
            "has_lager_clean": stats[2],
            "has_rekvirent_clean": stats[3],
            "has_resolved_vare_clean": stats[4],
            "mode": config.mode,
            "layer": "stg_fct (Layer 1)",
        }
    )
