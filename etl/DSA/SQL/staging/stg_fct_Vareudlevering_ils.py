# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vareudlevering from ILS (Fact Layer)

Select columns needed for fct_Vareudlevering from pre_stg_Vareudlevering_ils.
Intermediate staging table for the fact table load.

Grain: one row per delivery line, identified by `prrowid` from dbo.udlev.

All type conversions (date keys, measures, combined datetimes) are performed
in this layer. The kreditor CVR CleanCode and the constructed LEVVARE Vare
CleanCode are derived here from arc.ils_lev (staging never reads the map layer).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from utils.sql.leverandoervare_ilsvare_sql import sql_lev_current_cte

from etl.DSA.SQL.common.dato_dimension import dato_key, dato_tid


@asset(
    key=AssetKey(["erp", "stg_fct_Vareudlevering_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Vareudlevering_ils"]),
        AssetKey(["arc_ils_lev"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "ils",
        "entity": "vareudlevering",
    },
    description=(
        "Select columns needed for fct_Vareudlevering from pre_stg_Vareudlevering_ils "
        "with precomputed date keys and constructed LEVVARE Vare CleanCode"
    ),
)
def stg_fct_Vareudlevering_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Staging for ILS vareudlevering fact table.

    Performs all type conversions (date keys, measures) and derives the
    Vare-resolution inputs from arc.ils_lev:
      - clean_code_Kreditor: lnr -> arc.ils_lev.se_nr -> CVR CleanCode
      - clean_code_Leverandoervare: clean_code_Kreditor + LEVVARE_{lvnr_1}
        (constructed clean_code_Leverandoervare matching map.Leverandoervare PK format)

    The map lookups themselves live in fct_Vareudlevering — staging never
    reads the map layer.
    """

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "fct_Vareudlevering_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0
    stats = (0, 0, 0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:

                context.log.info("Creating staging table stg.fct_Vareudlevering_ils...")

                cursor.execute("DROP TABLE IF EXISTS [stg].[fct_Vareudlevering_ils];")

                cursor.execute(
                    f"""
                    ;WITH{sql_lev_current_cte()}
                    SELECT
                        -- CleanCodes for map lookups
                        src.clean_code_Vareudlevering,
                        -- Business key
                        CAST(src.prrowid AS NVARCHAR(100)) AS bk_prrowid,
                        src.clean_code_Lager,
                        src.clean_code_Rekvirent,

                        -- ILSVare CleanCode for map.ILSVare lookup
                        CASE WHEN src.vnr IS NOT NULL AND LTRIM(RTRIM(CAST(src.vnr AS NVARCHAR(100)))) <> ''
                            THEN CONCAT('ILSVARE_', UPPER(LTRIM(RTRIM(CAST(src.vnr AS NVARCHAR(100))))))
                            ELSE NULL
                        END AS clean_code_ILSVare,

                        -- Vare-resolution inputs (lookup happens in fct_Vareudlevering)
                        lev.clean_code_Kreditor,
                        CASE
                            WHEN lev.clean_code_Kreditor IS NOT NULL
                             AND src.lvnr_1 IS NOT NULL
                             AND LTRIM(RTRIM(src.lvnr_1)) <> ''
                            THEN lev.clean_code_Kreditor + '_LEVVARE_'
                                 + UPPER(LTRIM(RTRIM(src.lvnr_1)))
                        END AS clean_code_Leverandoervare,

                        -- Measures (typed)
                        CAST(src.udlant AS DECIMAL(18,4)) AS vareudlevering_antal,
                        CAST(src.udlbel AS DECIMAL(19,4)) AS vareudlevering_beloeb,
                        CAST(src.indkbel AS DECIMAL(19,4)) AS indkoeb_beloeb,
                        CAST(src.rabatbel AS DECIMAL(19,4)) AS rabatbeloeb,
                        CAST(src.lagerantal AS DECIMAL(18,4)) AS lager_antal,

                        -- Date key conversions (4 dates, precomputed for fact layer)
                        {dato_key('src.udldto')} AS dw_ek_Dato_vareudlevering,
                        DATEADD(
                            SECOND, ISNULL(src.udlklk, 0), CAST(src.udldto AS DATETIME2)
                        ) AS dato_tid_vareudlevering,
                        {dato_key('src.udlobdto')} AS dw_ek_Dato_udloeb,
                        {dato_tid('src.udlobdto')} AS dato_tid_udloeb,
                        {dato_key('src.postdto')} AS dw_ek_Dato_postering,
                        {dato_tid('src.postdto')} AS dato_tid_postering,
                        {dato_key('src.aflevdto')} AS dw_ek_Dato_aflevering,
                        DATEADD(
                            SECOND, ISNULL(src.aflevklk, 0), CAST(src.aflevdto AS DATETIME2)
                        ) AS dato_tid_aflevering,

                        DATEADD(
                            SECOND, ISNULL(src.udlklk, 0), CAST(src.udldto AS DATETIME2)
                        ) AS dw_haendelsestidspunkt

                    INTO [stg].[fct_Vareudlevering_ils]
                    FROM [stg].[pre_Vareudlevering_ils] src

                    LEFT JOIN lev_current lev
                        ON lev.lnr = src.lnr;
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.fct_Vareudlevering_ils with {row_count} rows")

                # Create clustered index on MERGE key
                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_fct_vareudlevering_ils_PK
                    ON [stg].[fct_Vareudlevering_ils](clean_code_Vareudlevering);
                """
                )

                # Create non-clustered indexes for map lookups
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_Lager
                    ON [stg].[fct_Vareudlevering_ils](clean_code_Lager);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_Vare
                    ON [stg].[fct_Vareudlevering_ils](clean_code_Leverandoervare);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_Rekvirent
                    ON [stg].[fct_Vareudlevering_ils](clean_code_Rekvirent);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_vareudlevering_ils_IlsVare
                    ON [stg].[fct_Vareudlevering_ils](clean_code_ILSVare);
                """
                )

                context.log.info("Created indexes for map lookups")

                conn.commit()

                # Diagnostic
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(clean_code_Vareudlevering) AS has_vareudlev_clean,
                        COUNT(clean_code_Lager) AS has_lager_clean,
                        COUNT(clean_code_Rekvirent) AS has_rekvirent_clean,
                        COUNT(clean_code_Leverandoervare) AS has_levvare_clean
                    FROM [stg].[fct_Vareudlevering_ils]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[1]}/{stats[0]} with clean_code_Vareudlevering, "
                    f"{stats[2]}/{stats[0]} with clean_code_Lager, "
                    f"{stats[3]}/{stats[0]} with clean_code_Rekvirent, "
                    f"{stats[4]}/{stats[0]} with clean_code_Leverandoervare"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.fct_Vareudlevering_ils",
            "row_count": row_count,
            "source_table": "stg.pre_Vareudlevering_ils",
            "grain": "prrowid (unique delivery line)",
            "has_vareudlev_clean": stats[1],
            "has_lager_clean": stats[2],
            "has_rekvirent_clean": stats[3],
            "has_levvare_clean": stats[4],
            "mode": config.mode,
            "layer": "stg_fct (Layer 1)",
        }
    )
