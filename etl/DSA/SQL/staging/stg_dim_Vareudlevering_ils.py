# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vareudlevering from ILS (Dimension Layer)

Selects dimension-relevant columns from pre_stg_Vareudlevering_ils
for map.Vareudlevering and dim.Vareudlevering downstream consumers.

Enriches with:
- projektnavn from arc.ils_projekt (projnr only — SAS Forbrug parity)
- lager_varetype / lager_varetype_navn from arc.ils_lgrvare.varetype2
  + arc.ils_varetype2.navn (sgh+lgr+vnr; UPPER match for name)

Grain: one row per clean_code_Vareudlevering (deduplicated by prrowid).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_Vareudlevering_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Vareudlevering_ils"]),
        AssetKey(["arc_ils_projekt"]),
        AssetKey(["arc_ils_lgrvare"]),
        AssetKey(["arc_ils_varetype2"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "vareudlevering",
    },
    description=(
        "Dimension staging for ILS delivery data — identifiers and descriptive "
        "attributes for map/dim Vareudlevering (incl. SAS Forbrug parity fields)"
    ),
)
def stg_dim_Vareudlevering_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Layer 1 dimension staging for ILS udlevering data.

    Selects identifier and descriptive columns from pre_stg_Vareudlevering_ils,
    deduplicated at clean_code_Vareudlevering grain. This feeds MapUpd_vareudlevering
    (which only needs CleanCode + prrowid) and DimUpd_vareudlevering (which needs
    the full set of descriptive attributes).
    """

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_Vareudlevering_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0
    stats = (0, 0, 0, 0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:

                context.log.info(
                    "Creating staging table stg.dim_Vareudlevering_ils from pre-staging..."
                )

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Vareudlevering_ils];")

                cursor.execute(
                    """
                    ;WITH ranked AS (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY clean_code_Vareudlevering
                                ORDER BY dw_timestamp_update DESC
                            ) AS rn
                        FROM [stg].[pre_Vareudlevering_ils]
                        WHERE clean_code_Vareudlevering IS NOT NULL
                    ),
                    projekt_dedup AS (
                        SELECT
                            projnr,
                            navn,
                            ROW_NUMBER() OVER (
                                PARTITION BY projnr
                                ORDER BY dw_timestamp_update DESC, prrowid ASC
                            ) AS rn
                        FROM [arc].[ils_projekt]
                        WHERE projnr IS NOT NULL
                    ),
                    lgrvare_dedup AS (
                        SELECT
                            sgh,
                            lgr,
                            vnr,
                            varetype2,
                            ROW_NUMBER() OVER (
                                PARTITION BY sgh, lgr, vnr
                                ORDER BY dw_timestamp_update DESC, prrowid ASC
                            ) AS rn
                        FROM [arc].[ils_lgrvare]
                    ),
                    varetype2_dedup AS (
                        SELECT
                            varetype2,
                            navn,
                            ROW_NUMBER() OVER (
                                PARTITION BY UPPER(LTRIM(RTRIM(varetype2)))
                                ORDER BY dw_timestamp_update DESC, prrowid ASC
                            ) AS rn
                        FROM [arc].[ils_varetype2]
                        WHERE varetype2 IS NOT NULL
                    )
                    SELECT
                        p.clean_code_Vareudlevering,
                        p.prrowid,

                        p.rvis AS rekvisitionsnummer,
                        CAST(p.flgsednr AS NVARCHAR(100)) AS foelgeseddelnummer,
                        CAST(p.brik AS NVARCHAR(100)) AS brik,
                        CAST(p.lnr AS NVARCHAR(100)) AS leverandoernummer,
                        CAST(p.ordre AS NVARCHAR(100)) AS ordrenummer,
                        CAST(p.ordlinie AS NVARCHAR(100)) AS ordrelinje,
                        CAST(p.sw_direkte AS NVARCHAR(10)) AS sw_direkte_udlevering,
                        CAST(p.sw_paksdl AS NVARCHAR(10)) AS sw_pakkeseddel,
                        CAST(p.sw_flgsdl AS NVARCHAR(10)) AS sw_foelgeseddel,
                        CAST(p.sw_fly AS NVARCHAR(10)) AS sw_fly,
                        CAST(p.sw_haster AS NVARCHAR(10)) AS sw_haster,
                        CAST(p.sw_erstat AS NVARCHAR(10)) AS sw_erstat,
                        CAST(p.projnr AS NVARCHAR(100)) AS projektnummer,
                        CAST(proj.navn AS NVARCHAR(80)) AS projektnavn,
                        CAST(p.kto AS NVARCHAR(100)) AS kontonummer_kode,
                        CAST(p.bruger AS NVARCHAR(100)) AS bruger,
                        CAST(p.id AS NVARCHAR(100)) AS bk_prrowid,
                        CAST(p.refnr AS NVARCHAR(100)) AS referencenummer,
                        CAST(p.reflinie AS NVARCHAR(100)) AS referencelinje,
                        CAST(p.salgsordre AS NVARCHAR(100)) AS salgsordre,
                        CAST(p.salgsordreint AS NVARCHAR(100)) AS salgsordre_id,
                        CAST(p.salgsfaknr AS NVARCHAR(100)) AS salgsfakturanummer,
                        CAST(p.rvisvareid AS NVARCHAR(100)) AS rekvisitionsvare_id,
                        CAST(p.plukvareid AS NVARCHAR(100)) AS plukvarer_id,
                        CAST(p.aflevoper AS NVARCHAR(100)) AS afleveringsoperatoer,
                        CAST(p.aflevtxt AS NVARCHAR(100)) AS afleveringstekst,
                        CAST(p.aflevstreg AS NVARCHAR(100)) AS afleveringsstregkode,
                        CAST(p.vognnummer AS NVARCHAR(100)) AS vognnummer,
                        CAST(p.lgrlok AS NVARCHAR(16)) AS lgrlok,
                        p.udtraknr3 AS ekspeditionsnummer,
                        CAST(lg.varetype2 AS NVARCHAR(12)) AS lager_varetype,
                        CAST(vt2.navn AS NVARCHAR(80)) AS lager_varetype_navn

                    INTO [stg].[dim_Vareudlevering_ils]
                    FROM ranked p
                    LEFT JOIN projekt_dedup proj
                        ON proj.projnr = p.projnr
                       AND proj.rn = 1
                    LEFT JOIN lgrvare_dedup lg
                        ON lg.sgh = p.sgh
                       AND lg.lgr = p.lgr
                       AND lg.vnr = p.vnr
                       AND lg.rn = 1
                    LEFT JOIN varetype2_dedup vt2
                        ON UPPER(LTRIM(RTRIM(vt2.varetype2))) = UPPER(LTRIM(RTRIM(lg.varetype2)))
                       AND vt2.rn = 1
                    WHERE p.rn = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.dim_Vareudlevering_ils with {row_count} rows")

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_dim_vareudlevering_ils_CleanCode
                    ON [stg].[dim_Vareudlevering_ils](clean_code_Vareudlevering);
                """
                )
                context.log.info("Created unique clustered index on clean_code_Vareudlevering")

                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(prrowid) AS has_prrowid,
                        COUNT(foelgeseddelnummer) AS has_foelgeseddel,
                        COUNT(lgrlok) AS has_lgrlok,
                        COUNT(ekspeditionsnummer) AS has_ekspeditionsnummer,
                        COUNT(lager_varetype) AS has_lager_varetype
                    FROM [stg].[dim_Vareudlevering_ils]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[1]}/{stats[0]} with prrowid, "
                    f"{stats[2]}/{stats[0]} with foelgeseddelnummer, "
                    f"{stats[3]}/{stats[0]} with lgrlok, "
                    f"{stats[4]}/{stats[0]} with ekspeditionsnummer, "
                    f"{stats[5]}/{stats[0]} with lager_varetype"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Vareudlevering_ils",
            "row_count": row_count,
            "source_table": "stg.pre_Vareudlevering_ils",
            "lookups": "arc.ils_projekt, arc.ils_lgrvare, arc.ils_varetype2",
            "deduplication": "ROW_NUMBER() on clean_code_Vareudlevering",
            "has_prrowid": stats[1],
            "has_foelgeseddel": stats[2],
            "has_lgrlok": stats[3],
            "has_ekspeditionsnummer": stats[4],
            "has_lager_varetype": stats[5],
            "mode": config.mode,
            "layer": "dim (Layer 1)",
        }
    )
