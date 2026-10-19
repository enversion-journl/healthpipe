# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Aftale from ILS (Dimension Layer)

Aggregate ILS agreement data at aftale grain.
Derived from pre_stg_Vareaftaler_ils with deterministic deduplication.
alle_maa_benytte er en korreleret EXISTS mod arc.ils_lafthsgh (lnr+aftale) —
pre_stg_Vareaftaler_ils og denne stager begge det FULDE billede hver koersel
(ingen checkpoint-filter), saa flagget forbliver friskt uden eget delta-signal.

Kendt begraensning: arc.ils_lafthsgh er upsert-only (detect_deletes=False,
se DSI/mssql/archive/ils_archive_configs.py's _full_load_config). En raekke
der fjernes i ILS forsvinder derfor IKKE af sig selv fra arc — EXISTS'en
forbliver sand, og alle_maa_benytte forbliver 0 for ophaevet styring.
Kraever manuel arc-oprydning eller delete-detektion paa arkivet.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_Aftale_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Vareaftaler_ils"]),
        AssetKey(["arc_ils_lafthsgh"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "aftale",
    },
    description="Aggregate ILS aftale data at aftale grain using latest start-date deduplication",
)
def stg_dim_Aftale_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Create staging table for aftale dimension with one row per aftale."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_Aftale_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0
    inconsistent_txt_aftaler = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:

                context.log.info("Creating staging table stg.dim_Aftale_ils from base layer...")

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Aftale_ils];")

                cursor.execute(
                    """
                    SELECT
                        clean_code_Aftale,
                        aftale AS aftale_nummer,
                        lnr AS leverandoernummer,
                        clean_code_Kreditor,
                        afttype AS aftaletype,
                        aftaletekst AS aftaletekst,
                        aftdto AS aftale_dato,
                        ansvar AS aftale_ansvarlig,
                        hoved_start AS hoved_start,
                        hoved_slut AS hoved_slut,
                        alle_maa_benytte,
                        deflevdage AS standard_leveringsdage,
                        sysdato,
                        bruger,
                        aktiv,
                        fragtbel AS fragt_beloeb,
                        sw_fragt,
                        gebyrbel AS gebyr_beloeb,
                        sw_gebyr,
                        graensebel AS graense_beloeb,
                        fragtgraensebel AS fragt_graense_beloeb,
                        dw_timestamp_update
                    INTO [stg].[dim_Aftale_ils]
                    FROM (
                        SELECT
                            pva.clean_code_Aftale,
                            pva.aftale,
                            pva.lnr,
                            pva.clean_code_Kreditor,
                            pva.afttype,
                            pva.aftaletekst,
                            pva.aftdto,
                            pva.ansvar,
                            pva.hoved_start,
                            pva.hoved_slut,
                            -- 1 = ingen lafthsgh-styring ("Benyttes af" blank i ILS) = alle
                            -- maa koebe ind; 0 = styret, se bridge.AftaleLager
                            -- Kendt begraensning: arc.ils_lafthsgh er upsert-only
                            -- (detect_deletes=False) — en ophaevet styringsraekke i
                            -- ILS forsvinder ikke af sig selv fra arc, saa flaget
                            -- forbliver 0 indtil manuel arc-oprydning/delete-detektion.
                            -- lhs/pva SKAL vaere kvalificeret i korrelationen: begge
                            -- tabeller har lnr/aftale-kolonner, saa et ukvalificeret
                            -- lnr/aftale binder til det INDERSTE scope (lhs) og goer
                            -- praedikatet til en tautologi (altid sand => flaget 0 for
                            -- ALLE aftaler).
                            CASE WHEN EXISTS (
                                SELECT 1 FROM [arc].[ils_lafthsgh] lhs
                                WHERE LTRIM(RTRIM(lhs.lnr)) = LTRIM(RTRIM(pva.lnr))
                                  AND LTRIM(RTRIM(lhs.aftale)) = LTRIM(RTRIM(pva.aftale))
                            ) THEN 0 ELSE 1 END AS alle_maa_benytte,
                            pva.deflevdage,
                            pva.sysdato,
                            pva.bruger,
                            pva.aktiv,
                            pva.fragtbel,
                            pva.sw_fragt,
                            pva.gebyrbel,
                            pva.sw_gebyr,
                            pva.graensebel,
                            pva.fragtgraensebel,
                            pva.dw_timestamp_update,
                            -- The base layer is at (aftale, lnr, vnr) grain, so the
                            -- ROW_NUMBER below collapses the agreement's LINES into
                            -- one row per agreement. The header attributes are
                            -- identical across the candidates because pre_stg has
                            -- already picked one lafth row per (lnr, aftale) — the
                            -- ordering therefore only decides which line lends its
                            -- clean_code_Kreditor, not the agreement's own values.
                            ROW_NUMBER() OVER (
                                PARTITION BY pva.clean_code_Aftale
                                ORDER BY
                                    COALESCE(pva.hoved_start, pva.linje_start) DESC,
                                    pva.dw_timestamp_update DESC
                            ) AS rn
                        FROM [stg].[pre_vareaftaler_ils] pva
                        WHERE pva.aftale IS NOT NULL
                          AND LTRIM(RTRIM(pva.aftale)) <> ''
                    ) ranked
                    WHERE rn = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.dim_Aftale_ils with {row_count} rows")

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_dim_aftale_ils_CleanCode_Aftale
                    ON [stg].[dim_Aftale_ils](clean_code_Aftale);
                """
                )

                cursor.execute(
                    """
                    SELECT COUNT(*) AS inconsistent_txt_aftaler
                    FROM (
                        SELECT
                            clean_code_Aftale
                        FROM [stg].[pre_vareaftaler_ils]
                        WHERE aftale IS NOT NULL
                          AND LTRIM(RTRIM(aftale)) <> ''
                        GROUP BY clean_code_Aftale
                        HAVING COUNT(DISTINCT COALESCE(LTRIM(RTRIM(aftaletekst)), '')) > 1
                    ) diff_txt;
                """
                )
                inconsistent_txt_aftaler = cursor.fetchone()[0]
                context.log.info(
                    "Aftaler with inconsistent aftaletekst across lines: "
                    f"{inconsistent_txt_aftaler}"
                )

                conn.commit()
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Aftale_ils",
            "row_count": row_count,
            "source_table": "stg.pre_vareaftaler_ils + arc.ils_lafthsgh (alle_maa_benytte)",
            "grain": "lnr + aftale (clean_code_Aftale)",
            "deduplication": "ROW_NUMBER() by clean_code_Aftale collapses the aftale's lines; the lafth header is already one row per (lnr, aftale) from pre_stg",
            "inconsistent_aftaletekst_aftaler": inconsistent_txt_aftaler,
            "layer": "dim (Layer 1)",
            "mode": config.mode,
        }
    )
