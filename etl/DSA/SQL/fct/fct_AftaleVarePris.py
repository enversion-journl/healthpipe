# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Update: AftaleVarePris

Prisversioner pr. aftalelinje. Grain: én række pr.
(lnr, aftale, vnr, start_, startklk) — unikt via bk_prrowid (laftdpris.prrowid).

Kildeleveret historik: dw_valid_from/dw_valid_to kommer fra kildens
start_/startklk/slut. dw_is_current beregnes i staging (nyeste ikrafttraadte
prisversion pr. aftalelinje; staging leverer alle versioner af beroerte linjer).

Ingen soft delete: ILS-arkiverne koerer detect_deletes=False (upsert-only),
saa raekker forsvinder aldrig fra arc.ils_laftdpris og en sletning kan ikke
observeres. Genindfoer delete-detektion her hvis arc-laget faar det.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

RESOLVE_SQL = """
DROP TABLE IF EXISTS #AftaleVarePris_Facts;

SELECT
    src.bk_prrowid,
    src.bk_leverandoernummer,
    src.bk_aftale_nummer,
    src.bk_ils_varenummer,
    src.clean_code_AftaleVarePris,

    av_map.dw_ek_AftaleVarePris,
    COALESCE(aftale_map.dw_ek_Aftale, -1) AS dw_ek_Aftale,
    COALESCE(vare_map.dw_ek_Leverandoervare, -1) AS dw_ek_Leverandoervare,
    COALESCE(ilsvare_map.dw_ek_ILSVare, -1) AS dw_ek_ILSVare,
    COALESCE(kreditor_map.dw_ek_Kreditor, -1) AS dw_ek_Kreditor,

    src.pris1,
    src.pris2,
    src.pris3,
    src.pris4,
    src.pakkestoerrelse1,
    src.pakkestoerrelse2,
    src.pakkestoerrelse3,
    src.pakkestoerrelse4,
    src.momskode,
    src.kreditorvarenummer1,
    src.kreditorvarenummer2,
    src.kreditorvarenummer3,
    src.maerke,
    src.linje_start,
    src.linje_slut,
    src.dw_valid_from,
    src.dw_valid_to,
    src.dw_is_current,
    src.source_aktiv

INTO #AftaleVarePris_Facts
FROM [stg].[fct_AftaleVarePris_ils] src

INNER JOIN [map].[AftaleVarePris] av_map
    ON av_map.clean_code_AftaleVarePris = src.clean_code_AftaleVarePris

LEFT JOIN [map].[Aftale] aftale_map
    ON aftale_map.clean_code_Aftale = src.clean_code_Aftale

LEFT JOIN [map].[Leverandoervare] vare_map
    ON vare_map.clean_code_Leverandoervare = src.clean_code_Leverandoervare
   AND src.clean_code_Leverandoervare IS NOT NULL
   AND src.clean_code_Leverandoervare <> ''

LEFT JOIN [map].[ILSVare] ilsvare_map
    ON ilsvare_map.clean_code_ILSVare = src.IlsVare_CleanCode

LEFT JOIN [map].[Kreditor] kreditor_map
    ON kreditor_map.clean_code_Kreditor = src.clean_code_Kreditor
   AND src.clean_code_Kreditor IS NOT NULL
   AND src.clean_code_Kreditor <> '';
"""

MERGE_SQL = """
MERGE [fct].[AftaleVarePris] AS tgt
USING #AftaleVarePris_Facts AS src
ON tgt.bk_prrowid = src.bk_prrowid

WHEN MATCHED THEN
    UPDATE SET
        tgt.dw_ek_AftaleVarePris = src.dw_ek_AftaleVarePris,
        tgt.dw_ek_Aftale = src.dw_ek_Aftale,
        tgt.dw_ek_Leverandoervare = src.dw_ek_Leverandoervare,
        tgt.dw_ek_ILSVare = src.dw_ek_ILSVare,
        tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
        tgt.clean_code_AftaleVarePris = src.clean_code_AftaleVarePris,
        tgt.bk_leverandoernummer = src.bk_leverandoernummer,
        tgt.bk_aftale_nummer = src.bk_aftale_nummer,
        tgt.bk_ils_varenummer = src.bk_ils_varenummer,
        tgt.pris1 = src.pris1,
        tgt.pris2 = src.pris2,
        tgt.pris3 = src.pris3,
        tgt.pris4 = src.pris4,
        tgt.pakkestoerrelse1 = src.pakkestoerrelse1,
        tgt.pakkestoerrelse2 = src.pakkestoerrelse2,
        tgt.pakkestoerrelse3 = src.pakkestoerrelse3,
        tgt.pakkestoerrelse4 = src.pakkestoerrelse4,
        tgt.momskode = src.momskode,
        tgt.kreditorvarenummer1 = src.kreditorvarenummer1,
        tgt.kreditorvarenummer2 = src.kreditorvarenummer2,
        tgt.kreditorvarenummer3 = src.kreditorvarenummer3,
        tgt.maerke = src.maerke,
        tgt.linje_start = src.linje_start,
        tgt.linje_slut = src.linje_slut,
        tgt.dw_valid_from = src.dw_valid_from,
        tgt.dw_valid_to = src.dw_valid_to,
        tgt.dw_is_current = src.dw_is_current,
        tgt.source_aktiv = src.source_aktiv,
        tgt.dw_is_deleted = 0,
        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dw_ek_AftaleVarePris, dw_ek_Aftale, dw_ek_Leverandoervare, dw_ek_ILSVare, dw_ek_Kreditor,
        clean_code_AftaleVarePris, bk_prrowid, bk_leverandoernummer, bk_aftale_nummer, bk_ils_varenummer,
        pris1, pris2, pris3, pris4,
        pakkestoerrelse1, pakkestoerrelse2, pakkestoerrelse3, pakkestoerrelse4,
        momskode, kreditorvarenummer1, kreditorvarenummer2, kreditorvarenummer3, maerke,
        linje_start, linje_slut, dw_valid_from, dw_valid_to, dw_is_current, source_aktiv,
        dw_is_deleted, dw_timestamp_insert, dw_timestamp_update
    )
    VALUES (
        src.dw_ek_AftaleVarePris, src.dw_ek_Aftale, src.dw_ek_Leverandoervare, src.dw_ek_ILSVare, src.dw_ek_Kreditor,
        src.clean_code_AftaleVarePris, src.bk_prrowid, src.bk_leverandoernummer, src.bk_aftale_nummer, src.bk_ils_varenummer,
        src.pris1, src.pris2, src.pris3, src.pris4,
        src.pakkestoerrelse1, src.pakkestoerrelse2, src.pakkestoerrelse3, src.pakkestoerrelse4,
        src.momskode, src.kreditorvarenummer1, src.kreditorvarenummer2, src.kreditorvarenummer3, src.maerke,
        src.linje_start, src.linje_slut, src.dw_valid_from, src.dw_valid_to, src.dw_is_current, src.source_aktiv,
        0,
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""


@asset(
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_AftaleVarePris_ils"]),
        "map_AftaleVarePris",
        "map_Aftale",
        "map_Leverandoervare",
        "map_ILSVare",
        "map_Kreditor",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "price_version_per_agreement_line",
    },
    description=(
        "Prisversioner pr. aftalelinje — kildeleveret historik "
        "fra ILS laftdpris, MERGE paa bk_prrowid (ingen soft delete — arc er upsert-only)"
    ),
)
def fct_AftaleVarePris(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map-opslag + MERGE ind i fct.AftaleVarePris. dw_is_current beregnes i staging."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct",
        "AftaleVarePris",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting AftaleVarePris fact table load...")

                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_AftaleVarePris_ils]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging count: {staging_count}")

                # Fail-fast: alle staging CleanCodes skal findes i map.AftaleVarePris
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [stg].[fct_AftaleVarePris_ils] src
                    LEFT JOIN [map].[AftaleVarePris] m
                        ON m.clean_code_AftaleVarePris = src.clean_code_AftaleVarePris
                    WHERE m.dw_ek_AftaleVarePris IS NULL
                """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"fct_AftaleVarePris: {unmapped_rows} staging rows have no match in "
                        "map.AftaleVarePris. Ensure map_AftaleVarePris runs before fct_AftaleVarePris."
                    )

                cursor.execute(RESOLVE_SQL)
                temp_count = cursor.rowcount
                context.log.info(f"Created temp table with {temp_count} rows")

                cursor.execute(
                    """
                    SELECT COUNT(*) - COUNT(DISTINCT bk_prrowid)
                    FROM #AftaleVarePris_Facts
                """
                )
                duplicate_source_rows = cursor.fetchone()[0]
                if duplicate_source_rows > 0:
                    raise ValueError(
                        "Duplicate grain violation in fct_AftaleVarePris: "
                        f"{duplicate_source_rows} duplicate bk_prrowid rows in #AftaleVarePris_Facts."
                    )

                cursor.execute(MERGE_SQL)
                rows_affected = cursor.rowcount

                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total_facts,
                        SUM(CASE WHEN dw_ek_Aftale = -1 THEN 1 ELSE 0 END) AS missing_aftale_fk,
                        SUM(CASE WHEN dw_ek_Leverandoervare = -1 THEN 1 ELSE 0 END) AS missing_vare_fk,
                        SUM(CASE WHEN dw_ek_ILSVare = -1 THEN 1 ELSE 0 END) AS missing_ilsvare_fk,
                        SUM(CASE WHEN dw_ek_Kreditor = -1 THEN 1 ELSE 0 END) AS missing_kreditor_fk,
                        SUM(CAST(dw_is_current AS INT)) AS current_rows
                    FROM [fct].[AftaleVarePris]
                    WHERE dw_is_deleted = 0
                """
                )
                diag = cursor.fetchone()

                context.log.info(
                    f"FK Matching: Total={diag[0]}, Missing Aftale={diag[1]}, "
                    f"Missing Vare={diag[2]}, Missing ILSVare={diag[3]}, "
                    f"Missing Kreditor={diag[4]}, dw_is_current={diag[5]}"
                )
                if diag[2] > 0:
                    context.log.warning(
                        f"{diag[2]} prisversioner uden Vare FK (lvnr1/CVR mangler i kilden — forventet)"
                    )

                cursor.execute("DROP TABLE IF EXISTS #AftaleVarePris_Facts")
                cursor.execute("UPDATE STATISTICS [fct].[AftaleVarePris]")

                context.log.info(f"Fact table load complete: {rows_affected} merged")
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "AftaleVarePris", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "AftaleVarePris")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.AftaleVarePris",
            "grain": "bk_prrowid (lnr + aftale + vnr + start_ + startklk)",
            "staging_source": "stg.fct_AftaleVarePris_ils (Layer 1)",
            "dw_ek_lookups": "AftaleVarePris, Aftale, Vare, ILSVare, Kreditor",
            "missing_aftale_fk": diag[1],
            "missing_vare_fk": diag[2],
            "missing_ilsvare_fk": diag[3],
            "missing_kreditor_fk": diag[4],
            "is_current_rows": diag[5],
        }
    )
