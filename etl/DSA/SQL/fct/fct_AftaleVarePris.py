# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Update: AftaleVarePris

Price versions — plus synthetic coverage rows for agreement lines that have
no price versions — per agreement line x supplier item number (lvnr).
Grain: (bk_prrowid, clean_code_AftaleVarePris). bk_prrowid carries a source
prefix ('LAFTDPRIS_<prrowid>' for real price versions, 'LAFTD_<prrowid>' for
synthetic rows), and clean_code_AftaleVarePris carries a _LEVVARE_{lvnr}
suffix so fanned-out lvnr rows of the same price version stay distinct.

Validity: dw_valid_from comes from the source (start_/startklk on price
versions; the line's own start on synthetic rows). dw_valid_to is LEAD-chained
per agreement line in staging — the source's slut only serves as fallback on a
line's last version — and synthetic rows are open-ended. dw_is_current is also
computed in staging (newest effective price version per line; synthetic rows
are always current). har_prisversion tells the two row kinds apart
(1 = real price version, 0 = synthetic coverage row).

Delete handling is a line-scoped pre-delete sweep: BEFORE the MERGE, every
live row belonging to a line represented in staging (#AftaleVarePris_Facts)
is marked deleted, whether or not the row itself reappears. The MERGE then
resurrects everything still present (WHEN MATCHED sets dw_is_deleted = 0).
One uniform mechanism covers shrinking lvnr sets, synthetic-to-real price
version transitions, and any other disappearance within a line. The scope is
deliberately the LINE (bk_leverandoernummer, bk_aftale_nummer,
bk_ils_varenummer), never the agreement: sibling lines on the same agreement
are not necessarily restaged in the same run and must not be swept. This is
safe because staging always carries the FULL history for an affected line
(delta_join gates on the line keys, never on price-version/timestamp level) —
pinned by test_aftalevarepris_delta_contract.py. The arc layer itself never
deletes (detect_deletes=False on all ILS archives).
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
    src.har_prisversion,

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
    src.kilder,
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
AND tgt.clean_code_AftaleVarePris = src.clean_code_AftaleVarePris

WHEN MATCHED THEN
    UPDATE SET
        tgt.dw_ek_AftaleVarePris = src.dw_ek_AftaleVarePris,
        tgt.dw_ek_Aftale = src.dw_ek_Aftale,
        tgt.dw_ek_Leverandoervare = src.dw_ek_Leverandoervare,
        tgt.dw_ek_ILSVare = src.dw_ek_ILSVare,
        tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
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
        tgt.kilder = src.kilder,
        tgt.har_prisversion = src.har_prisversion,
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
        momskode, kilder, har_prisversion, maerke,
        linje_start, linje_slut, dw_valid_from, dw_valid_to, dw_is_current, source_aktiv,
        dw_is_deleted, dw_timestamp_insert, dw_timestamp_update
    )
    VALUES (
        src.dw_ek_AftaleVarePris, src.dw_ek_Aftale, src.dw_ek_Leverandoervare, src.dw_ek_ILSVare, src.dw_ek_Kreditor,
        src.clean_code_AftaleVarePris, src.bk_prrowid, src.bk_leverandoernummer, src.bk_aftale_nummer, src.bk_ils_varenummer,
        src.pris1, src.pris2, src.pris3, src.pris4,
        src.pakkestoerrelse1, src.pakkestoerrelse2, src.pakkestoerrelse3, src.pakkestoerrelse4,
        src.momskode, src.kilder, src.har_prisversion, src.maerke,
        src.linje_start, src.linje_slut, src.dw_valid_from, src.dw_valid_to, src.dw_is_current, src.source_aktiv,
        0,
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

# Line-scoped pre-delete sweep, run BEFORE the MERGE. Scope: the agreement
# lines (bk_leverandoernummer, bk_aftale_nummer, bk_ils_varenummer) that are
# represented in #AftaleVarePris_Facts — every live row for such a line is
# marked deleted, including rows whose (bk_prrowid, clean_code_AftaleVarePris)
# does not itself reappear in staging. The MERGE resurrects everything still
# present (WHEN MATCHED sets dw_is_deleted = 0), so the net result is correct
# for shrinking lvnr sets, synthetic-to-real price version transitions, and
# any other disappearance within a line. The scope is deliberately the line,
# never the agreement: sibling lines are not necessarily restaged in the same
# run and must not be swept. Safe because staging always carries the FULL
# history for an affected line (delta_join gates on the line keys —
# contract-tested, load-bearing for this sweep). Swept-then-resurrected rows
# get two timestamp bumps in one run — accepted churn. The arc layer itself
# never deletes (detect_deletes=False on all ILS archives).
PRE_DELETE_SQL = """
UPDATE tgt
SET tgt.dw_is_deleted = 1,
    tgt.dw_is_current = 0,
    tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
FROM [fct].[AftaleVarePris] tgt
WHERE tgt.dw_is_deleted = 0
  AND EXISTS (
      SELECT 1 FROM #AftaleVarePris_Facts src
      WHERE src.bk_leverandoernummer = tgt.bk_leverandoernummer
        AND src.bk_aftale_nummer = tgt.bk_aftale_nummer
        AND src.bk_ils_varenummer = tgt.bk_ils_varenummer
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
        "grain": "price_version_per_agreement_line_per_lvnr",
    },
    description=(
        "Prisversioner (+ syntetiske daekningsraekker for prisloese linjer) pr. "
        "aftalelinje x lvnr — kildeleveret historik fra ILS laftd/laftdpris, "
        "MERGE paa (bk_prrowid, clean_code_AftaleVarePris) med linje-skopet "
        "pre-delete-sweep FOER MERGE"
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
                    SELECT COUNT(*) - COUNT(DISTINCT CONCAT(bk_prrowid, '|', clean_code_AftaleVarePris))
                    FROM #AftaleVarePris_Facts
                """
                )
                duplicate_source_rows = cursor.fetchone()[0]
                if duplicate_source_rows > 0:
                    raise ValueError(
                        "Duplicate grain violation in fct_AftaleVarePris: "
                        f"{duplicate_source_rows} duplicate (bk_prrowid, clean_code_AftaleVarePris) "
                        "rows in #AftaleVarePris_Facts."
                    )

                # Pre-delete-sweep FOER MERGE (spec 2026-08-31): linje-skopet,
                # se PRE_DELETE_SQL-kommentaren for begrundelsen.
                cursor.execute(PRE_DELETE_SQL)
                swept_rows = cursor.rowcount
                context.log.info(
                    f"Pre-delete-sweep: {swept_rows} raekker soft-deleted (linje-skopet, foer MERGE)"
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
            "swept_rows": swept_rows,
            "table": "fct.AftaleVarePris",
            "grain": "bk_prrowid x clean_code_AftaleVarePris (prisversion-eller-syntetisk-linje x lvnr)",
            "staging_source": "stg.fct_AftaleVarePris_ils (Layer 1)",
            "dw_ek_lookups": "AftaleVarePris, Aftale, Vare, ILSVare, Kreditor",
            "missing_aftale_fk": diag[1],
            "missing_vare_fk": diag[2],
            "missing_ilsvare_fk": diag[3],
            "missing_kreditor_fk": diag[4],
            "is_current_rows": diag[5],
        }
    )
