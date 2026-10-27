# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Fakturalinje

Fact table for invoice lines with dw_ek_Kreditor, dw_ek_Leverandoervare lookups from maps.
dw_ek_UNSPSC is stamped by fct_apply_fakturalinje_unspsc from
ref.KMDFakturalinjeUNSPSCMatch (tiered KMD match, applied after fact load).

Reads from stg.fct_Fakturalinje_mysupply (Layer 1 staging).

Grain: one row per fakturalinje (dw_ek_Fakturalinje from map.Fakturalinje).
Transaction dimension: dw_ek_Fakturalinje is the shared PK with dim.Fakturalinje,
resolved from map.Fakturalinje via clean_code_Fakturalinje (same pattern as
fct.Faktura / dim.Faktura).
"""

import traceback

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import dato_key, dato_tid

UNMAPPED_CHECK_SQL = """
SELECT COUNT(*)
FROM [stg].[fct_Fakturalinje_mysupply] src
LEFT JOIN [map].[Fakturalinje] fl_map
    ON fl_map.clean_code_Fakturalinje = src.clean_code_Fakturalinje
    AND src.clean_code_Fakturalinje IS NOT NULL
    AND src.clean_code_Fakturalinje <> ''
WHERE fl_map.dw_ek_Fakturalinje IS NULL;
"""

RESOLVE_SQL = f"""
DROP TABLE IF EXISTS #Fakturalinje_Facts;

SELECT
    -- Transaction dim PK: shared with dim.Fakturalinje via map.Fakturalinje
    COALESCE(fl_map.dw_ek_Fakturalinje, -1) AS dw_ek_Fakturalinje,

    src.clean_code_Fakturalinje,
    src.bk_faktura_lines,
    src.bk_faktura_header,
    src.bk_faktura_metadata,

    COALESCE(kred_map.dw_ek_Kreditor, -1) AS dw_ek_Kreditor,
    COALESCE(vare_map.dw_ek_Leverandoervare, -1) AS dw_ek_Leverandoervare,
    COALESCE(fak_map.dw_ek_Faktura, -1) AS dw_ek_Faktura,

    COALESCE(ean_map.dw_ek_EAN, -1) AS dw_ek_EAN_regningsmodtager,
    -- Populeres efter fact load af afterburner-asset fct_apply_fakturalinje_unspsc
    -- (DFM job: fakturalinje_unspsc_apply).
    CAST(-1 AS INT) AS dw_ek_UNSPSC,

    {dato_key('src.faktura_dato')}
        AS dw_ek_Dato_faktura,
    {dato_tid('src.faktura_dato')}
        AS dato_tid_faktura,
    {dato_key('src.ordre_dato')}
        AS dw_ek_Dato_ordre,
    {dato_tid('src.ordre_dato')}
        AS dato_tid_ordre,
    {dato_key('src.betalings_dato')}
        AS dw_ek_Dato_betaling,
    {dato_tid('src.betalings_dato')}
        AS dato_tid_betaling,
    CAST(-1 AS INT) AS dw_ek_Dato_modtagelse,
    CAST(NULL AS DATETIME2) AS dato_tid_modtagelse,

    CAST(src.faktura_nr AS NVARCHAR(100)) AS faktura_nr,
    TRY_CONVERT(DECIMAL(18, 4), src.antal_enheder_faktureret) AS antal_enheder_faktureret,
    TRY_CONVERT(DECIMAL(18, 4), src.linje_beloeb) AS linje_beloeb,
    TRY_CONVERT(DECIMAL(18, 4), src.base_price_enheds_pris) AS enheds_pris,
    CASE WHEN ISNUMERIC(src.linje_beloeb_valuta) = 0
         THEN LEFT(CAST(src.linje_beloeb_valuta AS NVARCHAR(10)), 3)
         ELSE NULL END AS valutakode_linje,
    CASE WHEN ISNUMERIC(src.base_price_enheds_pris_valuta) = 0
         THEN LEFT(CAST(src.base_price_enheds_pris_valuta AS NVARCHAR(10)), 3)
         ELSE NULL END AS valutakode_enhedspris,
    CASE WHEN ISNUMERIC(src.faktura_valuta_kode) = 0
         THEN LEFT(CAST(src.faktura_valuta_kode AS NVARCHAR(10)), 3)
         ELSE NULL END AS valutakode_faktura,

    src.gebyr_beloeb,
    src.rabatbeloeb

INTO #Fakturalinje_Facts
FROM [stg].[fct_Fakturalinje_mysupply] src

LEFT JOIN [map].[Fakturalinje] fl_map
    ON fl_map.clean_code_Fakturalinje = src.clean_code_Fakturalinje
    AND src.clean_code_Fakturalinje IS NOT NULL
    AND src.clean_code_Fakturalinje <> ''

LEFT JOIN [map].[Kreditor] kred_map
    ON kred_map.clean_code_Kreditor = src.clean_code_Kreditor
    AND src.clean_code_Kreditor IS NOT NULL
    AND src.clean_code_Kreditor <> ''

LEFT JOIN [map].[Leverandoervare] vare_map
    ON vare_map.clean_code_Leverandoervare = src.clean_code_Leverandoervare
   AND src.clean_code_Leverandoervare IS NOT NULL
   AND src.clean_code_Leverandoervare <> ''

LEFT JOIN [map].[ean] ean_map
    ON ean_map.clean_code_EAN = src.clean_code_EAN_regningsmodtager
   AND src.clean_code_EAN_regningsmodtager IS NOT NULL
   AND src.clean_code_EAN_regningsmodtager <> ''

LEFT JOIN [map].[Faktura] fak_map
    ON fak_map.clean_code_Faktura = CONCAT('CleanCode_', src.bk_faktura_metadata, '_', src.bk_faktura_header)
   AND src.bk_faktura_metadata IS NOT NULL
   AND src.bk_faktura_metadata <> ''
   AND src.bk_faktura_header IS NOT NULL
   AND src.bk_faktura_header <> '';
"""

RESOLVE_INDEX_SQL = """
CREATE UNIQUE CLUSTERED INDEX IX_tmp_Fakturalinje_Facts
    ON #Fakturalinje_Facts (dw_ek_Fakturalinje);
"""

MERGE_SQL = """
MERGE [fct].[Fakturalinje] AS tgt
USING #Fakturalinje_Facts AS src
ON tgt.dw_ek_Fakturalinje = src.dw_ek_Fakturalinje

WHEN MATCHED AND EXISTS (
    SELECT
        src.bk_faktura_lines, src.bk_faktura_header, src.bk_faktura_metadata,
        src.dw_ek_Kreditor, src.dw_ek_Leverandoervare, src.dw_ek_Faktura,
        src.dw_ek_EAN_regningsmodtager,
        src.dw_ek_Dato_faktura, src.dato_tid_faktura,
        src.dw_ek_Dato_ordre, src.dato_tid_ordre,
        src.dw_ek_Dato_betaling, src.dato_tid_betaling,
        src.dw_ek_Dato_modtagelse, src.dato_tid_modtagelse,
        src.faktura_nr, src.antal_enheder_faktureret, src.linje_beloeb, src.enheds_pris,
        src.valutakode_linje, src.valutakode_enhedspris, src.valutakode_faktura,
        src.gebyr_beloeb, src.rabatbeloeb
    EXCEPT
    SELECT
        tgt.bk_faktura_lines, tgt.bk_faktura_header, tgt.bk_faktura_metadata,
        tgt.dw_ek_Kreditor, tgt.dw_ek_Leverandoervare, tgt.dw_ek_Faktura,
        tgt.dw_ek_EAN_regningsmodtager,
        tgt.dw_ek_Dato_faktura, tgt.dato_tid_faktura,
        tgt.dw_ek_Dato_ordre, tgt.dato_tid_ordre,
        tgt.dw_ek_Dato_betaling, tgt.dato_tid_betaling,
        tgt.dw_ek_Dato_modtagelse, tgt.dato_tid_modtagelse,
        tgt.faktura_nr, tgt.antal_enheder_faktureret, tgt.linje_beloeb, tgt.enheds_pris,
        tgt.valutakode_linje, tgt.valutakode_enhedspris, tgt.valutakode_faktura,
        tgt.gebyr_beloeb, tgt.rabatbeloeb
) THEN
    UPDATE SET
        tgt.bk_faktura_lines = src.bk_faktura_lines,
        tgt.bk_faktura_header = src.bk_faktura_header,
        tgt.bk_faktura_metadata = src.bk_faktura_metadata,
        tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
        tgt.dw_ek_Leverandoervare = src.dw_ek_Leverandoervare,
        tgt.dw_ek_Faktura = src.dw_ek_Faktura,
        tgt.dw_ek_EAN_regningsmodtager = src.dw_ek_EAN_regningsmodtager,
        tgt.dw_ek_Dato_faktura = src.dw_ek_Dato_faktura,
        tgt.dato_tid_faktura = src.dato_tid_faktura,
        tgt.dw_ek_Dato_ordre = src.dw_ek_Dato_ordre,
        tgt.dato_tid_ordre = src.dato_tid_ordre,
        tgt.dw_ek_Dato_betaling = src.dw_ek_Dato_betaling,
        tgt.dato_tid_betaling = src.dato_tid_betaling,
        tgt.dw_ek_Dato_modtagelse = src.dw_ek_Dato_modtagelse,
        tgt.dato_tid_modtagelse = src.dato_tid_modtagelse,
        tgt.faktura_nr = src.faktura_nr,
        tgt.antal_enheder_faktureret = src.antal_enheder_faktureret,
        tgt.linje_beloeb = src.linje_beloeb,
        tgt.enheds_pris = src.enheds_pris,
        tgt.valutakode_linje = src.valutakode_linje,
        tgt.valutakode_enhedspris = src.valutakode_enhedspris,
        tgt.valutakode_faktura = src.valutakode_faktura,
        tgt.gebyr_beloeb = src.gebyr_beloeb,
        tgt.rabatbeloeb = src.rabatbeloeb,
        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dw_ek_Fakturalinje,
        bk_faktura_lines, bk_faktura_header, bk_faktura_metadata, clean_code_Fakturalinje,
        dw_ek_Kreditor, dw_ek_Leverandoervare, dw_ek_Faktura,
        dw_ek_EAN_regningsmodtager, dw_ek_UNSPSC,
        dw_ek_Dato_faktura, dato_tid_faktura,
        dw_ek_Dato_ordre, dato_tid_ordre,
        dw_ek_Dato_betaling, dato_tid_betaling,
        dw_ek_Dato_modtagelse, dato_tid_modtagelse,
        faktura_nr, antal_enheder_faktureret, linje_beloeb, enheds_pris,
        valutakode_linje, valutakode_enhedspris, valutakode_faktura,
        gebyr_beloeb, rabatbeloeb,
        dw_timestamp_insert, dw_timestamp_update
    )
    VALUES (
        src.dw_ek_Fakturalinje,
        src.bk_faktura_lines, src.bk_faktura_header, src.bk_faktura_metadata, src.clean_code_Fakturalinje,
        src.dw_ek_Kreditor, src.dw_ek_Leverandoervare, src.dw_ek_Faktura,
        src.dw_ek_EAN_regningsmodtager, src.dw_ek_UNSPSC,
        src.dw_ek_Dato_faktura, src.dato_tid_faktura,
        src.dw_ek_Dato_ordre, src.dato_tid_ordre,
        src.dw_ek_Dato_betaling, src.dato_tid_betaling,
        src.dw_ek_Dato_modtagelse, src.dato_tid_modtagelse,
        src.faktura_nr, src.antal_enheder_faktureret, src.linje_beloeb, src.enheds_pris,
        src.valutakode_linje, src.valutakode_enhedspris, src.valutakode_faktura,
        src.gebyr_beloeb, src.rabatbeloeb,
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = """
INSERT INTO [fct].[Fakturalinje] WITH (TABLOCK) (
    dw_ek_Fakturalinje,
    bk_faktura_lines, bk_faktura_header, bk_faktura_metadata, clean_code_Fakturalinje,
    dw_ek_Kreditor, dw_ek_Leverandoervare, dw_ek_Faktura,
    dw_ek_EAN_regningsmodtager, dw_ek_UNSPSC,
    dw_ek_Dato_faktura, dato_tid_faktura,
    dw_ek_Dato_ordre, dato_tid_ordre,
    dw_ek_Dato_betaling, dato_tid_betaling,
    dw_ek_Dato_modtagelse, dato_tid_modtagelse,
    faktura_nr, antal_enheder_faktureret, linje_beloeb, enheds_pris,
    valutakode_linje, valutakode_enhedspris, valutakode_faktura,
    gebyr_beloeb, rabatbeloeb,
    dw_timestamp_insert, dw_timestamp_update
)
SELECT
    src.dw_ek_Fakturalinje,
    src.bk_faktura_lines, src.bk_faktura_header, src.bk_faktura_metadata, src.clean_code_Fakturalinje,
    src.dw_ek_Kreditor, src.dw_ek_Leverandoervare, src.dw_ek_Faktura,
    src.dw_ek_EAN_regningsmodtager, src.dw_ek_UNSPSC,
    src.dw_ek_Dato_faktura, src.dato_tid_faktura,
    src.dw_ek_Dato_ordre, src.dato_tid_ordre,
    src.dw_ek_Dato_betaling, src.dato_tid_betaling,
    src.dw_ek_Dato_modtagelse, src.dato_tid_modtagelse,
    src.faktura_nr, src.antal_enheder_faktureret, src.linje_beloeb, src.enheds_pris,
    src.valutakode_linje, src.valutakode_enhedspris, src.valutakode_faktura,
    src.gebyr_beloeb, src.rabatbeloeb,
    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM #Fakturalinje_Facts src;
"""

DIAG_SQL = """
SELECT
    COUNT(*) AS total_facts,
    SUM(CASE WHEN dw_ek_Fakturalinje = -1 THEN 1 ELSE 0 END) AS missing_fakturalinje_fk,
    SUM(CASE WHEN dw_ek_Kreditor = -1 THEN 1 ELSE 0 END) AS missing_kreditor_fk,
    SUM(CASE WHEN dw_ek_Leverandoervare = -1 THEN 1 ELSE 0 END) AS missing_vare_fk,
    SUM(CASE WHEN dw_ek_Faktura = -1 THEN 1 ELSE 0 END) AS missing_faktura_fk,
    SUM(CASE WHEN dw_ek_EAN_regningsmodtager = -1 THEN 1 ELSE 0 END) AS missing_ean_fk,
    SUM(CASE WHEN dw_ek_UNSPSC = -1 THEN 1 ELSE 0 END) AS missing_unspsc_fk
FROM [fct].[Fakturalinje];
"""


@asset(
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_Fakturalinje_mysupply"]),
        "map_Kreditor",
        "map_Leverandoervare",
        "map_Faktura",
        "map_Fakturalinje",
        "fct_Postering",
        "dim_Leverandoervare",
        "map_EAN",
        "dim_EAN",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "invoice_line",
    },
    description=(
        "Fact table for invoice lines - reads from fct_Fakturalinje_mysupply, "
        "looks up dw_ek_Kreditor, dw_ek_Leverandoervare, dw_ek_Faktura, dw_ek_EAN_regningsmodtager "
        "from maps; dw_ek_UNSPSC applied by fct_apply_fakturalinje_unspsc"
    ),
)
def fct_Fakturalinje(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Calculate fact table with DW_EK lookups from maps (UNSPSC via apply-pass)."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct",
        "Fakturalinje",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    rows_affected = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Starting fact table load with DW_EK lookups and FK diagnostics..."
                )

                try:
                    cursor.execute("SELECT COUNT(*) FROM [stg].[pre_Fakturalinje_mysupply]")
                    pre_staging_count = cursor.fetchone()[0]
                    cursor.execute(
                        "SELECT COUNT(*) FROM [stg].[pre_Fakturalinje_mysupply] "
                        "WHERE statistic_invoice_line_id IS NOT NULL"
                    )
                    line_count = cursor.fetchone()[0]
                    context.log.info(
                        f"Pre-staging counts: Total={pre_staging_count}, With Lines={line_count}"
                    )
                except Exception as e:
                    error_msg = f"Failed to count pre-staging records: {str(e)}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # dw_ek_Fakturalinje is a transaction-dim PK shared with dim.Fakturalinje
                # via map.Fakturalinje. RESOLVE_SQL COALESCEs unmatched rows to -1, and
                # RESOLVE_INDEX_SQL creates a UNIQUE index on that column, so more than one
                # unmapped row would crash the index build. Fail fast with a clear message
                # instead (same pattern as dim/fct_Vareudlevering).
                try:
                    cursor.execute(UNMAPPED_CHECK_SQL)
                    unmapped_rows = cursor.fetchone()[0]
                    if unmapped_rows > 0:
                        raise ValueError(
                            f"fct_Fakturalinje: {unmapped_rows} staging rows have no match in "
                            "map.Fakturalinje on clean_code_Fakturalinje. "
                            "Ensure map_Fakturalinje runs before fct_Fakturalinje and staging "
                            "CleanCodes are valid (dw_ek_Fakturalinje is a shared transaction-dim "
                            "PK, not a routine -1/UNKNOWN FK)."
                        )
                except ValueError:
                    raise
                except Exception as e:
                    error_msg = f"Failed to check for unmapped Fakturalinje rows: {str(e)}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                try:
                    cursor.execute(RESOLVE_SQL)
                    cursor.execute(RESOLVE_INDEX_SQL)
                    context.log.info("Temp table #Fakturalinje_Facts created successfully")
                except Exception as e:
                    error_msg = f"Failed to create temp table with DW_EK lookups: {e}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                try:
                    # Short-circuit: fct.Fakturalinje has no -1 sentinel row, so on a
                    # fresh or rebuilt (empty) target a bulk INSERT ... WITH (TABLOCK)
                    # can be minimally logged, whereas MERGE is always fully logged.
                    cursor.execute(
                        "SELECT CASE WHEN EXISTS (SELECT 1 FROM [fct].[Fakturalinje]) "
                        "THEN 1 ELSE 0 END"
                    )
                    if cursor.fetchone()[0]:
                        context.log.info("Target has existing rows - MERGE upsert path")
                        cursor.execute(MERGE_SQL)
                    else:
                        context.log.info(
                            "Target empty - bulk INSERT WITH (TABLOCK), minimally logged"
                        )
                        cursor.execute(INSERT_SQL)
                    context.log.info("MERGE operation completed successfully")
                except Exception as e:
                    error_msg = f"Failed to MERGE into fct.Fakturalinje: {str(e)}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                rows_affected = cursor.rowcount
                conn.commit()
                context.log.info("Database transaction committed")

                try:
                    cursor.execute(DIAG_SQL)
                    columns = [col[0] for col in cursor.description]
                    diag = dict(zip(columns, cursor.fetchone(), strict=False))

                    context.log.info(
                        f"FK Matching: Total={diag['total_facts']}, "
                        f"Missing Fakturalinje (map)={diag['missing_fakturalinje_fk']}, "
                        f"Missing Kreditor={diag['missing_kreditor_fk']}, "
                        f"Missing Leverandoervare={diag['missing_vare_fk']}, "
                        f"Missing Faktura={diag['missing_faktura_fk']}, "
                        f"Missing ean={diag['missing_ean_fk']}, "
                        f"Missing UNSPSC={diag['missing_unspsc_fk']}"
                    )

                    # dw_ek_Fakturalinje = -1 means map.Fakturalinje had no row for this
                    # clean_code_Fakturalinje yet — this is the shared PK with
                    # dim.Fakturalinje, so a mismatch here is a data-integrity issue,
                    # not a routine "UNKNOWN" FK (map_Fakturalinje is a hard dep above).
                    if diag["missing_fakturalinje_fk"] and diag["missing_fakturalinje_fk"] > 0:
                        context.log.warning(
                            f"{diag['missing_fakturalinje_fk']} invoice lines missing from "
                            "map.Fakturalinje (dw_ek_Fakturalinje=-1) — check map_Fakturalinje ran first"
                        )

                    for key, label in [
                        ("missing_kreditor_fk", "Kreditor"),
                        ("missing_vare_fk", "Leverandoervare"),
                        ("missing_faktura_fk", "Faktura"),
                        ("missing_ean_fk", "ean"),
                        ("missing_unspsc_fk", "UNSPSC"),
                    ]:
                        if diag[key] and diag[key] > 0:
                            context.log.warning(
                                f"{diag[key]} invoice lines without {label} FK (using -1/UNKNOWN)"
                            )
                except Exception as e:
                    error_msg = f"Failed to query fact table for FK diagnostics: {str(e)}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                try:
                    cursor.execute("DROP TABLE IF EXISTS #Fakturalinje_Facts")
                except Exception as e:
                    context.log.warning(f"Failed to drop temp tables (may auto-cleanup): {e}")

                try:
                    cursor.execute("UPDATE STATISTICS [fct].[Fakturalinje]")
                except Exception as e:
                    context.log.warning(f"Failed to update statistics: {e}")

                context.log.info(f"Fact table load complete: {rows_affected} rows processed")
            finally:
                cursor.close()

        result: MaterializeResult = MaterializeResult(
            metadata={
                "rows_affected": rows_affected,
                "table": "fct.Fakturalinje",
                "grain": "dw_ek_Fakturalinje (one row per fakturalinje, from map.Fakturalinje)",
                "staging_source": "stg.fct_Fakturalinje_mysupply (Layer 1)",
                "dw_ek_lookups": (
                    "dw_ek_Kreditor, dw_ek_Leverandoervare, dw_ek_Faktura, "
                    "dw_ek_EAN_regningsmodtager from maps; "
                    "dw_ek_UNSPSC via fct_apply_fakturalinje_unspsc"
                ),
                "missing_unspsc_fk": diag.get("missing_unspsc_fk"),
                "surrogate_key": "dw_ek_Fakturalinje (shared PK sourced from map.Fakturalinje)",
            }
        )

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Fakturalinje", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "Fakturalinje")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return result
