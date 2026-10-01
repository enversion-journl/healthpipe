# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Postering

Fact table for financial postings with DW_EK lookups from maps.
Grain: ref_ledgertrans_id (unique BIGINT from SydAX ledger)

Reads from stg.fct_Postering_sydax (Layer 1 staging) which contains
precomputed date keys, typed measures, and the grain contract
(one row per ref_ledgertrans_id).

This asset is responsible only for map lookups + MERGE + diagnostics.
Includes dw_ek_Postering lookup via map.Postering (clean_code_Postering),
dw_ek_Okosted lookup via map.Okosted (clean_code_Okosted = omkostningsstednummer),
and dw_ek_Finanskladdelinje lookup via shared ref_ledgertrans_id.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_Postering_sydax"]),
        "map_Kreditor",
        "map_Kontoplan",
        "map_Okosted",
        "map_Postering",
        "map_Finanskladdelinje",
        "dim_Kreditor",
        "dim_Kontoplan",
        "dim_Finanskladdelinje",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "posting",
    },
    description=(
        "Fact table for financial postings - reads precomputed staging columns "
        "and looks up dw_ek_Postering, dw_ek_Kreditor, dw_ek_Kontoplan, dw_ek_Faktura, "
        "dw_ek_Okosted, dw_ek_Finanskladdelinje from maps. Grain: ref_ledgertrans_id."
    ),
)
def fct_Postering(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Map lookups (incl. dw_ek_Postering) + MERGE into fct.Postering. All conversions done in staging."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("fct", "Postering", dagster_run_id=context.run_id, mode=config.mode)
    logged_cp: str | None = None

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting fact table load with DW_EK lookups...")

                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_Postering_sydax]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging count: {staging_count}")

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Postering_Facts;

                SELECT
                    src.ref_ledgertrans_id,
                    src.clean_code_Postering,
                    COALESCE(post_map.dw_ek_Postering, -1) AS dw_ek_Postering,

                    COALESCE(kred_map.dw_ek_Kreditor, -1) AS dw_ek_Kreditor,
                    COALESCE(konto_map.dw_ek_Kontoplan, -1) AS dw_ek_Kontoplan,
                    CAST(-1 AS INT) AS dw_ek_Faktura,
                    COALESCE(oko_map.dw_ek_Okosted, -1) AS dw_ek_Okosted,
                    COALESCE(fkl_map.dw_ek_Finanskladdelinje, -1) AS dw_ek_Finanskladdelinje,

                    src.dw_ek_Dato_godkendelse,
                    src.dato_tid_godkendelse,
                    src.dw_ek_Dato_lukket,
                    src.dato_tid_lukket,
                    src.dw_ek_Dato_trans_oprettet,
                    src.dato_tid_trans_oprettet,
                    src.dw_ek_Dato_forfald,
                    src.dato_tid_forfald,
                    src.dw_ek_Dato_journal_faktura,
                    src.dato_tid_journal_faktura,
                    src.dw_ek_Dato_sidst_udlignet,
                    src.dato_tid_sidst_udlignet,
                    src.dw_ek_Dato_transaktion,
                    src.dato_tid_transaktion,
                    src.dw_ek_Dato_bogfoering,
                    src.dato_tid_bogfoering,
                    src.dw_ek_Dato_post_oprettet,
                    src.dato_tid_post_oprettet,
                    src.transaktionsbeloeb_valuta,
                    src.transaktionsbeloeb,
                    src.udlignet_beloeb_valuta,
                    src.udlignet_beloeb,
                    src.posteringsbeloeb,
                    src.faktura_nr,
                    src.dw_haendelsestidspunkt

                INTO #Postering_Facts
                FROM [stg].[fct_Postering_sydax] src

                LEFT JOIN [map].[Kreditor] kred_map
                    ON kred_map.clean_code_Kreditor = src.clean_code_Kreditor
                    AND src.clean_code_Kreditor IS NOT NULL
                    AND src.clean_code_Kreditor <> ''

                LEFT JOIN [map].[Kontoplan] konto_map
                    ON konto_map.bk_finanskonto_id = src.bk_finanskonto_id

                LEFT JOIN [map].[Okosted] oko_map
                    ON oko_map.clean_code_Okosted = src.omkostningsstednummer
                   AND src.omkostningsstednummer IS NOT NULL
                   AND src.omkostningsstednummer <> ''

                LEFT JOIN [map].[Postering] post_map
                    ON post_map.clean_code_Postering = src.clean_code_Postering
                   AND src.clean_code_Postering IS NOT NULL
                   AND src.clean_code_Postering <> ''

                LEFT JOIN [map].[Finanskladdelinje] fkl_map
                    ON fkl_map.clean_code_Finanskladdelinje = src.clean_code_Finanskladdelinje
                   AND src.clean_code_Finanskladdelinje IS NOT NULL
                   AND src.clean_code_Finanskladdelinje <> '';
                """
                )

                cursor.execute(
                    "SELECT COUNT(*) AS total, COUNT(DISTINCT ref_ledgertrans_id) AS distinct_ids "
                    "FROM #Postering_Facts"
                )
                total_rows, distinct_ids = cursor.fetchone()
                duplicate_source_rows = total_rows - distinct_ids
                if duplicate_source_rows > 0:
                    message = (
                        "Duplicate grain violation in fct_Postering: "
                        f"{duplicate_source_rows} duplicate source rows in #Postering_Facts. "
                        "Expected one row per ref_ledgertrans_id."
                    )
                    context.log.error(message)
                    raise ValueError(message)

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Postering_Facts
                ON #Postering_Facts (ref_ledgertrans_id)
                """
                )

                merge_sql = """
                MERGE [fct].[Postering] AS tgt
                USING #Postering_Facts AS src
                ON tgt.ref_ledgertrans_id = src.ref_ledgertrans_id

                WHEN MATCHED AND EXISTS (
                    SELECT
                        src.dw_ek_Postering, src.clean_code_Postering,
                        src.dw_ek_Kreditor, src.dw_ek_Kontoplan,
                        src.dw_ek_Okosted, src.dw_ek_Finanskladdelinje,
                        src.dw_ek_Dato_godkendelse, src.dato_tid_godkendelse,
                        src.dw_ek_Dato_lukket, src.dato_tid_lukket,
                        src.dw_ek_Dato_trans_oprettet, src.dato_tid_trans_oprettet,
                        src.dw_ek_Dato_forfald, src.dato_tid_forfald,
                        src.dw_ek_Dato_journal_faktura, src.dato_tid_journal_faktura,
                        src.dw_ek_Dato_sidst_udlignet, src.dato_tid_sidst_udlignet,
                        src.dw_ek_Dato_transaktion, src.dato_tid_transaktion,
                        src.dw_ek_Dato_bogfoering, src.dato_tid_bogfoering,
                        src.dw_ek_Dato_post_oprettet, src.dato_tid_post_oprettet,
                        src.transaktionsbeloeb_valuta, src.transaktionsbeloeb,
                        src.udlignet_beloeb_valuta, src.udlignet_beloeb,
                        src.posteringsbeloeb, src.faktura_nr, src.dw_haendelsestidspunkt
                    EXCEPT
                    SELECT
                        tgt.dw_ek_Postering, tgt.clean_code_Postering,
                        tgt.dw_ek_Kreditor, tgt.dw_ek_Kontoplan,
                        tgt.dw_ek_Okosted, tgt.dw_ek_Finanskladdelinje,
                        tgt.dw_ek_Dato_godkendelse, tgt.dato_tid_godkendelse,
                        tgt.dw_ek_Dato_lukket, tgt.dato_tid_lukket,
                        tgt.dw_ek_Dato_trans_oprettet, tgt.dato_tid_trans_oprettet,
                        tgt.dw_ek_Dato_forfald, tgt.dato_tid_forfald,
                        tgt.dw_ek_Dato_journal_faktura, tgt.dato_tid_journal_faktura,
                        tgt.dw_ek_Dato_sidst_udlignet, tgt.dato_tid_sidst_udlignet,
                        tgt.dw_ek_Dato_transaktion, tgt.dato_tid_transaktion,
                        tgt.dw_ek_Dato_bogfoering, tgt.dato_tid_bogfoering,
                        tgt.dw_ek_Dato_post_oprettet, tgt.dato_tid_post_oprettet,
                        tgt.transaktionsbeloeb_valuta, tgt.transaktionsbeloeb,
                        tgt.udlignet_beloeb_valuta, tgt.udlignet_beloeb,
                        tgt.posteringsbeloeb, tgt.faktura_nr, tgt.dw_haendelsestidspunkt
                ) THEN
                    UPDATE SET
                        tgt.dw_ek_Postering = src.dw_ek_Postering,
                        tgt.clean_code_Postering = src.clean_code_Postering,
                        tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
                        tgt.dw_ek_Kontoplan = src.dw_ek_Kontoplan,
                        tgt.dw_ek_Okosted = src.dw_ek_Okosted,
                        tgt.dw_ek_Finanskladdelinje = src.dw_ek_Finanskladdelinje,
                        tgt.dw_ek_Dato_godkendelse = src.dw_ek_Dato_godkendelse,
                        tgt.dato_tid_godkendelse = src.dato_tid_godkendelse,
                        tgt.dw_ek_Dato_lukket = src.dw_ek_Dato_lukket,
                        tgt.dato_tid_lukket = src.dato_tid_lukket,
                        tgt.dw_ek_Dato_trans_oprettet = src.dw_ek_Dato_trans_oprettet,
                        tgt.dato_tid_trans_oprettet = src.dato_tid_trans_oprettet,
                        tgt.dw_ek_Dato_forfald = src.dw_ek_Dato_forfald,
                        tgt.dato_tid_forfald = src.dato_tid_forfald,
                        tgt.dw_ek_Dato_journal_faktura = src.dw_ek_Dato_journal_faktura,
                        tgt.dato_tid_journal_faktura = src.dato_tid_journal_faktura,
                        tgt.dw_ek_Dato_sidst_udlignet = src.dw_ek_Dato_sidst_udlignet,
                        tgt.dato_tid_sidst_udlignet = src.dato_tid_sidst_udlignet,
                        tgt.dw_ek_Dato_transaktion = src.dw_ek_Dato_transaktion,
                        tgt.dato_tid_transaktion = src.dato_tid_transaktion,
                        tgt.dw_ek_Dato_bogfoering = src.dw_ek_Dato_bogfoering,
                        tgt.dato_tid_bogfoering = src.dato_tid_bogfoering,
                        tgt.dw_ek_Dato_post_oprettet = src.dw_ek_Dato_post_oprettet,
                        tgt.dato_tid_post_oprettet = src.dato_tid_post_oprettet,
                        tgt.transaktionsbeloeb_valuta = src.transaktionsbeloeb_valuta,
                        tgt.transaktionsbeloeb = src.transaktionsbeloeb,
                        tgt.udlignet_beloeb_valuta = src.udlignet_beloeb_valuta,
                        tgt.udlignet_beloeb = src.udlignet_beloeb,
                        tgt.posteringsbeloeb = src.posteringsbeloeb,
                        tgt.faktura_nr = src.faktura_nr,
                        tgt.dw_haendelsestidspunkt = src.dw_haendelsestidspunkt,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        ref_ledgertrans_id, dw_ek_Postering, clean_code_Postering,
                        dw_ek_Kreditor, dw_ek_Kontoplan, dw_ek_Faktura, dw_ek_Okosted,
                        dw_ek_Finanskladdelinje,
                        dw_ek_Dato_godkendelse, dato_tid_godkendelse,
                        dw_ek_Dato_lukket, dato_tid_lukket,
                        dw_ek_Dato_trans_oprettet, dato_tid_trans_oprettet,
                        dw_ek_Dato_forfald, dato_tid_forfald,
                        dw_ek_Dato_journal_faktura, dato_tid_journal_faktura,
                        dw_ek_Dato_sidst_udlignet, dato_tid_sidst_udlignet,
                        dw_ek_Dato_transaktion, dato_tid_transaktion,
                        dw_ek_Dato_bogfoering, dato_tid_bogfoering,
                        dw_ek_Dato_post_oprettet, dato_tid_post_oprettet,
                        transaktionsbeloeb_valuta, transaktionsbeloeb, udlignet_beloeb_valuta, udlignet_beloeb,
                        posteringsbeloeb, faktura_nr, dw_haendelsestidspunkt,
                        dw_timestamp_insert, dw_timestamp_update
                    )
                    VALUES (
                        src.ref_ledgertrans_id, src.dw_ek_Postering, src.clean_code_Postering,
                        src.dw_ek_Kreditor, src.dw_ek_Kontoplan, src.dw_ek_Faktura, src.dw_ek_Okosted,
                        src.dw_ek_Finanskladdelinje,
                        src.dw_ek_Dato_godkendelse, src.dato_tid_godkendelse,
                        src.dw_ek_Dato_lukket, src.dato_tid_lukket,
                        src.dw_ek_Dato_trans_oprettet, src.dato_tid_trans_oprettet,
                        src.dw_ek_Dato_forfald, src.dato_tid_forfald,
                        src.dw_ek_Dato_journal_faktura, src.dato_tid_journal_faktura,
                        src.dw_ek_Dato_sidst_udlignet, src.dato_tid_sidst_udlignet,
                        src.dw_ek_Dato_transaktion, src.dato_tid_transaktion,
                        src.dw_ek_Dato_bogfoering, src.dato_tid_bogfoering,
                        src.dw_ek_Dato_post_oprettet, src.dato_tid_post_oprettet,
                        src.transaktionsbeloeb_valuta, src.transaktionsbeloeb, src.udlignet_beloeb_valuta, src.udlignet_beloeb,
                        src.posteringsbeloeb, src.faktura_nr, src.dw_haendelsestidspunkt,
                        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                    );
                """

                insert_sql = """
                INSERT INTO [fct].[Postering] WITH (TABLOCK) (
                    ref_ledgertrans_id, dw_ek_Postering, clean_code_Postering,
                    dw_ek_Kreditor, dw_ek_Kontoplan, dw_ek_Faktura, dw_ek_Okosted,
                    dw_ek_Finanskladdelinje,
                    dw_ek_Dato_godkendelse, dato_tid_godkendelse,
                    dw_ek_Dato_lukket, dato_tid_lukket,
                    dw_ek_Dato_trans_oprettet, dato_tid_trans_oprettet,
                    dw_ek_Dato_forfald, dato_tid_forfald,
                    dw_ek_Dato_journal_faktura, dato_tid_journal_faktura,
                    dw_ek_Dato_sidst_udlignet, dato_tid_sidst_udlignet,
                    dw_ek_Dato_transaktion, dato_tid_transaktion,
                    dw_ek_Dato_bogfoering, dato_tid_bogfoering,
                    dw_ek_Dato_post_oprettet, dato_tid_post_oprettet,
                    transaktionsbeloeb_valuta, transaktionsbeloeb, udlignet_beloeb_valuta, udlignet_beloeb,
                    posteringsbeloeb, faktura_nr, dw_haendelsestidspunkt,
                    dw_timestamp_insert, dw_timestamp_update
                )
                SELECT
                    src.ref_ledgertrans_id, src.dw_ek_Postering, src.clean_code_Postering,
                    src.dw_ek_Kreditor, src.dw_ek_Kontoplan, src.dw_ek_Faktura, src.dw_ek_Okosted,
                    src.dw_ek_Finanskladdelinje,
                    src.dw_ek_Dato_godkendelse, src.dato_tid_godkendelse,
                    src.dw_ek_Dato_lukket, src.dato_tid_lukket,
                    src.dw_ek_Dato_trans_oprettet, src.dato_tid_trans_oprettet,
                    src.dw_ek_Dato_forfald, src.dato_tid_forfald,
                    src.dw_ek_Dato_journal_faktura, src.dato_tid_journal_faktura,
                    src.dw_ek_Dato_sidst_udlignet, src.dato_tid_sidst_udlignet,
                    src.dw_ek_Dato_transaktion, src.dato_tid_transaktion,
                    src.dw_ek_Dato_bogfoering, src.dato_tid_bogfoering,
                    src.dw_ek_Dato_post_oprettet, src.dato_tid_post_oprettet,
                    src.transaktionsbeloeb_valuta, src.transaktionsbeloeb, src.udlignet_beloeb_valuta, src.udlignet_beloeb,
                    src.posteringsbeloeb, src.faktura_nr, src.dw_haendelsestidspunkt,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                FROM #Postering_Facts src;
                """

                # Short-circuit: fct.Postering has no -1 sentinel row, so on a fresh
                # or rebuilt (empty) target a bulk INSERT ... WITH (TABLOCK) can be
                # minimally logged, whereas MERGE is always fully logged.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [fct].[Postering]) " "THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(merge_sql)
                else:
                    context.log.info("Target empty - bulk INSERT WITH (TABLOCK), minimally logged")
                    cursor.execute(insert_sql)

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_facts,
                    SUM(CASE WHEN dw_ek_Postering = -1 THEN 1 ELSE 0 END) AS missing_postering_fk,
                    SUM(CASE WHEN dw_ek_Kreditor = -1 THEN 1 ELSE 0 END) AS missing_kreditor_fk,
                    SUM(CASE WHEN dw_ek_Kontoplan = -1 THEN 1 ELSE 0 END) AS missing_kontoplan_fk,
                    SUM(CASE WHEN dw_ek_Faktura = -1 THEN 1 ELSE 0 END) AS missing_faktura_fk,
                    SUM(CASE WHEN dw_ek_Okosted = -1 THEN 1 ELSE 0 END) AS missing_okosted_fk,
                    SUM(CASE WHEN dw_ek_Finanskladdelinje = -1 THEN 1 ELSE 0 END) AS missing_finanskladdelinje_fk
                FROM [fct].[Postering]
                """
                )
                diag = cursor.fetchone()

                context.log.info(
                    f"FK Matching: Total={diag[0]}, Missing Postering={diag[1]}, "
                    f"Missing Kreditor={diag[2]}, Missing Kontoplan={diag[3]}, "
                    f"Missing Faktura={diag[4]}, Missing Okosted={diag[5]}, "
                    f"Missing Finanskladdelinje={diag[6]}"
                )

                cursor.execute("DROP TABLE IF EXISTS #Postering_Facts")
                cursor.execute("UPDATE STATISTICS [fct].[Postering]")

                context.log.info(f"Fact table load complete: {rows_affected} rows processed")
            finally:
                cursor.close()

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Postering", mode=config.mode)
            logged_cp = dfm.get_checkpoint("fct", "Postering")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Postering",
            "grain": "ref_ledgertrans_id",
            "staging_source": "stg.fct_Postering_sydax (Layer 1)",
            "dw_ek_lookups": "Postering, Kreditor, Kontoplan, Faktura, Okosted, Finanskladdelinje",
            "duplicate_source_rows": duplicate_source_rows,
            "checkpoint_after": logged_cp,
            "mode": config.mode,
        }
    )
