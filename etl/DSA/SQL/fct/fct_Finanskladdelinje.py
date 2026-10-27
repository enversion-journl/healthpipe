# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Finanskladdelinje

Fact table for financial journal lines with DW_EK lookups from maps.
Grain: ref_ledgertrans_id

Reads from stg.fct_Finanskladdelinje_sydax (Layer 1 staging).

dw_ek_Finanskladdelinje must resolve (no -1). Dimension FKs (Finanskladde, Kontoplan, Organisation)
may be -1 when unmatched.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="fct_Finanskladdelinje",
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_Finanskladdelinje_sydax"]),
        "map_Finanskladde",
        "map_Finanskladdelinje",
        "map_Kontoplan",
        "map_Okosted",
        "dim_Finanskladde",
        "dim_Kontoplan",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "journal_line",
    },
    description=(
        "Fact table for financial journal lines — required dw_ek_Finanskladdelinje; "
        "optional dim FKs via maps. MERGE on dw_ek_Finanskladdelinje."
    ),
)
def fct_Finanskladdelinje(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Map lookups + MERGE into fct.Finanskladdelinje on surrogate key."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct", "Finanskladdelinje", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Finanskladdelinje fact table load...")

                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_Finanskladdelinje_sydax]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging count: {staging_count}")

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Finanskladdelinje_Facts;

                SELECT
                    src.ref_ledgertrans_id,
                    COALESCE(lmap.dw_ek_Finanskladdelinje, -1) AS dw_ek_Finanskladdelinje,
                    src.bk_ref_ledgertrans_id,
                    COALESCE(kmap.dw_ek_Finanskladde, -1) AS dw_ek_Finanskladde,
                    COALESCE(kpmap.dw_ek_Kontoplan, -1) AS dw_ek_Kontoplan,
                    COALESCE(omap.dw_ek_Okosted, -1) AS dw_ek_Okosted,
                    src.debet,
                    src.kredit,
                    src.dw_ek_Dato_linje,
                    src.dato_tid_linje,
                    src.dw_ek_Dato_oprettet,
                    src.dato_tid_oprettet,
                    src.dw_haendelsestidspunkt
                INTO #Finanskladdelinje_Facts
                FROM [stg].[fct_Finanskladdelinje_sydax] src

                LEFT JOIN [map].[Finanskladdelinje] lmap
                    ON lmap.clean_code_Finanskladdelinje = src.clean_code_Finanskladdelinje
                   AND src.clean_code_Finanskladdelinje IS NOT NULL
                   AND src.clean_code_Finanskladdelinje <> ''

                LEFT JOIN [map].[Finanskladde] kmap
                    ON kmap.clean_code_Finanskladde = src.clean_code_Finanskladde
                   AND src.clean_code_Finanskladde IS NOT NULL
                   AND src.clean_code_Finanskladde <> ''

                LEFT JOIN [map].[Kontoplan] kpmap
                    ON kpmap.clean_code_Kontoplan = src.clean_code_Kontoplan
                   AND src.clean_code_Kontoplan IS NOT NULL
                   AND src.clean_code_Kontoplan <> ''

                LEFT JOIN [map].[Okosted] omap
                    ON omap.clean_code_Okosted = src.okosted_kode;
                """
                )

                cursor.execute(
                    """
                IF EXISTS (
                    SELECT 1
                    FROM #Finanskladdelinje_Facts
                    WHERE dw_ek_Finanskladdelinje = -1 OR dw_ek_Finanskladdelinje IS NULL
                )
                    THROW 50002,
                        'fct_Finanskladdelinje: unresolved dw_ek_Finanskladdelinje (-1 or NULL) after map join',
                        1;
                """
                )

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_source_rows,
                    COUNT(DISTINCT ref_ledgertrans_id) AS distinct_ids
                FROM #Finanskladdelinje_Facts
            """
                )
                source_stats = cursor.fetchone()
                duplicate_source_rows = source_stats[0] - source_stats[1]
                if duplicate_source_rows > 0:
                    message = (
                        "Duplicate grain violation in fct_Finanskladdelinje: "
                        f"{duplicate_source_rows} duplicate source rows found in "
                        "#Finanskladdelinje_Facts. Expected one row per ref_ledgertrans_id."
                    )
                    context.log.error(message)
                    raise ValueError(message)

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Finanskladdelinje_Facts
                ON #Finanskladdelinje_Facts (dw_ek_Finanskladdelinje)
                """
                )

                merge_sql = """
                MERGE [fct].[Finanskladdelinje] AS tgt
                USING #Finanskladdelinje_Facts AS src
                ON tgt.dw_ek_Finanskladdelinje = src.dw_ek_Finanskladdelinje

                WHEN MATCHED AND EXISTS (
                    SELECT
                        src.bk_ref_ledgertrans_id, src.dw_ek_Finanskladde,
                        src.dw_ek_Kontoplan, src.dw_ek_Okosted,
                        src.debet, src.kredit,
                        src.dw_ek_Dato_linje, src.dato_tid_linje,
                        src.dw_ek_Dato_oprettet, src.dato_tid_oprettet,
                        src.dw_haendelsestidspunkt
                    EXCEPT
                    SELECT
                        tgt.bk_ref_ledgertrans_id, tgt.dw_ek_Finanskladde,
                        tgt.dw_ek_Kontoplan, tgt.dw_ek_Okosted,
                        tgt.debet, tgt.kredit,
                        tgt.dw_ek_Dato_linje, tgt.dato_tid_linje,
                        tgt.dw_ek_Dato_oprettet, tgt.dato_tid_oprettet,
                        tgt.dw_haendelsestidspunkt
                ) THEN
                    UPDATE SET
                        tgt.bk_ref_ledgertrans_id = src.bk_ref_ledgertrans_id,
                        tgt.dw_ek_Finanskladde = src.dw_ek_Finanskladde,
                        tgt.dw_ek_Kontoplan = src.dw_ek_Kontoplan,
                        tgt.dw_ek_Okosted = src.dw_ek_Okosted,
                        tgt.debet = src.debet,
                        tgt.kredit = src.kredit,
                        tgt.dw_ek_Dato_linje = src.dw_ek_Dato_linje,
                        tgt.dato_tid_linje = src.dato_tid_linje,
                        tgt.dw_ek_Dato_oprettet = src.dw_ek_Dato_oprettet,
                        tgt.dato_tid_oprettet = src.dato_tid_oprettet,
                        tgt.dw_haendelsestidspunkt = src.dw_haendelsestidspunkt,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Finanskladdelinje,
                        bk_ref_ledgertrans_id,
                        dw_ek_Finanskladde,
                        dw_ek_Kontoplan,
                        dw_ek_Okosted,
                        debet,
                        kredit,
                        dw_ek_Dato_linje,
                        dato_tid_linje,
                        dw_ek_Dato_oprettet,
                        dato_tid_oprettet,
                        dw_haendelsestidspunkt,
                        dw_timestamp_insert,
                        dw_timestamp_update
                    )
                    VALUES (
                        src.dw_ek_Finanskladdelinje,
                        src.bk_ref_ledgertrans_id,
                        src.dw_ek_Finanskladde,
                        src.dw_ek_Kontoplan,
                        src.dw_ek_Okosted,
                        src.debet,
                        src.kredit,
                        src.dw_ek_Dato_linje,
                        src.dato_tid_linje,
                        src.dw_ek_Dato_oprettet,
                        src.dato_tid_oprettet,
                        src.dw_haendelsestidspunkt,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    );
            """

                insert_sql = """
                INSERT INTO [fct].[Finanskladdelinje] WITH (TABLOCK) (
                    dw_ek_Finanskladdelinje,
                    bk_ref_ledgertrans_id,
                    dw_ek_Finanskladde,
                    dw_ek_Kontoplan,
                    dw_ek_Okosted,
                    debet,
                    kredit,
                    dw_ek_Dato_linje,
                    dato_tid_linje,
                    dw_ek_Dato_oprettet,
                    dato_tid_oprettet,
                    dw_haendelsestidspunkt,
                    dw_timestamp_insert,
                    dw_timestamp_update
                )
                SELECT
                    src.dw_ek_Finanskladdelinje,
                    src.bk_ref_ledgertrans_id,
                    src.dw_ek_Finanskladde,
                    src.dw_ek_Kontoplan,
                    src.dw_ek_Okosted,
                    src.debet,
                    src.kredit,
                    src.dw_ek_Dato_linje,
                    src.dato_tid_linje,
                    src.dw_ek_Dato_oprettet,
                    src.dato_tid_oprettet,
                    src.dw_haendelsestidspunkt,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #Finanskladdelinje_Facts src;
            """

                # Short-circuit: on a fresh/rebuilt (empty) target a bulk
                # INSERT ... WITH (TABLOCK) can be minimally logged, whereas
                # MERGE is always fully logged. Facts carry no -1 sentinel, so
                # any existing row means the incremental MERGE path applies.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [fct].[Finanskladdelinje]) "
                    "THEN 1 ELSE 0 END"
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
                    SUM(CASE WHEN dw_ek_Finanskladde = -1 THEN 1 ELSE 0 END) AS missing_finanskladde_fk,
                    SUM(CASE WHEN dw_ek_Kontoplan = -1 THEN 1 ELSE 0 END) AS missing_kontoplan_fk,
                    SUM(CASE WHEN dw_ek_Okosted = -1 THEN 1 ELSE 0 END) AS missing_okosted_fk
                FROM [fct].[Finanskladdelinje]
            """
                )
                diag = cursor.fetchone()

                cursor.execute("DROP TABLE IF EXISTS #Finanskladdelinje_Facts")
                cursor.execute("UPDATE STATISTICS [fct].[Finanskladdelinje]")

                context.log.info(
                    f"Finanskladdelinje fact load complete: {rows_affected} rows — "
                    f"FK -1 counts: Finanskladde={diag[1]}, Kontoplan={diag[2]}, "
                    f"Okosted={diag[3]} (of {diag[0]} total)"
                )
            finally:
                cursor.close()

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Finanskladdelinje", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "Finanskladdelinje")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Finanskladdelinje",
            "grain": "ref_ledgertrans_id",
            "staging_source": "stg.fct_Finanskladdelinje_sydax (Layer 1)",
            "duplicate_source_rows": duplicate_source_rows,
            "missing_finanskladde_fk": diag[1],
            "missing_kontoplan_fk": diag[2],
            "missing_okosted_fk": diag[3],
        }
    )
