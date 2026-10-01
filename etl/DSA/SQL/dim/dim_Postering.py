# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Postering

Sync dimension from map + add contextual attributes from SydAX staging.
Logic: Type 1 SCD - update textual information for financial postings.

Uses:
- map.Postering: dw_ek_Postering assignment via IDENTITY
- stg.dim_Postering_sydax: Deduplicated postering attributes at clean_code_Postering grain

Business key: bk_ref_ledgertrans_id
Descriptive attributes: ref_ledgertrans_id (grain/PK), ref_vend_trans_id, dokument_id,
  kreditornummer, kreditornavn, juridisk_enhed, kontonummer, faktura_nr,
  sidst_udlignet_bilagsnummer, bilagsnummer_trans, bilagsnummer_postering,
  valutakode_trans, valutakode_postering, transaktionstekst, posteringstekst

dim.Postering is the EXCLUSIVE home of these descriptors — fct.Postering keeps
only keys, date keys, measures, dw_haendelsestidspunkt and faktura_nr
(degenerate dim for the FakturaToPostering bridge).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Postering"]),
        AssetKey(["erp", "stg_dim_Postering_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Postering from map + add contextual attributes from SydAX staging",
)
def dim_Postering(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add contextual attributes from SydAX."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Postering", dagster_run_id=context.run_id, mode=config.mode)

    logged_cp: str | None = None
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Postering dimension sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Postering]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Postering entries to sync")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Postering_sydax] ps
                LEFT JOIN [map].[Postering] m
                    ON m.clean_code_Postering = ps.clean_code_Postering
                WHERE m.dw_ek_Postering IS NULL
            """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    context.log.warning(
                        f"Found {unmapped_rows} stg.dim_Postering_sydax rows without map match"
                    )

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Postering_Attributes;

                SELECT
                    m.dw_ek_Postering,
                    ps.ref_ledgertrans_id,
                    ps.bk_ref_ledgertrans_id,
                    ps.ref_vend_trans_id,
                    ps.dokument_id,
                    ps.kreditornummer,
                    ps.kreditornavn,
                    ps.juridisk_enhed,
                    ps.kontonummer,
                    ps.faktura_nr,
                    ps.sidst_udlignet_bilagsnummer,
                    ps.bilagsnummer_trans,
                    ps.bilagsnummer_postering,
                    ps.valutakode_trans,
                    ps.valutakode_postering,
                    ps.transaktionstekst,
                    ps.posteringstekst
                INTO #Postering_Attributes
                FROM [stg].[dim_Postering_sydax] ps
                LEFT JOIN [map].[Postering] m
                    ON m.clean_code_Postering = ps.clean_code_Postering
                WHERE m.dw_ek_Postering IS NOT NULL;
            """
                )

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Postering_Attributes
                ON #Postering_Attributes (dw_ek_Postering)
                """
                )
                context.log.info("Temp table #Postering_Attributes created")

                merge_sql = """
                MERGE [dim].[Postering] AS tgt
                USING #Postering_Attributes AS src
                ON tgt.dw_ek_Postering = src.dw_ek_Postering

                WHEN MATCHED AND EXISTS (
                    SELECT
                        src.ref_ledgertrans_id, src.bk_ref_ledgertrans_id, src.ref_vend_trans_id,
                        src.dokument_id, src.kreditornummer, src.kreditornavn, src.juridisk_enhed,
                        src.kontonummer, src.faktura_nr, src.sidst_udlignet_bilagsnummer,
                        src.bilagsnummer_trans, src.bilagsnummer_postering,
                        src.valutakode_trans, src.valutakode_postering,
                        src.transaktionstekst, src.posteringstekst
                    EXCEPT
                    SELECT
                        tgt.ref_ledgertrans_id, tgt.bk_ref_ledgertrans_id, tgt.ref_vend_trans_id,
                        tgt.dokument_id, tgt.kreditornummer, tgt.kreditornavn, tgt.juridisk_enhed,
                        tgt.kontonummer, tgt.faktura_nr, tgt.sidst_udlignet_bilagsnummer,
                        tgt.bilagsnummer_trans, tgt.bilagsnummer_postering,
                        tgt.valutakode_trans, tgt.valutakode_postering,
                        tgt.transaktionstekst, tgt.posteringstekst
                ) THEN
                    UPDATE SET
                        tgt.ref_ledgertrans_id = src.ref_ledgertrans_id,
                        tgt.bk_ref_ledgertrans_id = src.bk_ref_ledgertrans_id,
                        tgt.ref_vend_trans_id = src.ref_vend_trans_id,
                        tgt.dokument_id = src.dokument_id,
                        tgt.kreditornummer = src.kreditornummer,
                        tgt.kreditornavn = src.kreditornavn,
                        tgt.juridisk_enhed = src.juridisk_enhed,
                        tgt.kontonummer = src.kontonummer,
                        tgt.faktura_nr = src.faktura_nr,
                        tgt.sidst_udlignet_bilagsnummer = src.sidst_udlignet_bilagsnummer,
                        tgt.bilagsnummer_trans = src.bilagsnummer_trans,
                        tgt.bilagsnummer_postering = src.bilagsnummer_postering,
                        tgt.valutakode_trans = src.valutakode_trans,
                        tgt.valutakode_postering = src.valutakode_postering,
                        tgt.transaktionstekst = src.transaktionstekst,
                        tgt.posteringstekst = src.posteringstekst,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Postering,
                        ref_ledgertrans_id,
                        bk_ref_ledgertrans_id,
                        ref_vend_trans_id,
                        dokument_id,
                        kreditornummer,
                        kreditornavn,
                        juridisk_enhed,
                        kontonummer,
                        faktura_nr,
                        sidst_udlignet_bilagsnummer,
                        bilagsnummer_trans,
                        bilagsnummer_postering,
                        valutakode_trans,
                        valutakode_postering,
                        transaktionstekst,
                        posteringstekst,
                        dw_timestamp_insert,
                        dw_timestamp_update
                    )
                    VALUES (
                        src.dw_ek_Postering,
                        src.ref_ledgertrans_id,
                        src.bk_ref_ledgertrans_id,
                        src.ref_vend_trans_id,
                        src.dokument_id,
                        src.kreditornummer,
                        src.kreditornavn,
                        src.juridisk_enhed,
                        src.kontonummer,
                        src.faktura_nr,
                        src.sidst_udlignet_bilagsnummer,
                        src.bilagsnummer_trans,
                        src.bilagsnummer_postering,
                        src.valutakode_trans,
                        src.valutakode_postering,
                        src.transaktionstekst,
                        src.posteringstekst,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    );
            """

                insert_sql = """
            INSERT INTO [dim].[Postering] WITH (TABLOCK) (
                    dw_ek_Postering, ref_ledgertrans_id, bk_ref_ledgertrans_id,
                    ref_vend_trans_id, dokument_id, kreditornummer,
                    kreditornavn, juridisk_enhed, kontonummer,
                    faktura_nr, sidst_udlignet_bilagsnummer, bilagsnummer_trans,
                    bilagsnummer_postering, valutakode_trans, valutakode_postering,
                    transaktionstekst, posteringstekst, dw_timestamp_insert,
                    dw_timestamp_update
            )
            SELECT
                    src.dw_ek_Postering, src.ref_ledgertrans_id, src.bk_ref_ledgertrans_id,
                    src.ref_vend_trans_id, src.dokument_id, src.kreditornummer,
                    src.kreditornavn, src.juridisk_enhed, src.kontonummer,
                    src.faktura_nr, src.sidst_udlignet_bilagsnummer, src.bilagsnummer_trans,
                    src.bilagsnummer_postering, src.valutakode_trans, src.valutakode_postering,
                    src.transaktionstekst, src.posteringstekst, CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
            FROM #Postering_Attributes src;
            """

                # Short-circuit: if the target holds only the -1 sentinel (fresh or
                # rebuilt table), a bulk INSERT ... WITH (TABLOCK) can be minimally
                # logged, whereas MERGE is always fully logged. The source draws
                # dw_ek_Postering from map.Postering (IDENTITY >= 1), so it never
                # collides with the -1 sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[Postering] "
                    "WHERE dw_ek_Postering <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(merge_sql)
                else:
                    context.log.info(
                        "Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK), "
                        "minimally logged"
                    )
                    cursor.execute(insert_sql)

                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Postering_Attributes")

                cursor.execute("UPDATE STATISTICS [dim].[Postering]")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_dim_rows,
                    SUM(CASE WHEN dw_timestamp_update = dw_timestamp_insert THEN 1 ELSE 0 END) AS inserts_only,
                    SUM(CASE WHEN dw_timestamp_update > dw_timestamp_insert THEN 1 ELSE 0 END) AS updated,
                    COUNT(kreditornavn) AS has_kreditornavn,
                    COUNT(faktura_nr) AS has_faktura_nr
                FROM [dim].[Postering]
                WHERE dw_ek_Postering <> -1
            """
                )
                stats = cursor.fetchone()
                total_dim_rows = stats[0]
                inserts_only = stats[1]
                updated_count = stats[2]
                has_kreditornavn = stats[3]
                has_faktura_nr = stats[4]

                context.log.info(
                    f"Dimension sync complete: {total_dim_rows} total rows "
                    f"({inserts_only} insert-only, {updated_count} updated)"
                )
                context.log.info(
                    f"Coverage: {has_kreditornavn}/{total_dim_rows} with kreditornavn, "
                    f"{has_faktura_nr}/{total_dim_rows} with faktura_nr"
                )
            finally:
                cursor.close()

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Postering", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "Postering")

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=merge_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": merge_count,
            "total_dim_rows": total_dim_rows,
            "inserts_only": inserts_only,
            "updated": updated_count,
            "has_kreditornavn": has_kreditornavn,
            "has_faktura_nr": has_faktura_nr,
            "unmapped_stg_rows": unmapped_rows,
            "table": "dim.Postering",
            "source_tables": "stg.dim_Postering_sydax + map.Postering",
            "scd_type": "Type 1 (overwrite)",
            "checkpoint_after": logged_cp,
            "mode": config.mode,
        }
    )
