# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Fakturalinje

Sync dimension from map + add textual attributes from MySupply staging.
Logic: Type 1 SCD - update textual information for invoice lines.

Uses:
- map.Fakturalinje: dw_ek_Fakturalinje assignment via IDENTITY
- stg.dim_Fakturalinje_mysupply: Deduplicated fakturalinje attributes at CleanCode grain
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Fakturalinje"]),
        AssetKey(["erp", "stg_dim_Fakturalinje_mysupply"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Fakturalinje from map + add textual attributes from MySupply staging",
)
def dim_Fakturalinje(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add textual attributes from MySupply."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "Fakturalinje", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Fakturalinje dimension sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Fakturalinje]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Fakturalinje entries to sync")

                cursor.execute(
                    """
            SELECT COUNT(*)
            FROM [stg].[dim_Fakturalinje_mysupply] ps
            LEFT JOIN [map].[Fakturalinje] m
                ON m.clean_code_Fakturalinje = ps.clean_code_Fakturalinje
            WHERE m.dw_ek_Fakturalinje IS NULL
        """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    context.log.warning(
                        f"Found {unmapped_rows} stg.dim_Fakturalinje_mysupply rows without map match"
                    )

                cursor.execute(
                    """
            DROP TABLE IF EXISTS #Fakturalinje_Attributes;

            SELECT
                m.dw_ek_Fakturalinje,
                LEFT(ps.item_faktura_udsteder_vare_nr, 255) AS item_faktura_udsteder_vare_nr,
                ps.item_faktura_udsteder_vare_navn,
                ps.faktura_linje_nr,
                ps.antal_enheder_faktureret_type,
                ps.item_faktura_udstederens_vare_nr,
                ps.item_faktura_udstederens_vare_nr_type,
                ps.base_price_enheds_stoerrelse,
                ps.base_price_enheds_stoerrelse_type,
                ps.item_faktura_udsteder_vare_navn_kort,
                ps.line_dimensions_konto_streng,
                ps.leverance_krav,
                ps.leverings_dato_2,
                ps.item_kundens_vare_nr,
                ps.item_kundens_vare_nr_type,
                ps.item_kundens_vare_beskrivelse,
                ps.item_faktura_udstederens_vare_beskrivelse,
                ps.item_fabrikantens_vare_nr,
                ps.item_fabrikantens_vare_nr_type,
                ps.item_fabrikantens_vare_beskrivelse,
                ps.item_standardiseret_vare_nr,
                ps.item_standardiseret_vare_nr_type,
                ps.item_standardiseret_vare_beskrivelse,
                ps.item_vare_klassifikation,
                ps.item_vare_klassifikation_type,
                ps.item_afgifts_kategori,
                ps.item_afgifts_kategori_procent,
                ps.faktura_udsteder_gebyr_rabat_aarsag,
                ps.faktura_udsteder_gebyr_rabat_kategori,
                ps.faktura_udsteder_gebyr_rabat_finanskonto,
                ps.faktura_udsteder_gebyr_tillaeg_aarsag,
                ps.faktura_udsteder_gebyr_tillaeg_kategori,
                ps.faktura_udsteder_gebyr_tillaeg_finanskonto
            INTO #Fakturalinje_Attributes
            FROM [stg].[dim_Fakturalinje_mysupply] ps
            LEFT JOIN [map].[Fakturalinje] m
                ON m.clean_code_Fakturalinje = ps.clean_code_Fakturalinje
            WHERE m.dw_ek_Fakturalinje IS NOT NULL;
        """
                )

                cursor.execute(
                    """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Fakturalinje_Attributes
            ON #Fakturalinje_Attributes (dw_ek_Fakturalinje)
            """
                )
                context.log.info("Temp table #Fakturalinje_Attributes created")

                merge_sql = """
            MERGE [dim].[Fakturalinje] WITH (TABLOCK) AS tgt
            USING #Fakturalinje_Attributes AS src
            ON tgt.dw_ek_Fakturalinje = src.dw_ek_Fakturalinje

            WHEN MATCHED AND EXISTS (
                SELECT
                    src.item_faktura_udsteder_vare_nr, src.item_faktura_udsteder_vare_navn,
                    src.faktura_linje_nr, src.antal_enheder_faktureret_type,
                    src.item_faktura_udstederens_vare_nr, src.item_faktura_udstederens_vare_nr_type,
                    src.base_price_enheds_stoerrelse, src.base_price_enheds_stoerrelse_type,
                    src.item_faktura_udsteder_vare_navn_kort,
                    src.line_dimensions_konto_streng, src.leverance_krav, src.leverings_dato_2,
                    src.item_kundens_vare_nr, src.item_kundens_vare_nr_type, src.item_kundens_vare_beskrivelse,
                    src.item_faktura_udstederens_vare_beskrivelse,
                    src.item_fabrikantens_vare_nr, src.item_fabrikantens_vare_nr_type, src.item_fabrikantens_vare_beskrivelse,
                    src.item_standardiseret_vare_nr, src.item_standardiseret_vare_nr_type, src.item_standardiseret_vare_beskrivelse,
                    src.item_vare_klassifikation, src.item_vare_klassifikation_type,
                    src.item_afgifts_kategori, src.item_afgifts_kategori_procent,
                    src.faktura_udsteder_gebyr_rabat_aarsag, src.faktura_udsteder_gebyr_rabat_kategori,
                    src.faktura_udsteder_gebyr_rabat_finanskonto,
                    src.faktura_udsteder_gebyr_tillaeg_aarsag, src.faktura_udsteder_gebyr_tillaeg_kategori,
                    src.faktura_udsteder_gebyr_tillaeg_finanskonto
                EXCEPT
                SELECT
                    tgt.item_faktura_udsteder_vare_nr, tgt.item_faktura_udsteder_vare_navn,
                    tgt.faktura_linje_nr, tgt.antal_enheder_faktureret_type,
                    tgt.item_faktura_udstederens_vare_nr, tgt.item_faktura_udstederens_vare_nr_type,
                    tgt.base_price_enheds_stoerrelse, tgt.base_price_enheds_stoerrelse_type,
                    tgt.item_faktura_udsteder_vare_navn_kort,
                    tgt.line_dimensions_konto_streng, tgt.leverance_krav, tgt.leverings_dato_2,
                    tgt.item_kundens_vare_nr, tgt.item_kundens_vare_nr_type, tgt.item_kundens_vare_beskrivelse,
                    tgt.item_faktura_udstederens_vare_beskrivelse,
                    tgt.item_fabrikantens_vare_nr, tgt.item_fabrikantens_vare_nr_type, tgt.item_fabrikantens_vare_beskrivelse,
                    tgt.item_standardiseret_vare_nr, tgt.item_standardiseret_vare_nr_type, tgt.item_standardiseret_vare_beskrivelse,
                    tgt.item_vare_klassifikation, tgt.item_vare_klassifikation_type,
                    tgt.item_afgifts_kategori, tgt.item_afgifts_kategori_procent,
                    tgt.faktura_udsteder_gebyr_rabat_aarsag, tgt.faktura_udsteder_gebyr_rabat_kategori,
                    tgt.faktura_udsteder_gebyr_rabat_finanskonto,
                    tgt.faktura_udsteder_gebyr_tillaeg_aarsag, tgt.faktura_udsteder_gebyr_tillaeg_kategori,
                    tgt.faktura_udsteder_gebyr_tillaeg_finanskonto
            ) THEN
                UPDATE SET
                    tgt.item_faktura_udsteder_vare_nr = src.item_faktura_udsteder_vare_nr,
                    tgt.item_faktura_udsteder_vare_navn = src.item_faktura_udsteder_vare_navn,
                    tgt.faktura_linje_nr = src.faktura_linje_nr,
                    tgt.antal_enheder_faktureret_type = src.antal_enheder_faktureret_type,
                    tgt.item_faktura_udstederens_vare_nr = src.item_faktura_udstederens_vare_nr,
                    tgt.item_faktura_udstederens_vare_nr_type = src.item_faktura_udstederens_vare_nr_type,
                    tgt.base_price_enheds_stoerrelse = src.base_price_enheds_stoerrelse,
                    tgt.base_price_enheds_stoerrelse_type = src.base_price_enheds_stoerrelse_type,
                    tgt.item_faktura_udsteder_vare_navn_kort = src.item_faktura_udsteder_vare_navn_kort,
                    tgt.line_dimensions_konto_streng = src.line_dimensions_konto_streng,
                    tgt.leverance_krav = src.leverance_krav,
                    tgt.leverings_dato_2 = src.leverings_dato_2,
                    tgt.item_kundens_vare_nr = src.item_kundens_vare_nr,
                    tgt.item_kundens_vare_nr_type = src.item_kundens_vare_nr_type,
                    tgt.item_kundens_vare_beskrivelse = src.item_kundens_vare_beskrivelse,
                    tgt.item_faktura_udstederens_vare_beskrivelse = src.item_faktura_udstederens_vare_beskrivelse,
                    tgt.item_fabrikantens_vare_nr = src.item_fabrikantens_vare_nr,
                    tgt.item_fabrikantens_vare_nr_type = src.item_fabrikantens_vare_nr_type,
                    tgt.item_fabrikantens_vare_beskrivelse = src.item_fabrikantens_vare_beskrivelse,
                    tgt.item_standardiseret_vare_nr = src.item_standardiseret_vare_nr,
                    tgt.item_standardiseret_vare_nr_type = src.item_standardiseret_vare_nr_type,
                    tgt.item_standardiseret_vare_beskrivelse = src.item_standardiseret_vare_beskrivelse,
                    tgt.item_vare_klassifikation = src.item_vare_klassifikation,
                    tgt.item_vare_klassifikation_type = src.item_vare_klassifikation_type,
                    tgt.item_afgifts_kategori = src.item_afgifts_kategori,
                    tgt.item_afgifts_kategori_procent = src.item_afgifts_kategori_procent,
                    tgt.faktura_udsteder_gebyr_rabat_aarsag = src.faktura_udsteder_gebyr_rabat_aarsag,
                    tgt.faktura_udsteder_gebyr_rabat_kategori = src.faktura_udsteder_gebyr_rabat_kategori,
                    tgt.faktura_udsteder_gebyr_rabat_finanskonto = src.faktura_udsteder_gebyr_rabat_finanskonto,
                    tgt.faktura_udsteder_gebyr_tillaeg_aarsag = src.faktura_udsteder_gebyr_tillaeg_aarsag,
                    tgt.faktura_udsteder_gebyr_tillaeg_kategori = src.faktura_udsteder_gebyr_tillaeg_kategori,
                    tgt.faktura_udsteder_gebyr_tillaeg_finanskonto = src.faktura_udsteder_gebyr_tillaeg_finanskonto,
                    tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    dw_ek_Fakturalinje, item_faktura_udsteder_vare_nr, item_faktura_udsteder_vare_navn,
                    faktura_linje_nr, antal_enheder_faktureret_type,
                    item_faktura_udstederens_vare_nr, item_faktura_udstederens_vare_nr_type,
                    base_price_enheds_stoerrelse, base_price_enheds_stoerrelse_type,
                    item_faktura_udsteder_vare_navn_kort,
                    line_dimensions_konto_streng, leverance_krav, leverings_dato_2,
                    item_kundens_vare_nr, item_kundens_vare_nr_type, item_kundens_vare_beskrivelse,
                    item_faktura_udstederens_vare_beskrivelse,
                    item_fabrikantens_vare_nr, item_fabrikantens_vare_nr_type, item_fabrikantens_vare_beskrivelse,
                    item_standardiseret_vare_nr, item_standardiseret_vare_nr_type, item_standardiseret_vare_beskrivelse,
                    item_vare_klassifikation, item_vare_klassifikation_type,
                    item_afgifts_kategori, item_afgifts_kategori_procent,
                    faktura_udsteder_gebyr_rabat_aarsag, faktura_udsteder_gebyr_rabat_kategori,
                    faktura_udsteder_gebyr_rabat_finanskonto,
                    faktura_udsteder_gebyr_tillaeg_aarsag, faktura_udsteder_gebyr_tillaeg_kategori,
                    faktura_udsteder_gebyr_tillaeg_finanskonto,
                    dw_timestamp_insert, dw_timestamp_update
                )
                VALUES (
                    src.dw_ek_Fakturalinje, src.item_faktura_udsteder_vare_nr, src.item_faktura_udsteder_vare_navn,
                    src.faktura_linje_nr, src.antal_enheder_faktureret_type,
                    src.item_faktura_udstederens_vare_nr, src.item_faktura_udstederens_vare_nr_type,
                    src.base_price_enheds_stoerrelse, src.base_price_enheds_stoerrelse_type,
                    src.item_faktura_udsteder_vare_navn_kort,
                    src.line_dimensions_konto_streng, src.leverance_krav, src.leverings_dato_2,
                    src.item_kundens_vare_nr, src.item_kundens_vare_nr_type, src.item_kundens_vare_beskrivelse,
                    src.item_faktura_udstederens_vare_beskrivelse,
                    src.item_fabrikantens_vare_nr, src.item_fabrikantens_vare_nr_type, src.item_fabrikantens_vare_beskrivelse,
                    src.item_standardiseret_vare_nr, src.item_standardiseret_vare_nr_type, src.item_standardiseret_vare_beskrivelse,
                    src.item_vare_klassifikation, src.item_vare_klassifikation_type,
                    src.item_afgifts_kategori, src.item_afgifts_kategori_procent,
                    src.faktura_udsteder_gebyr_rabat_aarsag, src.faktura_udsteder_gebyr_rabat_kategori,
                    src.faktura_udsteder_gebyr_rabat_finanskonto,
                    src.faktura_udsteder_gebyr_tillaeg_aarsag, src.faktura_udsteder_gebyr_tillaeg_kategori,
                    src.faktura_udsteder_gebyr_tillaeg_finanskonto,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                );
        """

                insert_sql = """
            INSERT INTO [dim].[Fakturalinje] WITH (TABLOCK) (
                    dw_ek_Fakturalinje, item_faktura_udsteder_vare_nr, item_faktura_udsteder_vare_navn,
                    faktura_linje_nr, antal_enheder_faktureret_type, item_faktura_udstederens_vare_nr,
                    item_faktura_udstederens_vare_nr_type, base_price_enheds_stoerrelse, base_price_enheds_stoerrelse_type,
                    item_faktura_udsteder_vare_navn_kort, line_dimensions_konto_streng, leverance_krav,
                    leverings_dato_2, item_kundens_vare_nr, item_kundens_vare_nr_type,
                    item_kundens_vare_beskrivelse, item_faktura_udstederens_vare_beskrivelse, item_fabrikantens_vare_nr,
                    item_fabrikantens_vare_nr_type, item_fabrikantens_vare_beskrivelse, item_standardiseret_vare_nr,
                    item_standardiseret_vare_nr_type, item_standardiseret_vare_beskrivelse, item_vare_klassifikation,
                    item_vare_klassifikation_type, item_afgifts_kategori, item_afgifts_kategori_procent,
                    faktura_udsteder_gebyr_rabat_aarsag, faktura_udsteder_gebyr_rabat_kategori, faktura_udsteder_gebyr_rabat_finanskonto,
                    faktura_udsteder_gebyr_tillaeg_aarsag, faktura_udsteder_gebyr_tillaeg_kategori, faktura_udsteder_gebyr_tillaeg_finanskonto,
                    dw_timestamp_insert, dw_timestamp_update
            )
            SELECT
                    src.dw_ek_Fakturalinje, src.item_faktura_udsteder_vare_nr, src.item_faktura_udsteder_vare_navn,
                    src.faktura_linje_nr, src.antal_enheder_faktureret_type, src.item_faktura_udstederens_vare_nr,
                    src.item_faktura_udstederens_vare_nr_type, src.base_price_enheds_stoerrelse, src.base_price_enheds_stoerrelse_type,
                    src.item_faktura_udsteder_vare_navn_kort, src.line_dimensions_konto_streng, src.leverance_krav,
                    src.leverings_dato_2, src.item_kundens_vare_nr, src.item_kundens_vare_nr_type,
                    src.item_kundens_vare_beskrivelse, src.item_faktura_udstederens_vare_beskrivelse, src.item_fabrikantens_vare_nr,
                    src.item_fabrikantens_vare_nr_type, src.item_fabrikantens_vare_beskrivelse, src.item_standardiseret_vare_nr,
                    src.item_standardiseret_vare_nr_type, src.item_standardiseret_vare_beskrivelse, src.item_vare_klassifikation,
                    src.item_vare_klassifikation_type, src.item_afgifts_kategori, src.item_afgifts_kategori_procent,
                    src.faktura_udsteder_gebyr_rabat_aarsag, src.faktura_udsteder_gebyr_rabat_kategori, src.faktura_udsteder_gebyr_rabat_finanskonto,
                    src.faktura_udsteder_gebyr_tillaeg_aarsag, src.faktura_udsteder_gebyr_tillaeg_kategori, src.faktura_udsteder_gebyr_tillaeg_finanskonto,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
            FROM #Fakturalinje_Attributes src;
        """

                # Short-circuit: if the target holds only the -1 sentinel (fresh or
                # rebuilt table), a bulk INSERT ... WITH (TABLOCK) can be minimally
                # logged, whereas MERGE is always fully logged. The source draws
                # dw_ek_Fakturalinje from map.Fakturalinje (IDENTITY >= 1), so it
                # never collides with the -1 sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[Fakturalinje] "
                    "WHERE dw_ek_Fakturalinje <> -1) THEN 1 ELSE 0 END"
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

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Fakturalinje_Attributes")

                cursor.execute("UPDATE STATISTICS [dim].[Fakturalinje]")

                cursor.execute(
                    """
            SELECT
                COUNT(*) AS total_dim_rows,
                SUM(CASE WHEN dw_timestamp_update IS NULL THEN 1 ELSE 0 END) AS inserts_only,
                SUM(CASE WHEN dw_timestamp_update IS NOT NULL THEN 1 ELSE 0 END) AS updated,
                COUNT(item_faktura_udsteder_vare_navn) AS has_vare_navn
            FROM [dim].[Fakturalinje]
            WHERE dw_ek_Fakturalinje <> -1
        """
                )
                stats = cursor.fetchone()
                total_dim_rows = stats[0]
                inserts_only = stats[1]
                updated_count = stats[2]
                has_vare_navn = stats[3]

                context.log.info(
                    f"Dimension sync complete: {total_dim_rows} total rows "
                    f"({inserts_only} insert-only, {updated_count} updated)"
                )
                context.log.info(f"Coverage: {has_vare_navn}/{total_dim_rows} with VareNavn")
            finally:
                cursor.close()

        result = MaterializeResult(
            metadata={
                "rows_affected": rows_affected,
                "total_dim_rows": total_dim_rows,
                "inserts_only": inserts_only,
                "updated": updated_count,
                "has_vare_navn": has_vare_navn,
                "unmapped_stg_rows": unmapped_rows,
                "table": "dim.Fakturalinje",
                "source_tables": "stg.dim_Fakturalinje_mysupply + map.Fakturalinje",
                "scd_type": "Type 1 (overwrite)",
            }
        )

        logged_cp: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Fakturalinje", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "Fakturalinje")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return result
