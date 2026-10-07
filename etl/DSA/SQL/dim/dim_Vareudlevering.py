# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Vareudlevering

Transaction dimension: dw_ek_Vareudlevering is the shared PK with
fct.Vareudlevering, resolved from map.Vareudlevering (same pattern as
fct.Faktura/dim.Faktura and fct.Fakturalinje/dim.Fakturalinje).

Enriches the fact's grain with descriptive attributes from the source.

Type 1 SCD — overwrite on change.

Uses:
- map.Vareudlevering: dw_ek_Vareudlevering assignment via IDENTITY
- stg.dim_Vareudlevering_ils: Dimension-layer attributes (identifiers + descriptives)
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Vareudlevering"]),
        AssetKey(["erp", "stg_dim_Vareudlevering_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Vareudlevering from map + add attributes from ILS dimension staging",
)
def dim_Vareudlevering(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add attributes from ILS dimension staging."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "Vareudlevering",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Vareudlevering dimension sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Vareudlevering]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Vareudlevering entries to sync")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Vareudlevering_ils] ds
                LEFT JOIN [map].[Vareudlevering] m
                    ON m.clean_code_Vareudlevering = ds.clean_code_Vareudlevering
                WHERE m.dw_ek_Vareudlevering IS NULL
            """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_Vareudlevering: {unmapped_rows} staging rows have no match in map.Vareudlevering. "
                        "Ensure map_Vareudlevering runs before dim_Vareudlevering and staging CleanCodes are valid."
                    )

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Vareudlevering_Attributes;

                SELECT
                    m.dw_ek_Vareudlevering,
                    m.clean_code_Vareudlevering,

                    ds.rekvisitionsnummer,
                    ds.foelgeseddelnummer,
                    ds.brik,
                    ds.leverandoernummer,
                    ds.ordrenummer,
                    ds.ordrelinje,
                    ds.sw_direkte_udlevering,
                    ds.sw_pakkeseddel,
                    ds.sw_foelgeseddel,
                    ds.sw_fly,
                    ds.sw_haster,
                    ds.sw_erstat,
                    ds.projektnummer,
                    ds.projektnavn,
                    ds.kontonummer_kode,
                    ds.bruger,
                    ds.bk_prrowid,
                    ds.referencenummer,
                    ds.referencelinje,
                    ds.salgsordre,
                    ds.salgsordre_id,
                    ds.salgsfakturanummer,
                    ds.rekvisitionsvare_id,
                    ds.plukvarer_id,
                    ds.afleveringsoperatoer,
                    ds.afleveringstekst,
                    ds.afleveringsstregkode,
                    ds.vognnummer,
                    ds.lgrlok,
                    ds.ekspeditionsnummer,
                    ds.lager_varetype,
                    ds.lager_varetype_navn

                INTO #Vareudlevering_Attributes
                FROM [stg].[dim_Vareudlevering_ils] ds

                LEFT JOIN [map].[Vareudlevering] m
                    ON m.clean_code_Vareudlevering = ds.clean_code_Vareudlevering
            """
                )

                context.log.info("Temp table #Vareudlevering_Attributes created")

                # MERGE into dimension (Type 1 SCD)
                merge_sql = """
                MERGE [dim].[Vareudlevering] AS tgt
                USING #Vareudlevering_Attributes AS src
                ON tgt.dw_ek_Vareudlevering = src.dw_ek_Vareudlevering

                WHEN MATCHED AND EXISTS (
                    SELECT
                        src.clean_code_Vareudlevering,
                        src.rekvisitionsnummer, src.foelgeseddelnummer, src.brik,
                        src.leverandoernummer, src.ordrenummer, src.ordrelinje,
                        src.sw_direkte_udlevering, src.sw_pakkeseddel, src.sw_foelgeseddel,
                        src.sw_fly, src.sw_haster, src.sw_erstat,
                        src.projektnummer, src.projektnavn, src.kontonummer_kode, src.bruger,
                        src.bk_prrowid, src.referencenummer, src.referencelinje,
                        src.salgsordre, src.salgsordre_id, src.salgsfakturanummer,
                        src.rekvisitionsvare_id, src.plukvarer_id, src.afleveringsoperatoer,
                        src.afleveringstekst, src.afleveringsstregkode, src.vognnummer,
                        src.lgrlok, src.ekspeditionsnummer,
                        src.lager_varetype, src.lager_varetype_navn
                    EXCEPT
                    SELECT
                        tgt.clean_code_Vareudlevering,
                        tgt.rekvisitionsnummer, tgt.foelgeseddelnummer, tgt.brik,
                        tgt.leverandoernummer, tgt.ordrenummer, tgt.ordrelinje,
                        tgt.sw_direkte_udlevering, tgt.sw_pakkeseddel, tgt.sw_foelgeseddel,
                        tgt.sw_fly, tgt.sw_haster, tgt.sw_erstat,
                        tgt.projektnummer, tgt.projektnavn, tgt.kontonummer_kode, tgt.bruger,
                        tgt.bk_prrowid, tgt.referencenummer, tgt.referencelinje,
                        tgt.salgsordre, tgt.salgsordre_id, tgt.salgsfakturanummer,
                        tgt.rekvisitionsvare_id, tgt.plukvarer_id, tgt.afleveringsoperatoer,
                        tgt.afleveringstekst, tgt.afleveringsstregkode, tgt.vognnummer,
                        tgt.lgrlok, tgt.ekspeditionsnummer,
                        tgt.lager_varetype, tgt.lager_varetype_navn
                ) THEN
                    UPDATE SET
                        tgt.clean_code_Vareudlevering = src.clean_code_Vareudlevering,
                        tgt.rekvisitionsnummer = src.rekvisitionsnummer,
                        tgt.foelgeseddelnummer = src.foelgeseddelnummer,
                        tgt.brik = src.brik,
                        tgt.leverandoernummer = src.leverandoernummer,
                        tgt.ordrenummer = src.ordrenummer,
                        tgt.ordrelinje = src.ordrelinje,
                        tgt.sw_direkte_udlevering = src.sw_direkte_udlevering,
                        tgt.sw_pakkeseddel = src.sw_pakkeseddel,
                        tgt.sw_foelgeseddel = src.sw_foelgeseddel,
                        tgt.sw_fly = src.sw_fly,
                        tgt.sw_haster = src.sw_haster,
                        tgt.sw_erstat = src.sw_erstat,
                        tgt.projektnummer = src.projektnummer,
                        tgt.projektnavn = src.projektnavn,
                        tgt.kontonummer_kode = src.kontonummer_kode,
                        tgt.bruger = src.bruger,
                        tgt.bk_prrowid = src.bk_prrowid,
                        tgt.referencenummer = src.referencenummer,
                        tgt.referencelinje = src.referencelinje,
                        tgt.salgsordre = src.salgsordre,
                        tgt.salgsordre_id = src.salgsordre_id,
                        tgt.salgsfakturanummer = src.salgsfakturanummer,
                        tgt.rekvisitionsvare_id = src.rekvisitionsvare_id,
                        tgt.plukvarer_id = src.plukvarer_id,
                        tgt.afleveringsoperatoer = src.afleveringsoperatoer,
                        tgt.afleveringstekst = src.afleveringstekst,
                        tgt.afleveringsstregkode = src.afleveringsstregkode,
                        tgt.vognnummer = src.vognnummer,
                        tgt.lgrlok = src.lgrlok,
                        tgt.ekspeditionsnummer = src.ekspeditionsnummer,
                        tgt.lager_varetype = src.lager_varetype,
                        tgt.lager_varetype_navn = src.lager_varetype_navn,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Vareudlevering,
                        clean_code_Vareudlevering,
                        rekvisitionsnummer,
                        foelgeseddelnummer,
                        brik,
                        leverandoernummer,
                        ordrenummer,
                        ordrelinje,
                        sw_direkte_udlevering,
                        sw_pakkeseddel,
                        sw_foelgeseddel,
                        sw_fly,
                        sw_haster,
                        sw_erstat,
                        projektnummer,
                        projektnavn,
                        kontonummer_kode,
                        bruger,
                        bk_prrowid,
                        referencenummer,
                        referencelinje,
                        salgsordre,
                        salgsordre_id,
                        salgsfakturanummer,
                        rekvisitionsvare_id,
                        plukvarer_id,
                        afleveringsoperatoer,
                        afleveringstekst,
                        afleveringsstregkode,
                        vognnummer,
                        lgrlok,
                        ekspeditionsnummer,
                        lager_varetype,
                        lager_varetype_navn,
                        dw_timestamp_insert,
                        dw_timestamp_update
                    )
                    VALUES (
                        src.dw_ek_Vareudlevering,
                        src.clean_code_Vareudlevering,
                        src.rekvisitionsnummer,
                        src.foelgeseddelnummer,
                        src.brik,
                        src.leverandoernummer,
                        src.ordrenummer,
                        src.ordrelinje,
                        src.sw_direkte_udlevering,
                        src.sw_pakkeseddel,
                        src.sw_foelgeseddel,
                        src.sw_fly,
                        src.sw_haster,
                        src.sw_erstat,
                        src.projektnummer,
                        src.projektnavn,
                        src.kontonummer_kode,
                        src.bruger,
                        src.bk_prrowid,
                        src.referencenummer,
                        src.referencelinje,
                        src.salgsordre,
                        src.salgsordre_id,
                        src.salgsfakturanummer,
                        src.rekvisitionsvare_id,
                        src.plukvarer_id,
                        src.afleveringsoperatoer,
                        src.afleveringstekst,
                        src.afleveringsstregkode,
                        src.vognnummer,
                        src.lgrlok,
                        src.ekspeditionsnummer,
                        src.lager_varetype,
                        src.lager_varetype_navn,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    );
            """

                insert_sql = """
            INSERT INTO [dim].[Vareudlevering] WITH (TABLOCK) (
                    dw_ek_Vareudlevering, clean_code_Vareudlevering, rekvisitionsnummer,
                    foelgeseddelnummer, brik, leverandoernummer,
                    ordrenummer, ordrelinje, sw_direkte_udlevering,
                    sw_pakkeseddel, sw_foelgeseddel, sw_fly,
                    sw_haster, sw_erstat, projektnummer, projektnavn,
                    kontonummer_kode, bruger, bk_prrowid,
                    referencenummer, referencelinje, salgsordre,
                    salgsordre_id, salgsfakturanummer, rekvisitionsvare_id,
                    plukvarer_id, afleveringsoperatoer, afleveringstekst,
                    afleveringsstregkode, vognnummer, lgrlok, ekspeditionsnummer,
                    lager_varetype, lager_varetype_navn, dw_timestamp_insert,
                    dw_timestamp_update
            )
            SELECT
                    src.dw_ek_Vareudlevering, src.clean_code_Vareudlevering, src.rekvisitionsnummer,
                    src.foelgeseddelnummer, src.brik, src.leverandoernummer,
                    src.ordrenummer, src.ordrelinje, src.sw_direkte_udlevering,
                    src.sw_pakkeseddel, src.sw_foelgeseddel, src.sw_fly,
                    src.sw_haster, src.sw_erstat, src.projektnummer, src.projektnavn,
                    src.kontonummer_kode, src.bruger, src.bk_prrowid,
                    src.referencenummer, src.referencelinje, src.salgsordre,
                    src.salgsordre_id, src.salgsfakturanummer, src.rekvisitionsvare_id,
                    src.plukvarer_id, src.afleveringsoperatoer, src.afleveringstekst,
                    src.afleveringsstregkode, src.vognnummer, src.lgrlok, src.ekspeditionsnummer,
                    src.lager_varetype, src.lager_varetype_navn, CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
            FROM #Vareudlevering_Attributes src;
            """

                # Short-circuit: if the target holds only the -1 sentinel (fresh or
                # rebuilt table), a bulk INSERT ... WITH (TABLOCK) can be minimally
                # logged, whereas MERGE is always fully logged. The source draws
                # dw_ek_Vareudlevering from map.Vareudlevering (IDENTITY >= 1), so it
                # never collides with the -1 sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[Vareudlevering] "
                    "WHERE dw_ek_Vareudlevering <> -1) THEN 1 ELSE 0 END"
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

                # Clean up temp table
                cursor.execute("DROP TABLE IF EXISTS #Vareudlevering_Attributes")

                cursor.execute("UPDATE STATISTICS [dim].[Vareudlevering]")

                # DIAGNOSTIC: Get dimension statistics
                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_dim_rows,
                    SUM(CASE WHEN dw_timestamp_update = dw_timestamp_insert THEN 1 ELSE 0 END) AS inserts_only,
                    SUM(CASE WHEN dw_timestamp_update <> dw_timestamp_insert THEN 1 ELSE 0 END) AS updated,
                    COUNT(rekvisitionsnummer) AS has_rekvisitionsnummer,
                    COUNT(foelgeseddelnummer) AS has_foelgeseddelnummer,
                    COUNT(brik) AS has_brik,
                    COUNT(leverandoernummer) AS has_leverandoernummer,
                    COUNT(ordrenummer) AS has_ordrenummer,
                    COUNT(ordrelinje) AS has_ordrelinje,
                    COUNT(sw_direkte_udlevering) AS has_sw_direkte,
                    COUNT(sw_pakkeseddel) AS has_sw_pakkeseddel,
                    COUNT(sw_foelgeseddel) AS has_sw_foelgeseddel,
                    COUNT(sw_fly) AS has_sw_fly,
                    COUNT(sw_haster) AS has_sw_haster,
                    COUNT(sw_erstat) AS has_sw_erstat,
                    COUNT(projektnummer) AS has_projektnummer,
                    COUNT(projektnavn) AS has_projektnavn,
                    COUNT(kontonummer_kode) AS has_kontonummer_kode,
                    COUNT(bruger) AS has_bruger,
                    COUNT(bk_prrowid) AS has_bk_prrowid,
                    COUNT(referencenummer) AS has_referencenummer,
                    COUNT(referencelinje) AS has_referencelinje,
                    COUNT(salgsordre) AS has_salgsordre,
                    COUNT(salgsordre_id) AS has_salgsordre_id,
                    COUNT(salgsfakturanummer) AS has_salgsfakturanummer,
                    COUNT(rekvisitionsvare_id) AS has_rekvisitionsvareid,
                    COUNT(plukvarer_id) AS has_plukvareid,
                    COUNT(afleveringsoperatoer) AS has_afleveringsoperatoer,
                    COUNT(afleveringstekst) AS has_afleveringstekst,
                    COUNT(afleveringsstregkode) AS has_afleveringsstregkode,
                    COUNT(vognnummer) AS has_vognnummer,
                    COUNT(lgrlok) AS has_lgrlok,
                    COUNT(ekspeditionsnummer) AS has_ekspeditionsnummer,
                    COUNT(lager_varetype) AS has_lager_varetype,
                    COUNT(lager_varetype_navn) AS has_lager_varetype_navn
                FROM [dim].[Vareudlevering]
                WHERE dw_ek_Vareudlevering <> -1
            """
                )
                stats = cursor.fetchone()
                total_dim_rows = stats[0]
                inserts_only = stats[1]
                updated_count = stats[2]
                has_rekvisitionsnummer = stats[3]
                has_foelgeseddelnummer = stats[4]
                has_brik = stats[5]
                has_leverandoernummer = stats[6]
                has_ordrenummer = stats[7]
                has_ordrelinje = stats[8]
                has_sw_direkte = stats[9]
                has_sw_pakkeseddel = stats[10]
                has_sw_foelgeseddel = stats[11]
                has_sw_fly = stats[12]
                has_sw_haster = stats[13]
                has_sw_erstat = stats[14]
                has_projektnummer = stats[15]
                has_projektnavn = stats[16]
                has_kontonummer_kode = stats[17]
                has_bruger = stats[18]
                has_bk_prrowid = stats[19]
                has_referencenummer = stats[20]
                has_referencelinje = stats[21]
                has_salgsordre = stats[22]
                has_salgsordre_id = stats[23]
                has_salgsfakturanummer = stats[24]
                has_rekvisitionsvareid = stats[25]
                has_plukvareid = stats[26]
                has_afleveringsoperatoer = stats[27]
                has_afleveringstekst = stats[28]
                has_afleveringsstregkode = stats[29]
                has_vognnummer = stats[30]
                has_lgrlok = stats[31]
                has_ekspeditionsnummer = stats[32]
                has_lager_varetype = stats[33]
                has_lager_varetype_navn = stats[34]

                context.log.info(
                    f"Dimension sync complete: {total_dim_rows} total rows "
                    f"({inserts_only} insert-only, {updated_count} updated)"
                )
                context.log.info(
                    f"Coverage: {has_rekvisitionsnummer}/{total_dim_rows} with rekvisitionsnummer, "
                    f"{has_foelgeseddelnummer}/{total_dim_rows} with foelgeseddelnummer, "
                    f"{has_brik}/{total_dim_rows} with brik, "
                    f"{has_leverandoernummer}/{total_dim_rows} with leverandoernummer, "
                    f"{has_ordrenummer}/{total_dim_rows} with ordrenummer, "
                    f"{has_ordrelinje}/{total_dim_rows} with ordrelinje, "
                    f"{has_sw_direkte}/{total_dim_rows} with sw_direkte_udlevering, "
                    f"{has_sw_pakkeseddel}/{total_dim_rows} with sw_pakkeseddel, "
                    f"{has_sw_foelgeseddel}/{total_dim_rows} with sw_foelgeseddel, "
                    f"{has_sw_fly}/{total_dim_rows} with sw_fly, "
                    f"{has_sw_haster}/{total_dim_rows} with sw_haster, "
                    f"{has_sw_erstat}/{total_dim_rows} with sw_erstat, "
                    f"{has_projektnummer}/{total_dim_rows} with projektnummer, "
                    f"{has_projektnavn}/{total_dim_rows} with projektnavn, "
                    f"{has_kontonummer_kode}/{total_dim_rows} with kontonummer_kode, "
                    f"{has_bruger}/{total_dim_rows} with bruger, "
                    f"{has_bk_prrowid}/{total_dim_rows} with bk_prrowid, "
                    f"{has_referencenummer}/{total_dim_rows} with referencenummer, "
                    f"{has_referencelinje}/{total_dim_rows} with referencelinje, "
                    f"{has_salgsordre}/{total_dim_rows} with salgsordre, "
                    f"{has_salgsordre_id}/{total_dim_rows} with salgsordre_id, "
                    f"{has_salgsfakturanummer}/{total_dim_rows} with salgsfakturanummer, "
                    f"{has_rekvisitionsvareid}/{total_dim_rows} with rekvisitionsvare_id, "
                    f"{has_plukvareid}/{total_dim_rows} with plukvarer_id, "
                    f"{has_afleveringsoperatoer}/{total_dim_rows} with afleveringsoperatoer, "
                    f"{has_afleveringstekst}/{total_dim_rows} with afleveringstekst, "
                    f"{has_afleveringsstregkode}/{total_dim_rows} with afleveringsstregkode, "
                    f"{has_vognnummer}/{total_dim_rows} with vognnummer, "
                    f"{has_lgrlok}/{total_dim_rows} with lgrlok, "
                    f"{has_ekspeditionsnummer}/{total_dim_rows} with ekspeditionsnummer, "
                    f"{has_lager_varetype}/{total_dim_rows} with lager_varetype, "
                    f"{has_lager_varetype_navn}/{total_dim_rows} with lager_varetype_navn"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Vareudlevering", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Vareudlevering")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "inserts_only": inserts_only,
            "updated": updated_count,
            "has_rekvisitionsnummer": has_rekvisitionsnummer,
            "has_foelgeseddelnummer": has_foelgeseddelnummer,
            "has_brik": has_brik,
            "has_leverandoernummer": has_leverandoernummer,
            "has_ordrenummer": has_ordrenummer,
            "has_ordrelinje": has_ordrelinje,
            "has_sw_direkte_udlevering": has_sw_direkte,
            "has_sw_pakkeseddel": has_sw_pakkeseddel,
            "has_sw_foelgeseddel": has_sw_foelgeseddel,
            "has_sw_fly": has_sw_fly,
            "has_sw_haster": has_sw_haster,
            "has_sw_erstat": has_sw_erstat,
            "has_projektnummer": has_projektnummer,
            "has_projektnavn": has_projektnavn,
            "has_kontonummer_kode": has_kontonummer_kode,
            "has_bruger": has_bruger,
            "has_bk_prrowid": has_bk_prrowid,
            "has_referencenummer": has_referencenummer,
            "has_referencelinje": has_referencelinje,
            "has_salgsordre": has_salgsordre,
            "has_salgsordre_id": has_salgsordre_id,
            "has_salgsfakturanummer": has_salgsfakturanummer,
            "has_rekvisitionsvareid": has_rekvisitionsvareid,
            "has_plukvareid": has_plukvareid,
            "has_afleveringsoperatoer": has_afleveringsoperatoer,
            "has_afleveringstekst": has_afleveringstekst,
            "has_afleveringsstregkode": has_afleveringsstregkode,
            "has_vognnummer": has_vognnummer,
            "has_lgrlok": has_lgrlok,
            "has_ekspeditionsnummer": has_ekspeditionsnummer,
            "has_lager_varetype": has_lager_varetype,
            "has_lager_varetype_navn": has_lager_varetype_navn,
            "table": "dim.Vareudlevering",
            "source_tables": "stg.dim_Vareudlevering_ils + map.Vareudlevering",
            "scd_type": "Type 1 (overwrite)",
        }
    )
