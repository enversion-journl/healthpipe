# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Kreditor

Delta-driven Type 1 SCD at CVR grain.

Flow:
- stg_dim_Kreditor extracts the DELTA of changed CVRs from the archives
  (dim.Kreditor checkpoint); map.Kreditor MERGE stamps dw_timestamp_update
  for every staged CVR and keeps sticky dw_source_<system> flags.
- The dim delta is therefore simply the map rows changed since the
  checkpoint — the map already carries new CVRs, flag flips AND attribute
  drift.
- Delta CVRs are enriched with descriptive attributes directly from the
  arc.* archives (per-source current-row picks) and MERGEd into dim.Kreditor
  with COALESCE priority SydAX -> ILS -> MySupply.

dim_Kreditor owns the ("dim", "Kreditor") DFM checkpoint; full_load mode
reprocesses every map row.

Source-internal account keys (KreditorId, Kreditornummer, lnr) are used only
as deterministic dedup tiebreakers — never exposed (one CVR has many
parallel accounts).
"""

import traceback

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from utils.sql.tax_id_sql import (
    sql_cvr_cleancode_from_column,
    sql_cvr_cleancode_from_juridisk_enhed,
    sql_cvr_cleancode_from_se_nr,
)

# CleanCode formulas — shared with stg_dim_Kreditor via utils.sql.tax_id_sql
SYDAX_CC = sql_cvr_cleancode_from_juridisk_enhed("k.JuridiskEnhed")
ILS_CC = sql_cvr_cleancode_from_se_nr("lev.se_nr")
MYSUPPLY_CC = sql_cvr_cleancode_from_column("m.LeverandorCVR")


def _build_resolve_sql(checkpoint: str | None) -> str:
    """Temp-table build: materialize the map delta, then per-source attribute
    picks from the archives restricted to the delta CVRs.

    The map is the complete delta signal — stg_dim_Kreditor stages the
    archive delta and map_Kreditor stamps every staged CVR.

    The delta is materialized as #Kreditor_Delta (not a CTE) so the picks can
    semi-join it BEFORE their ROW_NUMBER windows: filtering on the partition
    key cannot change which row wins per surviving CVR, but it keeps the
    sort/window from running over the full archive history for a small delta
    (and avoids re-evaluating an inlined delta CTE per reference).
    checkpoint None => full reprocess (all map rows).
    """
    if checkpoint is None:
        cp_filter = ""
    else:
        cp_filter = f"\n    WHERE m.dw_timestamp_update > '{checkpoint}'"

    return f"""
    DROP TABLE IF EXISTS #Kreditor_Delta;
    DROP TABLE IF EXISTS #Kreditor_Attributes;

    -- Map rows changed since checkpoint (new CVRs, flag flips, attribute drift)
    SELECT
        m.dw_ek_Kreditor,
        m.clean_code_kreditor,
        m.dw_source_sydax,
        m.dw_source_ils,
        m.dw_source_mysupply
    INTO #Kreditor_Delta
    FROM [map].[Kreditor] m{cp_filter};

    CREATE UNIQUE CLUSTERED INDEX IX_tmp_Kreditor_Delta
    ON #Kreditor_Delta (clean_code_kreditor);

    ;WITH syd_base AS (
        SELECT
            {SYDAX_CC} AS clean_code_Kreditor,
            CAST(k.KreditorId AS BIGINT) AS kreditor_id,
            k.Kreditornavn AS kreditornavn,
            k.Soegenavn AS soegenavn,
            k.JuridiskEnhed AS juridisk_enhed,
            k.StartDateTime AS start_datetime,
            k.dw_timestamp_update
        FROM [arc].[sydax_kreditor] k
        WHERE k.JuridiskEnhed IS NOT NULL AND LTRIM(RTRIM(k.JuridiskEnhed)) <> ''
    ),
    syd_pick AS (
        -- Current SydAX attributes per delta-CVR (named first, newest,
        -- newest version, KreditorId tail)
        SELECT clean_code_Kreditor, kreditornavn, juridisk_enhed
        FROM (
            SELECT b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.clean_code_Kreditor
                    ORDER BY
                        CASE WHEN b.kreditornavn IS NOT NULL AND b.kreditornavn <> '' THEN 0 ELSE 1 END,
                        b.dw_timestamp_update DESC,
                        b.start_datetime DESC,
                        b.kreditor_id DESC
                ) AS rn
            FROM syd_base b
            INNER JOIN #Kreditor_Delta dc
                ON dc.clean_code_kreditor = b.clean_code_Kreditor
        ) r
        WHERE rn = 1
    ),
    syd_soegenavn_pick AS (
        -- SydAX search name per delta-CVR, picked on its own presence so a
        -- name-less winner in syd_pick cannot drop it (non-empty first, newest,
        -- newest version, KreditorId tail)
        SELECT clean_code_Kreditor, soegenavn
        FROM (
            SELECT b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.clean_code_Kreditor
                    ORDER BY
                        CASE WHEN b.soegenavn IS NOT NULL AND b.soegenavn <> '' THEN 0 ELSE 1 END,
                        b.dw_timestamp_update DESC,
                        b.start_datetime DESC,
                        b.kreditor_id DESC
                ) AS rn
            FROM syd_base b
            INNER JOIN #Kreditor_Delta dc
                ON dc.clean_code_kreditor = b.clean_code_Kreditor
        ) r
        WHERE rn = 1
    ),
    ils_pick AS (
        -- Current ILS attributes per delta-CVR (named first, newest oprdto, lnr tail)
        SELECT clean_code_Kreditor, se_nr, cvr_nr, navn, adr, adr2, postdi, kontakt
        FROM (
            SELECT b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.clean_code_Kreditor
                    ORDER BY
                        CASE WHEN b.navn IS NOT NULL AND b.navn <> '' THEN 0 ELSE 1 END,
                        b.oprdto DESC,
                        b.lnr DESC
                ) AS rn
            FROM (
                SELECT
                    {ILS_CC} AS clean_code_Kreditor,
                    lev.se_nr, lev.cvr_nr, lev.lnr, lev.navn,
                    lev.adr, lev.adr2, lev.postdi, lev.kontakt, lev.oprdto
                FROM [arc].[ils_lev] lev
                WHERE lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
            ) b
            INNER JOIN #Kreditor_Delta dc
                ON dc.clean_code_kreditor = b.clean_code_Kreditor
        ) r
        WHERE rn = 1
    ),
    mys_pick AS (
        -- Current MySupply attributes per delta-CVR (named+cvr first, newest invoice)
        SELECT clean_code_Kreditor, leverandoer_cvr, firma_navn, bynavn, adresse,
               postnummer, faktura_valuta_kode
        FROM (
            SELECT b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.clean_code_Kreditor
                    ORDER BY
                        CASE WHEN b.firma_navn IS NOT NULL AND b.firma_navn <> '' THEN 0 ELSE 1 END,
                        CASE WHEN b.leverandoer_cvr IS NOT NULL AND b.leverandoer_cvr <> '' THEN 0 ELSE 1 END,
                        b.FakturaDato DESC
                ) AS rn
            FROM (
                SELECT
                    {MYSUPPLY_CC} AS clean_code_Kreditor,
                    CAST(NULLIF(SUBSTRING({MYSUPPLY_CC}, 5, 8000), '') AS NVARCHAR(50)) AS leverandoer_cvr,
                    m.FirmaNavn AS firma_navn,
                    m.[By] AS bynavn,
                    m.Adresse AS adresse,
                    m.Postnummer AS postnummer,
                    h.FakturaValutaKode AS faktura_valuta_kode,
                    h.FakturaDato
                FROM [arc].[mysupply_metadata] m
                LEFT JOIN [arc].[mysupply_statistic_invoice_header] h
                    ON h.metadata_id = m.id
                WHERE m.LeverandorCVR IS NOT NULL AND LTRIM(RTRIM(m.LeverandorCVR)) <> ''
            ) b
            INNER JOIN #Kreditor_Delta dc
                ON dc.clean_code_kreditor = b.clean_code_Kreditor
        ) r
        WHERE rn = 1
    )

    SELECT
        d.dw_ek_Kreditor,
        d.clean_code_kreditor,

        -- cvrnr: COALESCE priority SydAX -> ILS (se_nr, ellers cvr_nr) -> MySupply -> UNKNOWN
        COALESCE(
            LTRIM(RTRIM(syd.juridisk_enhed)),
            LTRIM(RTRIM(COALESCE(ils.se_nr, ils.cvr_nr))),
            LTRIM(RTRIM(mys.leverandoer_cvr)),
            'UNKNOWN'
        ) AS cvrnr,

        -- kreditornavn: COALESCE priority SydAX -> ILS -> MySupply, plus per-source columns
        COALESCE(syd.kreditornavn, ils.navn, mys.firma_navn) AS kreditornavn,
        syd.kreditornavn AS kreditornavn_sydax,
        ils.navn AS kreditornavn_ils,
        mys.firma_navn AS kreditornavn_mysupply,

        -- vejnavn: COALESCE priority ILS -> MySupply, plus per-source columns
        COALESCE(ils.adr, mys.adresse) AS vejnavn,
        ils.adr AS vejnavn_ils,
        mys.adresse AS vejnavn_mysupply,

        -- bynavn: COALESCE priority ILS -> MySupply, plus per-source columns
        COALESCE(ils.adr2, mys.bynavn) AS bynavn,
        ils.adr2 AS bynavn_ils,
        mys.bynavn AS bynavn_mysupply,

        -- postnummer: COALESCE priority ILS -> MySupply, plus per-source columns
        COALESCE(
            LTRIM(RTRIM(ils.postdi)),
            CAST(mys.postnummer AS NVARCHAR(50))
        ) AS postnummer,
        LTRIM(RTRIM(ils.postdi)) AS postnummer_ils,
        CAST(mys.postnummer AS NVARCHAR(50)) AS postnummer_mysupply,

        -- Single-source attributes
        syd_sn.soegenavn AS soegenavn_sydax,
        ils.kontakt AS kontakt,
        mys.faktura_valuta_kode AS valutakode,

        -- Source presence flags: sticky truth carried from map.Kreditor
        d.dw_source_sydax,
        d.dw_source_ils,
        d.dw_source_mysupply

    INTO #Kreditor_Attributes
    FROM #Kreditor_Delta d

    LEFT JOIN syd_pick syd
        ON syd.clean_code_Kreditor = d.clean_code_kreditor

    LEFT JOIN syd_soegenavn_pick syd_sn
        ON syd_sn.clean_code_Kreditor = d.clean_code_kreditor

    LEFT JOIN ils_pick ils
        ON ils.clean_code_Kreditor = d.clean_code_kreditor

    LEFT JOIN mys_pick mys
        ON mys.clean_code_Kreditor = d.clean_code_kreditor;
    """


MERGE_SQL = """
    MERGE [dim].[Kreditor] AS tgt
    USING #Kreditor_Attributes AS src
    ON tgt.dw_ek_Kreditor = src.dw_ek_Kreditor

    WHEN MATCHED THEN
        UPDATE SET
            tgt.clean_code_kreditor = src.clean_code_kreditor,
            tgt.cvrnr = src.cvrnr,
            tgt.kreditornavn = src.kreditornavn,
            tgt.kreditornavn_sydax = src.kreditornavn_sydax,
            tgt.kreditornavn_ils = src.kreditornavn_ils,
            tgt.kreditornavn_mysupply = src.kreditornavn_mysupply,
            tgt.soegenavn_sydax = src.soegenavn_sydax,
            tgt.vejnavn = src.vejnavn,
            tgt.vejnavn_ils = src.vejnavn_ils,
            tgt.vejnavn_mysupply = src.vejnavn_mysupply,
            tgt.bynavn = src.bynavn,
            tgt.bynavn_ils = src.bynavn_ils,
            tgt.bynavn_mysupply = src.bynavn_mysupply,
            tgt.postnummer = src.postnummer,
            tgt.postnummer_ils = src.postnummer_ils,
            tgt.postnummer_mysupply = src.postnummer_mysupply,
            tgt.kontakt = src.kontakt,
            tgt.valutakode = src.valutakode,
            tgt.dw_source_sydax = src.dw_source_sydax,
            tgt.dw_source_ils = src.dw_source_ils,
            tgt.dw_source_mysupply = src.dw_source_mysupply,
            tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            dw_ek_Kreditor, clean_code_kreditor, cvrnr,
            kreditornavn, kreditornavn_sydax, kreditornavn_ils, kreditornavn_mysupply,
            soegenavn_sydax,
            vejnavn, vejnavn_ils, vejnavn_mysupply,
            bynavn, bynavn_ils, bynavn_mysupply,
            postnummer, postnummer_ils, postnummer_mysupply,
            kontakt, valutakode,
            dw_source_sydax, dw_source_ils, dw_source_mysupply,
            dw_timestamp_insert
        )
        VALUES (
            src.dw_ek_Kreditor, src.clean_code_kreditor, src.cvrnr,
            src.kreditornavn, src.kreditornavn_sydax, src.kreditornavn_ils, src.kreditornavn_mysupply,
            src.soegenavn_sydax,
            src.vejnavn, src.vejnavn_ils, src.vejnavn_mysupply,
            src.bynavn, src.bynavn_ils, src.bynavn_mysupply,
            src.postnummer, src.postnummer_ils, src.postnummer_mysupply,
            src.kontakt, src.valutakode,
            src.dw_source_sydax, src.dw_source_ils, src.dw_source_mysupply,
            CAST(SYSDATETIME() AS datetime2(0))
        );
"""


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_Kreditor"]),
        AssetKey(["arc_sydax_kreditor"]),
        AssetKey(["arc_ils_lev"]),
        AssetKey(["arc_mysupply_metadata"]),
        AssetKey(["arc_mysupply_statistic_invoice_header"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=(
        "Delta-driven sync of dim.Kreditor: map.Kreditor delta (stamped by the staged archive "
        "delta), enriched from archives with COALESCE priority SydAX -> ILS -> MySupply"
    ),
)
def dim_Kreditor(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: delta from map, enrich from archives, MERGE."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Kreditor", dagster_run_id=context.run_id, mode=config.mode)
    rows_affected = 0

    try:
        checkpoint = dfm.get_checkpoint("dim", "Kreditor")
        if config.mode == "full_load":
            checkpoint = None

        if checkpoint is not None:
            context.log.info(f"Delta mode: processing changes since {checkpoint}")
        else:
            context.log.info("No checkpoint — full reprocess of all map.Kreditor rows")

        resolve_sql = _build_resolve_sql(checkpoint)

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Kreditor dimension sync (delta + arc enrichment)...")

                try:
                    cursor.execute(resolve_sql)
                    cursor.execute(
                        """
                        CREATE UNIQUE CLUSTERED INDEX IX_tmp_Kreditor_Attributes
                        ON #Kreditor_Attributes (dw_ek_Kreditor)
                        """
                    )
                    cursor.execute("SELECT COUNT(*) FROM #Kreditor_Attributes")
                    delta_count = cursor.fetchone()[0]
                    context.log.info(f"📊 Delta: {delta_count} CVRs to merge")
                except Exception as e:
                    error_msg = f"Failed to build delta temp table: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                try:
                    cursor.execute(MERGE_SQL)
                    context.log.info("✅ MERGE operation completed successfully")
                except Exception as e:
                    error_msg = f"Failed to MERGE into dim.Kreditor: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                rows_affected = cursor.rowcount
                context.log.info(f"🔄 MERGE complete: {rows_affected} rows affected")

                cursor.execute("DROP TABLE IF EXISTS #Kreditor_Attributes")
                cursor.execute("DROP TABLE IF EXISTS #Kreditor_Delta")

                conn.commit()
                context.log.info("✅ Database transaction committed")

                try:
                    cursor.execute("UPDATE STATISTICS [dim].[Kreditor]")
                    context.log.info("✅ Table statistics updated")
                except Exception as e:
                    # Log but don't fail - statistics update is not critical
                    context.log.warning(f"⚠️ Failed to update statistics: {str(e)}")

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(NULLIF(cvrnr, 'UNKNOWN')) AS has_cvr,
                        COUNT(kreditornavn) AS has_navn
                    FROM [dim].[Kreditor]
                    WHERE dw_ek_Kreditor <> -1
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"📈 Dimension state: {stats[0]} rows, cvr={stats[1]}, navn={stats[2]}"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Kreditor", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Kreditor")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        context.log.error(f"❌ Kreditor dimension sync failed: {exc}")
        raise ValueError(f"Kreditor dimension sync failed: {exc}") from exc

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "dim.Kreditor",
            "delta_source": "map.Kreditor.dw_timestamp_update (stamped by staged archive delta)",
            "coalesce_sources": "SydAX → ILS → MySupply",
            "scd_type": "Type 1 (update in place, delta-driven)",
            "clean_architecture_note": "dw_ek_Kreditor synced from map table",
            "mode": config.mode,
        }
    )
