# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Bridge Asset: bridge.AftaleLager (DSA Layer)

Full-rebuild-driven aftale <-> lager/sygehus-styring ("Benyttes af" i ILS).
stg.bridge_AftaleLager (Task 4) er en FULD rebuild af hele lafthsgh-billedet
hver koersel — der findes intet delta-signal at scope et soft-delete til, saa
sweepet daekker HELE tabellen (i modsaetning til den delta-drevne
LeverandoervareILSVare-bro, som scoper til stg's egen delta-entitetsliste).
Sweep (mark) og MERGE (upsert) koerer i EEN transaktion: fejler koerslen
midtvejs, ruller begge tilbage sammen.

dw_ek_Aftale/dw_ek_Lager resolves via map.Aftale/map.Lager (LEFT JOIN,
COALESCE(..., -1) fallback) — en lafthsgh-aftale/lager uden map-match faar
seed-vaerdien -1 i stedet for at blokere koerslen (ingen fail-fast her, i
modsaetning til bridge.LeverandoervareILSVare's INNER JOIN-krav).
Sygehus-only raekker resolver mod SGH_{sygehus}-konceptet (blandet
granularitet i dw_ek_Lager; lager IS NULL markerer niveauet), saa -1
betyder nu altid aegte map-miss.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    RetryPolicy,
    asset,
)


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_bridge_AftaleLager"]),
        AssetKey(["map_Aftale"]),
        AssetKey(["map_Lager"]),
    ],
    tags={"domain": "erp", "layer": "bridge", "module": "datawarehouse"},
    description=(
        "bridge.AftaleLager — full-rebuild-vedligeholdt aftale <-> lager/sygehus-styring "
        "fra stg.bridge_AftaleLager. Full-table pre-delete-sweep (fuld staging => fuldt "
        "scope) + MERGE i EEN transaktion paa (clean_code_Aftale, sgh_raw)."
    ),
)
def bridge_AftaleLager(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "bridge", "AftaleLager", dagster_run_id=context.run_id, mode=config.mode
    )
    rows_affected = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Resolving stg.bridge_AftaleLager via map.Aftale/map.Lager...")

                cursor.execute(
                    """
                    DROP TABLE IF EXISTS #AftaleLager_Resolved;

                    -- clean_code_Lager er aldrig NULL: sygehus-only raekker (lager IS NULL)
                    -- falder tilbage til codesystemets SGH_{sygehus}-koncept, som findes i
                    -- map.Lager som hierarki-foraelder. dw_ek_Lager er dermed blandet
                    -- granularitet — raekkens lager IS NULL markerer sygehus-niveauet.
                    SELECT
                        COALESCE(aftale_map.dw_ek_Aftale, -1) AS dw_ek_Aftale,
                        COALESCE(lager_map.dw_ek_Lager, -1) AS dw_ek_Lager,
                        src.clean_code_Aftale,
                        src.sgh_raw,
                        src.sygehus,
                        src.lager,
                        src.maa_benyttes,
                        src.foerste_prioritet
                    INTO #AftaleLager_Resolved
                    FROM [stg].[bridge_AftaleLager] src
                    LEFT JOIN [map].[Aftale] aftale_map
                        ON aftale_map.clean_code_Aftale = src.clean_code_Aftale
                    LEFT JOIN [map].[Lager] lager_map
                        ON lager_map.clean_code_Lager = src.clean_code_Lager;
                """
                )

                cursor.execute("SELECT COUNT(*) FROM #AftaleLager_Resolved")
                rows_affected = cursor.fetchone()[0]

                # Fail-fast: (clean_code_Aftale, sgh_raw) skal vaere unik i
                # resolve-temptabellen FOER index/MERGE — mirrors
                # fct_AftaleVarePris.py's duplicate-grain-tjek. Koeres FOER den
                # unikke klyngede indeks-oprettelse nedenfor: uden dette tjek
                # ville en kollision blot fejle med en raa
                # indeks-constraint-fejl fra motoren i stedet for en klar
                # ValueError, fordi indekset ellers oprettes i samme batch som
                # SELECT INTO ovenfor.
                cursor.execute(
                    """
                    SELECT COUNT(*) - COUNT(DISTINCT CONCAT(clean_code_Aftale, '|', sgh_raw))
                    FROM #AftaleLager_Resolved
                """
                )
                duplicate_source_rows = cursor.fetchone()[0]
                if duplicate_source_rows > 0:
                    raise ValueError(
                        "Duplicate grain violation in bridge_AftaleLager: "
                        f"{duplicate_source_rows} duplicate (clean_code_Aftale, sgh_raw) "
                        "rows in #AftaleLager_Resolved."
                    )

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_tmp_AftaleLager_Resolved
                    ON #AftaleLager_Resolved (clean_code_Aftale, sgh_raw);
                """
                )

                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN dw_ek_Aftale = -1 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN dw_ek_Lager = -1 THEN 1 ELSE 0 END)
                    FROM #AftaleLager_Resolved
                """
                )
                unresolved_aftale, unresolved_lager = cursor.fetchone()

                context.log.info("Full-table pre-delete-sweep + MERGE (single transaction)...")

                cursor.execute(
                    """
                    -- (1) SWEEP: hele tabellen — INGEN scoping EXISTS. Staging er en
                    --     fuld rebuild af hele lafthsgh-billedet hver koersel, saa
                    --     fuldt scope er korrekt (ikke en delta-scoped sweep).
                    UPDATE tgt
                    SET tgt.dw_is_deleted = 1,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    FROM [bridge].[AftaleLager] tgt
                    WHERE tgt.dw_is_deleted = 0;

                    -- (2) MERGE: upsert det genberegnede saet; genopstaaede
                    --     styringsraekker afmarkeres.
                    MERGE [bridge].[AftaleLager] AS tgt
                    USING #AftaleLager_Resolved AS src
                        ON tgt.clean_code_Aftale = src.clean_code_Aftale
                       AND tgt.sgh_raw = src.sgh_raw
                    WHEN MATCHED THEN UPDATE SET
                        tgt.dw_ek_Aftale = src.dw_ek_Aftale,
                        tgt.dw_ek_Lager = src.dw_ek_Lager,
                        tgt.sygehus = src.sygehus,
                        tgt.lager = src.lager,
                        tgt.maa_benyttes = src.maa_benyttes,
                        tgt.foerste_prioritet = src.foerste_prioritet,
                        tgt.dw_is_deleted = 0,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    WHEN NOT MATCHED BY TARGET THEN INSERT (
                        dw_ek_Aftale, dw_ek_Lager, clean_code_Aftale, sgh_raw, sygehus, lager,
                        maa_benyttes, foerste_prioritet, dw_is_deleted,
                        dw_timestamp_insert, dw_timestamp_update
                    ) VALUES (
                        src.dw_ek_Aftale, src.dw_ek_Lager, src.clean_code_Aftale, src.sgh_raw,
                        src.sygehus, src.lager, src.maa_benyttes, src.foerste_prioritet, 0,
                        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                    );
                """
                )

                cursor.execute("UPDATE STATISTICS [bridge].[AftaleLager];")
                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total_rows,
                        SUM(CASE WHEN dw_is_deleted = 1 THEN 1 ELSE 0 END) AS deleted_rows
                    FROM [bridge].[AftaleLager]
                """
                )
                total_rows, deleted_rows = cursor.fetchone()
                context.log.info(
                    f"bridge.AftaleLager: {total_rows} rows, {deleted_rows} soft-deleted, "
                    f"{unresolved_aftale} unresolved Aftale, {unresolved_lager} unresolved Lager"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=rows_affected)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "bridge.AftaleLager",
            "rows_affected": rows_affected,
            "total_rows": total_rows,
            "deleted_rows": deleted_rows,
            "unresolved_aftale": unresolved_aftale,
            "unresolved_lager": unresolved_lager,
            "source_table": "stg.bridge_AftaleLager (full rebuild each run)",
            "grain": "one row per (clean_code_Aftale, sgh_raw); dw_is_deleted soft-delete",
            "mode": config.mode,
        }
    )
