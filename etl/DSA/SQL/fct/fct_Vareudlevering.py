# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Update: Vareudlevering

Fact table for deliveries from central warehouses to departments.
Grain: one row per delivery line (dbo.udlev.prrowid).
Source: ILS dbo.udlev (single source, full load).

Reads from stg.fct_Vareudlevering_ils (Layer 1 staging) which contains
precomputed date keys, typed measures, combined datetimes, and the
constructed clean_code_Leverandoervare.

Vare resolution: solely via the constructed clean_code_Leverandoervare from
staging (clean_code_Kreditor + LEVVARE_{lvnr_1}, matches map.Leverandoervare PK format) —
i.e. only what the arc.udlev source row itself carries. Rows without
lvnr_1 resolve to dw_ek_Leverandoervare = -1; additional Vare relations reachable
via bridge.LeverandoervareILSVare are deliberately not baked into the model.

Transaction dimension: dw_ek_Vareudlevering is the shared PK with
dim.Vareudlevering, resolved from map.Vareudlevering (same pattern as
fct.Faktura/dim.Faktura).

This asset is responsible only for simple CleanCode -> DW_EK map
lookups + MERGE + diagnostics.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

RESOLVE_SQL = """
DROP TABLE IF EXISTS #Vareudlevering_Facts;

SELECT
    vu_map.dw_ek_Vareudlevering,
    src.clean_code_Vareudlevering,
    src.bk_prrowid,

    -- ILSVare lookup via vnr -> ILSVARE_{vnr} CleanCode -> map.ILSVare
    COALESCE(ilsvare_map.dw_ek_ILSVare, -1) AS dw_ek_ILSVare,
    COALESCE(di.[dw_ek_UNSPSC], -1) AS dw_ek_UNSPSC,

    -- Lager lookup
    COALESCE(lager_map.dw_ek_Lager, -1) AS dw_ek_Lager,

    -- Rekvirent lookup
    COALESCE(rekvirent_map.dw_ek_Rekvirent, -1) AS dw_ek_Rekvirent,

    -- Vare lookup: constructed LEVVARE CleanCode from staging
    COALESCE(vare_map.dw_ek_Leverandoervare, -1) AS dw_ek_Leverandoervare,

    -- Precomputed in staging
    src.dw_ek_Dato_vareudlevering,
    src.dato_tid_vareudlevering,
    src.dw_ek_Dato_udloeb,
    src.dato_tid_udloeb,
    src.dw_ek_Dato_postering,
    src.dato_tid_postering,
    src.dw_ek_Dato_aflevering,
    src.dato_tid_aflevering,

    -- Measures
    src.vareudlevering_antal,
    src.vareudlevering_beloeb,
    src.indkoeb_beloeb,
    src.rabatbeloeb,
    src.lager_antal,

    src.dw_haendelsestidspunkt

INTO #Vareudlevering_Facts
FROM [stg].[fct_Vareudlevering_ils] src

-- Vareudlevering map (provides the shared PK with dim.Vareudlevering)
INNER JOIN [map].[Vareudlevering] vu_map
    ON vu_map.clean_code_Vareudlevering = src.clean_code_Vareudlevering

-- Lager lookup (CleanCode from udlev.sgh + udlev.lgr)
LEFT JOIN [map].[Lager] lager_map
    ON lager_map.clean_code_Lager = src.clean_code_Lager
   AND src.clean_code_Lager IS NOT NULL
   AND src.clean_code_Lager <> ''

-- ILSVare lookup (vnr -> ILSVARE_{vnr} CleanCode -> map.ILSVare)
LEFT JOIN [map].[ILSVare] ilsvare_map
    ON ilsvare_map.clean_code_ILSVare = src.clean_code_ILSVare
   AND src.clean_code_ILSVare IS NOT NULL
   AND src.clean_code_ILSVare <> ''

LEFT JOIN [dim].[ILSVare] di
    ON di.dw_ek_ILSVare = ilsvare_map.dw_ek_ILSVare
   AND ilsvare_map.dw_ek_ILSVare IS NOT NULL
   AND ilsvare_map.dw_ek_ILSVare <> -1

-- Rekvirent lookup (CleanCode from udlev.sgh + udlev.rnr)
LEFT JOIN [map].[Rekvirent] rekvirent_map
    ON rekvirent_map.clean_code_Rekvirent = src.clean_code_Rekvirent
   AND src.clean_code_Rekvirent IS NOT NULL
   AND src.clean_code_Rekvirent <> ''

-- Vare lookup (constructed LEVVARE CleanCode from udlev.lnr + udlev.lvnr_1)
LEFT JOIN [map].[Leverandoervare] vare_map
    ON vare_map.clean_code_Leverandoervare = src.clean_code_Leverandoervare
   AND src.clean_code_Leverandoervare IS NOT NULL
   AND src.clean_code_Leverandoervare <> '';
"""

MERGE_SQL = """
MERGE [fct].[Vareudlevering] AS tgt
USING #Vareudlevering_Facts AS src
ON tgt.dw_ek_Vareudlevering = src.dw_ek_Vareudlevering

WHEN MATCHED AND EXISTS (
    SELECT
        src.clean_code_Vareudlevering, src.bk_prrowid,
        src.dw_ek_ILSVare, src.dw_ek_UNSPSC, src.dw_ek_Lager,
        src.dw_ek_Rekvirent, src.dw_ek_Leverandoervare,
        src.dw_ek_Dato_vareudlevering, src.dato_tid_vareudlevering,
        src.dw_ek_Dato_udloeb, src.dato_tid_udloeb,
        src.dw_ek_Dato_postering, src.dato_tid_postering,
        src.dw_ek_Dato_aflevering, src.dato_tid_aflevering,
        src.vareudlevering_antal, src.vareudlevering_beloeb,
        src.indkoeb_beloeb, src.rabatbeloeb, src.lager_antal,
        src.dw_haendelsestidspunkt
    EXCEPT
    SELECT
        tgt.clean_code_Vareudlevering, tgt.bk_prrowid,
        tgt.dw_ek_ILSVare, tgt.dw_ek_UNSPSC, tgt.dw_ek_Lager,
        tgt.dw_ek_Rekvirent, tgt.dw_ek_Leverandoervare,
        tgt.dw_ek_Dato_vareudlevering, tgt.dato_tid_vareudlevering,
        tgt.dw_ek_Dato_udloeb, tgt.dato_tid_udloeb,
        tgt.dw_ek_Dato_postering, tgt.dato_tid_postering,
        tgt.dw_ek_Dato_aflevering, tgt.dato_tid_aflevering,
        tgt.vareudlevering_antal, tgt.vareudlevering_beloeb,
        tgt.indkoeb_beloeb, tgt.rabatbeloeb, tgt.lager_antal,
        tgt.dw_haendelsestidspunkt
) THEN
    UPDATE SET
        tgt.clean_code_Vareudlevering = src.clean_code_Vareudlevering,
        tgt.bk_prrowid = src.bk_prrowid,
        tgt.dw_ek_ILSVare = src.dw_ek_ILSVare,
        tgt.dw_ek_UNSPSC = src.dw_ek_UNSPSC,
        tgt.dw_ek_Lager = src.dw_ek_Lager,
        tgt.dw_ek_Rekvirent = src.dw_ek_Rekvirent,
        tgt.dw_ek_Leverandoervare = src.dw_ek_Leverandoervare,
        tgt.dw_ek_Dato_vareudlevering = src.dw_ek_Dato_vareudlevering,
        tgt.dato_tid_vareudlevering = src.dato_tid_vareudlevering,
        tgt.dw_ek_Dato_udloeb = src.dw_ek_Dato_udloeb,
        tgt.dato_tid_udloeb = src.dato_tid_udloeb,
        tgt.dw_ek_Dato_postering = src.dw_ek_Dato_postering,
        tgt.dato_tid_postering = src.dato_tid_postering,
        tgt.dw_ek_Dato_aflevering = src.dw_ek_Dato_aflevering,
        tgt.dato_tid_aflevering = src.dato_tid_aflevering,
        tgt.vareudlevering_antal = src.vareudlevering_antal,
        tgt.vareudlevering_beloeb = src.vareudlevering_beloeb,
        tgt.indkoeb_beloeb = src.indkoeb_beloeb,
        tgt.rabatbeloeb = src.rabatbeloeb,
        tgt.lager_antal = src.lager_antal,
        tgt.dw_haendelsestidspunkt = src.dw_haendelsestidspunkt,
        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dw_ek_Vareudlevering, clean_code_Vareudlevering, bk_prrowid,
        dw_ek_ILSVare, dw_ek_UNSPSC, dw_ek_Lager, dw_ek_Rekvirent, dw_ek_Leverandoervare,
        dw_ek_Dato_vareudlevering, dato_tid_vareudlevering,
        dw_ek_Dato_udloeb, dato_tid_udloeb,
        dw_ek_Dato_postering, dato_tid_postering,
        dw_ek_Dato_aflevering, dato_tid_aflevering,
        vareudlevering_antal, vareudlevering_beloeb, indkoeb_beloeb, rabatbeloeb, lager_antal,
        dw_haendelsestidspunkt, dw_timestamp_insert, dw_timestamp_update
    )
    VALUES (
        src.dw_ek_Vareudlevering, src.clean_code_Vareudlevering, src.bk_prrowid,
        src.dw_ek_ILSVare, src.dw_ek_UNSPSC, src.dw_ek_Lager, src.dw_ek_Rekvirent, src.dw_ek_Leverandoervare,
        src.dw_ek_Dato_vareudlevering, src.dato_tid_vareudlevering,
        src.dw_ek_Dato_udloeb, src.dato_tid_udloeb,
        src.dw_ek_Dato_postering, src.dato_tid_postering,
        src.dw_ek_Dato_aflevering, src.dato_tid_aflevering,
        src.vareudlevering_antal, src.vareudlevering_beloeb, src.indkoeb_beloeb, src.rabatbeloeb, src.lager_antal,
        src.dw_haendelsestidspunkt,
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = """
INSERT INTO [fct].[Vareudlevering] WITH (TABLOCK) (
    dw_ek_Vareudlevering, clean_code_Vareudlevering, bk_prrowid,
    dw_ek_ILSVare, dw_ek_UNSPSC, dw_ek_Lager, dw_ek_Rekvirent, dw_ek_Leverandoervare,
    dw_ek_Dato_vareudlevering, dato_tid_vareudlevering,
    dw_ek_Dato_udloeb, dato_tid_udloeb,
    dw_ek_Dato_postering, dato_tid_postering,
    dw_ek_Dato_aflevering, dato_tid_aflevering,
    vareudlevering_antal, vareudlevering_beloeb, indkoeb_beloeb, rabatbeloeb, lager_antal,
    dw_haendelsestidspunkt, dw_timestamp_insert, dw_timestamp_update
)
SELECT
    src.dw_ek_Vareudlevering, src.clean_code_Vareudlevering, src.bk_prrowid,
    src.dw_ek_ILSVare, src.dw_ek_UNSPSC, src.dw_ek_Lager, src.dw_ek_Rekvirent, src.dw_ek_Leverandoervare,
    src.dw_ek_Dato_vareudlevering, src.dato_tid_vareudlevering,
    src.dw_ek_Dato_udloeb, src.dato_tid_udloeb,
    src.dw_ek_Dato_postering, src.dato_tid_postering,
    src.dw_ek_Dato_aflevering, src.dato_tid_aflevering,
    src.vareudlevering_antal, src.vareudlevering_beloeb, src.indkoeb_beloeb, src.rabatbeloeb, src.lager_antal,
    src.dw_haendelsestidspunkt,
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM #Vareudlevering_Facts src;
"""


@asset(
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_Vareudlevering_ils"]),
        "map_Vareudlevering",
        "map_Lager",
        "map_Rekvirent",
        "map_Leverandoervare",
        "map_ILSVare",
        "dim_ILSVare",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "delivery_line",
    },
    description=(
        "Fact table for deliveries - reads precomputed staging columns "
        "and looks up DW_EK from maps via CleanCodes"
    ),
)
def fct_Vareudlevering(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map lookups + MERGE into fct.Vareudlevering. All conversions done in staging."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct",
        "Vareudlevering",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Vareudlevering fact table load...")

                # DIAGNOSTIC: Count staging source records
                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_Vareudlevering_ils]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging count: {staging_count}")

                # Fail-fast: every staging CleanCode must already exist in map.Vareudlevering
                # (map_Vareudlevering must run before fct_Vareudlevering)
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [stg].[fct_Vareudlevering_ils] src
                    LEFT JOIN [map].[Vareudlevering] m
                        ON m.clean_code_Vareudlevering = src.clean_code_Vareudlevering
                    WHERE m.dw_ek_Vareudlevering IS NULL
                """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"fct_Vareudlevering: {unmapped_rows} staging rows have no match in map.Vareudlevering. "
                        "Ensure map_Vareudlevering runs before fct_Vareudlevering and staging CleanCodes are valid."
                    )

                cursor.execute(RESOLVE_SQL)

                temp_count = cursor.rowcount
                context.log.info(f"Created temp table with {temp_count} rows")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_source_rows,
                    COUNT(DISTINCT clean_code_Vareudlevering) AS distinct_vareudlevering_cleancodes
                FROM #Vareudlevering_Facts
            """
                )
                source_stats = cursor.fetchone()
                duplicate_source_rows = source_stats[0] - source_stats[1]
                if duplicate_source_rows > 0:
                    message = (
                        "Duplicate grain violation in fct_Vareudlevering: "
                        f"{duplicate_source_rows} duplicate source rows found in #Vareudlevering_Facts. "
                        "Expected one row per clean_code_Vareudlevering from pre_stg/stg assets."
                    )
                    context.log.error(message)
                    raise ValueError(message)

                # Short-circuit: on a fresh/rebuilt (empty) target a bulk
                # INSERT ... WITH (TABLOCK) can be minimally logged, whereas
                # MERGE is always fully logged. Facts carry no -1 sentinel, so
                # any existing row means the incremental MERGE path applies.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [fct].[Vareudlevering]) "
                    "THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty - bulk INSERT WITH (TABLOCK), minimally logged")
                    cursor.execute(INSERT_SQL)

                rows_affected = cursor.rowcount
                conn.commit()

                # DIAGNOSTIC: FK matching statistics
                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_facts,
                    SUM(CASE WHEN dw_ek_ILSVare = -1 THEN 1 ELSE 0 END) AS missing_ilsvare_fk,
                    SUM(CASE WHEN dw_ek_UNSPSC = -1 THEN 1 ELSE 0 END) AS missing_unspsc_fk,
                    SUM(CASE WHEN dw_ek_Lager = -1 THEN 1 ELSE 0 END) AS missing_lager_fk,
                    SUM(CASE WHEN dw_ek_Rekvirent = -1 THEN 1 ELSE 0 END) AS missing_rekvirent_fk,
                    SUM(CASE WHEN dw_ek_Leverandoervare = -1 THEN 1 ELSE 0 END) AS missing_vare_fk,
                    SUM(CASE WHEN dw_ek_Dato_vareudlevering = -1 THEN 1 ELSE 0 END) AS missing_dato_fk
                FROM [fct].[Vareudlevering]
            """
                )
                diag = cursor.fetchone()

                context.log.info(
                    f"FK Matching: Total={diag[0]}, "
                    f"Missing ILSVare={diag[1]}, Missing UNSPSC={diag[2]}, Missing Lager={diag[3]}, "
                    f"Missing Rekvirent={diag[4]}, Missing Vare={diag[5]}, Missing Dato={diag[6]}"
                )

                if diag[1] > 0:
                    context.log.warning(
                        f"{diag[1]} deliveries without ILSVare FK (using -1/UNKNOWN)"
                    )

                if diag[2] > 0:
                    context.log.warning(
                        f"{diag[2]} deliveries without UNSPSC FK (using -1/UNKNOWN)"
                    )

                if diag[3] > 0:
                    context.log.warning(f"{diag[3]} deliveries without Lager FK (using -1/UNKNOWN)")

                if diag[4] > 0:
                    context.log.warning(
                        f"{diag[4]} deliveries without Rekvirent FK (using -1/UNKNOWN)"
                    )

                if diag[5] > 0:
                    context.log.warning(f"{diag[5]} deliveries without Vare FK (using -1/UNKNOWN)")

                if diag[6] > 0:
                    context.log.warning(f"{diag[6]} deliveries without Dato FK (using -1/UNKNOWN)")

                # Clean up temp table
                cursor.execute("DROP TABLE IF EXISTS #Vareudlevering_Facts")

                # Update statistics
                cursor.execute("UPDATE STATISTICS [fct].[Vareudlevering]")

                context.log.info(f"Fact table load complete: {rows_affected} rows processed")
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Vareudlevering", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "Vareudlevering")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Vareudlevering",
            "grain": "clean_code_Vareudlevering (dbo.udlev.prrowid)",
            "staging_source": "stg.fct_Vareudlevering_ils (Layer 1)",
            "dw_ek_lookups": "ILSVare, UNSPSC, Lager, Rekvirent, Vare, Dato",
            "duplicate_source_rows": duplicate_source_rows,
            "missing_ilsvare_fk": diag[1],
            "missing_unspsc_fk": diag[2],
            "missing_lager_fk": diag[3],
            "missing_rekvirent_fk": diag[4],
            "missing_vare_fk": diag[5],
            "missing_dato_fk": diag[6],
        }
    )
