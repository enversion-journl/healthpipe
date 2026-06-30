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

Reads from stg.fct_vareudlevering_ils (Layer 1 staging) which contains
precomputed date keys, typed measures, combined datetimes, and
pre-resolved Resolved_Vare_CleanCode.

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
    vu_map.DW_EK_Vareudlevering,
    src.CleanCode_Vareudlevering,
    src.bk_prrowid,

    -- IlsVare lookup via vnr -> ILSVARE_{vnr} CleanCode -> map.IlsVare
    COALESCE(ilsvare_map.DW_EK_IlsVare, -1) AS DW_EK_IlsVare,
    COALESCE(di.[DW_EK_UNSPSC], -1) AS DW_EK_UNSPSC,

    -- Lager lookup
    COALESCE(lager_map.DW_EK_Lager, -1) AS DW_EK_Lager,

    -- Rekvirent lookup
    COALESCE(rekvirent_map.DW_EK_Rekvirent, -1) AS DW_EK_Rekvirent,

    -- Vare lookup via pre-resolved CleanCode (PK-based, guaranteed unique)
    COALESCE(vare_map.DW_EK_Vare, -1) AS DW_EK_Vare,

    -- VareudleveringEgenskaber FK (1:1 dim — same value as fact PK from same map)
    vu_map.DW_EK_Vareudlevering AS DW_EK_VareudleveringEgenskaber,

    -- Precomputed in staging
    src.DW_EK_Dato_Vareudlevering,
    src.DW_EK_Dato_Udloeb,
    src.DW_EK_Dato_Postering,
    src.DW_EK_Dato_Aflevering,

    -- Measures
    src.Vareudlevering_Antal,
    src.Vareudlevering_Beloeb,
    src.Indkoeb_Beloeb,
    src.Rabat_Beloeb,
    src.Lager_Antal,

    -- Combined datetimes
    src.Datotid_Vareudlevering,
    src.Datotid_Udloeb,
    src.Datotid_Postering,
    src.Datotid_Aflevering,

    src.DW_haendelsestidspunkt

INTO #Vareudlevering_Facts
FROM [stg].[fct_vareudlevering_ils] src

-- Vareudlevering map (provides fact PK and the egenskaber FK — same value)
INNER JOIN [map].[Vareudlevering] vu_map
    ON vu_map.CleanCode_Vareudlevering = src.CleanCode_Vareudlevering

-- Lager lookup (CleanCode from udlev.sgh + udlev.lgr)
LEFT JOIN [map].[Lager] lager_map
    ON lager_map.CleanCode_Lager = src.Lager_CleanCode
   AND src.Lager_CleanCode IS NOT NULL
   AND src.Lager_CleanCode <> ''

-- IlsVare lookup (vnr -> ILSVARE_{vnr} CleanCode -> map.IlsVare)
LEFT JOIN [map].[IlsVare] ilsvare_map
    ON ilsvare_map.CleanCode_IlsVare = src.IlsVare_CleanCode
   AND src.IlsVare_CleanCode IS NOT NULL
   AND src.IlsVare_CleanCode <> ''

LEFT JOIN [dim].[IlsVare] di
    ON di.DW_EK_IlsVare = ilsvare_map.DW_EK_IlsVare
   AND ilsvare_map.DW_EK_IlsVare IS NOT NULL
   AND ilsvare_map.DW_EK_IlsVare <> -1

-- Rekvirent lookup (CleanCode from udlev.sgh + udlev.rnr)
LEFT JOIN [map].[Rekvirent] rekvirent_map
    ON rekvirent_map.CleanCode_Rekvirent = src.Rekvirent_CleanCode
   AND src.Rekvirent_CleanCode IS NOT NULL
   AND src.Rekvirent_CleanCode <> ''

-- Vare lookup via pre-resolved Resolved_Vare_CleanCode (map.Vare PK)
LEFT JOIN [map].[Vare] vare_map
    ON vare_map.CleanCode_Vare = src.Resolved_Vare_CleanCode
   AND src.Resolved_Vare_CleanCode IS NOT NULL
   AND src.Resolved_Vare_CleanCode <> '';
"""

MERGE_SQL = """
MERGE [fct].[Vareudlevering] AS tgt
USING #Vareudlevering_Facts AS src
ON tgt.DW_EK_Vareudlevering = src.DW_EK_Vareudlevering

WHEN MATCHED THEN
    UPDATE SET
        tgt.CleanCode_Vareudlevering = src.CleanCode_Vareudlevering,
        tgt.bk_prrowid = src.bk_prrowid,
        tgt.DW_EK_IlsVare = src.DW_EK_IlsVare,
        tgt.DW_EK_UNSPSC = src.DW_EK_UNSPSC,
        tgt.DW_EK_Lager = src.DW_EK_Lager,
        tgt.DW_EK_Rekvirent = src.DW_EK_Rekvirent,
        tgt.DW_EK_Vare = src.DW_EK_Vare,
        tgt.DW_EK_VareudleveringEgenskaber = src.DW_EK_VareudleveringEgenskaber,
        tgt.DW_EK_Dato_Vareudlevering = src.DW_EK_Dato_Vareudlevering,
        tgt.DW_EK_Dato_Udloeb = src.DW_EK_Dato_Udloeb,
        tgt.DW_EK_Dato_Postering = src.DW_EK_Dato_Postering,
        tgt.DW_EK_Dato_Aflevering = src.DW_EK_Dato_Aflevering,
        tgt.Vareudlevering_Antal = src.Vareudlevering_Antal,
        tgt.Vareudlevering_Beloeb = src.Vareudlevering_Beloeb,
        tgt.Indkoeb_Beloeb = src.Indkoeb_Beloeb,
        tgt.Rabat_Beloeb = src.Rabat_Beloeb,
        tgt.Lager_Antal = src.Lager_Antal,
        tgt.Datotid_Vareudlevering = src.Datotid_Vareudlevering,
        tgt.Datotid_Udloeb = src.Datotid_Udloeb,
        tgt.Datotid_Postering = src.Datotid_Postering,
        tgt.Datotid_Aflevering = src.Datotid_Aflevering,
        tgt.DW_haendelsestidspunkt = src.DW_haendelsestidspunkt,
        tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        DW_EK_Vareudlevering, CleanCode_Vareudlevering, bk_prrowid,
        DW_EK_IlsVare, DW_EK_UNSPSC, DW_EK_Lager, DW_EK_Rekvirent, DW_EK_Vare,
        DW_EK_VareudleveringEgenskaber,
        DW_EK_Dato_Vareudlevering, DW_EK_Dato_Udloeb, DW_EK_Dato_Postering, DW_EK_Dato_Aflevering,
        Vareudlevering_Antal, Vareudlevering_Beloeb, Indkoeb_Beloeb, Rabat_Beloeb, Lager_Antal,
        Datotid_Vareudlevering, Datotid_Udloeb, Datotid_Postering, Datotid_Aflevering,
        DW_haendelsestidspunkt, DW_Timestamp_Insert, DW_Timestamp_Update
    )
    VALUES (
        src.DW_EK_Vareudlevering, src.CleanCode_Vareudlevering, src.bk_prrowid,
        src.DW_EK_IlsVare, src.DW_EK_UNSPSC, src.DW_EK_Lager, src.DW_EK_Rekvirent, src.DW_EK_Vare,
        src.DW_EK_VareudleveringEgenskaber,
        src.DW_EK_Dato_Vareudlevering, src.DW_EK_Dato_Udloeb, src.DW_EK_Dato_Postering, src.DW_EK_Dato_Aflevering,
        src.Vareudlevering_Antal, src.Vareudlevering_Beloeb, src.Indkoeb_Beloeb, src.Rabat_Beloeb, src.Lager_Antal,
        src.Datotid_Vareudlevering, src.Datotid_Udloeb, src.Datotid_Postering, src.Datotid_Aflevering,
        src.DW_haendelsestidspunkt,
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""


@asset(
    group_name="facts_vareudlevering",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_vareudlevering_ils"]),
        "map_vareudlevering",
        "map_lager",
        "map_rekvirent",
        "map_vare",
        "map_ilsvare",
        "dim_ilsvare",
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
def fct_vareudlevering(
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
                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_vareudlevering_ils]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging count: {staging_count}")

                # Fail-fast: every staging CleanCode must already exist in map.Vareudlevering
                # (map_vareudlevering must run before fct_vareudlevering)
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [stg].[fct_vareudlevering_ils] src
                    LEFT JOIN [map].[Vareudlevering] m
                        ON m.CleanCode_Vareudlevering = src.CleanCode_Vareudlevering
                    WHERE m.DW_EK_Vareudlevering IS NULL
                """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"fct_vareudlevering: {unmapped_rows} staging rows have no match in map.Vareudlevering. "
                        "Ensure map_vareudlevering runs before fct_vareudlevering and staging CleanCodes are valid."
                    )

                cursor.execute(RESOLVE_SQL)

                temp_count = cursor.rowcount
                context.log.info(f"Created temp table with {temp_count} rows")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_source_rows,
                    COUNT(DISTINCT CleanCode_Vareudlevering) AS distinct_vareudlevering_cleancodes
                FROM #Vareudlevering_Facts
            """
                )
                source_stats = cursor.fetchone()
                duplicate_source_rows = source_stats[0] - source_stats[1]
                if duplicate_source_rows > 0:
                    message = (
                        "Duplicate grain violation in fct_vareudlevering: "
                        f"{duplicate_source_rows} duplicate source rows found in #Vareudlevering_Facts. "
                        "Expected one row per CleanCode_Vareudlevering from pre_stg/stg assets."
                    )
                    context.log.error(message)
                    raise ValueError(message)

                cursor.execute(MERGE_SQL)

                rows_affected = cursor.rowcount
                conn.commit()

                # DIAGNOSTIC: FK matching statistics
                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_facts,
                    SUM(CASE WHEN DW_EK_IlsVare = -1 THEN 1 ELSE 0 END) AS missing_ilsvare_fk,
                    SUM(CASE WHEN DW_EK_UNSPSC = -1 THEN 1 ELSE 0 END) AS missing_unspsc_fk,
                    SUM(CASE WHEN DW_EK_Lager = -1 THEN 1 ELSE 0 END) AS missing_lager_fk,
                    SUM(CASE WHEN DW_EK_Rekvirent = -1 THEN 1 ELSE 0 END) AS missing_rekvirent_fk,
                    SUM(CASE WHEN DW_EK_Vare = -1 THEN 1 ELSE 0 END) AS missing_vare_fk,
                    SUM(CASE WHEN DW_EK_Dato_Vareudlevering = -1 THEN 1 ELSE 0 END) AS missing_dato_fk,
                    SUM(CASE WHEN DW_EK_VareudleveringEgenskaber = -1 THEN 1 ELSE 0 END) AS missing_egenskaber_fk
                FROM [fct].[Vareudlevering]
            """
                )
                diag = cursor.fetchone()

                context.log.info(
                    f"FK Matching: Total={diag[0]}, "
                    f"Missing IlsVare={diag[1]}, Missing UNSPSC={diag[2]}, Missing Lager={diag[3]}, "
                    f"Missing Rekvirent={diag[4]}, Missing Vare={diag[5]}, Missing Dato={diag[6]}, "
                    f"Missing Egenskaber={diag[7]}"
                )

                if diag[1] > 0:
                    context.log.warning(f"{diag[1]} deliveries without IlsVare FK (using -1/UNKNOWN)")

                if diag[2] > 0:
                    context.log.warning(f"{diag[2]} deliveries without UNSPSC FK (using -1/UNKNOWN)")

                if diag[3] > 0:
                    context.log.warning(f"{diag[3]} deliveries without Lager FK (using -1/UNKNOWN)")

                if diag[4] > 0:
                    context.log.warning(f"{diag[4]} deliveries without Rekvirent FK (using -1/UNKNOWN)")

                if diag[5] > 0:
                    context.log.warning(f"{diag[5]} deliveries without Vare FK (using -1/UNKNOWN)")

                if diag[6] > 0:
                    context.log.warning(f"{diag[6]} deliveries without Dato FK (using -1/UNKNOWN)")

                if diag[7] > 0:
                    context.log.warning(
                        f"{diag[7]} deliveries without VareudleveringEgenskaber FK (using -1/UNKNOWN)"
                    )

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
            "grain": "CleanCode_Vareudlevering (dbo.udlev.prrowid)",
            "staging_source": "stg.fct_vareudlevering_ils (Layer 1)",
            "dw_ek_lookups": "IlsVare, UNSPSC, Lager, Rekvirent, Vare, Dato",
            "duplicate_source_rows": duplicate_source_rows,
            "missing_ilsvare_fk": diag[1],
            "missing_unspsc_fk": diag[2],
            "missing_lager_fk": diag[3],
            "missing_rekvirent_fk": diag[4],
            "missing_vare_fk": diag[5],
            "missing_dato_fk": diag[6],
            "missing_egenskaber_fk": diag[7],
        }
    )
