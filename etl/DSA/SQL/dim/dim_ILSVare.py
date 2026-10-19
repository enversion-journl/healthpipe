# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: ILSVare

Type 1 SCD -- MERGE from stg.dim_ILSVare (delta) via map.ILSVare for DW_EK resolution.
dw_ek_UNSPSC and dw_ek_UNSPSC_intern resolved here (at dim-time) via map.UNSPSC.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import dato_key

RESOLVE_SQL = f"""
    DROP TABLE IF EXISTS #IlsVare_Resolved;

    SELECT
        m.dw_ek_ILSVare,
        stg.clean_code_ILSVare,
        stg.vnr AS varenummer,
        stg.navn AS varenavn,
        stg.navn2 AS varenavn2,
        stg.varestr AS varestr,
        stg.basisenh AS basisenhed,
        stg.vareart AS gruppering,
        stg.vareart_navn AS gruppering_navn,
        stg.varegrp AS art,
        stg.varegrp_navn AS art_navn,
        stg.varetype AS varetype,
        stg.varetype_navn AS varetype_navn,
        stg.momskd AS momskode,
        stg.momskode_navn AS momskode_navn,
        stg.momspct AS momsprocent,
        {dato_key('stg.oprdto')} AS dw_ek_Dato_oprettet,
        CASE
            WHEN stg.oprdto IS NOT NULL AND stg.oprtid IS NOT NULL
            THEN DATEADD(SECOND, stg.oprtid, CAST(stg.oprdto AS DATETIME2))
            WHEN stg.oprdto IS NOT NULL
            THEN CAST(stg.oprdto AS DATETIME2)
            ELSE NULL
        END AS dato_tid_oprettet,
        COALESCE(um_primary.[dw_ek_UNSPSC], -1) AS dw_ek_UNSPSC,
        COALESCE(um_intern.[dw_ek_UNSPSC], -1) AS dw_ek_UNSPSC_intern,
        NULLIF(stg.faelvnr, '') AS faelles_varenummer,
        NULLIF(stg.ean_vnr, '') AS ean_varenummer,
        NULLIF(stg.billed, '') AS billede,
        UPPER(NULLIF(stg.oprbruger, '')) AS opr_bruger,
        NULLIF(stg.oprbruger_navn, '') AS opr_bruger_navn,
        NULLIF(stg.billedli, '') AS weblink,
        NULLIF(stg.ejlukkes, '') AS maa_ikke_lukkes,
        NULLIF(stg.steril, '') AS hygiejne,
        stg.aktiv AS aktiv
    INTO #IlsVare_Resolved
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY clean_code_ILSVare
                ORDER BY pro2modified DESC, clean_code_ILSVare ASC
            ) AS rn
        FROM [stg].[dim_ILSVare]
        WHERE clean_code_ILSVare IS NOT NULL
          AND clean_code_ILSVare <> ''
    ) stg
    INNER JOIN [map].[ILSVare] m
        ON m.clean_code_ILSVare = stg.clean_code_ILSVare
    LEFT JOIN [map].[UNSPSC] um_primary
        ON um_primary.[clean_code_UNSPSC] = NULLIF(LTRIM(RTRIM(stg.unspc)), '')
    LEFT JOIN [map].[UNSPSC] um_intern
        ON um_intern.[clean_code_UNSPSC] = CASE
            WHEN stg.unspsc2 LIKE '%.0'
            THEN LEFT(LTRIM(RTRIM(stg.unspsc2)), LEN(LTRIM(RTRIM(stg.unspsc2))) - 2)
            ELSE NULLIF(LTRIM(RTRIM(stg.unspsc2)), '')
        END
    WHERE stg.rn = 1;
"""

MERGE_SQL = """
    MERGE INTO [dim].[ILSVare] AS tgt
    USING #IlsVare_Resolved AS src
        ON tgt.dw_ek_ILSVare = src.dw_ek_ILSVare
    WHEN MATCHED AND EXISTS (
        SELECT
            src.clean_code_ILSVare, src.varenummer, src.varenavn, src.varenavn2,
            src.varestr, src.basisenhed, src.gruppering, src.gruppering_navn,
            src.art, src.art_navn, src.varetype, src.varetype_navn,
            src.momskode, src.momskode_navn, src.momsprocent,
            src.dw_ek_Dato_oprettet, src.dato_tid_oprettet,
            src.dw_ek_UNSPSC, src.dw_ek_UNSPSC_intern,
            src.faelles_varenummer, src.ean_varenummer, src.billede,
            src.opr_bruger, src.opr_bruger_navn, src.weblink,
            src.maa_ikke_lukkes, src.hygiejne, src.aktiv
        EXCEPT
        SELECT
            tgt.clean_code_ILSVare, tgt.varenummer, tgt.varenavn, tgt.varenavn2,
            tgt.varestr, tgt.basisenhed, tgt.gruppering, tgt.gruppering_navn,
            tgt.art, tgt.art_navn, tgt.varetype, tgt.varetype_navn,
            tgt.momskode, tgt.momskode_navn, tgt.momsprocent,
            tgt.dw_ek_Dato_oprettet, tgt.dato_tid_oprettet,
            tgt.dw_ek_UNSPSC, tgt.dw_ek_UNSPSC_intern,
            tgt.faelles_varenummer, tgt.ean_varenummer, tgt.billede,
            tgt.opr_bruger, tgt.opr_bruger_navn, tgt.weblink,
            tgt.maa_ikke_lukkes, tgt.hygiejne, tgt.aktiv
    ) THEN UPDATE SET
        tgt.clean_code_ILSVare = src.clean_code_ILSVare,
        tgt.varenummer = src.varenummer,
        tgt.varenavn = src.varenavn,
        tgt.varenavn2 = src.varenavn2,
        tgt.varestr = src.varestr,
        tgt.basisenhed = src.basisenhed,
        tgt.gruppering = src.gruppering,
        tgt.gruppering_navn = src.gruppering_navn,
        tgt.art = src.art,
        tgt.art_navn = src.art_navn,
        tgt.varetype = src.varetype,
        tgt.varetype_navn = src.varetype_navn,
        tgt.momskode = src.momskode,
        tgt.momskode_navn = src.momskode_navn,
        tgt.momsprocent = src.momsprocent,
        tgt.dw_ek_Dato_oprettet = src.dw_ek_Dato_oprettet,
        tgt.dato_tid_oprettet = src.dato_tid_oprettet,
        tgt.dw_ek_UNSPSC = src.dw_ek_UNSPSC,
        tgt.dw_ek_UNSPSC_intern = src.dw_ek_UNSPSC_intern,
        tgt.faelles_varenummer = src.faelles_varenummer,
        tgt.ean_varenummer = src.ean_varenummer,
        tgt.billede = src.billede,
        tgt.opr_bruger = src.opr_bruger,
        tgt.opr_bruger_navn = src.opr_bruger_navn,
        tgt.weblink = src.weblink,
        tgt.maa_ikke_lukkes = src.maa_ikke_lukkes,
        tgt.hygiejne = src.hygiejne,
        tgt.aktiv = src.aktiv,
        tgt.dw_timestamp_update = SYSDATETIME()
    WHEN NOT MATCHED THEN INSERT (
        dw_ek_ILSVare, clean_code_ILSVare, varenummer,
        varenavn, varenavn2,
        varestr, basisenhed,
        gruppering, gruppering_navn, art, art_navn,
        varetype, varetype_navn,
        momskode, momskode_navn, momsprocent,
        dw_ek_Dato_oprettet, dato_tid_oprettet,
        dw_ek_UNSPSC, dw_ek_UNSPSC_intern,
        faelles_varenummer, ean_varenummer, billede,
        opr_bruger, opr_bruger_navn, weblink,
        maa_ikke_lukkes, hygiejne,
        aktiv, dw_timestamp_insert, dw_timestamp_update
    ) VALUES (
        src.dw_ek_ILSVare, src.clean_code_ILSVare, src.varenummer,
        src.varenavn, src.varenavn2,
        src.varestr, src.basisenhed,
        src.gruppering, src.gruppering_navn, src.art, src.art_navn,
        src.varetype, src.varetype_navn, src.momskode, src.momskode_navn,
        src.momsprocent,
        src.dw_ek_Dato_oprettet, src.dato_tid_oprettet,
        src.dw_ek_UNSPSC, src.dw_ek_UNSPSC_intern,
        src.faelles_varenummer, src.ean_varenummer, src.billede,
        src.opr_bruger, src.opr_bruger_navn, src.weblink,
        src.maa_ikke_lukkes, src.hygiejne,
        src.aktiv, SYSDATETIME(), SYSDATETIME()
    );
"""

# Short-circuit for a fresh/rebuilt target (only the -1 sentinel present): a bulk
# INSERT ... WITH (TABLOCK) can be minimally logged, whereas MERGE is always fully
# logged. The source draws dw_ek_ILSVare from map.ILSVare (IDENTITY >= 1), so it
# never collides with the -1 sentinel. Columns/order/timestamp expressions mirror
# the WHEN NOT MATCHED THEN INSERT branch exactly (bare SYSDATETIME()).
INSERT_SQL = """
    INSERT INTO [dim].[ILSVare] WITH (TABLOCK) (
        dw_ek_ILSVare, clean_code_ILSVare, varenummer,
        varenavn, varenavn2,
        varestr, basisenhed,
        gruppering, gruppering_navn, art, art_navn,
        varetype, varetype_navn,
        momskode, momskode_navn, momsprocent,
        dw_ek_Dato_oprettet, dato_tid_oprettet,
        dw_ek_UNSPSC, dw_ek_UNSPSC_intern,
        faelles_varenummer, ean_varenummer, billede,
        opr_bruger, opr_bruger_navn, weblink,
        maa_ikke_lukkes, hygiejne,
        aktiv, dw_timestamp_insert, dw_timestamp_update
    )
    SELECT
        src.dw_ek_ILSVare, src.clean_code_ILSVare, src.varenummer,
        src.varenavn, src.varenavn2,
        src.varestr, src.basisenhed,
        src.gruppering, src.gruppering_navn, src.art, src.art_navn,
        src.varetype, src.varetype_navn, src.momskode, src.momskode_navn,
        src.momsprocent,
        src.dw_ek_Dato_oprettet, src.dato_tid_oprettet,
        src.dw_ek_UNSPSC, src.dw_ek_UNSPSC_intern,
        src.faelles_varenummer, src.ean_varenummer, src.billede,
        src.opr_bruger, src.opr_bruger_navn, src.weblink,
        src.maa_ikke_lukkes, src.hygiejne,
        src.aktiv, SYSDATETIME(), SYSDATETIME()
    FROM #IlsVare_Resolved src;
"""


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_dim_ILSVare"]),
        "map_ILSVare",
        AssetKey(["map_UNSPSC"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="MERGE dim.ILSVare (Type 1 SCD) from stg.dim_ILSVare (delta)",
)
def dim_ILSVare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension maintenance: MERGE ILS item attributes."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "ILSVare", dagster_run_id=context.run_id, mode=config.mode)

    merge_count = 0
    total = 0
    resolved_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(RESOLVE_SQL)
                resolved_count = cursor.rowcount
                context.log.info(f"Resolved {resolved_count} rows via map")

                # Sentinel-aware emptiness check: treat a target holding only the
                # -1 sentinel as empty and take the minimally logged bulk-INSERT
                # path; otherwise MERGE-upsert.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[ILSVare] "
                    "WHERE dw_ek_ILSVare <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info(
                        "Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK), "
                        "minimally logged"
                    )
                    cursor.execute(INSERT_SQL)

                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #IlsVare_Resolved;")
                cursor.execute("UPDATE STATISTICS [dim].[ILSVare];")

                cursor.execute("SELECT COUNT(*) FROM [dim].[ILSVare]")
                total = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"dim.ILSVare: {merge_count} merged, {total} total")

        rows_affected = merge_count
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "ILSVare", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "ILSVare")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "dim.ILSVare",
            "merged_rows": merge_count,
            "total_rows": total,
            "resolved_from_map": resolved_count,
            "pattern": "MERGE (Type 1 SCD)",
            "mode": config.mode,
        }
    )
