# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Ordre from ILS (Dimension Layer)

Descriptive attributes of the ILS order headers for map.Ordre and dim.Ordre,
selected from stg.pre_Ordre_ils.

Grain: one row per clean_code_Ordre (sgh, lgr, ordre).

Codes are trimmed with blanks as NULL, and upper-cased where ILS users write
them in either case (lager, leverandoer, status, users). The order number is
carried as text: it identifies the order and is not a quantity.

ordre_oprettet_af is masked to '[CPR-FORM]' when bd_oprid holds a value shaped
like a CPR number, which ILS orders from 2008-2013 do. The archive keeps the raw
value, as it does for every ILS table.

The creator is resolved to the clean_code_ILSBruger of the user it names, from
arc.ils_zz_oper and not from dim.ILSBruger, so the result depends only on the
archive. Each set of initials is represented by the row stg_dim_ILSBruger keeps
for it (same ranking). The value is matched on bruger_id first; when several
users share that bruger_id, the same ranking picks one. When no user has it as
bruger_id, it is matched on the initials: ILS orders from before about 2014 carry
the initials in bd_oprid. ilsbruger_match_kilde records which match was used.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.cpr_form_sql import CPR_FORM_MARKER, sql_mask_cpr_form
from DSA.SQL.common.ils_clean_code_shared import sql_norm_code
from DSA.SQL.staging.stg_dim_ILSBruger import (
    ILSBRUGER_PREFIX,
    WINNER_ORDER,
    sql_bruger_id,
    sql_initialer,
)

BUILD_SQL = f"""
    DROP TABLE IF EXISTS [stg].[dim_Ordre_ils];

    ;WITH zz AS (
        SELECT
            {sql_initialer("z.bd_init")} AS initialer,
            {sql_bruger_id("z.bd_oprid")} AS bruger_id,
            z.bd_aktiv AS active,
            z.sysdato,
            z.pro2modified,
            z.prrowid
        FROM [arc].[ils_zz_oper] z
    ),
    -- One row per initials: the row stg_dim_ILSBruger stages for them.
    bruger AS (
        SELECT
            zz.*,
            ROW_NUMBER() OVER (
                PARTITION BY initialer
                ORDER BY {WINNER_ORDER}
            ) AS rn
        FROM zz
        WHERE initialer IS NOT NULL
    ),
    -- One user per bruger_id, ranked the same way.
    bruger_id_vinder AS (
        SELECT
            bruger.bruger_id,
            bruger.initialer,
            ROW_NUMBER() OVER (
                PARTITION BY bruger.bruger_id
                ORDER BY {WINNER_ORDER}
            ) AS rn_id
        FROM bruger
        WHERE bruger.rn = 1
          AND bruger.bruger_id IS NOT NULL
    ),
    p AS (
        SELECT
            pre.*,
            {sql_mask_cpr_form(f'UPPER({sql_norm_code("pre.bd_oprid")})')} AS opretter
        FROM [stg].[pre_Ordre_ils] pre
    )
    SELECT
        p.clean_code_Ordre,
        CAST(p.prrowid AS NVARCHAR(100)) AS bk_prrowid,
        CAST(p.ordre AS NVARCHAR(20)) AS ordrenummer,
        CAST({sql_norm_code("p.sgh")} AS NVARCHAR(8)) AS sygehus,
        CAST(UPPER({sql_norm_code("p.lgr")}) AS NVARCHAR(8)) AS lager,
        CAST(UPPER({sql_norm_code("p.lnr")}) AS NVARCHAR(20)) AS leverandoernummer,
        CAST(p.orddto AS DATE) AS ordredato,
        CAST(UPPER({sql_norm_code("p.ordstat")}) AS NVARCHAR(4)) AS ordrestatus,
        CAST(p.opretter AS NVARCHAR(20)) AS ordre_oprettet_af,
        CAST('{ILSBRUGER_PREFIX}' + COALESCE(bi.initialer, bn.initialer) AS NVARCHAR(100)) AS clean_code_ILSBruger_oprettet_af,
        CAST(CASE WHEN bi.initialer IS NOT NULL THEN 'bruger_id' WHEN bn.initialer IS NOT NULL THEN 'initialer' END AS NVARCHAR(10)) AS ilsbruger_match_kilde,
        CAST(p.sysdato AS DATETIME2) AS dato_tid_ajourfoert,
        CAST(UPPER({sql_norm_code("p.bruger")}) AS NVARCHAR(6)) AS ajourfoert_bruger
    INTO [stg].[dim_Ordre_ils]
    FROM p
    LEFT JOIN bruger_id_vinder bi
        ON bi.rn_id = 1
       AND bi.bruger_id = p.opretter
    -- Initials only when no user has the value as bruger_id.
    LEFT JOIN bruger bn
        ON bi.initialer IS NULL
       AND bn.rn = 1
       AND bn.initialer = p.opretter;
"""

INDEX_SQL = """
    CREATE UNIQUE CLUSTERED INDEX IX_dim_ordre_ils_CleanCode
    ON [stg].[dim_Ordre_ils](clean_code_Ordre);
"""

COVERAGE_SQL = f"""
    SELECT
        COUNT(*),
        COUNT(ordrestatus),
        COUNT(ordre_oprettet_af),
        COUNT(ordredato),
        SUM(CASE WHEN ordre_oprettet_af = '{CPR_FORM_MARKER}' THEN 1 ELSE 0 END),
        SUM(CASE WHEN ilsbruger_match_kilde = 'initialer' THEN 1 ELSE 0 END),
        SUM(CASE WHEN ordre_oprettet_af IS NOT NULL AND ilsbruger_match_kilde IS NULL THEN 1 ELSE 0 END)
    FROM [stg].[dim_Ordre_ils];
"""


@asset(
    key=AssetKey(["erp", "stg_dim_Ordre_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "pre_stg_Ordre_ils"]), AssetKey(["arc_ils_zz_oper"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "ordre",
    },
    description=(
        "Dimension staging for ILS order headers — identifiers and descriptive "
        "attributes for map/dim Ordre"
    ),
)
def stg_dim_Ordre_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension staging for ILS order headers from stg.pre_Ordre_ils."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "dim_Ordre_ils", dagster_run_id=context.run_id, mode=config.mode
    )

    stats: tuple[int, ...] = (0, 0, 0, 0, 0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(BUILD_SQL)
                cursor.execute(INDEX_SQL)
                conn.commit()

                cursor.execute(COVERAGE_SQL)
                stats = tuple(v or 0 for v in cursor.fetchone())
            finally:
                cursor.close()

        context.log.info(
            f"stg.dim_Ordre_ils: {stats[0]} rows "
            f"({stats[1]} with ordrestatus, {stats[2]} with ordre_oprettet_af, "
            f"{stats[3]} with ordredato, {stats[4]} creators masked as CPR form, "
            f"{stats[5]} creators matched on initials, {stats[6]} creators with no ILS user)"
        )
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=stats[0])
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Ordre_ils",
            "row_count": stats[0],
            "has_ordrestatus": stats[1],
            "has_ordre_oprettet_af": stats[2],
            "has_ordredato": stats[3],
            "ordre_oprettet_af_cpr_form_masked": stats[4],
            "ordre_oprettet_af_via_initialer": stats[5],
            "ordre_oprettet_af_without_user": stats[6],
            "source_table": "stg.pre_Ordre_ils",
            "mode": config.mode,
            "layer": "dim (Layer 1)",
        }
    )
