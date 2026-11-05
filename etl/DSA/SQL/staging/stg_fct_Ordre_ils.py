# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Ordre from ILS (Fact Layer)

The header-level measures of the ILS order headers for fct.Ordre, selected from
stg.pre_Ordre_ils: freight and fee, which belong to the order and would be
counted once per line if they sat on the order lines.

Grain: one row per clean_code_Ordre (sgh, lgr, ordre).

All type conversions (date key, measures) are done here; the map lookups live
in fct_Ordre.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import dato_key, dato_tid

BUILD_SQL = f"""
    DROP TABLE IF EXISTS [stg].[fct_Ordre_ils];

    SELECT
        p.clean_code_Ordre,
        CAST(p.prrowid AS NVARCHAR(100)) AS bk_prrowid,
        p.clean_code_Lager,
        p.clean_code_ILSLeverandoer,
        {dato_key("p.orddto")} AS dw_ek_Dato_ordre,
        {dato_tid("p.orddto")} AS dato_tid_ordre,
        CAST(p.fragtbel AS DECIMAL(17,2)) AS fragt_beloeb,
        CAST(p.gebyrbel AS DECIMAL(17,2)) AS gebyr_beloeb,
        {dato_tid("p.orddto")} AS dw_haendelsestidspunkt
    INTO [stg].[fct_Ordre_ils]
    FROM [stg].[pre_Ordre_ils] p;
"""

INDEX_SQL = """
    CREATE UNIQUE CLUSTERED INDEX IX_fct_ordre_ils_PK
    ON [stg].[fct_Ordre_ils](clean_code_Ordre);
"""

COVERAGE_SQL = """
    SELECT
        COUNT(*),
        COUNT(clean_code_Lager),
        COUNT(clean_code_ILSLeverandoer),
        SUM(CASE WHEN fragt_beloeb <> 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN gebyr_beloeb <> 0 THEN 1 ELSE 0 END)
    FROM [stg].[fct_Ordre_ils];
"""


@asset(
    key=AssetKey(["erp", "stg_fct_Ordre_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "pre_stg_Ordre_ils"])],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "ils",
        "entity": "ordre",
    },
    description=(
        "Fact staging for ILS order headers — freight and fee per order with "
        "the order date key, for fct_Ordre"
    ),
)
def stg_fct_Ordre_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Fact staging for ILS order headers from stg.pre_Ordre_ils."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "fct_Ordre_ils", dagster_run_id=context.run_id, mode=config.mode
    )

    stats: tuple[int, ...] = (0, 0, 0, 0, 0)

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
            f"stg.fct_Ordre_ils: {stats[0]} rows "
            f"({stats[1]} with clean_code_Lager, {stats[2]} with clean_code_ILSLeverandoer, "
            f"{stats[3]} with freight, {stats[4]} with a fee)"
        )
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=stats[0])
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.fct_Ordre_ils",
            "row_count": stats[0],
            "has_lager_clean": stats[1],
            "has_ilsleverandoer_clean": stats[2],
            "with_fragt": stats[3],
            "with_gebyr": stats[4],
            "source_table": "stg.pre_Ordre_ils",
            "grain": "clean_code_Ordre (sgh, lgr, ordre)",
            "mode": config.mode,
            "layer": "stg_fct (Layer 1)",
        }
    )
