# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pre-Staging Asset: Ordre from ILS

Order headers (arc.ils_ordh) at the grain (sgh, lgr, ordre), one row per
clean_code_Ordre. Feeds both stg_dim_Ordre_ils and stg_fct_Ordre_ils.

arc.ils_ordh is keyed on prrowid, so two source rows can share an order. Should
they, the latest sysdato wins, then pro2modified, then the latest archive
update; prrowid breaks the remaining tie.

Delta: an order is restaged when any of its archive rows changed after the MIN
of the dim/fct checkpoints, and the winner is then picked among ALL its archive
rows, so a delta run picks the same winner as a full load.
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
from DSA.SQL.common.ils_clean_code_shared import sql_clean_code_lager
from DSA.SQL.common.ils_leverandoer_clean_code import sql_clean_code_ils_leverandoer
from DSA.SQL.common.ils_ordre_clean_code import sql_clean_code_ordre
from DSA.SQL.staging.pre_stg_Fakturalinje_mysupply import resolve_min_checkpoint

ORDRE_CLEAN_CODE_SQL = sql_clean_code_ordre("o.sgh", "o.lgr", "o.ordre")


def build_sql(delta: bool) -> str:
    """The pre-staging SELECT INTO; ``delta`` binds the checkpoint as the only parameter."""
    touched = (
        f"""
        AND clean_code_Ordre IN (
            SELECT {ORDRE_CLEAN_CODE_SQL}
            FROM [arc].[ils_ordh] o
            WHERE o.dw_timestamp_update > ?
        )"""
        if delta
        else ""
    )
    return f"""
    ;WITH src AS (
        SELECT
            o.*,
            {ORDRE_CLEAN_CODE_SQL} AS clean_code_Ordre
        FROM [arc].[ils_ordh] o
    ),
    ranked AS (
        SELECT
            src.*,
            ROW_NUMBER() OVER (
                PARTITION BY clean_code_Ordre
                ORDER BY sysdato DESC, pro2modified DESC, dw_timestamp_update DESC, prrowid ASC
            ) AS rn
        FROM src
        WHERE clean_code_Ordre IS NOT NULL{touched}
    )
    SELECT
        clean_code_Ordre,
        {sql_clean_code_lager("sgh", "lgr")} AS clean_code_Lager,
        {sql_clean_code_ils_leverandoer("lnr")} AS clean_code_ILSLeverandoer,
        prrowid,
        sgh,
        lgr,
        ordre,
        lnr,
        orddto,
        ordstat,
        bd_oprid,
        sysdato,
        bruger,
        fragtbel,
        gebyrbel,
        dw_timestamp_update
    INTO [stg].[pre_Ordre_ils]
    FROM ranked
    WHERE rn = 1;
"""


INDEX_SQL = """
    CREATE UNIQUE CLUSTERED INDEX IX_pre_ordre_ils_PK
    ON [stg].[pre_Ordre_ils](clean_code_Ordre);
"""

# Source rows the grain drops: no order number, or a second row for the same order.
DROPPED_SQL = f"""
    SELECT
        SUM(CASE WHEN clean_code_Ordre IS NULL THEN 1 ELSE 0 END),
        COUNT(clean_code_Ordre) - COUNT(DISTINCT clean_code_Ordre)
    FROM (
        SELECT {ORDRE_CLEAN_CODE_SQL} AS clean_code_Ordre
        FROM [arc].[ils_ordh] o
    ) c;
"""


@asset(
    key=AssetKey(["erp", "pre_stg_Ordre_ils"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["arc_ils_ordh"])],
    tags={
        "domain": "erp",
        "layer": "pre_stg",
        "source": "ils",
        "entity": "ordre",
    },
    description=(
        "Pre-staging for ILS order headers from arc.ils_ordh at one row per "
        "(sgh, lgr, ordre), with the Ordre, Lager and ILSLeverandoer CleanCodes."
    ),
)
def pre_stg_Ordre_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Pre-staging for ILS order headers with grain-level deduplication."""

    dfm = DfmManager(mssql_target_staging, context)

    # Delta filter from the downstream checkpoints (MIN of the dim/fct owners).
    min_checkpoint = resolve_min_checkpoint(
        [dfm.get_checkpoint("dim", "Ordre"), dfm.get_checkpoint("fct", "Ordre")]
    )
    if config.mode == "full_load":
        min_checkpoint = None

    run_ctx = dfm.start_dfm_run(
        "pre_stg", "pre_Ordre_ils", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    without_ordre = 0
    duplicate_ordre = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_Ordre_ils];")
                if min_checkpoint is not None:
                    context.log.info(f"Delta: orders touched after {min_checkpoint}")
                    cursor.execute(build_sql(delta=True), (min_checkpoint,))
                else:
                    context.log.info("No checkpoint filter — full reconstruction from archive")
                    cursor.execute(build_sql(delta=False))
                cursor.execute(INDEX_SQL)
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[pre_Ordre_ils]")
                row_count = cursor.fetchone()[0]

                cursor.execute(DROPPED_SQL)
                dropped = cursor.fetchone()
                without_ordre = dropped[0] or 0
                duplicate_ordre = dropped[1] or 0
            finally:
                cursor.close()

        context.log.info(
            f"stg.pre_Ordre_ils: {row_count} rows "
            f"(archive: {without_ordre} rows without an order number, "
            f"{duplicate_ordre} duplicate rows per order)"
        )
        if duplicate_ordre > 0:
            context.log.warning(
                f"{duplicate_ordre} archive rows share (sgh, lgr, ordre) with another row; "
                "the latest sysdato wins"
            )
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_Ordre_ils",
            "row_count": row_count,
            "archive_rows_without_ordre": without_ordre,
            "archive_duplicate_rows_per_ordre": duplicate_ordre,
            "source_tables": "arc.ils_ordh",
            "grain": "clean_code_Ordre (sgh, lgr, ordre)",
            "mode": config.mode,
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "layer": "pre_stg",
        }
    )
