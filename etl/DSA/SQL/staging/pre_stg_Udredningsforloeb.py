# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pre-staging asset for udredningsforloeb (assessment courses).

stg.pre_Udredningsforloeb holds ONE row per (helbredsforloeb_noegle, F_ID)
over the LIVE arc markers of the helbredsforloeb in scope
(stg.pre_Helbredsforloeb), spec 3.3 in docs/source_systems/udredningsret/
udredningsret_model.md. The SQL (udredningsret_staging_sql.py) picks the
start marker (earliest referral, else earliest marker — U4), the end marker
(SDS' MRK_H='1' end, else latest "endeligt udredt", else none — U2), the
attribute marker (end marker, else latest — U3), derives indgaar_i_indikator
and the counters that the forloeb fact carries (U6), and numbers parallel
forloeb per start date so the clean_code survives SDS' renumbering of F_ID.

The grain is enforced physically by the unique clustered index on
clean_code_Udredningsforloeb and checked again here with COUNT vs
COUNT DISTINCT: a violation fails the run with counts only.

GDPR: reads arc columns other than CPR only; the borger key lives on
stg.pre_Helbredsforloeb and is joined in by the fact staging. Nothing here
stages or logs a row value.
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
from DSA.SQL.staging.udredningsret_kolonner import (
    PRE_FORLOEB_TABLE,
    henvist_predicate_sql,
)
from DSA.SQL.staging.udredningsret_staging_sql import (
    build_grain_check_sql,
    build_pre_udredningsforloeb_sql,
)

_GRAIN_COLUMN = "clean_code_Udredningsforloeb"


def _build_diagnostics_sql() -> str:
    """Counts only: indicator forloeb, forloeb without end/referral marker, loebenummer > 1."""
    henvist = henvist_predicate_sql("[start_markoer_kode]")
    return f"""
SELECT
    COUNT(*),
    SUM(CASE WHEN [indgaar_i_indikator] = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN [slutdato] IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN {henvist} THEN 0 ELSE 1 END),
    SUM(CASE WHEN [loebenummer] > 1 THEN 1 ELSE 0 END)
FROM {PRE_FORLOEB_TABLE}
"""


@asset(
    key=AssetKey(["udredningsret", "pre_stg_Udredningsforloeb"]),
    group_name="udredningsret_pre_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["udredningsret", "pre_stg_Helbredsforloeb"])],
    tags={
        "domain": "udredningsret",
        "layer": "pre_stg",
        "source": "udredningsret",
        "entity": "udredningsforloeb",
    },
    description=(
        "Pre-staging for udredningsforloeb: live markers of the scoped "
        "helbredsforloeb grouped by F_ID with start/end/attribute marker "
        "selection, indgaar_i_indikator, marker counters, per-start-date "
        "loebenummer and clean_code_Udredningsforloeb; explicit grain check."
    ),
)
def pre_stg_Udredningsforloeb(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "pre_stg", "pre_Udredningsforloeb", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    indikator = 0
    uden_slut = 0
    uden_henvist = 0
    loebenummer_over_1 = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_pre_udredningsforloeb_sql())
                # ODBC can defer a later THROW until its result set is read.
                # Surface identity conflicts before committing any staging DDL.
                while cursor.nextset():
                    pass
                conn.commit()

                cursor.execute(build_grain_check_sql(PRE_FORLOEB_TABLE, _GRAIN_COLUMN))
                antal, distinkte = cursor.fetchone()
                if antal != distinkte:
                    raise ValueError(
                        f"Grain violation in {PRE_FORLOEB_TABLE}: {antal:,} rows but "
                        f"{distinkte:,} distinct {_GRAIN_COLUMN}"
                    )

                cursor.execute(_build_diagnostics_sql())
                row_count, indikator, uden_slut, uden_henvist, loebenummer_over_1 = (
                    int(v or 0) for v in cursor.fetchone()
                )
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    context.log.info(
        f"stg.pre_Udredningsforloeb: {row_count:,} rows; {indikator:,} in the indicator, "
        f"{uden_slut:,} without end marker, {uden_henvist:,} without referral marker, "
        f"{loebenummer_over_1:,} with loebenummer > 1"
    )
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "indikatorforloeb": indikator,
            "forloeb_uden_slutmarkoer": uden_slut,
            "forloeb_uden_henvist_markoer": uden_henvist,
            "loebenummer_over_1": loebenummer_over_1,
        }
    )
