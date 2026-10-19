# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension staging for udredningsforloeb attributes (transaction dimension).

stg.dim_UdredningsretEgenskaber shares grain and clean_code with
stg.fct_Udredningsforloeb — the fact and its transaction dimension describe
the same rows and share the PK downstream via map.Udredningsforloeb — and
reads stg.pre_Udredningsforloeb only (spec 3.6 in docs/source_systems/
udredningsret/udredningsret_model.md). It carries the classification
clean_code plus the attribute list udredningsret_kolonner.EGENSKABER_ATTRIBUTTER,
the ONE tuple the dim SQL and the DDL test are derived from: SDS'
surrogates (F_ID, PK, helbredsforloeb id, UUIDs) as "latest delivered"
degenerate properties (decisions B9/U1), the start/end marker codes, raw
SOR codes, diagnosis, reporting period, regions, speciality and label.

GDPR: no CPR and no borger key — the borger lives on the fact side.
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
from DSA.SQL.staging.udredningsret_kolonner import DIM_EGENSKABER_TABLE
from DSA.SQL.staging.udredningsret_staging_sql import build_stg_dim_egenskaber_sql


@asset(
    key=AssetKey(["udredningsret", "stg_dim_UdredningsretEgenskaber"]),
    group_name="udredningsret_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["udredningsret", "pre_stg_Udredningsforloeb"])],
    tags={
        "domain": "udredningsret",
        "layer": "stg",
        "source": "udredningsret",
        "entity": "udredningsforloeb",
    },
    description=(
        "Dimension staging for udredningsforloeb attributes (SDS surrogates, "
        "marker codes, raw SOR codes, diagnosis, reporting period, regions, "
        "speciality, label) plus the classification clean_code; shares grain "
        "and clean_code with stg_fct_Udredningsforloeb."
    ),
)
def stg_dim_UdredningsretEgenskaber(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "dim_UdredningsretEgenskaber", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_stg_dim_egenskaber_sql())
                conn.commit()
                cursor.execute(f"SELECT COUNT(*) FROM {DIM_EGENSKABER_TABLE}")
                row_count = cursor.fetchone()[0]
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    context.log.info(f"stg.dim_UdredningsretEgenskaber: {row_count:,} rows")
    return MaterializeResult(metadata={"row_count": row_count})
