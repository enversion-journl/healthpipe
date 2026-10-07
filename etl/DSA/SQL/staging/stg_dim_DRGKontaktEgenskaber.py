# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension staging for DRG contact attributes.

Shares grain and clean_code with stg_fct — the fact and its transaction
dimension must describe the same rows — and reads stg.pre_DRGKontakt only. The delivery's own demographics (bo_kom/-region,
betaler_kom/-region, bo_kom_kilde) are staged as authoritative for the
settlement even though dim.Borger holds similar fields — the delivery is
what the payment was computed from.

Kommune/regions-NAMES are deliberately absent: no classification
source exists in the repo — the codes are staged
as delivered, and a name lookup can be added when a classification lands.

No raw CPR: pre already resolved it to dw_ek_Borger (which lives on the
fact side; this dimension carries none of it).
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
from DSA.SQL.staging.drg_staging_sql import build_stg_dim_sql


@asset(
    key=AssetKey(["drg", "stg_dim_DRGKontaktEgenskaber"]),
    group_name="drg_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["drg", "pre_stg_DRGKontakt"])],
    tags={"domain": "drg", "layer": "stg", "source": "drg", "entity": "drgkontakt"},
    description=(
        "Dimension staging for DRG contact attributes incl. the delivery's "
        "own demographics; shares grain and clean_code with stg_fct."
    ),
)
def stg_dim_DRGKontaktEgenskaber(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "dim_DRGKontaktEgenskaber", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_stg_dim_sql())
                conn.commit()
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_DRGKontaktEgenskaber]")
                row_count = cursor.fetchone()[0]
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    context.log.info(f"stg.dim_DRGKontaktEgenskaber: {row_count:,} rows")
    return MaterializeResult(metadata={"row_count": row_count})
