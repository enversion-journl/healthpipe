# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension staging for the forloebsmarkoer (SDS course marker) code list.

stg.dim_Forloebsmarkoer holds ONE row per distinct live FORLOEBSMARKOER in
arc.udredningsret_mont (spec 3.4 in docs/source_systems/udredningsret/
udredningsret_model.md): the code, its group from SDS' appendix 1
(udredningsret_kolonner.MARKOER_GRUPPER), and the display text from
codesystem.SKSConcept (decision U7). The dimension is small (433 codes
measured) and therefore NOT delta-scoped — it is rebuilt from the whole
archive every run. An unknown code is still minted with tekst 'Ukendt' and
tekst_kilde 'ukendt'; never a hard fail.

GDPR: a pure code list — no borger, no CPR, no row-level data.
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
from DSA.SQL.staging.udredningsret_kolonner import DIM_FORLOEBSMARKOER_TABLE
from DSA.SQL.staging.udredningsret_staging_sql import build_stg_dim_forloebsmarkoer_sql


@asset(
    key=AssetKey(["udredningsret", "stg_dim_Forloebsmarkoer"]),
    group_name="udredningsret_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["arc_udredningsret_mont"]), AssetKey(["cs_sks"])],
    tags={
        "domain": "udredningsret",
        "layer": "stg",
        "source": "udredningsret",
        "entity": "forloebsmarkoer",
    },
    description=(
        "Dimension staging for the SDS forloebsmarkoer code list: one row per "
        "distinct live marker code in arc with its appendix-1 group and the "
        "SKS display text (unknown codes minted as 'Ukendt'). Not delta-scoped."
    ),
)
def stg_dim_Forloebsmarkoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "dim_Forloebsmarkoer", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    uden_sks_tekst = 0
    pr_gruppe: dict[str, int] = {}
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_stg_dim_forloebsmarkoer_sql())
                conn.commit()

                cursor.execute(
                    f"SELECT COUNT(*), SUM(CASE WHEN [tekst_kilde] = 'ukendt' THEN 1 ELSE 0 END) "
                    f"FROM {DIM_FORLOEBSMARKOER_TABLE}"
                )
                row_count, uden_sks_tekst = (int(v or 0) for v in cursor.fetchone())

                cursor.execute(
                    f"SELECT [gruppe], COUNT(*) FROM {DIM_FORLOEBSMARKOER_TABLE} "
                    f"GROUP BY [gruppe] ORDER BY [gruppe]"
                )
                for gruppe, antal in cursor.fetchall():
                    pr_gruppe[str(gruppe)] = antal
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    context.log.info(
        f"stg.dim_Forloebsmarkoer: {row_count:,} codes; {uden_sks_tekst:,} without SKS text"
    )
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "koder_uden_sks_tekst": uden_sks_tekst,
            "antal_pr_gruppe": pr_gruppe,
        }
    )
