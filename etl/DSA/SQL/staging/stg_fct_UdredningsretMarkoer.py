# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Fact staging for udredningsret markers (one row per archive row).

stg.fct_UdredningsretMarkoer holds ONE row per arc.udredningsret_mont row —
live AND soft-deleted — whose helbredsforloeb is in scope
(stg.pre_Helbredsforloeb), spec 3.5 in docs/source_systems/udredningsret/
udredningsret_model.md. clean_code_UdredningsretMarkoer is exactly arc's
merge key, so a soft-delete in arc retires the same fact row downstream.
The forloeb link (clean_code_Udredningsforloeb) comes from
stg.pre_Udredningsforloeb on (helbredsforloeb_noegle, F_ID) and is NULL for
markers without F_ID or with a stale one — the fact loader maps those to -1
(decision U5; ~14 % by design, not a lookup failure). The forloeb unit is
resolved against dim.Organisation via #organisation_sor; blank and unmatched key to -1
with the raw code kept beside the key.

The grain is enforced physically by the unique clustered index and checked
again here with COUNT vs COUNT DISTINCT (hard fail, counts only).

GDPR: the borger key and lookup outcome are joined in from
stg.pre_Helbredsforloeb; CPR is never read, staged or logged.
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
    FCT_MARKOER_TABLE,
    MARKOER_SOR_ROLLER,
)
from DSA.SQL.staging.udredningsret_staging_sql import (
    build_grain_check_sql,
    build_sor_coverage_sql,
    build_stg_fct_markoer_sql,
    create_organisation_sor_lookup_table_sql,
)

_GRAIN_COLUMN = "clean_code_UdredningsretMarkoer"


@asset(
    key=AssetKey(["udredningsret", "stg_fct_UdredningsretMarkoer"]),
    group_name="udredningsret_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["arc_udredningsret_mont"]),
        AssetKey(["udredningsret", "pre_stg_Udredningsforloeb"]),
        AssetKey(["udredningsret", "pre_stg_Helbredsforloeb"]),
        AssetKey(["dim_Organisation"]),
    ],
    tags={
        "domain": "udredningsret",
        "layer": "stg",
        "source": "udredningsret",
        "entity": "udredningsretmarkoer",
    },
    description=(
        "Fact staging for udredningsret markers: one row per arc row (live and "
        "soft-deleted) in scope with the forloeb/helbredsforloeb/marker-code "
        "clean_codes, borger key from pre_stg, forloeb SOR key via #organisation_sor, "
        "dato key, indicator flags and antal_kilderaekker; explicit grain check."
    ),
)
def stg_fct_UdredningsretMarkoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "fct_UdredningsretMarkoer", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    uden_forloeb = 0
    soft_slettede = 0
    kilderaekker = 0
    kilderaekker_levende = 0
    sor_rates: dict[str, dict] = {}
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(create_organisation_sor_lookup_table_sql())
                cursor.execute(build_stg_fct_markoer_sql())
                conn.commit()

                cursor.execute(build_grain_check_sql(FCT_MARKOER_TABLE, _GRAIN_COLUMN))
                antal, distinkte = cursor.fetchone()
                if antal != distinkte:
                    raise ValueError(
                        f"Grain violation in {FCT_MARKOER_TABLE}: {antal:,} rows but "
                        f"{distinkte:,} distinct {_GRAIN_COLUMN}"
                    )

                cursor.execute(
                    f"""
SELECT
    COUNT(*),
    SUM(CASE WHEN [clean_code_Udredningsforloeb] IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN [dw_is_deleted] = 1 THEN 1 ELSE 0 END),
    SUM([antal_kilderaekker]),
    SUM(CASE WHEN [dw_is_deleted] = 0 THEN [antal_kilderaekker] ELSE 0 END)
FROM {FCT_MARKOER_TABLE}
"""
                )
                row_count, uden_forloeb, soft_slettede, kilderaekker, kilderaekker_levende = (
                    int(v or 0) for v in cursor.fetchone()
                )

                cursor.execute(build_sor_coverage_sql(FCT_MARKOER_TABLE, MARKOER_SOR_ROLLER))
                for rolle, tom, uden_match, matchet, ialt in cursor.fetchall():
                    sor_rates[str(rolle)] = {
                        "tom_kode_forventet": int(tom or 0),
                        "udfyldt_uden_match": int(uden_match or 0),
                        "matchet": int(matchet or 0),
                        "ialt": int(ialt or 0),
                    }
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    uventede = sum(r["udfyldt_uden_match"] for r in sor_rates.values())
    context.log.info(
        f"stg.fct_UdredningsretMarkoer: {row_count:,} rows ({soft_slettede:,} soft-deleted, "
        f"{uden_forloeb:,} without forloeb, {kilderaekker:,} source rows); "
        f"SOR filled-without-match: {uventede:,}"
    )
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "markoerer_uden_forloeb": uden_forloeb,
            "soft_slettede": soft_slettede,
            # all staged rows (live and soft-deleted) vs live only — the latter
            # is the figure spec 4.3's fact control total reconciles against
            "sum_antal_kilderaekker": kilderaekker,
            "sum_antal_kilderaekker_levende": kilderaekker_levende,
            "sor_opslag_pr_rolle": sor_rates,
            "sor_udfyldt_uden_match_ialt": uventede,
        }
    )
