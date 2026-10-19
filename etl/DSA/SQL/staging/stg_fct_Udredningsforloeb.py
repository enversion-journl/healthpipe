# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Fact staging for udredningsforloeb (assessment courses).

stg.fct_Udredningsforloeb holds ONE row per stg.pre_Udredningsforloeb row
(spec 3.6 in docs/source_systems/udredningsret/udredningsret_model.md):
the forloeb and helbredsforloeb clean_codes, the classification clean_code
(hash over the eight indicator attributes), the borger key from
stg.pre_Helbredsforloeb, both SOR roles resolved against dim.Organisation
via #organisation_sor (the raw code is staged next to the resolved dw_ek; blank and
unmatched codes resolve to -1), dato keys through the shared dato_key() — dw_ek_Dato_slut and dw_ek_Dato_opgoerelse
land on the BLANK sentinel (99999998) when the forloeb has no end marker
(decision U2) or no parseable reporting quarter — and the typed indicator
measures as delivered (B5). udredningstid_dage is only carried when
indgaar_i_indikator = 1.

The grain is enforced physically by the unique clustered index and checked
again here with COUNT vs COUNT DISTINCT (hard fail, counts only).

GDPR: CPR never enters this table — only the resolved dw_ek_Borger and the
lookup outcome from pre_stg; only counts are logged and reported.
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
from DSA.SQL.common.dato_dimension import DATO_BLANK_KEY
from DSA.SQL.staging.udredningsret_kolonner import (
    FCT_FORLOEB_TABLE,
    SOR_ROLLER,
)
from DSA.SQL.staging.udredningsret_staging_sql import (
    build_grain_check_sql,
    build_sor_coverage_sql,
    build_stg_fct_udredningsforloeb_sql,
    create_organisation_sor_lookup_table_sql,
)

_GRAIN_COLUMN = "clean_code_Udredningsforloeb"


def _build_diagnostics_sql() -> str:
    """Counts only: rows, indicator forloeb, BLANK sentinels on the end and reporting dates."""
    return f"""
SELECT
    COUNT(*),
    SUM(CASE WHEN [indgaar_i_indikator] = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN [dw_ek_Dato_slut] = {DATO_BLANK_KEY} THEN 1 ELSE 0 END),
    SUM(CASE WHEN [dw_ek_Dato_opgoerelse] = {DATO_BLANK_KEY} THEN 1 ELSE 0 END)
FROM {FCT_FORLOEB_TABLE}
"""


@asset(
    key=AssetKey(["udredningsret", "stg_fct_Udredningsforloeb"]),
    group_name="udredningsret_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["udredningsret", "pre_stg_Udredningsforloeb"]),
        AssetKey(["udredningsret", "pre_stg_Helbredsforloeb"]),
        AssetKey(["dim_Organisation"]),
    ],
    tags={
        "domain": "udredningsret",
        "layer": "stg",
        "source": "udredningsret",
        "entity": "udredningsforloeb",
    },
    description=(
        "Fact staging for udredningsforloeb: classification clean_code, borger "
        "key from pre_stg, SOR keys per role via #organisation_sor, dato keys (BLANK for "
        "missing end marker / reporting quarter) and typed indicator measures "
        "as delivered; explicit grain check."
    ),
)
def stg_fct_Udredningsforloeb(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "fct_Udredningsforloeb", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    indikator = 0
    slut_blank = 0
    opgoerelse_blank = 0
    sor_rates: dict[str, dict] = {}
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(create_organisation_sor_lookup_table_sql())
                cursor.execute(build_stg_fct_udredningsforloeb_sql())
                conn.commit()

                cursor.execute(build_grain_check_sql(FCT_FORLOEB_TABLE, _GRAIN_COLUMN))
                antal, distinkte = cursor.fetchone()
                if antal != distinkte:
                    raise ValueError(
                        f"Grain violation in {FCT_FORLOEB_TABLE}: {antal:,} rows but "
                        f"{distinkte:,} distinct {_GRAIN_COLUMN}"
                    )

                cursor.execute(_build_diagnostics_sql())
                row_count, indikator, slut_blank, opgoerelse_blank = (
                    int(v or 0) for v in cursor.fetchone()
                )

                cursor.execute(build_sor_coverage_sql(FCT_FORLOEB_TABLE, SOR_ROLLER))
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
        f"stg.fct_Udredningsforloeb: {row_count:,} rows; {indikator:,} in the indicator, "
        f"{slut_blank:,} without end date (BLANK), {opgoerelse_blank:,} without reporting "
        f"quarter (BLANK); SOR filled-without-match: {uventede:,}"
    )
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "indikatorforloeb": indikator,
            "dato_slut_blank": slut_blank,
            "dato_opgoerelse_blank": opgoerelse_blank,
            "sor_opslag_pr_rolle": sor_rates,
            "sor_udfyldt_uden_match_ialt": uventede,
        }
    )
