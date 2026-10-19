# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Fact staging for DRG contacts.

Typed measures, dato keys, SOR keys per role (raw code kept beside the key)
and forloeb measures on the CARRYING contact: somatik = the contact flagged
TUNGESTE_DRGKONTAKT='1'; psyk = the forloeb's first contact (the psyk
schema has no tungeste flag — see drg_noegler_beslutningsnotat.md
§Forloebsidentitet).

The control total FAILS the run on breach: for every
forloeb represented by a carrying contact in stg, the carried measure must
equal the arc forloeb value, and there must be at most one carrying contact
per (takstsystem, forloeb). Scoped to stg's content because staging is
delta-scoped — an archive-wide sum only matches on a full load.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Failure,
    MaterializeResult,
    RetryPolicy,
    asset,
)
from DSA.SQL.staging.drg_staging_sql import (
    build_forloeb_kontroltotal_sql,
    build_sor_coverage_sql,
    build_stg_fct_sql,
    create_organisation_sor_lookup_table_sql,
)


@asset(
    key=AssetKey(["drg", "stg_fct_DRGKontakt"]),
    group_name="drg_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["drg", "pre_stg_DRGKontakt"]),
        AssetKey(["arc_drg_sghforlob"]),
        AssetKey(["arc_drgpsyk_forloeb"]),
        AssetKey(["dim_Organisation"]),
    ],
    tags={"domain": "drg", "layer": "stg", "source": "drg", "entity": "drgkontakt"},
    description=(
        "Fact staging for DRG contacts: typed measures, dato keys, SOR keys "
        "per role via #organisation_sor, forloeb measures on the carrying contact with a "
        "hard control total against the arc forloeb tables."
    ),
)
def stg_fct_DRGKontakt(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "fct_DRGKontakt", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    sor_rates: dict[str, dict] = {}
    kontrol: dict[str, dict] = {}
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(create_organisation_sor_lookup_table_sql())
                cursor.execute(build_stg_fct_sql())
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_DRGKontakt]")
                row_count = cursor.fetchone()[0]

                cursor.execute(build_sor_coverage_sql())
                for rolle, leverancetype, tom, uden_match, matchet, ialt in cursor.fetchall():
                    sor_rates[f"{rolle}_{leverancetype}"] = {
                        "tom_kode_forventet": tom,
                        "udfyldt_uden_match": uden_match,
                        "matchet": matchet,
                        "ialt": ialt,
                    }

                cursor.execute(build_forloeb_kontroltotal_sql())
                for leverancetype, flere_baerende, afvigende in cursor.fetchall():
                    kontrol[leverancetype] = {
                        "forloeb_med_flere_baerende": flere_baerende,
                        "afvigende_maal": afvigende,
                    }
            finally:
                cursor.close()

        # Evaluate the control total BEFORE marking the run complete: a breach
        # must fail the DFM run, never be recorded in [meta].[DFMRunLog] as
        # status = 'success' while the asset itself fails.
        for leverancetype, tal in kontrol.items():
            context.log.info(f"kontroltotal {leverancetype}: {tal}")
        brud = {lt: t for lt, t in kontrol.items() if any(v for v in t.values())}
        if brud:
            dfm.fail_dfm_run(
                run_ctx.log_id,
                error_message=f"forloebsmaal-kontroltotal brud: {brud}",
            )
            raise Failure(
                description=(
                    f"Forloebsmaal-kontroltotalen holder ikke: {brud}. Enten baerer "
                    f"flere kontakter samme forloebs maal, eller det baarne maal "
                    f"afviger fra arc-forloebstabellen."
                ),
                metadata={"kontroltotal": str(brud)},
            )
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Failure:
        raise
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    uventede = sum(r["udfyldt_uden_match"] for r in sor_rates.values())
    context.log.info(
        f"stg.fct_DRGKontakt: {row_count:,} rows; SOR udfyldt-uden-match: {uventede:,}"
    )
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "sor_opslag_pr_rolle": sor_rates,
            "sor_udfyldt_uden_match_ialt": uventede,
            "forloeb_kontroltotal": kontrol,
        }
    )
