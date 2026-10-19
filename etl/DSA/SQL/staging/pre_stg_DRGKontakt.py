# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pre-staging asset for DRG contacts.

ONE asset for both delivery types (somatik + psyk): they land in the same
stg table and the same fact, and two assets writing one table would race.
The SQL lives in drg_staging_sql.py so it is unit-testable without a
database.

GDPR: CPR is resolved to dw_ek_Borger inside the SELECT (via #borger_current_cpr /
#borger_previous_cpr against dim.Borger, tidligere_cpr as fallback) and never
enters the staging table. Unknown CPRs get -1 (created in FHIR by the
two-pass asset fhir_patient_drg); blank gets -2.

Delta scope propagates across tables: a contact is in
the delta when the contact ITSELF, any child row (diagnoser/procedurer/
tillaeg/interne koder/betalingsoplysninger) OR its forloeb changed since
the checkpoint — the checkpoint being the MIN of the downstream dim/fct
checkpoints, mirroring pre_stg_Vareudlevering_ils.
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
from DSA.SQL.staging.drg_staging_sql import (
    build_borger_coverage_sql,
    build_delta_scope_sql,
    build_pre_stg_sql,
    build_unresolved_borger_scope_sql,
    create_borger_cpr_lookup_tables_sql,
)

_ARC_DEPS = [
    "arc_drg_kontakt",
    "arc_drg_sghforlob",
    "arc_drg_diagnose",
    "arc_drg_diagnose_tillaeg",
    "arc_drg_procedure",
    "arc_drg_procedure_tillaeg",
    "arc_drg_interne_koder",
    "arc_drgpsyk_kontakt",
    "arc_drgpsyk_forloeb",
    "arc_drgpsyk_diagnose",
    "arc_drgpsyk_diagnose_tillaeg",
    "arc_drgpsyk_procedure",
    "arc_drgpsyk_procedure_tillaeg",
    "arc_drgpsyk_betalingsoplysninger",
]


@asset(
    key=AssetKey(["drg", "pre_stg_DRGKontakt"]),
    group_name="drg_pre_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey([d]) for d in _ARC_DEPS] + [AssetKey(["dim_Borger"])],
    tags={"domain": "drg", "layer": "pre_stg", "source": "drg", "entity": "drgkontakt"},
    description=(
        "Pre-staging for DRG contacts (somatik + psyk, union) from arc with "
        "clean_code_DRGKontakt, CPR resolved to dw_ek_Borger (tidligere_cpr "
        "fallback, no raw CPR staged), cross-table delta scope and defensive "
        "ROW_NUMBER dedup."
    ),
)
def pre_stg_DRGKontakt(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)

    # Delta-vinduet er MIN over ALLE downstream checkpoint-ejere, saa en
    # ejer der er bagud (fejlet koersel) faar sine raekker gen-staget indtil
    # den indhenter — ellers ville dens medlemmer aldrig blive praesenteret
    # igen, og facten ville pege paa en dimension uden raekke.
    downstream = (
        dfm.get_checkpoint("dim", "DRGKontaktEgenskaber"),
        dfm.get_checkpoint("dim", "DRGKontaktKlassifikation"),
        dfm.get_checkpoint("fct", "DRGKontakt"),
    )
    checkpoints = [cp for cp in downstream if cp is not None]
    checkpoint = min(checkpoints) if checkpoints else None
    if config.mode == "full_load":
        checkpoint = None

    run_ctx = dfm.start_dfm_run(
        "pre_stg", "pre_DRGKontakt", dagster_run_id=context.run_id, mode=config.mode
    )
    if checkpoint is not None:
        context.log.info(f"Delta scope: rows touched after {checkpoint}")
    else:
        context.log.info("No checkpoint — full load from arc")

    row_count = 0
    coverage: dict[str, int] = {}
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(create_borger_cpr_lookup_tables_sql())
                delta_sql = build_delta_scope_sql(checkpoint)
                if delta_sql:
                    cursor.execute(delta_sql)
                    # Own execute() so a failure is not masked by the batch
                    # above's row counts (see build_unresolved_borger_scope_sql).
                    cursor.execute(build_unresolved_borger_scope_sql())
                cursor.execute(build_pre_stg_sql(checkpoint))
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[pre_DRGKontakt]")
                row_count = cursor.fetchone()[0]

                cursor.execute(build_borger_coverage_sql())
                for leverancetype, opslag, antal in cursor.fetchall():
                    coverage[f"{leverancetype}_{opslag}"] = antal
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    unresolved = sum(v for k, v in coverage.items() if k.endswith("_ukendt"))
    fallback = sum(v for k, v in coverage.items() if k.endswith("_tidligere_cpr"))
    context.log.info(
        f"stg.pre_DRGKontakt: {row_count:,} rows; borger-opslag: "
        f"{fallback:,} via tidligere_cpr, {unresolved:,} unresolved (-1)"
    )
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "checkpoint": str(checkpoint) if checkpoint else "full load",
            "borger_opslag": coverage,
            "borger_uoploeste": unresolved,
            "borger_via_tidligere_cpr": fallback,
        }
    )
