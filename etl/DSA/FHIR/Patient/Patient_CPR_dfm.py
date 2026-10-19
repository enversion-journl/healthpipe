# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Register producer: persons changed by a CPR delivery are rebuilt via the Patient write point.

``scope`` is the person scope: ``known`` builds only persons with an EPJ row or a resource,
``register`` also those only the register knows. Delta runs on the archive tables'
``dw_timestamp_update`` from the checkpoint fhir_server/Patient_CPR.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Patient.patient_cpr_contribution import PERSON_TABLE, REGISTER_TABLES
from DSA.FHIR.Patient.patient_identity import HISTORY_TABLE
from DSA.FHIR.Patient.patient_service import SCOPE_KNOWN, SCOPES, write_register_persons
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from pydantic import Field

BATCH_SIZE = 10_000

# 022 carries the person in pnrgaeld; the historical number in pnr is not rebuilt.
CHANGE_TABLES: tuple[tuple[str, str], ...] = tuple(
    (table, "pnr") for table, _ in REGISTER_TABLES.values()
) + ((HISTORY_TABLE, "pnrgaeld"),)


class PatientRegisterConfig(RunModeConfig):
    """Run configuration: DFM mode plus person scope."""

    scope: str = Field(
        default=SCOPE_KNOWN,
        description=(
            "Person scope. 'known': only persons with an EPJ row or an "
            "existing resource — the default. 'register': also persons only the "
            "register knows; with mode=full_load the whole register is built."
        ),
    )


def _changed_persons_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Numbers the archive tables touched since the checkpoint, deleted too; full_load: all live."""
    if config.mode == "full_load":
        schema, table = PERSON_TABLE.split(".")
        return f"SELECT [pnr] FROM [{schema}].[{table}] WHERE [dw_is_deleted] = 0"
    parts = []
    for index, (table, column) in enumerate(CHANGE_TABLES):
        query = build_fhir_archive_query(
            table,
            checkpoint,
            config,
            include_deleted=True,
            select_clause=f"SELECT DISTINCT [{column}] AS [pnr]",
        )
        parts.append(f"SELECT [pnr] FROM ({query}) AS [t{index}]")
    return " UNION ".join(parts)


@dg.asset(
    name="fhir_server_patient_cpr_dfm",
    group_name="cpr",
    compute_kind="fhir",
    # The delivery's number changes must be in the union before the resources are built.
    deps=["arc_cpr", "ref_patient_identity"],
    tags={"domain": "cpr", "layer": "dsa", "source": "cpr", "dagster/concurrency_key": "cpr"},
    description=(
        "Rebuild the persons the latest CPR delivery has changed: EPJ's row and the "
        "register's contribution are gathered by the Patient write point and written once. "
        "scope=known (default) touches only known persons; scope=register also creates "
        "the register's others. DFM checkpoint fhir_server/Patient_CPR."
    ),
)
def fhir_server_patient_cpr_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: PatientRegisterConfig,
) -> dg.MaterializeResult:
    if config.scope not in SCOPES:
        raise ValueError(f"scope must be one of {SCOPES}")
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server", "Patient_CPR", dagster_run_id=context.run_id, mode=config.mode
    )
    try:
        query = _changed_persons_query(run_ctx.checkpoint, config)
        context.log.info(f"Finding changed persons in the register (mode={config.mode})")
        totals = {
            "fetched": 0,
            "written": 0,
            "rebuilt": 0,
            "created": 0,
            "skipped_unknown": 0,
            "untouched": 0,
            "missing_in_register": 0,
            "conflicts": 0,
            "withheld": 0,
            "uninterpreted": 0,
            "batches": 0,
        }
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(BATCH_SIZE)
                    if not rows:
                        break
                    numbers = [row[0] for row in rows if row[0]]
                    totals["batches"] += 1
                    totals["fetched"] += len(numbers)
                    result = write_register_persons(
                        context,
                        mssql_target_staging,
                        fhir_database,
                        numbers,
                        scope=config.scope,
                        source_name="cpr_register_dfm",
                    )
                    for key in (
                        "written",
                        "rebuilt",
                        "created",
                        "skipped_unknown",
                        "untouched",
                        "missing_in_register",
                        "conflicts",
                        "withheld",
                        "uninterpreted",
                    ):
                        totals[key] += getattr(result, key)
                    context.log.info(
                        "Batch %d: %d numbers, %d written (%d in total)",
                        totals["batches"],
                        len(numbers),
                        result.written,
                        totals["written"],
                    )
            finally:
                cursor.close()

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "Patient_CPR", config, totals["fetched"]
        )
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=new_checkpoint, rows_affected=totals["written"]
        )
        return dg.MaterializeResult(metadata={**totals, "mode": config.mode, "scope": config.scope})
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=type(exc).__name__)
        raise


__all__ = ["PatientRegisterConfig", "fhir_server_patient_cpr_dfm"]
