# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Sensor tying the HAIBA publish job to the nightly Borger chain.

``haiba_ingest_job`` runs at 05:00 and, for every CPR the FHIR server has never
seen, posts a stub Patient to Aidbox. Those stubs only become a dw_ek_Borger
once ``ifdb_dfm_daily_job`` has run sof_patient_dfm -> ref_patient_borger
-> dim_Borger -> dso_sync_dim_borger. This sensor fires the publish job on that
job's success, so the dm rebuild resolves the citizen keys on its first attempt
and DSO never sees dw_ek_Borger = -1 for a fresh case.

Both gates read the warehouse, not Dagster's event log
------------------------------------------------------
``meta.HAIBALeverance`` is the archive's own delivery register — one row per
(kildetabel, leverance_dato), and ``guard_delivery_direction`` already treats
MAX(leverance_dato) there as the watermark. That is the authoritative answer to
"which delivery is the archive standing on", so the sensor asks it directly
rather than mirroring it out of asset materialization metadata, which would add
a second source of truth that can drift or age out of the event log.

The unresolved count comes from ``dm.HAIBA_Cases_*`` the same way — an index
seek on IX_dm_HAIBA_Cases_*_Borger, not a scan.

Gate 1: has this delivery been published?
-----------------------------------------
The monitored job runs every night and the HAIBA ingest runs every night, but
SDS delivers weekly. Firing on every success would rebuild an 8M-row dm layer
and re-publish ~6.7M rows to DSO daily to discover nothing changed. So: skip
unless the watermark differs from the one a SUCCESSFUL publish already carries.

Asking for a *successful* publish rather than a requested one is deliberate. A
publish that fails is retried on the next nightly success instead of being
swallowed by run-key deduplication, and the run_key carries the triggering run
id so each nightly success yields at most one attempt.

Gate 2: recovery, while the borger chain is still making progress
-----------------------------------------------------------------
A delivery can be published correctly and still leave cases at
dw_ek_Borger = -1: the stub creation was capped with ``max_create``, Aidbox
lagged, or a chain step failed after the publish had already run. Those cases
carry no FHIR contact either (match_kvalitet_* = 'ingen_borger'), and with only
gate 1 they would sit on DSO until the next delivery a week later.

So when the watermark is unchanged but cases are still unresolved, the sensor
re-publishes — bounded by progress rather than by a retry count. Each recovery
run is tagged with the count that triggered it; the next tick compares today's
count against that tag. Fewer unresolved than last time means the chain is
still minting keys, so try again. No improvement means it has stopped helping,
and the sensor goes quiet until a new delivery reopens gate 1.

That bound is not optional. Some cases never resolve — a CPR the FHIR server
knows only as a ``previous_ssn`` is "known" to the stub creation (so no stub is
made) but never reaches ref.PatientToBorger, which only ever carries the
current patient_ssn. Without plateau detection the sensor would rebuild the dm
layer every night forever chasing a count that cannot reach zero.
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from schedules.schedule_default_status import get_sensor_default_status
from sensors.publish_sensor_utils import (
    last_successful_publish,
    run_tags,
    table_exists,
)

SENSOR_NAME = "haiba_publish_sensor"

# Tags carried by a publish run so the next tick can read its own history back.
DELIVERY_SIGNATURE_TAG = "haiba/delivery_signature"
UNRESOLVED_TAG = "haiba/unresolved_at_request"

LEVERANCE_TABLE = "HAIBALeverance"

# The kildetabel values the archive registers, i.e. the ext/arc table names
# without a schema prefix. Named rather than derived from the extract schemas so
# importing this sensor does not drag the HAIBA asset graph into the sensor.
ARC_SOURCE_TABLES = (
    "haiba_agg_infektion",
    "haiba_agg_ortopaedi",
    "haiba_cases_infektion",
    "haiba_cases_ssi",
)

# The two dm facts that carry a citizen key. The agg tables hold no person data.
CASE_FACT_TABLES = ("HAIBA_CasesInfektion", "HAIBA_CasesSSI")

# Written by the dm rebuild when no citizen could be resolved.
UNKNOWN_BORGER = -1


def read_delivery_signature(cursor) -> str | None:
    """The delivery the archive stands on, across all four source tables.

    MAX(leverance_dato) per kildetabel, mirroring ``_recorded_delivery`` in
    DSI/csv_files/haiba_archive.py — a backfill inserts an older date but never
    moves the maximum, so the watermark only goes forward.

    Returns None when the register is missing or does not yet cover all four
    tables. Callers skip rather than guess: a partial signature that happened to
    compare equal to a published one would silently suppress a publish.
    """
    if not table_exists(cursor, "meta", LEVERANCE_TABLE):
        return None

    placeholders = ", ".join("?" for _ in ARC_SOURCE_TABLES)
    cursor.execute(
        f"SELECT [kildetabel], MAX([leverance_dato]) FROM [meta].[{LEVERANCE_TABLE}] "
        f"WHERE [kildetabel] IN ({placeholders}) GROUP BY [kildetabel]",
        tuple(ARC_SOURCE_TABLES),
    )
    registered = {kildetabel: dato for kildetabel, dato in cursor.fetchall() if dato}
    if len(registered) != len(ARC_SOURCE_TABLES):
        return None
    return ";".join(f"{name}={registered[name]}" for name in ARC_SOURCE_TABLES)


def read_unresolved_cases(cursor) -> int | None:
    """Cases across both fact tables that resolved no citizen.

    An index seek on IX_dm_HAIBA_Cases_*_Borger. Returns None when the dm layer
    has not been created yet, which is not the same as zero.
    """
    total = 0
    for table in CASE_FACT_TABLES:
        if not table_exists(cursor, "dm", table):
            return None
        cursor.execute(
            f"SELECT COUNT(*) FROM [dm].[{table}] WHERE [dw_ek_Borger] = {UNKNOWN_BORGER}"
        )
        total += cursor.fetchone()[0]
    return total


def read_state(mssql: MSSQLResource) -> tuple[str | None, int | None]:
    """(delivery signature, unresolved case count) in one connection."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            return read_delivery_signature(cursor), read_unresolved_cases(cursor)
        finally:
            cursor.close()


def _request(
    *, publish_job_name: str, signature: str, parent_run_id: str, unresolved: int | None
) -> dg.SensorResult:
    tags = {
        "dagster/triggered_by": SENSOR_NAME,
        "dagster/parent_run_id": parent_run_id,
        DELIVERY_SIGNATURE_TAG: signature,
    }
    # Only recovery runs carry the count. Its absence on the first publish of a
    # delivery is what grants the first recovery attempt an unconditional try:
    # the initial publish's leftover count is the baseline, not a plateau.
    if unresolved is not None:
        tags[UNRESOLVED_TAG] = str(unresolved)
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                job_name=publish_job_name,
                run_key=f"haiba_publish:{signature}:{parent_run_id}",
                tags=tags,
            )
        ]
    )


def evaluate(context, *, publish_job_name: str, mssql: MSSQLResource):
    """Core sensor logic, kept plain so it unit-tests without the wrapper."""
    parent_run_id = context.dagster_run.run_id

    try:
        signature, unresolved = read_state(mssql)
    except Exception as exc:  # a DB blip must not fail the tick
        return dg.SkipReason(
            f"{SENSOR_NAME}: could not read the HAIBA state from staging "
            f"({type(exc).__name__}: {exc}). Retrying on the next success."
        )

    if signature is None:
        return dg.SkipReason(
            f"{SENSOR_NAME}: meta.{LEVERANCE_TABLE} does not yet register a "
            f"delivery for all of {', '.join(ARC_SOURCE_TABLES)}. The archive "
            f"has not run for every source table."
        )

    last = last_successful_publish(
        context.instance, publish_job_name, signature, DELIVERY_SIGNATURE_TAG
    )

    if last is None:
        context.log.info(
            f"{SENSOR_NAME}: delivery {signature} has not been published — "
            f"requesting {publish_job_name}."
        )
        return _request(
            publish_job_name=publish_job_name,
            signature=signature,
            parent_run_id=parent_run_id,
            unresolved=None,
        )

    # Published. The only reason to run again is unresolved citizen keys that
    # the nightly chain may since have minted.
    if not unresolved:
        return dg.SkipReason(
            f"{SENSOR_NAME}: delivery {signature} is published and every case "
            f"resolved a citizen. Waiting for a new delivery."
        )

    previous = run_tags(last).get(UNRESOLVED_TAG)
    if previous is not None and unresolved >= int(previous):
        return dg.SkipReason(
            f"{SENSOR_NAME}: {unresolved} case(s) still without a citizen for "
            f"delivery {signature}, unchanged from the last recovery attempt "
            f"({previous}) — the Borger chain has stopped resolving them. Not "
            f"rebuilding again until a new delivery arrives."
        )

    context.log.info(
        f"{SENSOR_NAME}: {unresolved} case(s) without a citizen for delivery "
        f"{signature}"
        + (f", down from {previous}" if previous is not None else " after the initial publish")
        + f" — requesting {publish_job_name} to re-resolve them."
    )
    return _request(
        publish_job_name=publish_job_name,
        signature=signature,
        parent_run_id=parent_run_id,
        unresolved=unresolved,
    )


def build_haiba_publish_sensor(
    borger_chain_job: UnresolvedAssetJobDefinition,
    publish_job: UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Build the sensor with concrete job references.

    Must be called **after** branch-postfixing so the sensor monitors and
    requests the correctly-named jobs.
    """

    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[borger_chain_job],
        request_jobs=[publish_job],
        name=SENSOR_NAME,
        default_status=get_sensor_default_status(),
        description=(
            f"On {borger_chain_job.name} SUCCESS: publish the HAIBA dm layer to DSO "
            f"when meta.{LEVERANCE_TABLE} shows an unpublished delivery, or "
            f"re-publish while the Borger chain is still resolving cases left "
            f"at dw_ek_Borger = {UNKNOWN_BORGER}. Runs after the chain so stub "
            f"patients created by haiba_ingest_job resolve to a real citizen."
        ),
    )
    def haiba_publish_sensor(
        context: dg.RunStatusSensorContext,
        mssql_target_staging: MSSQLResource,
    ):
        return evaluate(context, publish_job_name=publish_job.name, mssql=mssql_target_staging)

    return haiba_publish_sensor


__all__ = [
    "ARC_SOURCE_TABLES",
    "CASE_FACT_TABLES",
    "DELIVERY_SIGNATURE_TAG",
    "SENSOR_NAME",
    "UNRESOLVED_TAG",
    "build_haiba_publish_sensor",
    "evaluate",
    "read_delivery_signature",
    "read_state",
    "read_unresolved_cases",
]
