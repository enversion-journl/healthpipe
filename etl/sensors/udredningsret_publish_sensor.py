# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Sensor tying the udredningsret publish job to the nightly Borger chain.

``udredningsret_ingest_job`` archives the delivery and, for every CPR the FHIR
server has never seen, posts a stub Patient to Aidbox
(``fhir_patient_udredningsret``). Those stubs only become a dw_ek_Borger once
``ifdb_dfm_daily_job`` has run sof_patient_dfm -> ref_patient_borger ->
dim_Borger -> dso_sync_dim_borger. This sensor fires the publish job on that
job's success, so the staging rebuild resolves the citizen keys on its first
attempt and DSO never sees dw_ek_Borger = -1 for a fresh forloeb.

Built after ``haiba_publish_sensor.py`` (spec section 8 in
docs/source_systems/udredningsret/udredningsret_model.md); the three places the
shape differs are marked below.

Both gates read the warehouse, not Dagster's event log
------------------------------------------------------
``meta.UdredningsretLeverance`` is the archive's own delivery register — one row
per (kildetabel, leverance_tidsstempel), upserted by the archive asset. That is
the authoritative answer to "which delivery is the archive standing on", so the
sensor asks it directly rather than mirroring it out of asset materialization
metadata, which would add a second source of truth that can drift or age out of
the event log.

The unresolved count comes from ``fct.Udredningsforloeb`` the same way — a seek
on IX_Fct_Udredningsforloeb_Borger, not a scan.

Gate 1: has this delivery been published?
-----------------------------------------
The monitored job runs every night and the udredningsret ingest runs every
night, but SDS delivers weekly. Firing on every success would re-stage 8.2M
archive rows and re-publish two facts to DSO daily to discover nothing changed.
So: skip only when the latest attempt for the delivery succeeded and no
recovery is needed. Failed/canceled attempts are retried before checking DSA
counts; pending attempts are allowed to finish first.

Asking for a *successful* publish rather than a requested one is deliberate. A
publish that fails is retried on the next nightly success instead of being
swallowed by run-key deduplication, and the run_key carries the triggering run
id so each nightly success yields at most one attempt. That retry matters more
here than for HAIBA: the control total (decision U13) and the regrouping
detector (U15) both fail the publish run outright, so a failed publish is a
state the sensor must be able to walk out of on its own.

Gate 2: recovery, while the borger chain is still making progress
-----------------------------------------------------------------
A delivery can be published correctly and still leave forloeb at
dw_ek_Borger = -1: the stub creation was capped, Aidbox lagged, or a chain step
failed after the publish had already run. With only gate 1 they would sit on DSO
until the next delivery a week later.

So when the watermark is unchanged but forloeb are still unresolved, the sensor
re-publishes — bounded by progress rather than by a retry count. Each recovery
run is tagged with the count that triggered it; the next tick compares today's
count against that tag. Fewer unresolved than last time means the chain is still
minting keys, so try again. No improvement means it has stopped helping, and the
sensor goes quiet until a new delivery reopens gate 1.

That bound is not optional here either. Some forloeb never resolve — a CPR the
FHIR server knows only as a ``previous_ssn`` is "known" to the stub creation (so
no stub is made) but never reaches ref.PatientToBorger, which only ever carries
the current patient_ssn. Without plateau detection the sensor would re-stage the
whole delta every night forever chasing a count that cannot reach zero.

The recovery run is cheap by construction: ``stg.pre_Helbredsforloeb`` scopes a
delta run to the helbredsforloeb touched since the checkpoint PLUS every
helbredsforloeb whose live fact rows still carry dw_ek_Borger = -1 (decision U9,
spec section 3.1). The sensor and that scope branch therefore count the same
rows, which is why the count below filters dw_is_deleted = 0 exactly as the
branch does.

Three ways this differs from the HAIBA sensor
---------------------------------------------
1. One archived source table, not four. ``ARCHIVED_DATASETS`` in
   DSI/csv_files/udredningsret_datasets.py has a single member, so the signature
   is one MAX(leverance_tidsstempel); the "not every table registered yet" skip
   degenerates to "the register is empty", but is kept in the same shape.
2. The count is filtered to LIVE fact rows. A udredningsforloeb that vanishes
   from a delivery is retired, not deleted (spec section 6), and a retired row
   at -1 is never repaired — U9's recovery branch does not pick it up. Counting
   retired rows would plateau the gate above zero forever AND ask for a run that
   cannot change the number the sensor just read.
3. A blank CPR resolves to -2, not -1 (NA_BORGER vs UNKNOWN_BORGER in
   DSA/SQL/staging/udredningsret_kolonner.py), so the legitimately
   citizen-less rows never enter this count in the first place.
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from schedules.schedule_default_status import get_sensor_default_status
from sensors.publish_sensor_utils import (
    run_tags,
    table_exists,
)

SENSOR_NAME = "udredningsret_publish_sensor"

# Tags carried by a publish run so the next tick can read its own history back.
DELIVERY_SIGNATURE_TAG = "udredningsret/delivery_signature"
UNRESOLVED_TAG = "udredningsret/unresolved_at_request"

LEVERANCE_TABLE = "UdredningsretLeverance"

# The kildetabel values the archive registers, i.e. the ext/arc table names
# without a schema prefix — ARCHIVED_DATASETS in
# DSI/csv_files/udredningsret_datasets.py, which has one member today
# (LEVERANCEINFO is ext-only and is never archived). Named rather than derived
# from the extract schemas so importing this sensor does not drag the
# udredningsret asset graph into the sensor.
ARC_SOURCE_TABLES = ("udredningsret_mont",)

# The fact the recovery scope is defined on. fct.UdredningsretMarkoer carries
# dw_ek_Borger too (resolved per helbredsforloeb, so its markers share the
# forloeb's key), but it is deliberately NOT counted: U9's recovery branch
# selects helbredsforloeb from live rows of THIS fact, so a helbredsforloeb
# whose markers all lack an F_ID has no forloeb row and is never re-staged.
# Counting its markers would give the gate a number no publish run can move.
FORLOEB_FACT_TABLE = "Udredningsforloeb"

# CPR present but not (yet) in dim.Borger. A blank CPR is -2 and is not counted.
UNKNOWN_BORGER = -1


def read_delivery_signature(cursor) -> str | None:
    """The delivery the archive stands on, across every archived source table.

    MAX(leverance_tidsstempel) per kildetabel. The timestamp is the delivery's
    packaging time from the filename — the DFM delivery identity, to the second
    — and it is the register's PK together with kildetabel, so a rerun of the
    same delivery updates its row rather than adding one. A backfill writes an
    older delivery but never moves the maximum, so the watermark only goes
    forward.

    Returns None when the register is missing or does not yet cover every
    archived table. Callers skip rather than guess: a partial signature that
    happened to compare equal to a published one would silently suppress a
    publish.
    """
    if not table_exists(cursor, "meta", LEVERANCE_TABLE):
        return None

    placeholders = ", ".join("?" for _ in ARC_SOURCE_TABLES)
    cursor.execute(
        f"SELECT [kildetabel], MAX([leverance_tidsstempel]) "
        f"FROM [meta].[{LEVERANCE_TABLE}] "
        f"WHERE [kildetabel] IN ({placeholders}) GROUP BY [kildetabel]",
        tuple(ARC_SOURCE_TABLES),
    )
    registered = {kildetabel: stempel for kildetabel, stempel in cursor.fetchall() if stempel}
    if len(registered) != len(ARC_SOURCE_TABLES):
        return None
    return ";".join(f"{name}={registered[name]}" for name in ARC_SOURCE_TABLES)


def read_unresolved_forloeb(cursor) -> int | None:
    """Live fact rows that resolved no citizen.

    A seek on IX_Fct_Udredningsforloeb_Borger (dw_is_deleted is a lookup, and
    the -1 population is small once the chain has run). Live rows only: the
    same predicate U9's recovery branch uses, so the sensor never asks for a
    run that cannot move the number it just read.

    Returns None when the fact has not been created yet, which is not the same
    as zero.
    """
    if not table_exists(cursor, "fct", FORLOEB_FACT_TABLE):
        return None
    cursor.execute(
        f"SELECT COUNT(*) FROM [fct].[{FORLOEB_FACT_TABLE}] "
        f"WHERE [dw_ek_Borger] = {UNKNOWN_BORGER} AND [dw_is_deleted] = 0"
    )
    return cursor.fetchone()[0]


def read_state(mssql: MSSQLResource) -> tuple[str | None, int | None]:
    """(delivery signature, unresolved forloeb count) in one connection."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            return read_delivery_signature(cursor), read_unresolved_forloeb(cursor)
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
                run_key=f"udredningsret_publish:{signature}:{parent_run_id}",
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
            f"{SENSOR_NAME}: could not read the udredningsret state from staging "
            f"({type(exc).__name__}: {exc}). Retrying on the next success."
        )

    if signature is None:
        return dg.SkipReason(
            f"{SENSOR_NAME}: meta.{LEVERANCE_TABLE} does not yet register a "
            f"delivery for all of {', '.join(ARC_SOURCE_TABLES)}. The archive "
            f"has not run for every source table."
        )

    # Inspect the latest attempt, not only successful runs: DSA can commit its
    # citizen repairs before a DSO sync fails, leaving zero unresolved in DSA
    # while the previously published DSO snapshot is still stale.
    records = context.instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=publish_job_name,
            tags={DELIVERY_SIGNATURE_TAG: signature},
        ),
        limit=1,
    )
    last = records[0] if records else None
    if last is not None and last.dagster_run.status != dg.DagsterRunStatus.SUCCESS:
        if last.dagster_run.status not in (
            dg.DagsterRunStatus.FAILURE,
            dg.DagsterRunStatus.CANCELED,
        ):
            return dg.SkipReason(f"{SENSOR_NAME}: a publish for this delivery is still pending.")
        # Preserve the failed attempt's baseline until the entire publish has
        # succeeded. A partial DSA commit must not advance the recovery gate.
        previous = run_tags(last).get(UNRESOLVED_TAG)
        return _request(
            publish_job_name=publish_job_name,
            signature=signature,
            parent_run_id=parent_run_id,
            unresolved=int(previous) if previous is not None else None,
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
    if unresolved is None:
        return dg.SkipReason(
            f"{SENSOR_NAME}: fct.{FORLOEB_FACT_TABLE} is missing; citizen coverage is unknown. "
            "Restore the warehouse infrastructure and run the publish job with mode=full_load."
        )
    if unresolved == 0:
        return dg.SkipReason(
            f"{SENSOR_NAME}: delivery {signature} is published and every live "
            f"forloeb resolved a citizen. Waiting for a new delivery."
        )

    previous = run_tags(last).get(UNRESOLVED_TAG)
    if previous is not None and unresolved >= int(previous):
        return dg.SkipReason(
            f"{SENSOR_NAME}: {unresolved} forloeb still without a citizen for "
            f"delivery {signature}, unchanged from the last recovery attempt "
            f"({previous}) — the Borger chain has stopped resolving them. Not "
            f"re-publishing again until a new delivery arrives."
        )

    context.log.info(
        f"{SENSOR_NAME}: {unresolved} forloeb without a citizen for delivery "
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


def build_udredningsret_publish_sensor(
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
            f"On {borger_chain_job.name} SUCCESS: publish the udredningsret DSA layer "
            f"and sync it to DSO when meta.{LEVERANCE_TABLE} shows an unpublished "
            f"delivery, or re-publish while the Borger chain is still resolving "
            f"live forloeb left at dw_ek_Borger = {UNKNOWN_BORGER}. Runs after the "
            f"chain so stub patients created by udredningsret_ingest_job resolve "
            f"to a real citizen."
        ),
    )
    def udredningsret_publish_sensor(
        context: dg.RunStatusSensorContext,
        mssql_target_staging: MSSQLResource,
    ):
        return evaluate(context, publish_job_name=publish_job.name, mssql=mssql_target_staging)

    return udredningsret_publish_sensor


__all__ = [
    "ARC_SOURCE_TABLES",
    "DELIVERY_SIGNATURE_TAG",
    "FORLOEB_FACT_TABLE",
    "SENSOR_NAME",
    "UNRESOLVED_TAG",
    "build_udredningsret_publish_sensor",
    "evaluate",
    "read_delivery_signature",
    "read_state",
    "read_unresolved_forloeb",
]
