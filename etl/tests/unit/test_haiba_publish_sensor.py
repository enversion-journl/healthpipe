# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the HAIBA publish sensor.

Three failures this guards against, all quiet:

* firing on every nightly success — an 8M-row dm rebuild and a ~6.7M-row DSO
  publish every night to discover SDS delivered nothing;
* NOT firing after a failed publish, which would strand a delivery until the
  next one arrives a week later;
* looping forever on cases that can never resolve — a CPR the FHIR server knows
  only as a previous_ssn never reaches ref.PatientToBorger at all.
"""

import dagster as dg
import pytest
from sensors.haiba_publish_sensor import (
    ARC_SOURCE_TABLES,
    CASE_FACT_TABLES,
    DELIVERY_SIGNATURE_TAG,
    UNRESOLVED_TAG,
    evaluate,
    read_delivery_signature,
    read_unresolved_cases,
)

pytestmark = pytest.mark.unit

PUBLISH_JOB = "haiba_publish_job"


class _FakeCursor:
    """Answers the three shapes of query the sensor issues.

    ``leverancer`` maps kildetabel -> registered delivery date (a table absent
    from the dict is one the archive has never run for). ``unresolved`` maps dm
    fact table -> count of dw_ek_Borger = -1, or None for "table not created".
    """

    def __init__(self, leverancer, unresolved, meta_exists=True):
        self.leverancer = leverancer
        self.unresolved = unresolved
        self.meta_exists = meta_exists
        self._result = None
        self.seek_only = True

    def execute(self, sql, params=None):
        if "sys.tables" in sql:
            schema, table = params
            exists = (
                self.meta_exists if schema == "meta" else self.unresolved.get(table) is not None
            )
            self._result = [(int(exists),)]
        elif "HAIBALeverance" in sql:
            self._result = [(k, v) for k, v in self.leverancer.items()]
        elif "dw_ek_Borger" in sql:
            # A scan here would be a real regression: the count runs on every
            # nightly tick, and IX_dm_HAIBA_Cases_*_Borger exists for it.
            assert "COUNT(*)" in sql and "WHERE [dw_ek_Borger] = -1" in sql
            table = next(t for t in CASE_FACT_TABLES if t in sql)
            self._result = [(self.unresolved[table],)]
        else:
            raise AssertionError(f"unexpected query: {sql}")

    def fetchone(self):
        return self._result[0]

    def fetchall(self):
        return self._result

    def close(self):
        pass


class _FakeMSSQL:
    def __init__(self, leverancer, unresolved, meta_exists=True, raises=None):
        self.leverancer = leverancer
        self.unresolved = unresolved
        self.meta_exists = meta_exists
        self.raises = raises

    def get_connection(self):
        if self.raises:
            raise self.raises
        cursor = _FakeCursor(self.leverancer, self.unresolved, self.meta_exists)

        class _Conn:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

            def cursor(self_inner):
                return cursor

        return _Conn()


class _FakeInstance:
    """Run history for the publish job. ``runs`` is newest-first."""

    def __init__(self, runs=()):
        self.runs = list(runs)

    def get_run_records(self, filters=None, limit=None):
        wanted = dict(getattr(filters, "tags", None) or {})
        matching = [r for r in self.runs if all(r["tags"].get(k) == v for k, v in wanted.items())]
        return [
            type("Rec", (), {"dagster_run": type("Run", (), {"tags": r["tags"]})()})()
            for r in matching[: limit or len(matching)]
        ]


class _FakeContext:
    def __init__(self, instance, run_id="run-abc"):
        self.instance = instance
        self.log = dg.get_dagster_logger()
        self.dagster_run = type("Run", (), {"run_id": run_id, "job_name": "ifdb_dfm_daily_job"})()


def _leverancer(dato="2026-08-15"):
    return {name: dato for name in ARC_SOURCE_TABLES}


def _clean():
    return {table: 0 for table in CASE_FACT_TABLES}


def _signature(dato="2026-08-15"):
    return ";".join(f"{name}={dato}" for name in ARC_SOURCE_TABLES)


def _published(signature, unresolved_tag=None):
    tags = {DELIVERY_SIGNATURE_TAG: signature}
    if unresolved_tag is not None:
        tags[UNRESOLVED_TAG] = str(unresolved_tag)
    return {"tags": tags}


def _run(mssql, instance, run_id="run-abc"):
    return evaluate(_FakeContext(instance, run_id), publish_job_name=PUBLISH_JOB, mssql=mssql)


# --------------------------------------------------------------------------
# Reading the warehouse
# --------------------------------------------------------------------------


def test_signature_covers_every_source_table():
    """One table lagging a delivery behind is a different state of the world
    than all four being current, and must not compare equal to it."""
    cursor = _FakeCursor(_leverancer(), _clean())
    signature = read_delivery_signature(cursor)

    for name in ARC_SOURCE_TABLES:
        assert name in signature
    assert signature != read_delivery_signature(_FakeCursor(_leverancer("2026-08-08"), _clean()))

    lagging = _leverancer() | {"haiba_cases_ssi": "2026-08-08"}
    assert read_delivery_signature(_FakeCursor(lagging, _clean())) != signature


def test_partial_or_missing_register_yields_no_signature():
    """A partial signature that happened to compare equal to a published one
    would silently suppress a publish, so it must not be produced at all."""
    partial = {name: "2026-08-15" for name in ARC_SOURCE_TABLES[:3]}
    assert read_delivery_signature(_FakeCursor(partial, _clean())) is None
    assert read_delivery_signature(_FakeCursor({}, _clean(), meta_exists=False)) is None


def test_unresolved_count_spans_both_case_facts_and_seeks():
    assert read_unresolved_cases(_FakeCursor(_leverancer(), _clean())) == 0
    assert (
        read_unresolved_cases(
            _FakeCursor(_leverancer(), {"HAIBA_CasesInfektion": 7, "HAIBA_CasesSSI": 2})
        )
        == 9
    )
    # "dm not created yet" is not the same as "zero unresolved".
    missing = {"HAIBA_CasesInfektion": None, "HAIBA_CasesSSI": 0}
    assert read_unresolved_cases(_FakeCursor(_leverancer(), missing)) is None


# --------------------------------------------------------------------------
# Gate 1 — publish once per delivery
# --------------------------------------------------------------------------


def test_fires_for_an_unpublished_delivery():
    result = _run(_FakeMSSQL(_leverancer(), _clean()), _FakeInstance())

    assert isinstance(result, dg.SensorResult)
    (request,) = result.run_requests
    assert request.job_name == PUBLISH_JOB
    assert request.tags[DELIVERY_SIGNATURE_TAG] == _signature()
    # The initial publish carries no count — that absence is what grants the
    # first recovery attempt an unconditional try.
    assert UNRESOLVED_TAG not in request.tags


def test_does_not_republish_a_resolved_delivery():
    """SDS delivers weekly, the monitored job runs nightly. Without this the
    sensor rebuilds the whole dm layer and republishes DSO every night."""
    instance = _FakeInstance([_published(_signature())])
    result = _run(_FakeMSSQL(_leverancer(), _clean()), instance)

    assert isinstance(result, dg.SkipReason)


def test_a_new_delivery_reopens_the_gate():
    instance = _FakeInstance([_published(_signature("2026-08-08"))])
    result = _run(_FakeMSSQL(_leverancer("2026-08-15"), _clean()), instance)

    assert isinstance(result, dg.SensorResult)


def test_a_failed_publish_is_retried_on_the_next_nightly_success():
    """The gate asks for a SUCCESSFUL publish, not a requested one — otherwise
    run-key dedup would strand the delivery until the next one arrives.

    The run_key carries the triggering run id so each nightly success yields at
    most one attempt, rather than one attempt per delivery for all time."""
    mssql, instance = _FakeMSSQL(_leverancer(), _clean()), _FakeInstance()

    first = _run(mssql, instance, run_id="nightly-1")
    second = _run(mssql, instance, run_id="nightly-2")

    assert isinstance(first, dg.SensorResult) and isinstance(second, dg.SensorResult)
    assert first.run_requests[0].run_key != second.run_requests[0].run_key


# --------------------------------------------------------------------------
# Gate 2 — recovery while the Borger chain is still resolving
# --------------------------------------------------------------------------


def test_recovers_cases_left_without_a_citizen_after_the_initial_publish():
    """Published, but some cases still resolved no citizen — the stub creation
    was capped, or a chain step lagged. Those carry no FHIR contact either, so
    they must not sit on DSO until next week's delivery."""
    instance = _FakeInstance([_published(_signature())])
    mssql = _FakeMSSQL(_leverancer(), {"HAIBA_CasesInfektion": 40, "HAIBA_CasesSSI": 2})

    result = _run(mssql, instance)

    assert isinstance(result, dg.SensorResult)
    # The count that triggered this attempt becomes the baseline for the next.
    assert result.run_requests[0].tags[UNRESOLVED_TAG] == "42"


def test_keeps_recovering_while_the_count_falls():
    instance = _FakeInstance([_published(_signature(), unresolved_tag=42)])
    mssql = _FakeMSSQL(_leverancer(), {"HAIBA_CasesInfektion": 9, "HAIBA_CasesSSI": 0})

    result = _run(mssql, instance)

    assert isinstance(result, dg.SensorResult)
    assert result.run_requests[0].tags[UNRESOLVED_TAG] == "9"


def test_stops_when_the_count_plateaus():
    """Some cases never resolve: a CPR the FHIR server knows only as a
    previous_ssn is 'known' to the stub creation, so no stub is made, but
    ref.PatientToBorger only ever carries the current patient_ssn. Without this
    the sensor would rebuild the dm layer every night forever."""
    instance = _FakeInstance([_published(_signature(), unresolved_tag=9)])
    mssql = _FakeMSSQL(_leverancer(), {"HAIBA_CasesInfektion": 9, "HAIBA_CasesSSI": 0})

    result = _run(mssql, instance)

    assert isinstance(result, dg.SkipReason)
    assert "stopped resolving" in str(result.skip_message)


def test_a_plateau_does_not_block_the_next_delivery():
    """Gate 1 must still fire even while cases from the previous delivery are
    permanently stuck."""
    instance = _FakeInstance([_published(_signature("2026-08-08"), unresolved_tag=9)])
    mssql = _FakeMSSQL(_leverancer("2026-08-15"), {"HAIBA_CasesInfektion": 9, "HAIBA_CasesSSI": 0})

    assert isinstance(_run(mssql, instance), dg.SensorResult)


def test_recovery_stops_once_everything_resolves():
    instance = _FakeInstance([_published(_signature(), unresolved_tag=9)])

    assert isinstance(_run(_FakeMSSQL(_leverancer(), _clean()), instance), dg.SkipReason)


# --------------------------------------------------------------------------
# Failure modes and wiring
# --------------------------------------------------------------------------


def test_a_database_error_skips_rather_than_failing_the_tick():
    mssql = _FakeMSSQL(_leverancer(), _clean(), raises=RuntimeError("login timeout"))

    result = _run(mssql, _FakeInstance())

    assert isinstance(result, dg.SkipReason)
    assert "login timeout" in str(result.skip_message)


def test_sensor_is_registered_and_requests_the_publish_job():
    from definitions import haiba_publish_sensor, postfixed_sensors

    assert haiba_publish_sensor in postfixed_sensors
    assert [t.job_name for t in haiba_publish_sensor.targets] == [PUBLISH_JOB]
    assert haiba_publish_sensor.required_resource_keys == frozenset({"mssql_target_staging"})


def test_sensor_waits_for_the_borger_chain():
    """It must trigger off the job that runs sof_patient_dfm ->
    ref_patient_borger -> dim_Borger -> dso_sync_dim_borger. Hanging it on a job
    that stops earlier would publish before the citizen keys exist.

    The monitored job is not exposed on the definition, so this pins the
    description the factory builds from ``borger_chain_job.name``."""
    from definitions import haiba_publish_sensor

    assert "ifdb_dfm_daily_job SUCCESS" in haiba_publish_sensor.description


def test_ingest_is_scheduled_before_the_ifdb_pipeline():
    """05:00 vs 06:00 is the whole point of the time: stubs must be in Aidbox
    before the chain that projects them into ref.PatientToBorger runs."""
    from schedules import all_schedules
    from schedules.haiba_schedules import haiba_ingest_daily_schedule
    from schedules.ifdb_dfm_schedules import ifdb_dfm_daily_schedule

    assert haiba_ingest_daily_schedule in all_schedules
    ingest_hour = int(haiba_ingest_daily_schedule.cron_schedule.split()[1])
    pipeline_hour = int(ifdb_dfm_daily_schedule.cron_schedule.split()[1])
    assert ingest_hour < pipeline_hour, (ingest_hour, pipeline_hour)
