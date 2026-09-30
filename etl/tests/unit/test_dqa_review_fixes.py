# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Regression tests for the pre-PR review fixes:

- s1: dqa_fhir_server_cleanup refuses to soft-delete when the server returns 0 live
  ids (would otherwise retire the whole table), and fhir_server_reconciliation
  skips the stale diff (no false 100%-stale) in the same situation.
- s2: dso_reconciliation reports counts only and skips the (second, linked-server)
  example-key diff on large tables.
- s3: replay._build_run_config binds config to the resolved job's ACTUAL op nodes
  and drops fields an op doesn't declare (mirrors period_orchestrator), so a
  renamed op or an unaccepted field can't silently corrupt the replay config.

Mock cursors / fakes only -- no DB.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity, Track
from dqa.registry import CHECK_TYPES

pytestmark = pytest.mark.unit


def _conn_cm(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    return cm


def _make_resource(cursor):
    resource = MagicMock()
    resource.get_connection.return_value = _conn_cm(cursor)
    return resource


def _all_sql(cursor) -> str:
    return "\n".join(str(c.args[0]) for c in cursor.execute.call_args_list if c.args)


# --- s1: fhir_server_cleanup refuses when the server returned 0 live ids ----------


def test_fhir_server_cleanup_refuses_on_zero_live_ids():
    from dqa.remediation.fhir_server_cleanup import (
        FhirServerCleanupConfig,
        dqa_fhir_server_cleanup,
    )

    cursor = MagicMock()
    staging = _make_resource(cursor)
    ctx = dg.build_asset_context(
        resources={"mssql_target_staging": staging, "fhir_database": MagicMock()}
    )
    with patch(
        "dqa.remediation.fhir_server_cleanup.stage_fhir_server_ids",
        return_value=("##stg", 0),  # server returned ZERO live ids
    ):
        # The guard aborts (dg.Failure) before any target is counted or mutated.
        with pytest.raises(dg.Failure):
            dqa_fhir_server_cleanup(ctx, FhirServerCleanupConfig(entity="encounter"))
    # Crucially, NO soft-delete UPDATE (indeed no query at all) was ever issued.
    assert "UPDATE" not in _all_sql(cursor).upper()


def test_fhir_server_cleanup_scopes_to_rows_predating_snapshot():
    from dqa.remediation.fhir_server_cleanup import (
        FhirServerCleanupConfig,
        dqa_fhir_server_cleanup,
    )

    cursor = MagicMock()
    # 1) cutoff (SELECT SYSDATETIME()), then 2) the Pass-1 stale COUNT for fhir.Patient.
    cursor.fetchone.side_effect = [("2026-07-02 12:00:00",), (0,), (0,)]
    staging = _make_resource(cursor)
    ctx = dg.build_asset_context(
        resources={"mssql_target_staging": staging, "fhir_database": MagicMock()}
    )
    with patch(
        "dqa.remediation.fhir_server_cleanup.stage_fhir_server_ids",
        return_value=("##stg", 5),  # live ids present -> proceeds past the live==0 guard
    ):
        dqa_fhir_server_cleanup(ctx, FhirServerCleanupConfig(entity="patient"))

    calls = [str(c.args[0]) for c in cursor.execute.call_args_list if c.args]
    # The cutoff is captured BEFORE the server snapshot (staging).
    assert calls[0] == "SELECT SYSDATETIME()"
    sql = "\n".join(calls)
    # Stale predicate now excludes rows inserted after the snapshot (race with a
    # concurrent SoF projection), while still including legacy NULL-timestamp rows.
    assert "NOT EXISTS" in sql
    assert "f.[dw_timestamp_insert] <= '2026-07-02 12:00:00'" in sql
    assert "f.[dw_timestamp_insert] IS NULL" in sql
    assert "UPDATE" not in sql.upper()  # 0 stale -> nothing soft-deleted


# --- s1: fhir_server_reconciliation skips the stale diff on 0 live ids ------------


def _fsr_subject():
    return DqaSubject(
        name="fhir_encounter",
        track=Track.FHIR,
        table="fhir.Encounter",
        resource_key="mssql_target_staging",
        identity_column="encounter_id",
    )


def test_fhir_server_reconciliation_fails_without_diff_on_zero_live_ids():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(5,)]  # only active_in_flat is queried
    resources = SimpleNamespace(
        mssql_target_staging=_make_resource(cursor), fhir_database=MagicMock()
    )
    spec = CheckSpecification(
        "fhir_server_reconciliation",
        Severity.WARN,
        Cadence.WEEKLY,
        {"resource_table": "encounter", "deleted_column": "dw_is_deleted"},
        requires=("fhir_database",),
    )
    with patch(
        "dqa.checks.fhir_server_reconciliation.stage_fhir_server_ids",
        return_value=("##stg", 0),  # server returned ZERO live ids
    ):
        outcome = CHECK_TYPES["fhir_server_reconciliation"](
            MagicMock(), _fsr_subject(), spec, resources
        )
    # The anomaly FAILS the check (visible in dqa.Results/dqa_summary), but with
    # violations=0 -- no row was actually judged stale.
    assert not outcome.passed and outcome.violations == 0
    assert outcome.extra["live_in_server"] == 0
    assert outcome.extra["stale_in_flat"] == -1
    assert "note" in outcome.extra
    # The expensive stale anti-join was NOT run (no false 100%-stale).
    assert "NOT EXISTS" not in _all_sql(cursor)


# --- s2: dso_reconciliation gates the example-key diff by table size --------------


def _run_dso(*, checked, missing, extra, mismatch, fetchall_first, examples_max=None):
    from dqa.checks.dso_reconciliation import dso_reconciliation

    cursor = MagicMock()
    cursor.fetchall.side_effect = fetchall_first
    # checked, then the single-pass (missing, extra, mismatch) row.
    cursor.fetchone.side_effect = [(checked,), (missing, extra, mismatch)]
    resources = SimpleNamespace(mssql_source_dso=_make_resource(cursor))
    params = {"pk": ["encounter_id"], "staging_db": "staging", "linked_server": "STAGING_LINK"}
    if examples_max is not None:
        params["examples_max_checked"] = examples_max
    subject = DqaSubject(
        name="dso_x",
        track=Track.FHIR,
        table="fhir.Encounter",
        resource_key="mssql_source_dso",
        results_resource_key="mssql_target_staging",
        identity_column="encounter_id",
    )
    spec = CheckSpecification("dso_reconciliation", Severity.ERROR, Cadence.DSO, params)
    outcome = dso_reconciliation(MagicMock(), subject, spec, resources)
    return outcome, cursor


def test_dso_reconciliation_skips_examples_on_large_table():
    # checked (6M) > default threshold (5M), with violations -> examples skipped.
    outcome, cursor = _run_dso(
        checked=6_000_000, missing=2, extra=0, mismatch=0, fetchall_first=[[]]
    )
    assert not outcome.passed and outcome.violations == 2
    assert outcome.examples == []
    assert outcome.extra.get("examples_skipped_large_table") is True
    # The second (linked-server) example diff was NOT run.
    assert "TOP 30" not in _all_sql(cursor)


def test_dso_reconciliation_runs_examples_on_small_table():
    # small table with a violation -> examples query DOES run (behaviour preserved).
    outcome, cursor = _run_dso(
        checked=10,
        missing=2,
        extra=0,
        mismatch=0,
        fetchall_first=[[], [("missing_in_dso", "k1", None)]],
    )
    assert not outcome.passed and outcome.violations == 2
    assert outcome.examples == [{"kind": "missing_in_dso", "key": "k1"}]
    assert "examples_skipped_large_table" not in outcome.extra
    assert "TOP 30" in _all_sql(cursor)


# --- s3: replay._build_run_config binds to actual op nodes + filters fields -------


def _node(name, fields):
    return SimpleNamespace(
        name=name,
        config_schema=SimpleNamespace(config_type=SimpleNamespace(fields=fields)),
    )


_RUNMODE_FIELDS = {
    "mode": 1,
    "advance_checkpoint": 1,
    "id_filter_table": 1,
    "id_filter_column": 1,
}


def test_build_run_config_binds_ids_config_to_server_op():
    from dqa.remediation.replay import _build_run_config

    job = SimpleNamespace(
        all_node_defs=[
            _node("fhir_server_encounter_dfm", dict(_RUNMODE_FIELDS)),
            _node("sof_encounter_dfm", dict(_RUNMODE_FIELDS)),
            _node("dso_sync_fhir_encounter_dfm", dict(_RUNMODE_FIELDS)),
            _node("some_infra_op", {}),  # no config fields -> skipped
        ]
    )
    desired = {
        "fhir_server_encounter_dfm": {
            "mode": "ids",
            "id_filter_column": "OBJECT_ID",
            "id_filter_table": "dqa.v_replay_ifdb_encounter",
            "advance_checkpoint": False,
        },
        "sof_encounter_dfm": {"advance_checkpoint": False},
        "dso_sync_fhir_encounter_dfm": {"advance_checkpoint": False},
    }
    rc = _build_run_config(job, desired)
    srv = rc["ops"]["fhir_server_encounter_dfm"]["config"]
    assert srv["mode"] == "ids" and srv["id_filter_table"] == "dqa.v_replay_ifdb_encounter"
    assert rc["ops"]["sof_encounter_dfm"]["config"] == {"advance_checkpoint": False}
    assert rc["ops"]["dso_sync_fhir_encounter_dfm"]["config"] == {"advance_checkpoint": False}
    assert "some_infra_op" not in rc["ops"]


def test_build_run_config_drops_fields_the_op_does_not_declare():
    from dqa.remediation.replay import _build_run_config

    # op declares only advance_checkpoint -> the ids fields are dropped, not sent.
    job = SimpleNamespace(all_node_defs=[_node("x", {"advance_checkpoint": 1})])
    rc = _build_run_config(job, {"x": {"mode": "ids", "advance_checkpoint": False}})
    assert rc["ops"]["x"]["config"] == {"advance_checkpoint": False}


def test_build_run_config_leaves_unknown_op_unbound():
    from dqa.remediation.replay import _build_run_config

    # If a rename made the desired key miss every node, nothing is bound (then
    # _check_replay_binding aborts on the missing node / unbound server op).
    job = SimpleNamespace(
        all_node_defs=[_node("fhir_server_encounter_dfm_feature_x", dict(_RUNMODE_FIELDS))]
    )
    rc = _build_run_config(job, {"fhir_server_encounter_dfm": {"mode": "ids"}})
    assert rc["ops"] == {}


# --- _check_replay_binding: only the DANGEROUS binding gaps abort ------------------

_REPLAY_ASSETS = [
    "fhir_server_encounter_dfm",
    "sof_encounter_dfm",
    "dso_sync_fhir_encounter_dfm",
]
_SERVER_OK = {
    "fhir_server_encounter_dfm": {
        "config": {"mode": "ids", "id_filter_table": "dqa.v_replay_ifdb_encounter"}
    }
}


def test_binding_allows_configless_op_to_run_with_defaults():
    from dqa.remediation.replay import _check_replay_binding

    # Regression: dso_sync_fhir_*_dfm takes NO run config, so it binds nothing --
    # that must not abort the replay (it runs with defaults; its delta sync is
    # watermark-driven). Previously any unbound op raised
    # "could not bind run config for ops [...]".
    run_config = {
        "ops": {
            **_SERVER_OK,
            "sof_encounter_dfm": {"config": {"advance_checkpoint": False}},
            # dso_sync_fhir_encounter_dfm: not bound (no config schema)
        }
    }
    defaulted = _check_replay_binding(
        _REPLAY_ASSETS, "fhir_server_encounter_dfm", set(_REPLAY_ASSETS), run_config
    )
    assert defaulted == ["dso_sync_fhir_encounter_dfm"]


def test_binding_aborts_when_an_intended_op_node_is_missing():
    from dqa.remediation.replay import _check_replay_binding

    # The op node itself is absent from the resolved job (rename / drift) -> abort.
    node_names = {"fhir_server_encounter_dfm", "sof_encounter_dfm"}  # dso missing
    with pytest.raises(dg.Failure, match="no op node found"):
        _check_replay_binding(
            _REPLAY_ASSETS, "fhir_server_encounter_dfm", node_names, {"ops": _SERVER_OK}
        )


def test_binding_aborts_when_server_op_lacks_ids_config():
    from dqa.remediation.replay import _check_replay_binding

    # Server op bound without ids mode (or not bound at all) -> abort: replaying
    # would re-post EVERY resource in delta/full mode.
    with pytest.raises(dg.Failure, match="ids-mode config"):
        _check_replay_binding(
            _REPLAY_ASSETS, "fhir_server_encounter_dfm", set(_REPLAY_ASSETS), {"ops": {}}
        )
    with pytest.raises(dg.Failure, match="ids-mode config"):
        _check_replay_binding(
            _REPLAY_ASSETS,
            "fhir_server_encounter_dfm",
            set(_REPLAY_ASSETS),
            {"ops": {"fhir_server_encounter_dfm": {"config": {"mode": "delta"}}}},
        )


# --- N2: replay recheck scopes to active rows (ignores soft-deleted stragglers) ---


def _replay_subject(*, deleted_column, required=False):
    from dqa.model import ReplaySpec

    params = {
        "column": "service_provider_id",
        "parent_table": "fhir.Organization",
        "parent_key": "organization_id",
    }
    if required:
        params["required"] = True
    return DqaSubject(
        name="fhir_encounter",
        track=Track.FHIR,
        table="fhir.Encounter",
        resource_key="mssql_target_staging",
        identity_column="encounter_id",
        replay=ReplaySpec("identifier_value", "ifdb_encounter", "OBJECT_ID"),
        deleted_column=deleted_column,
        checks=(
            CheckSpecification(
                "referential_integrity",
                Severity.ERROR,
                Cadence.WEEKLY,
                params,
                name_suffix="service_provider_id",
            ),
        ),
    )


def _run_recheck(subject):
    from dqa.remediation.replay import _recheck_and_resolve

    cursor = MagicMock()
    cursor.fetchall.return_value = [("service_provider_id",)]  # one pending referential ref
    cursor.rowcount = 0  # int, so the `rowcount > 0` guard doesn't hit a MagicMock
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    _recheck_and_resolve(MagicMock(), resources, subject, "ifdb_encounter")
    return _all_sql(cursor)


def test_recheck_scopes_dangling_to_active_rows_with_deleted_column():
    sql = _run_recheck(_replay_subject(deleted_column="dw_is_deleted"))
    # The "still dangling?" EXISTS ignores soft-deleted stragglers, matching the
    # active-scoped referential_integrity check -> the key can converge to resolved.
    assert "(src.[dw_is_deleted] IS NULL OR src.[dw_is_deleted] <> 1)" in sql


def test_recheck_no_active_filter_without_deleted_column():
    sql = _run_recheck(_replay_subject(deleted_column=None))
    # No soft-delete column on the subject -> no active-row filter in the recheck.
    assert "dw_is_deleted" not in sql


def test_recheck_uses_full_predicate_for_required_edges():
    # For a required FK, the check flags NULL/sentinel values too -- the recheck
    # must rebuild the SAME predicate (violation_predicate), or a still-NULL FK
    # would pass the dangling-only test and be wrongly flipped to 'resolved'
    # (then re-captured next week: endless churn).
    sql = _run_recheck(_replay_subject(deleted_column=None, required=True))
    assert "src.[service_provider_id] IS NULL" in sql
    assert "NOT EXISTS" in sql  # the dangling branch is still there too


def test_recheck_flips_unresolvable_after_max_attempts():
    # Keys still failing after N attempts leave the replay loop: a third UPDATE
    # flips the still-pending rows with attempts >= unresolvable_after (default 3)
    # to status='unresolvable' (feeder views select 'pending' only).
    sql = _run_recheck(_replay_subject(deleted_column=None))
    assert "status = 'unresolvable'" in sql
    assert "q.attempts >= 3" in sql


def test_capture_clears_overflow_marker_on_successful_merge():
    from dqa.remediation.capture import capture_referential_violations

    cursor = MagicMock()
    cursor.fetchone.return_value = (5,)  # under the guard -> MERGE path
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    subject = _replay_subject(deleted_column=None)
    capture_referential_violations(
        MagicMock(),
        subject,
        subject.checks[0],
        resources,
        where="src.[service_provider_id] IS NOT NULL",
        reference_column="service_provider_id",
        target="fhir.Organization.organization_id",
    )
    sql = _all_sql(cursor)
    assert "MERGE [dqa].[RemediationQueue]" in sql
    # A stale overflow marker from a previous oversized capture is cleared on the
    # successful (under-guard) path too, not only on the next overflow.
    assert "DELETE FROM [dqa].[RemediationQueue]" in sql
    assert "[business_key] IS NULL" in sql


def test_check_and_recheck_share_the_violation_predicate():
    from dqa.checks.referential_integrity import violation_predicate

    params = {
        "column": "service_provider_id",
        "parent_table": "fhir.Organization",
        "parent_key": "organization_id",
        "required": True,
        "ignore_values": [""],
    }
    pred = violation_predicate(params, "src")
    # Both branches, qualified to the alias.
    assert "src.[service_provider_id] IS NULL" in pred
    assert "OR src.[service_provider_id] = ''" in pred
    assert "NOT EXISTS (SELECT 1 FROM [fhir].[Organization] tgt" in pred
    # Optional edge -> dangling branch only.
    optional = violation_predicate({**params, "required": False}, "src")
    assert "IS NULL" not in optional.split("NOT EXISTS")[0]


# --- N1: dqa.results persistence uses N-prefixed (Unicode-safe) literals ----------


def test_persist_uses_nprefixed_literals():
    from dqa.model import CheckOutcome
    from dqa.result import _persist

    cursor = MagicMock()
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    subject = DqaSubject(
        name="fhir_encounter",
        track=Track.FHIR,
        table="fhir.Encounter",
        resource_key="mssql_target_staging",
        identity_column="encounter_id",
    )
    spec = CheckSpecification(
        "uniqueness",
        Severity.ERROR,
        Cadence.DAILY,
        {"grain": "identifier_value"},
        name_suffix="business_key",
    )
    outcome = CheckOutcome(
        passed=False, checked=10, violations=2, examples=["Ærø", "café"], extra={"note": "dåb"}
    )
    _persist(SimpleNamespace(run_id="run-1"), subject, spec, outcome, False, resources)
    sql = _all_sql(cursor)
    assert "N'fhir'" in sql  # a plain text column is N-prefixed
    assert "N'[" in sql  # examples_json literal is N-prefixed
    assert "N'{" in sql  # detail_json literal is N-prefixed


# --- N3: not_null counts offending ROWS, not summed NULL cells --------------------


def _plain_subject():
    return DqaSubject(
        name="fhir_x",
        track=Track.FHIR,
        table="fhir.X",
        resource_key="mssql_target_staging",
        identity_column="x_id",
    )


def test_not_null_counts_rows_not_cells():
    cursor = MagicMock()
    # (COUNT(*), rows-with->=1-null, null_a, null_b): 10 rows, 3 bad rows, 3+2 = 5 cells.
    cursor.fetchone.return_value = (10, 3, 3, 2)
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    spec = CheckSpecification("not_null", Severity.ERROR, Cadence.DAILY, {"columns": ["a", "b"]})
    outcome = CHECK_TYPES["not_null"](MagicMock(), _plain_subject(), spec, resources)

    assert outcome.checked == 10
    assert outcome.violations == 3  # ROWS, not the 5 summed cells
    assert outcome.extra["null_cells"] == 5  # old cell metric preserved in extra
    assert outcome.extra["null_counts"] == {"a": 3, "b": 2}
    # SQL asks for rows with any required NULL.
    assert "[a] IS NULL OR [b] IS NULL" in _all_sql(cursor)


# --- sentinel_rate: DWH -1 unknown-member rate (replaces the DWH not_null) --------


def _dwh_subject():
    return DqaSubject(
        name="dwh_x",
        track=Track.DWH,
        table="fct.X",
        resource_key="mssql_target_staging",
        identity_column="DW_EK_X",
    )


def test_sentinel_rate_observability_reports_rates_and_passes():
    cursor = MagicMock()
    # (COUNT(*), sentinels_a, sentinels_b): 200 rows; a: 50 (-1), b: 0.
    cursor.fetchone.return_value = (200, 50, 0)
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    spec = CheckSpecification(
        "sentinel_rate", Severity.INFO, Cadence.DAILY, {"columns": ["a", "b"]}
    )
    outcome = CHECK_TYPES["sentinel_rate"](MagicMock(), _dwh_subject(), spec, resources)

    assert outcome.passed and outcome.violations == 0  # no max_rate -> observability
    assert outcome.checked == 200
    assert outcome.extra["rates"] == {"a": 0.25, "b": 0.0}
    assert outcome.examples[0] == {"column": "a", "rate": 0.25, "sentinel_rows": 50}
    assert "[a] = -1" in _all_sql(cursor)  # default sentinel -1


def test_sentinel_rate_fails_when_max_rate_exceeded():
    cursor = MagicMock()
    cursor.fetchone.return_value = (200, 50, 10)  # a: 25%, b: 5%
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    spec = CheckSpecification(
        "sentinel_rate", Severity.WARN, Cadence.DAILY, {"columns": ["a", "b"], "max_rate": 0.10}
    )
    outcome = CHECK_TYPES["sentinel_rate"](MagicMock(), _dwh_subject(), spec, resources)

    assert not outcome.passed
    assert outcome.violations == 1  # only 'a' exceeds 10%
    assert outcome.extra["columns_exceeding"] == ["a"]


def test_sentinel_rate_per_column_dict_caps():
    cursor = MagicMock()
    cursor.fetchone.return_value = (200, 50, 10, 40)  # a: 25%, b: 5%, c: 20%
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    spec = CheckSpecification(
        "sentinel_rate",
        Severity.WARN,
        Cadence.DAILY,
        # a capped at 30% (under -> ok), b capped at 2% (over -> flags),
        # c has NO cap (observability-only despite its 20%).
        {"columns": ["a", "b", "c"], "max_rate": {"a": 0.30, "b": 0.02}},
    )
    outcome = CHECK_TYPES["sentinel_rate"](MagicMock(), _dwh_subject(), spec, resources)

    assert not outcome.passed
    assert outcome.violations == 1
    assert outcome.extra["columns_exceeding"] == ["b"]
    assert outcome.extra["rates"] == {"a": 0.25, "b": 0.05, "c": 0.2}


def test_dwh_subjects_use_sentinel_rate_not_not_null():
    from dqa.config.dwh_subjects import _SENTINEL_MAX_RATES, DWH_SUBJECTS

    for s in DWH_SUBJECTS:
        types = {c.check_type for c in s.checks}
        assert "not_null" not in types, f"{s.name} still carries a redundant not_null"
        sr = [c for c in s.checks if c.check_type == "sentinel_rate"]
        if sr:  # facts with DW_EK_ FK columns in lineage
            (spec,) = sr
            # A configured cap flips the fact to WARN (must be able to flag);
            # no cap -> INFO observability. Derived from the mapping so adding a
            # cap later does not break this test.
            capped = s.table in _SENTINEL_MAX_RATES
            assert spec.severity is (Severity.WARN if capped else Severity.INFO)
            assert ("max_rate" in spec.params) == capped
            # PK (DW_EK_<Fact>) is excluded -- it is a real key, never the sentinel.
            assert s.identity_column not in spec.params["columns"]
            assert spec.params["columns"], f"{s.name} sentinel_rate has no columns"


# --- N4: uniqueness refuses ignore_values on a composite grain --------------------


def test_uniqueness_rejects_ignore_values_on_composite_grain():
    resources = SimpleNamespace()  # must not be touched -- guard fires first
    spec = CheckSpecification(
        "uniqueness",
        Severity.ERROR,
        Cadence.DAILY,
        {"grain": "a,b", "ignore_values": ["-1"]},
    )
    with pytest.raises(ValueError, match="single-column grain"):
        CHECK_TYPES["uniqueness"](MagicMock(), _plain_subject(), spec, resources)


def test_uniqueness_allows_composite_grain_without_ignore_values():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(0,), (5,)]  # 0 dup groups, 5 rows checked
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    spec = CheckSpecification("uniqueness", Severity.ERROR, Cadence.DAILY, {"grain": "a,b"})
    outcome = CHECK_TYPES["uniqueness"](MagicMock(), _plain_subject(), spec, resources)
    assert outcome.passed and outcome.violations == 0


def test_uniqueness_allows_ignore_values_on_single_column_grain():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(0,), (5,)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    spec = CheckSpecification(
        "uniqueness",
        Severity.ERROR,
        Cadence.DAILY,
        {"grain": "identifier_value", "ignore_values": [""]},
    )
    outcome = CHECK_TYPES["uniqueness"](MagicMock(), _plain_subject(), spec, resources)
    assert outcome.passed


# --- uniqueness: explicit deleted_column=None opts OUT of active-row scoping ------


def _soft_delete_subject():
    return DqaSubject(
        name="fhir_encounter",
        track=Track.FHIR,
        table="fhir.Encounter",
        resource_key="mssql_target_staging",
        identity_column="encounter_id",
        deleted_column="dw_is_deleted",
    )


def _run_uniqueness(params):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(0,), (5,)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    spec = CheckSpecification("uniqueness", Severity.ERROR, Cadence.DAILY, params)
    outcome = CHECK_TYPES["uniqueness"](MagicMock(), _soft_delete_subject(), spec, resources)
    return outcome, _all_sql(cursor)


def test_uniqueness_falls_back_to_subject_deleted_column():
    # No deleted_column in params -> the subject's soft-delete column scopes the
    # check to active rows (business-key semantics).
    outcome, sql = _run_uniqueness({"grain": "identifier_value"})
    assert "[dw_is_deleted] IS NULL OR [dw_is_deleted] <> 1" in sql
    assert outcome.extra["active_only"] is True


def test_uniqueness_explicit_none_enforces_strict_all_rows():
    # Explicit deleted_column=None -> STRICT uniqueness over all rows (the PK /
    # resource-id grain case), even though the subject declares a soft-delete column.
    outcome, sql = _run_uniqueness({"grain": "encounter_id", "deleted_column": None})
    assert "dw_is_deleted" not in sql
    assert outcome.extra["active_only"] is False


# --- v1 coverage: non-DFM FHIR subjects are detection-only (no replay wiring) -----


def test_every_replayspec_entity_has_dfm_replay_assets():
    """A ReplaySpec whose entity is missing from replay._DFM_ASSETS only fails
    the day an operator actually runs the remediation job -- catch it here."""
    from dqa.config.fhir_subjects import FHIR_SUBJECTS
    from dqa.remediation.replay import _DFM_ASSETS

    entities = {s.replay.entity for s in FHIR_SUBJECTS if s.replay is not None}
    assert entities <= set(_DFM_ASSETS), entities - set(_DFM_ASSETS)


def test_non_dfm_subjects_have_no_replay_wiring():
    from dqa.config.fhir_subjects import _NON_DFM_SUBJECTS

    assert {s.name for s in _NON_DFM_SUBJECTS} == {
        "fhir_observation",
        "fhir_careplan",
        "fhir_careplan_activity",
        "fhir_patient_address",
        "fhir_questionnaire",
        "fhir_questionnaire_response",
    }
    for s in _NON_DFM_SUBJECTS:
        # Not DFM-migrated: no ids-mode replay pipeline -> nothing may feed the
        # remediation queue for these subjects.
        assert s.replay is None, s.name
        for spec in s.checks:
            assert not spec.params.get("capture_replay"), s.name
            # No arc.* archives for these resources either.
            assert spec.check_type not in (
                "source_reconciliation",
                "archive_reconciliation",
            ), s.name


def test_non_dfm_subjects_are_registered():
    from dqa.config import ALL_SUBJECTS
    from dqa.config.fhir_subjects import _NON_DFM_SUBJECTS

    registered = {s.name for s in ALL_SUBJECTS}
    assert {s.name for s in _NON_DFM_SUBJECTS} <= registered


def test_cleanup_entities_cover_all_is_deleted_tables():
    """Every fhir.* table carrying dw_is_deleted must be reachable by the cleanup.

    Derived from the base DDL rather than a hardcoded list, so adding a
    flag-bearing table without wiring an entity fails here instead of leaving a
    table whose soft-delete flag nothing can ever set.
    """
    import re
    from pathlib import Path

    from dqa.remediation.fhir_server_cleanup import _ENTITIES

    ddl = (
        Path(__file__).parents[2] / "infrastructure" / "ddl" / "tables" / "fhir" / "01_tables.sql"
    ).read_text()

    starts = [
        (m.group(1), m.start()) for m in re.finditer(r"CREATE TABLE \[fhir\]\.\[(\w+)\]\s*\(", ddl)
    ]
    with_flag = set()
    for i, (name, pos) in enumerate(starts):
        stop = starts[i + 1][1] if i + 1 < len(starts) else len(ddl)
        if "[dw_is_deleted]" in ddl[pos:stop]:
            with_flag.add(f"[fhir].[{name}]")

    covered = {t["flat"] for e in _ENTITIES.values() for t in e["targets"]}
    assert with_flag - covered == set(), (
        f"fhir.* tables carry dw_is_deleted but no cleanup entity targets them: "
        f"{sorted(with_flag - covered)}"
    )
    assert covered - with_flag == set(), (
        f"cleanup targets tables that have no dw_is_deleted column: "
        f"{sorted(covered - with_flag)}"
    )

    # Every target keys on a resource id and the uniform flag column.
    for entity, spec in _ENTITIES.items():
        assert spec["resource_table"].islower() and "_" not in spec["resource_table"], (
            f"{entity}: resource_table must be the lowercased one-word FHIR "
            f"resource type (the Aidbox table), got {spec['resource_table']!r}"
        )
        for t in spec["targets"]:
            assert t["deleted"] == "dw_is_deleted", (entity, t)

    # Spot-check the child-grain wiring: child tables diff on the PARENT resource id.
    patient_flats = [t["flat"] for t in _ENTITIES["patient"]["targets"]]
    assert patient_flats == ["[fhir].[Patient]", "[fhir].[Patient_Address]"]
    assert {t["id"] for t in _ENTITIES["patient"]["targets"]} == {"patient_id"}
    assert {t["id"] for t in _ENTITIES["service_request"]["targets"]} == {"service_request_id"}
    # Aidbox tables are the lowercased resource type (one word).
    assert _ENTITIES["service_request"]["resource_table"] == "servicerequest"


def test_org_location_server_recon_now_fixable():
    from dqa.config.fhir_subjects import FHIR_SUBJECTS

    by_name = {s.name: s for s in FHIR_SUBJECTS}
    for name in ("fhir_organization", "fhir_location"):
        s = by_name[name]
        # dw_is_deleted added by migration 20260702 -> subject scoped + cleanup wired,
        # so the server reconciliation is no longer detection-only WARN.
        assert s.deleted_column == "dw_is_deleted", name
        (recon,) = (c for c in s.checks if c.check_type == "fhir_server_reconciliation")
        assert recon.severity is Severity.ERROR, name
        assert recon.params["deleted_column"] == "dw_is_deleted", name


# --- arc sensor weekday gate: fires for Saturday's base run only ------------------


def _business_weekday_of(start_dt):
    from dqa.sensors import _TZ, _business_date

    ctx = MagicMock()
    ctx.dagster_run.run_id = "run-1"
    ctx.instance.get_run_record_by_id.return_value = SimpleNamespace(
        start_time=start_dt.replace(tzinfo=_TZ).timestamp()
    )
    return _business_date(ctx).weekday()


def test_business_date_anchors_to_the_run_record():
    from datetime import datetime

    # 2026-07-04 is a Saturday, 2026-07-02 a Thursday -- the business day comes from
    # the monitored run's OWN start time, not from when the daemon processes it.
    assert _business_weekday_of(datetime(2026, 7, 4, 6, 0)) == 5  # Saturday
    assert _business_weekday_of(datetime(2026, 7, 2, 6, 0)) == 3  # Thursday


def test_arc_sensor_spec_is_weekly_only():
    from dqa.sensors import DQA_AFTER_ETL_SENSOR_SPECS

    arc = next(s for s in DQA_AFTER_ETL_SENSOR_SPECS if s["name"] == "dqa_arc_ifdb_after_archive")
    # Weekly-only: no daily job, and the arc weekly job is requested (the builder
    # gates weekly_jobs to Saturday, so the heavy cross-DB recon stays weekly even
    # though the base job succeeds daily).
    assert arc["daily_job"] is None
    assert "dqa_arc_ifdb_weekly_job" in arc["weekly_jobs"]
    # The FHIR and ERP pipelines DO have a daily job (fired every day).
    daily = {s["name"] for s in DQA_AFTER_ETL_SENSOR_SPECS if s["daily_job"]}
    assert daily == {"dqa_fhir_after_etl", "dqa_erp_daily_after_dfm"}


# --- arc replay: ids-mode re-extract for archive_reconciliation gaps ---------------


def test_arc_subjects_replay_wiring_excludes_location():
    from dqa.config.arc_subjects import ARC_SUBJECTS

    by_name = {s.name: s for s in ARC_SUBJECTS}
    # Delta single-merge-key entities get a ReplaySpec + capture; the check's
    # capture also requires n == 1 key column at runtime.
    for name, key in (
        ("arc_ifdb_encounter", "OBJECT_ID"),
        ("arc_ifdb_episode_of_care", "OBJECT_ID"),
        ("arc_ifdb_patient", "PATIENT_ID"),
    ):
        s = by_name[name]
        assert s.replay is not None and s.replay.entity == name, name
        assert s.replay.filter_column == key, name
        assert s.checks[0].params.get("capture_replay") is True, name
    # Full-load composite-key Location: NO replay (its archive merge runs
    # NOT-MATCHED-BY-SOURCE delete detection -- a partial ids ext would
    # mass-soft-delete) and NO capture.
    loc = by_name["arc_ifdb_location"]
    assert loc.replay is None
    assert "capture_replay" not in loc.checks[0].params


def test_arc_replay_entities_match_subjects():
    from dqa.config.arc_subjects import ARC_SUBJECTS
    from dqa.remediation.arc_replay import _ARC_DFM_ASSETS

    replayable = {s.replay.entity for s in ARC_SUBJECTS if s.replay is not None}
    assert set(_ARC_DFM_ASSETS) == replayable
    # Ext assets carry the DFM factory naming ext_<target_table> (ext_ifdb_<base>);
    # arc assets keep the arc_<base>_ifdb spelling.
    for entity, (ext_asset, arc_asset) in _ARC_DFM_ASSETS.items():
        base = entity.removeprefix("arc_ifdb_")
        assert ext_asset == f"ext_ifdb_{base}" and arc_asset == f"arc_{base}_ifdb", entity


def test_arc_replay_chunks_at_oracle_in_list_cap():
    from dqa.remediation.arc_replay import _ORACLE_IN_LIMIT, _chunk

    assert _ORACLE_IN_LIMIT == 1000
    keys = [str(i) for i in range(2500)]
    chunks = _chunk(keys)
    assert [len(c) for c in chunks] == [1000, 1000, 500]
    assert [k for c in chunks for k in c] == keys
    assert _chunk([]) == []


def test_arc_replay_recheck_resolves_and_parks_unresolvable():
    from dqa.config.arc_subjects import ARC_SUBJECTS
    from dqa.remediation.arc_replay import _recheck_and_resolve

    subject = next(s for s in ARC_SUBJECTS if s.name == "arc_ifdb_encounter")
    cursor = MagicMock()
    cursor.rowcount = 0
    _recheck_and_resolve(cursor, subject, "arc_ifdb_encounter", 3)
    sql = _all_sql(cursor)
    # resolved iff the key now EXISTS in the archive...
    assert "EXISTS (SELECT 1 FROM [arc].[ifdb_encounter] a" in sql
    assert "a.[OBJECT_ID] = q.business_key" in sql
    assert "status = 'resolved'" in sql
    # ...still-missing keys bump attempts and park as unresolvable after N.
    assert "still missing in arc after replay" in sql
    assert "status = 'unresolvable'" in sql and "q.attempts >= 3" in sql
    # Scoped to the arc entity namespace (never touches FHIR replay rows).
    assert "replay_entity = 'arc_ifdb_encounter'" in sql


def test_capture_oracle_gaps_enqueues_under_arc_entity():
    from dqa.config.arc_subjects import ARC_SUBJECTS
    from dqa.remediation.capture import capture_archive_reconciliation_gaps

    subject = next(s for s in ARC_SUBJECTS if s.name == "arc_ifdb_encounter")
    cursor = MagicMock()
    cursor.fetchone.return_value = (3,)  # under the guard -> MERGE path
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    capture_archive_reconciliation_gaps(
        MagicMock(),
        subject,
        resources,
        missing_source="FROM (SELECT k0 FROM ##stg) s WHERE NOT EXISTS (...)",
        key_expr="s.k0",
        source_table="IFDB.ENCOUNTER",
    )
    sql = _all_sql(cursor)
    assert "MERGE [dqa].[RemediationQueue]" in sql
    assert "'archive_reconciliation'" in sql  # reference_column
    assert "'arc_ifdb_encounter'" in sql  # replay_entity namespace
    assert "'IFDB.ENCOUNTER'" in sql  # target


# --- arc vanished cleanup: soft-delete keys gone from IFDB -------------------------


def test_arc_vanished_cleanup_refuses_on_zero_staged_keys():
    from dqa.remediation.arc_vanished_cleanup import (
        ArcVanishedCleanupConfig,
        dqa_arc_ifdb_vanished_cleanup,
    )

    cursor = MagicMock()
    cursor.fetchone.side_effect = [("2026-07-03 12:00:00",)]  # cutoff only
    staging = _make_resource(cursor)
    ctx = dg.build_asset_context(
        resources={"mssql_target_staging": staging, "ifdb_oracle": MagicMock()}
    )
    with patch(
        "dqa.remediation.arc_vanished_cleanup.stage_ifdb_keys",
        return_value=("##stg", 0),  # staging returned ZERO live keys
    ):
        with pytest.raises(dg.Failure):
            dqa_arc_ifdb_vanished_cleanup(
                ctx, ArcVanishedCleanupConfig(entity="arc_ifdb_encounter")
            )
    assert "UPDATE" not in _all_sql(cursor).upper()


def test_arc_vanished_cleanup_scopes_to_rows_predating_snapshot():
    from dqa.remediation.arc_vanished_cleanup import (
        ArcVanishedCleanupConfig,
        dqa_arc_ifdb_vanished_cleanup,
    )

    cursor = MagicMock()
    # cutoff, then the vanished COUNT (0 -> nothing to do, no UPDATE).
    cursor.fetchone.side_effect = [("2026-07-03 12:00:00",), (0,)]
    staging = _make_resource(cursor)
    ctx = dg.build_asset_context(
        resources={"mssql_target_staging": staging, "ifdb_oracle": MagicMock()}
    )
    with patch(
        "dqa.remediation.arc_vanished_cleanup.stage_ifdb_keys",
        return_value=("##stg", 5),
    ):
        dqa_arc_ifdb_vanished_cleanup(ctx, ArcVanishedCleanupConfig(entity="arc_ifdb_encounter"))

    calls = [str(c.args[0]) for c in cursor.execute.call_args_list if c.args]
    assert calls[0] == "SELECT SYSDATETIME()"  # cutoff BEFORE staging
    sql = "\n".join(calls)
    # Only active rows, key gone from the staged live set, archived BEFORE the
    # snapshot (concurrent daily-run TOCTOU guard) -- mirrors fhir_server_cleanup.
    assert "f.[dw_is_deleted] = 0" in sql
    assert "NOT EXISTS (SELECT 1 FROM ##stg t WHERE t.k = f.[OBJECT_ID])" in sql
    assert "f.[dw_timestamp_insert] <= '2026-07-03 12:00:00'" in sql
    assert "UPDATE" not in sql.upper()  # 0 vanished -> nothing soft-deleted


def test_arc_vanished_cleanup_handles_composite_key_location():
    """Location's composite grain (OBJECT_ID, PERIOD_START) must diff on
    BOTH columns -- a single-column diff would collapse ~14M archived rows onto a
    few hundred distinct OBJECT_IDs and mass-retire live period rows."""
    from dqa.remediation.arc_vanished_cleanup import (
        ArcVanishedCleanupConfig,
        dqa_arc_ifdb_vanished_cleanup,
    )

    cursor = MagicMock()
    # cutoff, then the vanished COUNT (0 -> nothing to do, no UPDATE).
    cursor.fetchone.side_effect = [("2026-07-03 12:00:00",), (0,)]
    staging = _make_resource(cursor)
    ctx = dg.build_asset_context(
        resources={"mssql_target_staging": staging, "ifdb_oracle": MagicMock()}
    )
    with patch(
        "dqa.remediation.arc_vanished_cleanup.stage_ifdb_keys",
        return_value=("##stg", 5),
    ) as staged:
        dqa_arc_ifdb_vanished_cleanup(ctx, ArcVanishedCleanupConfig(entity="arc_ifdb_location"))

    # Both key columns are staged...
    assert staged.call_args.args[4] == ["OBJECT_ID", "PERIOD_START"]
    sql = _all_sql(cursor)
    # ...and the diff matches on the FULL composite key, with NULL-guards on both.
    assert "t.k0 = f.[OBJECT_ID] AND t.k1 = f.[PERIOD_START]" in sql
    assert "f.[OBJECT_ID] IS NOT NULL AND f.[PERIOD_START] IS NOT NULL" in sql
    assert "UPDATE" not in sql.upper()  # 0 vanished -> nothing soft-deleted


def test_stage_ifdb_keys_single_column_shape_is_unchanged():
    """Backward-compat: single-key staging keeps column name ``k`` (existing diff
    SQL in the cleanup and any operator muscle memory depend on it)."""
    from dqa.checks.oracle_source import stage_ifdb_keys

    cur = MagicMock()
    ora_cursor = MagicMock()
    ora_cursor.fetchmany.side_effect = [[(123.0,), (None,)], []]
    stage, loaded = stage_ifdb_keys(
        MagicMock(), cur, _make_resource(ora_cursor), "IFDB.X", "OBJECT_ID"
    )

    create = str(cur.execute.call_args_list[0].args[0])
    assert "(k NVARCHAR(255) NOT NULL)" in create
    # 123.0 normalized to '123'; the NULL row skipped.
    assert cur.bulkcopy.call_args.args[1] == [("123",)]
    assert cur.bulkcopy.call_args.kwargs["column_mappings"] == ["k"]
    assert loaded == 1


def test_stage_ifdb_keys_composite_normalizes_each_part():
    """Composite staging: k0/k1 columns, NUMBER ids lose the .0 artifact, DATE
    keys stringify to the archive's exact format, any-NULL rows are skipped."""
    import datetime as dt

    from dqa.checks.oracle_source import stage_ifdb_keys

    cur = MagicMock()
    ora_cursor = MagicMock()
    ora_cursor.fetchmany.side_effect = [
        [(100014109707.0, dt.datetime(2026, 1, 2, 3, 4, 5)), (7.0, None)],
        [],
    ]
    stage, loaded = stage_ifdb_keys(
        MagicMock(),
        cur,
        _make_resource(ora_cursor),
        "IFDB.LOCATION",
        ["OBJECT_ID", "PERIOD_START"],
    )

    create = str(cur.execute.call_args_list[0].args[0])
    assert "k0 NVARCHAR(255) NOT NULL, k1 NVARCHAR(255) NOT NULL" in create
    assert cur.bulkcopy.call_args.args[1] == [("100014109707", "2026-01-02 03:04:05")]
    assert cur.bulkcopy.call_args.kwargs["column_mappings"] == ["k0", "k1"]
    assert loaded == 1  # raekken med NULL PERIOD_START er skippet
    index = str(cur.execute.call_args_list[-1].args[0])
    assert index.endswith("(k0, k1)")


# --- archive_reconciliation: source-generic (dialect / source_resource / flow-start)


def _run_archive_recon(params, *, source_resource_key, flow_start="2026-07-04 06:00:00"):
    """Run archive_reconciliation with scripted cursors; all diff counts are zero."""
    staging_cursor = MagicMock()
    # flow-start row, (source_count, within), missing, (arc_total, extra, stale)
    staging_cursor.fetchone.side_effect = [(flow_start,), (10, 10), (0,), (10, 0, 0)]
    source_cursor = MagicMock()
    source_cursor.fetchmany.side_effect = [[(123.0, 1)], []]  # one key row, then done
    resources = SimpleNamespace(
        mssql_target_staging=_make_resource(staging_cursor),
        **{source_resource_key: _make_resource(source_cursor)},
    )
    subject = DqaSubject(
        name="arc_x",
        track=Track.ARCHIVE,
        table="arc.x_tbl",
        resource_key="mssql_target_staging",
        identity_column=params["key_columns"][0],
    )
    spec = CheckSpecification("archive_reconciliation", Severity.ERROR, Cadence.WEEKLY, params)
    outcome = CHECK_TYPES["archive_reconciliation"](MagicMock(), subject, spec, resources)
    return outcome, staging_cursor, source_cursor


def test_archive_reconciliation_mssql_dialect():
    params = {
        "source_table": "dbo.Finanspostering",
        "key_columns": ["RECID"],
        "deleted_column": "dw_is_deleted",
        "dialect": "mssql",
        "source_resource": "sydax_mssql",
        "flow_start_bound_column": "ModifiedDateTime",
        "flow_start_state": ["dfm", "sydax_flow_start"],
    }
    outcome, staging_cursor, source_cursor = _run_archive_recon(
        params, source_resource_key="sydax_mssql"
    )
    assert outcome.passed and outcome.extra["dialect"] == "mssql"
    # Flow-start read from the PARAMETERIZED DFMState row, not the IFDB one.
    fs_call = staging_cursor.execute.call_args_list[0]
    assert fs_call.args[1] == ("dfm", "sydax_flow_start")
    # Source-side bound uses the T-SQL literal, not Oracle's TO_DATE...
    osql = str(source_cursor.execute.call_args_list[0].args[0])
    assert "CONVERT(DATETIME2, '2026-07-04 06:00:00', 120)" in osql
    assert "TO_DATE" not in osql
    # ...and the mssql default does NOT Oracle-normalize keys: str(123.0) survives.
    batch = staging_cursor.bulkcopy.call_args.args[1]
    assert batch == [("123.0", 1)]


def test_archive_reconciliation_oracle_defaults_preserved():
    params = {
        "source_table": "IFDB.ENCOUNTER",
        "key_columns": ["OBJECT_ID"],
        "deleted_column": "dw_is_deleted",
        "flow_start_bound_column": "EPJI_MODIFICATION_DATETIME",
        # dialect / source_resource / flow_start_state omitted -> IFDB Oracle defaults
    }
    outcome, staging_cursor, source_cursor = _run_archive_recon(
        params, source_resource_key="ifdb_oracle"
    )
    assert outcome.passed and outcome.extra["dialect"] == "oracle"
    fs_call = staging_cursor.execute.call_args_list[0]
    assert fs_call.args[1] == ("dfm", "ifdb_flow_start")
    osql = str(source_cursor.execute.call_args_list[0].args[0])
    assert "TO_DATE('2026-07-04 06:00:00', 'YYYY-MM-DD HH24:MI:SS')" in osql
    # Oracle default normalizes *_ID key columns: 123.0 -> '123' (NUMBER artifact).
    batch = staging_cursor.bulkcopy.call_args.args[1]
    assert batch == [("123", 1)]


def test_archive_reconciliation_rejects_unknown_dialect():
    resources = SimpleNamespace()  # must not be touched -- validation fires first
    subject = DqaSubject(
        name="arc_x",
        track=Track.ARCHIVE,
        table="arc.x_tbl",
        resource_key="mssql_target_staging",
        identity_column="RECID",
    )
    spec = CheckSpecification(
        "archive_reconciliation",
        Severity.ERROR,
        Cadence.WEEKLY,
        {"source_table": "dbo.X", "key_columns": ["RECID"], "dialect": "postgres"},
    )
    with pytest.raises(ValueError, match="unknown dialect"):
        CHECK_TYPES["archive_reconciliation"](MagicMock(), subject, spec, resources)


def test_arc_subjects_pin_ifdb_oracle_and_flow_start_state():
    from dqa.config.arc_subjects import ARC_SUBJECTS

    for s in ARC_SUBJECTS:
        (spec,) = s.checks
        assert spec.check_type == "archive_reconciliation"
        assert spec.params["dialect"] == "oracle"
        assert spec.params["source_resource"] == "ifdb_oracle"
        assert spec.params["source_resource"] in spec.requires
        # Subjects whose extract sets upper_bound_column carry the flow-start
        # row; everything else is reconciled unbounded and must be listed below.
        #
        # Two reasons to land in the allow-list, and they are NOT equivalent:
        #   1. full-load tables (Location + the two receiver tables) — the whole
        #      table is re-streamed on every scheduled run. Verify the row count
        #      before adding one; see the decision rule in
        #      docs/guides/2026-08-07_condition_arkivmigrering_rollout.md.
        #   2. inner-join-delta children — bounded by the PARENT's timestamp, so
        #      the nightly cost follows the change volume. They are unbounded for
        #      DQA only because their delta expression is alias-qualified.
        if "flow_start_bound_column" in spec.params:
            assert spec.params["flow_start_state"] == ["dfm", "ifdb_flow_start"], s.name
        else:
            assert s.name in {
                "arc_ifdb_location",
                "arc_ifdb_diagnose_tillaegskode",
                "arc_ifdb_pas_diagnose_encounter_ref",
                "arc_ifdb_pas_diagnose_tillaegskode",
                "arc_ifdb_pas_diagnose_henvisning_ref",
                "arc_ifdb_adm_procedure_suppl_codes",
                "arc_ifdb_aktivitetsudfoerelse_speciale",
                # BOM has no JOB_LOG flow-start at all, so the whole family is
                # reconciled unbounded (checkpoints are stamped with server time).
                "arc_bom_ifdb_aftale",
                "arc_bom_ifdb_aftaleressourcedeltager",
                "arc_bom_ifdb_patientinfo",
                "arc_bom_ifdb_orgenhed_sor",
            }, s.name


def test_archive_reconciliation_commits_staging_before_capture():
    # Regression (local Encounter hang): the capture reads the ##staging temp from
    # its OWN connection. Autocommit is off, so unless the staging transaction
    # (INSERTs + CREATE CLUSTERED INDEX) is committed first, that cross-session
    # read blocks on the index's Sch-M lock FOREVER (no lock timeout).
    from dqa.model import ReplaySpec

    staging_cursor = MagicMock()
    # no flow-start bound configured -> (source_count, within), missing=5, extra triple
    staging_cursor.fetchone.side_effect = [(10, 10), (5,), (10, 0, 0)]
    staging_cursor.fetchall.return_value = []  # missing-key examples
    conn = MagicMock()
    conn.cursor.return_value = staging_cursor
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    staging = MagicMock()
    staging.get_connection.return_value = cm

    source_cursor = MagicMock()
    source_cursor.fetchmany.side_effect = [[(1.0, 1)], []]
    resources = SimpleNamespace(
        mssql_target_staging=staging, ifdb_oracle=_make_resource(source_cursor)
    )
    subject = DqaSubject(
        name="arc_ifdb_encounter",
        track=Track.ARCHIVE,
        table="arc.ifdb_encounter",
        resource_key="mssql_target_staging",
        identity_column="OBJECT_ID",
        replay=ReplaySpec("OBJECT_ID", "arc_ifdb_encounter", "OBJECT_ID"),
    )
    spec = CheckSpecification(
        "archive_reconciliation",
        Severity.ERROR,
        Cadence.WEEKLY,
        {
            "source_table": "IFDB.ENCOUNTER",
            "key_columns": ["OBJECT_ID"],
            "deleted_column": "dw_is_deleted",
            "capture_replay": True,
        },
    )
    commits_when_captured = []
    with patch(
        "dqa.remediation.capture.capture_archive_reconciliation_gaps",
        side_effect=lambda *a, **k: commits_when_captured.append(conn.commit.call_count),
    ):
        outcome = CHECK_TYPES["archive_reconciliation"](MagicMock(), subject, spec, resources)
    # Commit #1 releases the CREATE TABLE schema lock (for bulkcopy's connection);
    # commit #2 must land AFTER the index build and BEFORE the capture runs.
    assert commits_when_captured == [2]
    assert not outcome.passed and outcome.extra["missing_in_arc"] == 5


def test_arc_replay_desired_config_passes_run_mode_validation():
    # Regression: the arc MERGE op was given {"mode": "ids", "advance_checkpoint":
    # False} -- RunModeConfig's pydantic validator rejects mode='ids' without the
    # id-filter fields, failing the child run before the op body even executed.
    # Both ops must therefore carry the full ids config; construct the REAL
    # RunModeConfig from each to exercise the validation layer.
    from core.dfm_config import RunModeConfig
    from dqa.remediation.arc_replay import _desired_config

    desired = _desired_config("ext_encounter_ifdb", "arc_encounter_ifdb", "OBJECT_ID", ["1", "2"])
    assert set(desired) == {"ext_encounter_ifdb", "arc_encounter_ifdb"}
    for op, cfg in desired.items():
        rm = RunModeConfig(**cfg)  # must not raise
        assert rm.mode == "ids", op
        assert rm.id_filter_column == "OBJECT_ID", op
        assert rm.id_filter_values == ["1", "2"], op
        # Checkpoint untouched -- explicitly, not just by ids-mode default.
        assert rm.advance_checkpoint is False and rm.should_advance_checkpoint is False, op
