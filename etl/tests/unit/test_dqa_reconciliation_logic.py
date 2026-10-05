# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests pinning the subtle reconciliation decisions made while hardening DQA,
so a future edit can't silently regress them:

- source_reconciliation aggregates the delete flag with MIN (not MAX).
- patient_arc_integrity's kept_deleted_dup requires the fhir row to still be active.
- capture builds MIN aggregates, honours the source_key==filter_column guard.
- fhir_server_reconciliation degrades to `1 = 1` when the table has no delete column.

Mock cursors only -- no DB.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dqa.model import Cadence, CheckSpecification, DqaSubject, ReplaySpec, Severity, Track
from dqa.registry import CHECK_TYPES

pytestmark = pytest.mark.unit


def _make_resource(cursor):
    resource = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    resource.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    resource.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    return resource


def _all_sql(cursor) -> str:
    return "\n".join(str(c.args[0]) for c in cursor.execute.call_args_list)


def _fhir_subject(**kw):
    return DqaSubject(
        name="fhir_encounter",
        track=Track.FHIR,
        table="fhir.Encounter",
        resource_key="mssql_target_staging",
        identity_column="encounter_id",
        replay=ReplaySpec("identifier_value", "ifdb_encounter", "OBJECT_ID"),
        **kw,
    )


# --- source_reconciliation: MIN aggregation --------------------------------------


def _recon_spec(**overrides):
    params = {
        "source_table": "arc.ifdb_encounter",
        "source_key": "OBJECT_ID",
        "target_key": "identifier_value",
        "source_deleted": "DELETED",
        "target_deleted": "dw_is_deleted",
    }
    params.update(overrides)
    return CheckSpecification("source_reconciliation", Severity.ERROR, Cadence.WEEKLY, params)


def test_source_reconciliation_aggregates_delete_flag_with_min():
    cursor = MagicMock()
    cursor.fetchone.return_value = (100, 100, 0, 0, 0)  # src, tgt, missing, extra, mismatch
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["source_reconciliation"](
        MagicMock(), _fhir_subject(), _recon_spec(), resources
    )
    assert outcome.passed and outcome.violations == 0
    sql = _all_sql(cursor)
    assert "MIN(" in sql and "MAX(" not in sql


def test_source_reconciliation_capture_gated_on_flag():
    # capture_replay=True + a gap -> capture is invoked.
    cursor = MagicMock()
    cursor.fetchone.return_value = (100, 100, 5, 0, 0)  # missing=5
    cursor.fetchall.return_value = [("missing_in_target", "k1", None)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    with patch("dqa.remediation.capture.capture_source_reconciliation_gaps") as cap:
        CHECK_TYPES["source_reconciliation"](
            MagicMock(), _fhir_subject(), _recon_spec(capture_replay=True), resources
        )
        assert cap.called

    # capture_replay not set -> never invoked, even with a gap.
    cursor2 = MagicMock()
    cursor2.fetchone.return_value = (100, 100, 5, 0, 0)
    cursor2.fetchall.return_value = [("missing_in_target", "k1", None)]
    resources2 = SimpleNamespace(mssql_target_staging=_make_resource(cursor2))
    with patch("dqa.remediation.capture.capture_source_reconciliation_gaps") as cap2:
        CHECK_TYPES["source_reconciliation"](
            MagicMock(), _fhir_subject(), _recon_spec(), resources2
        )
        assert not cap2.called


# --- patient_arc_integrity: kept_deleted_dup requires an ACTIVE fhir row ----------


def _patient_subject():
    return DqaSubject(
        name="fhir_patient",
        track=Track.FHIR,
        table="fhir.Patient",
        resource_key="mssql_target_staging",
        identity_column="patient_id",
    )


def _patient_spec():
    return CheckSpecification(
        "patient_arc_integrity",
        Severity.ERROR,
        Cadence.WEEKLY,
        {
            "source_table": "arc.ifdb_patient",
            "source_key": "PATIENT_ID",
            "source_cpr": "CPR",
            "source_deleted": "SLETTET",
            "target_key": "ifdb_patient_id",
            "target_deleted": "dw_is_deleted",
        },
    )


def test_patient_kept_deleted_dup_requires_active_fhir_row():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(0,), (0, 0, 0)]  # checked, then (orphan, kept, mismatch)
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["patient_arc_integrity"](
        MagicMock(), _patient_subject(), _patient_spec(), resources
    )
    assert outcome.passed
    sql = _all_sql(cursor)
    # kept_pred = "<arc deleted> = 1 AND <fhir deleted> <> 1 AND EXISTS(active sibling)"
    # so the soft-deleted-but-mirrored fhir row is excluded.
    assert "<> 1 AND EXISTS (SELECT 1 FROM [arc]" in sql


# --- capture_source_reconciliation_gaps: guard + MIN ------------------------------


def test_capture_noop_when_source_key_not_filter_column():
    from dqa.remediation.capture import capture_source_reconciliation_gaps

    ctx = MagicMock()
    subject = DqaSubject(
        name="fhir_patient",
        track=Track.FHIR,
        table="fhir.Patient",
        resource_key="mssql_target_staging",
        identity_column="patient_id",
        replay=ReplaySpec("ifdb_patient_id", "ifdb_patient", "PATIENT_ID"),
    )
    resources = SimpleNamespace()  # must not be touched
    capture_source_reconciliation_gaps(
        ctx,
        subject,
        resources,
        source_table="arc.ifdb_patient",
        source_key="CPR",  # != filter_column PATIENT_ID -> no-op + warning
        target_key="patient_ssn",
        source_deleted=None,
        target_deleted=None,
    )
    assert ctx.log.warning.called


def test_capture_builds_min_aggregates():
    from dqa.remediation.capture import capture_source_reconciliation_gaps

    cursor = MagicMock()
    cursor.fetchone.return_value = (5,)  # distinct-key count, under the guard
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    capture_source_reconciliation_gaps(
        MagicMock(),
        _fhir_subject(),
        resources,
        source_table="arc.ifdb_encounter",
        source_key="OBJECT_ID",
        target_key="identifier_value",
        source_deleted="DELETED",
        target_deleted="dw_is_deleted",
    )
    assert "MIN(" in _all_sql(cursor)


# --- fhir_server_reconciliation: no delete column -> "1 = 1" ----------------------


def _fsr(deleted_column, resource_table, fetchone_seq):
    cursor = MagicMock()
    cursor.fetchone.side_effect = fetchone_seq
    resources = SimpleNamespace(
        mssql_target_staging=_make_resource(cursor), fhir_database=MagicMock()
    )
    subject = DqaSubject(
        name=f"fhir_{resource_table}",
        track=Track.FHIR,
        table=f"fhir.{resource_table.capitalize()}",
        resource_key="mssql_target_staging",
        identity_column=f"{resource_table}_id",
    )
    spec = CheckSpecification(
        "fhir_server_reconciliation",
        Severity.WARN,
        Cadence.WEEKLY,
        {"resource_table": resource_table, "deleted_column": deleted_column},
        requires=("fhir_database",),
    )
    with patch(
        "dqa.checks.fhir_server_reconciliation.stage_fhir_server_ids",
        return_value=("##stg", 3),
    ):
        outcome = CHECK_TYPES["fhir_server_reconciliation"](MagicMock(), subject, spec, resources)
    return outcome, cursor


def test_fhir_server_reconciliation_no_delete_column_uses_all_rows():
    outcome, cursor = _fsr(None, "organization", [(5,), (0,), (0,)])
    assert outcome.passed
    sql = _all_sql(cursor)
    assert "WHERE 1 = 1" in sql


def test_fhir_server_reconciliation_scopes_to_active_rows_with_delete_column():
    outcome, cursor = _fsr("dw_is_deleted", "encounter", [(5,), (0,), (0,)])
    sql = _all_sql(cursor)
    assert "IS NULL OR" in sql and "<> 1" in sql
