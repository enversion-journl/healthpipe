# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests pinning the subtle reconciliation decisions made while hardening DQA,
so a future edit can't silently regress them:

- archive_fhir_reconciliation aggregates the delete flag with MIN (not MAX).
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


# --- archive_fhir_reconciliation: MIN aggregation --------------------------------------


def _recon_spec(**overrides):
    params = {
        "source_table": "arc.ifdb_encounter",
        "source_key": "OBJECT_ID",
        "target_key": "identifier_value",
        "source_deleted": "DELETED",
        "target_deleted": "dw_is_deleted",
    }
    params.update(overrides)
    return CheckSpecification("archive_fhir_reconciliation", Severity.ERROR, Cadence.WEEKLY, params)


def test_archive_fhir_reconciliation_aggregates_delete_flag_with_min():
    cursor = MagicMock()
    cursor.fetchone.return_value = (100, 100, 0, 0, 0)  # src, tgt, missing, extra, mismatch
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["archive_fhir_reconciliation"](
        MagicMock(), _fhir_subject(), _recon_spec(), resources
    )
    assert outcome.passed and outcome.violations == 0
    sql = _all_sql(cursor)
    assert "MIN(" in sql and "MAX(" not in sql


def test_archive_fhir_reconciliation_capture_gated_on_flag():
    # capture_replay=True + a gap -> capture is invoked.
    cursor = MagicMock()
    cursor.fetchone.return_value = (100, 100, 5, 0, 0)  # missing=5
    cursor.fetchall.return_value = [("missing_in_target", "k1", None)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    with patch("dqa.remediation.capture.capture_archive_fhir_reconciliation_gaps") as cap:
        CHECK_TYPES["archive_fhir_reconciliation"](
            MagicMock(), _fhir_subject(), _recon_spec(capture_replay=True), resources
        )
        assert cap.called

    # capture_replay not set -> never invoked, even with a gap.
    cursor2 = MagicMock()
    cursor2.fetchone.return_value = (100, 100, 5, 0, 0)
    cursor2.fetchall.return_value = [("missing_in_target", "k1", None)]
    resources2 = SimpleNamespace(mssql_target_staging=_make_resource(cursor2))
    with patch("dqa.remediation.capture.capture_archive_fhir_reconciliation_gaps") as cap2:
        CHECK_TYPES["archive_fhir_reconciliation"](
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


# --- capture_archive_fhir_reconciliation_gaps: guard + MIN ------------------------------


def test_capture_noop_when_source_key_not_filter_column():
    from dqa.remediation.capture import capture_archive_fhir_reconciliation_gaps

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
    capture_archive_fhir_reconciliation_gaps(
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
    from dqa.remediation.capture import capture_archive_fhir_reconciliation_gaps

    cursor = MagicMock()
    cursor.fetchone.return_value = (5,)  # distinct-key count, under the guard
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    capture_archive_fhir_reconciliation_gaps(
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


class TestBoundColumnResolvesInTheSource:
    """The reconciliation's bound column must exist where it is queried.

    ``source_archive_reconciliation`` builds «SELECT ... FROM <source_table>» and puts the
    extract's ``upper_bound_column`` in a CASE over that FROM, with its alias
    stripped. If the extract qualifies the column because it lives on a JOINED
    table, the column is not in the source and Oracle answers ORA-00904 — and the
    whole weekly archive reconciliation fails. The fix is a derived table that
    projects the bound column next to the merge keys.
    """

    def test_a_qualified_bound_needs_a_derived_source(self) -> None:
        from dqa.config.arc_subjects import _ARC

        offenders: list[str] = []
        for spec in _ARC:
            name, _table, source, _archive_cfg, extract_cfg = spec[:5]
            bound = getattr(extract_cfg, "upper_bound_column", None)
            if not bound or "." not in bound:
                continue  # unqualified: the column sits on the source table itself
            derived = source.lstrip().startswith("(SELECT")
            projected = bound.split(".")[-1] in source
            if not (derived and projected):
                offenders.append(f"{name}: extract bounds on {bound!r}, source is {source!r}")
        assert not offenders, "bound column cannot resolve in the source: " + "; ".join(offenders)

    def test_a_derived_source_carries_the_merge_keys(self) -> None:
        """Otherwise staging the source keys fails."""
        from dqa.config.arc_subjects import _ARC

        offenders: list[str] = []
        for spec in _ARC:
            name, _table, source, archive_cfg = spec[:4]
            if not source.lstrip().startswith("(SELECT"):
                continue
            for key in archive_cfg.merge_keys:
                if key not in source:
                    offenders.append(f"{name}: key {key} is not projected")
        assert not offenders, "derived source lacks keys: " + "; ".join(offenders)


class TestMissingOnly:
    """A bounded source against an unbounded archive makes the extra direction noise."""

    @staticmethod
    def _run(params: dict, fetchone: list) -> list[str]:
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from dqa.checks.source_archive_reconciliation import source_archive_reconciliation

        executed: list[str] = []
        cur = MagicMock()
        cur.execute.side_effect = lambda sql, *a, **k: executed.append(sql)
        cur.fetchone.side_effect = fetchone
        conn = MagicMock()
        conn.cursor.return_value = cur
        scur = MagicMock()
        # One source key plus its within_bound flag, as the staging query returns them.
        scur.fetchmany.side_effect = [[("1", 1)], []]
        sconn = MagicMock()
        sconn.cursor.return_value = scur
        resources = SimpleNamespace(mssql_target_staging=MagicMock(), ifdb_oracle=MagicMock())
        resources.mssql_target_staging.get_connection.return_value.__enter__.return_value = conn
        resources.ifdb_oracle.get_connection.return_value.__enter__.return_value = sconn
        subject = SimpleNamespace(
            name="arc_x", table="arc.ifdb_x", resource_key="mssql_target_staging", replay=None
        )
        spec = SimpleNamespace(
            params={"source_table": "IFDB.X", "key_columns": ["OBJEKT_ID"], **params}
        )
        source_archive_reconciliation(MagicMock(), subject, spec, resources)
        return executed

    def test_skips_the_full_archive_anti_join(self) -> None:
        # Two counts only: source keys, then missing keys.
        executed = self._run({"missing_only": True}, [(1, 1), (0,)])
        assert not any("LEFT JOIN" in sql for sql in executed)

    def test_without_it_the_extra_direction_still_runs(self) -> None:
        executed = self._run({}, [(1, 1), (0,), (5, 0, 0)])
        assert any("LEFT JOIN" in sql for sql in executed)
