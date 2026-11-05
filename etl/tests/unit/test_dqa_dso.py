# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DSO-vs-DSA reconciliation subsystem (dqa/dso.py) and the
``dso_reconciliation`` check logic. Derivation/build are pure; the check is
exercised against a mock cursor (no DB).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from dqa.dso import _NOT_SYNCED_TO_DSO, _dso_subjects, build_dso_all
from dqa.model import Track
from dqa.registry import CHECK_TYPES

pytestmark = pytest.mark.unit

_EXPECTED_DOMAINS = {
    "fhir",
    "erp",
    "shared",
    "security",
    "codesystem",
    "conceptmap",
    "haiba",
    "drg",
    "udredningsret",
}


def _make_resource(cursor):
    resource = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    resource.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    resource.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    return resource


def test_dso_subjects_derived_from_all_sync_configs():
    from DSO.load.sync_configs import ALL_SYNC_CONFIGS

    subs = _dso_subjects()
    assert _NOT_SYNCED_TO_DSO <= ALL_SYNC_CONFIGS.keys()
    assert {s.name for s, _ in subs} == {
        f"dso_{key}" for key in ALL_SYNC_CONFIGS.keys() - _NOT_SYNCED_TO_DSO
    }

    for subject, domain in subs:
        assert domain in _EXPECTED_DOMAINS
        assert subject.resource_key == "mssql_source_dso"
        assert subject.results_resource_key == "mssql_target_staging"
        assert subject.name.startswith("dso_")
        (spec,) = subject.checks
        assert spec.check_type == "dso_reconciliation"
        assert isinstance(spec.params["pk"], list) and spec.params["pk"]
        # fhir domain -> FHIR track; everything else -> DWH track.
        assert subject.track is (Track.FHIR if domain == "fhir" else Track.DWH)


def test_dso_composite_pk_plumbed():
    subs = {s.name: s for s, _ in _dso_subjects()}
    el = subs["dso_fhir_encounter_location"]
    assert el.checks[0].params["pk"] == [
        "encounter_id",
        "location_id",
        "location_period_start",
    ]


def test_build_dso_all_per_domain_anchors_and_jobs():
    assets, checks, jobs = build_dso_all()
    anchor_keys = {a.key.to_user_string() for a in assets}
    job_names = {j.name for j in jobs}
    assert anchor_keys == {f"dqa_dso_{d}" for d in _EXPECTED_DOMAINS}
    assert job_names == {f"dqa_dso_{d}_job" for d in _EXPECTED_DOMAINS}
    # one check per synced table
    assert len(checks) == len(_dso_subjects())


def _run_dso(subject_table, pk, *, fetchall_first, fetchone_seq, link="STAGING_LINK"):
    """Invoke dso_reconciliation with a canned cursor. Returns (outcome, cursor)."""
    from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity

    cursor = MagicMock()
    # fetchall #1 is _column_types (rows of (COLUMN_NAME, DATA_TYPE)); later calls
    # (the examples UNION) reuse the list.
    cursor.fetchall.side_effect = fetchall_first
    cursor.fetchone.side_effect = fetchone_seq
    resources = SimpleNamespace(mssql_source_dso=_make_resource(cursor))
    subject = DqaSubject(
        name="dso_x",
        track=Track.FHIR,
        table=subject_table,
        resource_key="mssql_source_dso",
        results_resource_key="mssql_target_staging",
        identity_column=pk[0],
    )
    spec = CheckSpecification(
        "dso_reconciliation",
        Severity.ERROR,
        Cadence.DSO,
        {"pk": pk, "staging_db": "staging", "linked_server": link},
    )
    outcome = CHECK_TYPES["dso_reconciliation"](MagicMock(), subject, spec, resources)
    return outcome, cursor


def _all_sql(cursor) -> str:
    return "\n".join(str(c.args[0]) for c in cursor.execute.call_args_list)


def test_dso_reconciliation_composite_join_and_linked_ref():
    # column_types returns the composite PK (all non-char) + dw_is_deleted;
    # checked=10, then ONE row (missing, extra, mismatch).
    outcome, cursor = _run_dso(
        "fhir.Encounter_Location",
        ["encounter_id", "location_id", "location_period_start"],
        fetchall_first=[
            [
                ("encounter_id", "int"),
                ("location_id", "int"),
                ("location_period_start", "datetime"),
                ("dw_is_deleted", "bit"),
            ]
        ],
        fetchone_seq=[(10,), (0, 0, 0)],
    )
    assert outcome.passed and outcome.violations == 0
    assert outcome.extra["delete_status_mismatch"] == 0
    sql = _all_sql(cursor)
    # composite PK -> join on all three columns; none are char, so no COLLATE
    assert "d.[encounter_id] = s.[encounter_id]" in sql
    assert "d.[location_id] = s.[location_id]" in sql
    assert "d.[location_period_start] = s.[location_period_start]" in sql
    # non-char PK columns must NOT be wrapped in COLLATE (illegal on int/datetime)
    assert "encounter_id] COLLATE" not in sql
    assert "location_id] COLLATE" not in sql
    assert "location_period_start] COLLATE" not in sql
    # all three counts come from ONE pass over the linked server
    assert "FULL OUTER JOIN" in sql
    # DSA reached over the linked server (4-part name)
    assert "[STAGING_LINK].[staging].[fhir].[Encounter_Location]" in sql


def test_dso_reconciliation_key_set_only_when_no_delete_column():
    # detect returns nothing -> no delete-status comparison (the mismatch SUM in
    # the single-pass diff is a constant 0).
    outcome, cursor = _run_dso(
        "fhir.Organization",
        ["organization_id"],
        fetchall_first=[[("organization_id", "int")]],  # no delete candidate present
        fetchone_seq=[(98479,), (0, 0, 0)],
    )
    assert outcome.passed
    assert "delete_status_mismatch" not in outcome.extra
    sql = _all_sql(cursor)
    assert "dso_deleted" not in sql  # no mismatch branch built


def test_dso_reconciliation_direct_ref_when_no_linked_server():
    outcome, cursor = _run_dso(
        "fhir.Patient",
        ["patient_id"],
        fetchall_first=[[("patient_id", "int")]],
        fetchone_seq=[(5,), (0, 0, 0)],
        link="",
    )
    sql = _all_sql(cursor)
    assert "[staging].[fhir].[Patient]" in sql
    assert "[STAGING_LINK]" not in sql


def test_dso_reconciliation_counts_violations():
    outcome, _ = _run_dso(
        "fhir.Encounter",
        ["encounter_id"],
        # column_types (PK + delete col), then [] for the examples UNION fetchall
        fetchall_first=[[("encounter_id", "int"), ("dw_is_deleted", "bit")], []],
        fetchone_seq=[(844,), (2, 1, 3)],  # checked, then (missing, extra, mismatch)
    )
    assert not outcome.passed
    assert outcome.violations == 2 + 1 + 3
    assert outcome.extra["missing_in_dso"] == 2
    assert outcome.extra["extra_in_dso"] == 1
    assert outcome.extra["delete_status_mismatch"] == 3


def test_dso_reconciliation_collates_char_pk_columns_only():
    # A composite PK mixing a char column (ssn) and an int column (seq): the cross-
    # server join must COLLATE DATABASE_DEFAULT the char side (to survive a DSO/DSA
    # collation mismatch on QA/Prod) but leave the int side alone (COLLATE is illegal
    # on non-character types). checked=10, missing=1 -> the examples UNION also runs.
    outcome, cursor = _run_dso(
        "fhir.SomeCharKeyed",
        ["ssn", "seq"],
        fetchall_first=[
            [("ssn", "varchar"), ("seq", "int"), ("dw_is_deleted", "bit")],
            [],  # examples UNION fetchall
        ],
        fetchone_seq=[(10,), (1, 0, 0)],
    )
    assert not outcome.passed and outcome.violations == 1
    sql = _all_sql(cursor)
    # char column collated on BOTH sides of the join equality
    assert "d.[ssn] COLLATE DATABASE_DEFAULT = s.[ssn] COLLATE DATABASE_DEFAULT" in sql
    # int column joined raw -- never wrapped in COLLATE
    assert "d.[seq] = s.[seq]" in sql
    assert "seq] COLLATE" not in sql
    # examples UNION key expression carries COLLATE so the branches reconcile
    assert "CAST(s.[ssn] AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT" in sql
    # delete-flag comparison cast is collated too (defensive, always legal on NVARCHAR)
    assert "AS NVARCHAR(10)) COLLATE DATABASE_DEFAULT = '1'" in sql
    # DATABASE_DEFAULT (portable) -- never a hard-coded collation name
    assert "Danish_Norwegian" not in sql and "SQL_Latin1" not in sql
