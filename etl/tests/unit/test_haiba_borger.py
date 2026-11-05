# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for HAIBA's Patient stubs, written through patient_service, and the dw_ek_Borger lookup."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.FHIR.Patient import patient_service
from DSA.FHIR.Patient.cpr_stub_asset import TIER_STATEMENTS
from DSA.FHIR.Patient.patient_builder import build
from DSA.FHIR.Patient.patient_contributions import stub_contribution
from DSA.FHIR.Patient.Patient_HAIBA import CPR_SQL, post_missing_patients
from DSA.FHIR.Patient.patient_identity import IdentityResolution, Person
from DSA.FHIR.Patient.patient_ownership import SOURCE_HAIBA
from DSA.SQL.dm.haiba import DM_TABLES, build_rebuild_sql

pytestmark = pytest.mark.unit


class _FakeFhirDatabase:
    """Aidbox stand-in: records what was written and gives new resources an id."""

    def __init__(self, fejl: int = 0):
        self.posted: list[str] = []
        self._fejl = fejl
        self.mark_stale = MagicMock()
        self.safe_clear_identifier_caches_for_resources = MagicMock()

    def batch_upsert_resources(self, resources, resource_type, **kwargs):
        for n, r in enumerate(resources):
            if not r.id:
                r.id = f"ny-{n}"
        self.posted = [r.identifier[0].value for r in resources]
        return len(resources) - self._fejl, self._fejl, ["driver text with 0101011234"] * self._fejl


def _kendt(monkeypatch, known: dict[str, str]):
    """Make identity resolution know exactly these CPRs, without EPJ rows or register."""

    @contextmanager
    def _uden_register(mssql):
        yield SimpleNamespace(available=False, members=lambda c: set(), persons=lambda p: {})

    monkeypatch.setattr(patient_service, "open_register", _uden_register)

    def _resolve(request, mssql, fhir_database, rows=(), members=None):
        persons = [
            Person(key=c, cprs=(c,), epj_ids=(), rows=(), fhir_id=known[c])
            for c in sorted(request.cprs)
            if c in known
        ]
        return IdentityResolution(
            persons=persons,
            by_ifdb_id={},
            by_cpr={p.key: p for p in persons},
            unknown_ifdb_ids=(),
            unknown_cprs=tuple(sorted(c for c in request.cprs if c not in known)),
        )

    monkeypatch.setattr(patient_service, "resolve", _resolve)
    monkeypatch.setattr(
        patient_service, "prepare_resources_for_upsert", lambda **kw: kw["resources"]
    )


def test_stub_patient_carries_only_cpr_and_its_origin():
    """HAIBA delivers a CPR and nothing else. Asserting a birth date or gender
    we do not have would be worse than an empty record. The builder sets provenance."""
    p = build([stub_contribution("0101010101", SOURCE_HAIBA)])
    d = p.dict()
    assert d["identifier"][0]["system"] == "urn:dk:cpr"
    assert d["identifier"][0]["value"] == "0101010101"
    assert [t["code"] for t in d["meta"]["tag"]] == [SOURCE_HAIBA]
    for absent in ("name", "gender", "birthDate", "address", "deceasedBoolean"):
        assert not d.get(absent)


def test_stub_patients_are_created_regardless_of_cpr_format():
    """Decided 2026-08-19: no format validation, no rejection path. A malformed
    CPR still gets a resource."""
    cprs = ["0101010101", "123", "99999999999", "01010X0101"]
    built = [stub_contribution(c, SOURCE_HAIBA).resource for c in cprs]
    assert [p.identifier[0].value for p in built] == cprs


def test_empty_cpr_cannot_become_an_identifier():
    """FHIR rejects an empty identifier value, so an empty CPR would raise
    rather than silently produce a patient with no identity. CPR_SQL filters
    them out upstream (cprnr <> ''), so this is a guard on that contract."""
    from pydantic.v1.error_wrappers import ValidationError

    with pytest.raises(ValidationError):
        stub_contribution("", SOURCE_HAIBA)
    assert "cprnr <> ''" in CPR_SQL


def test_a_cpr_aidbox_already_holds_is_never_posted_over(monkeypatch):
    """The upsert replaces the whole resource, so posting a CPR-only stub onto an
    existing patient's id would erase its name, birth date and addresses. Tier C
    comes from fhir.Patient, which lags Aidbox, so this case is reachable on any
    rerun before sof_patient_dfm has caught up."""
    _kendt(monkeypatch, {"0101010101": "existing-uuid"})
    fake = _FakeFhirDatabase()
    posted, failed, skipped = post_missing_patients(
        dg.build_asset_context(), MagicMock(), fake, ["0101010101", "0202020202"]
    )
    assert fake.posted == ["0202020202"]
    assert (posted, failed, skipped) == (1, 0, 1)


def test_a_rerun_posts_nothing_once_aidbox_holds_every_cpr(monkeypatch):
    """Second pass of the capped flow the config recommends: the CPRs the first
    run created must not become a second set of resources."""
    _kendt(monkeypatch, {"0101010101": "uuid-a", "0202020202": "uuid-b"})
    fake = _FakeFhirDatabase()
    posted, failed, skipped = post_missing_patients(
        dg.build_asset_context(), MagicMock(), fake, ["0101010101", "0202020202"]
    )
    assert fake.posted == []
    assert (posted, failed, skipped) == (0, 0, 2)


def test_a_failed_upsert_is_a_hard_error_that_carries_only_the_count(monkeypatch):
    """failed_ids carries driver error text of unbounded provenance, and the
    only data in these resources IS the CPR — a failure may put nothing but
    the count in the message."""
    cpr = "0101011234"
    _kendt(monkeypatch, {})
    context = MagicMock()
    with pytest.raises(dg.Failure) as fejl:
        post_missing_patients(context, MagicMock(), _FakeFhirDatabase(fejl=1), [cpr])
    assert cpr not in str(fejl.value)
    assert "1 of 1" in str(fejl.value)
    logged = " | ".join(
        " ".join(str(arg) for arg in logged_call.args)
        for method in ("debug", "info", "warning", "error", "exception", "critical")
        for logged_call in getattr(context.log, method).call_args_list
    )
    assert cpr not in logged


def test_tier_query_treats_a_former_cpr_as_known():
    """A person already in fhir.Patient under previous_ssn must not get a second
    resource — that would duplicate them under two CPRs."""
    sql = "\n".join(TIER_STATEMENTS)
    assert "previous_ssn" in sql
    assert "patient_ssn" in sql
    # Tier A is decided by ref.PatientToBorger, not by fhir.Patient.
    assert "[ref].[PatientToBorger]" in sql
    assert "dw_is_deleted = 0" in sql


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_only_case_tables_resolve_a_borger(t):
    """The agg tables carry no person data at all; joining them to a borger
    lookup would be meaningless work on 8M rows."""
    sql = build_rebuild_sql(t)

    if t.cpr_column:
        assert "#borger" in sql
        assert "[dw_ek_Borger]" in sql
    else:
        assert "#borger" not in sql
        assert "[dw_ek_Borger]" not in sql


def test_borger_lookup_collapses_duplicate_cprs():
    """cpr is NOT unique in ref.PatientToBorger — one CPR currently maps to two
    patient_ids. Without MIN the join would multiply the case row and break the
    dm primary key."""
    sql = build_rebuild_sql(DM_TABLES[2])

    assert "MIN([dw_ek_Borger])" in sql
    assert "GROUP BY [cpr]" in sql
    assert "CREATE UNIQUE CLUSTERED INDEX [IX_borger_cpr]" in sql
    # Same conversion trap as dim.Organisation: ref.cpr is varchar, the source
    # is nvarchar. The cast belongs in the temp build, not the predicate.
    assert "CAST([cpr] AS NVARCHAR(10))" in sql
    assert "b.[cpr] = a.[cprnr]" in sql


def test_unresolved_borger_falls_to_the_sentinel():
    """A case whose CPR is not yet in ref must still land in dm — flagged, not
    dropped."""
    sql = build_rebuild_sql(DM_TABLES[3])
    assert "COALESCE(b.[dw_ek_Borger], -1)" in sql
