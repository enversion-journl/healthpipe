# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for HAIBA's Patient stubs and the dw_ek_Borger lookup."""

import dagster as dg
import pytest
from DSA.FHIR.Patient.Patient_HAIBA import (
    META_SOURCE,
    TIER_SQL,
    build_stub_patients,
    post_missing_patients,
)
from DSA.SQL.dm.haiba import DM_TABLES, build_rebuild_sql

pytestmark = pytest.mark.unit


class _FakeFhirDatabase:
    """Aidbox stand-in: resolves ids for the CPRs it already holds."""

    def __init__(self, known: dict[str, str]):
        self.known = known
        self.posted: list[str] = []

    def resolve_resource_ids(self, resources, resource_type, preferred_systems, **kwargs):
        return {i: self.known.get(r.identifier[0].value) for i, r in enumerate(resources)}

    def batch_upsert_resources(self, resources, resource_type, **kwargs):
        self.posted = [r.identifier[0].value for r in resources]
        return len(resources), 0, []

    def is_stale(self, resource_type):
        return False


def test_stub_patient_carries_only_cpr_and_its_origin():
    """HAIBA delivers a CPR and nothing else. Asserting a birth date or gender
    we do not have would be worse than an empty record."""
    (p,) = build_stub_patients(["0101010101"])
    d = p.dict()

    assert d["meta"]["source"] == META_SOURCE
    assert d["identifier"][0]["system"] == "urn:dk:cpr"
    assert d["identifier"][0]["value"] == "0101010101"
    for absent in ("name", "gender", "birthDate", "address", "deceasedBoolean"):
        assert not d.get(absent)


def test_stub_patients_are_created_regardless_of_cpr_format():
    """Decided 2026-08-19: no format validation, no rejection path. A malformed
    CPR still gets a resource."""
    cprs = ["0101010101", "123", "99999999999", "01010X0101"]
    built = build_stub_patients(cprs)

    assert len(built) == len(cprs)
    assert [p.identifier[0].value for p in built] == cprs


def test_empty_cpr_cannot_become_an_identifier():
    """FHIR rejects an empty identifier value, so an empty CPR would raise
    rather than silently produce a patient with no identity. TIER_SQL filters
    them out upstream (cprnr <> ''), so this is a guard on that contract."""
    from pydantic.v1.error_wrappers import ValidationError

    with pytest.raises(ValidationError):
        build_stub_patients([""])

    assert "cprnr <> ''" in TIER_SQL


def test_a_cpr_aidbox_already_holds_is_never_posted_over():
    """The upsert replaces the whole resource, so posting a CPR-only stub onto an
    existing patient's id would erase its name, birth date and addresses. Tier C
    comes from fhir.Patient, which lags Aidbox, so this case is reachable on any
    rerun before sof_patient_dfm has caught up."""
    fake = _FakeFhirDatabase(known={"0101010101": "existing-uuid"})

    posted, failed, skipped = post_missing_patients(
        dg.build_asset_context(), fake, ["0101010101", "0202020202"]
    )

    assert fake.posted == ["0202020202"]
    assert (posted, failed, skipped) == (1, 0, 1)


def test_a_rerun_posts_nothing_once_aidbox_holds_every_cpr():
    """Second pass of the capped flow the config recommends: the CPRs the first
    run created must not become a second set of resources."""
    fake = _FakeFhirDatabase(known={"0101010101": "uuid-a", "0202020202": "uuid-b"})

    posted, failed, skipped = post_missing_patients(
        dg.build_asset_context(), fake, ["0101010101", "0202020202"]
    )

    assert fake.posted == []
    assert (posted, failed, skipped) == (0, 0, 2)


def test_tier_query_treats_a_former_cpr_as_known():
    """A person already in fhir.Patient under previous_ssn must not get a second
    resource — that would duplicate them under two CPRs."""
    assert "previous_ssn" in TIER_SQL
    assert "patient_ssn" in TIER_SQL
    # Tier A is decided by ref.PatientToBorger, not by fhir.Patient.
    assert "ref.PatientToBorger" in TIER_SQL
    assert "dw_is_deleted = 0" in TIER_SQL


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
