# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The write point without a database: who is built, from what, and what the caller gets back."""

from contextlib import contextmanager
from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.FHIR.Patient import patient_service as modul
from DSA.FHIR.Patient.field_ownership import SOURCE_CPR, SOURCE_HAIBA, SOURCE_IFDB
from DSA.FHIR.Patient.patient_builder import NotOwner
from DSA.FHIR.Patient.patient_contributions import (
    USE_OLD,
    USE_SECONDARY,
    contribution_from_bom_patientinfo,
    register_identity_contribution,
    stub_contribution,
    with_additional_epj_ids,
)
from DSA.FHIR.Patient.patient_identity import IdentityResolution, Person
from fhir.resources.R4B.identifier import Identifier
from utils.fhir.identifier_systems import (
    SOURCE_PATIENT_BUILDER,
    SYSTEM_BOM_IFDB_PATIENTINFO_OID,
    SYSTEM_CPR,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOURCE_TAG,
)

pytestmark = pytest.mark.unit

_stub_sources_on_resources = modul.stub_sources_on_resources


@pytest.fixture(autouse=True)
def _ingen_stub_kilder(monkeypatch):
    """The fake FHIR databases have no connection; the stub sources are given per test."""
    monkeypatch.setattr(modul, "stub_sources_on_resources", lambda fhir_database, persons: {})


PNR_A = "0000000001"
PNR_B = "0000000002"


def _row(patient_id: str, cpr: str, slettet: str = "0") -> dict:
    return {
        "patient_id": patient_id,
        "PATIENT_ID": patient_id,
        "identifier_cpr": cpr,
        "CPR": cpr,
        "SLETTET": slettet,
        "name_given": "Test",
        "name_family": "Testesen",
        "gender_code": "K",
        "birth_date": "1980-01-01",
        "person_status_code": "PAS_ks.ur.CPRStatus.01",
        "is_deleted": slettet,
    }


class _FalskFhir:
    """Writes nothing; gives new resources an id like the real upsert."""

    def __init__(self, fejl: int = 0):
        self.skrevet: list = []
        self._fejl = fejl
        self.mark_stale = MagicMock()
        self.safe_clear_identifier_caches_for_resources = MagicMock()

    def batch_upsert_resources(self, resources, resource_type, **_):
        assert resource_type == "Patient"
        for n, r in enumerate(resources):
            if not r.id:
                r.id = f"ny-{n}"
        self.skrevet.extend(resources)
        return len(resources) - self._fejl, self._fejl, ["x"] * self._fejl


class _IntetRegister:
    """The register does not exist in the test environment: no members, no contributions."""

    available = False

    def members(self, cprs):
        return set()

    def persons(self, persons):
        return {}


@contextmanager
def _uden_register(mssql):
    yield _IntetRegister()


def _opsaet(monkeypatch, persons, unknown_cprs=(), unknown_ifdb=()):
    """Make the identity resolution answer as given; make the pre-write id lookup transparent."""
    res = IdentityResolution(
        persons=list(persons),
        by_ifdb_id={e: p for p in persons for e in p.epj_ids},
        by_cpr={c: p for p in persons for c in p.cprs},
        unknown_ifdb_ids=tuple(unknown_ifdb),
        unknown_cprs=tuple(unknown_cprs),
    )
    monkeypatch.setattr(
        modul, "resolve", lambda request, mssql, fhir_database, rows=(), members=None: res
    )
    monkeypatch.setattr(modul, "open_register", _uden_register)
    monkeypatch.setattr(modul, "prepare_resources_for_upsert", lambda **kw: kw["resources"])


def _ids(patient, system):
    return [(i.value, i.use) for i in patient.identifier or [] if i.system == system]


# ------------------------------------------------------------------ the contributions


def test_stub_contribution_carries_only_personal_number_and_source():
    c = stub_contribution(PNR_A, SOURCE_HAIBA)
    assert c.kilde == SOURCE_HAIBA
    assert _ids(c.patient, SYSTEM_CPR) == [(PNR_A, None)]
    assert c.patient.name is None and c.patient.meta is None and c.patient.id is None


def test_stub_requires_a_source_that_owns_the_personal_number():
    with pytest.raises(NotOwner):
        stub_contribution(PNR_A, "BOM")


def test_bom_contribution_is_epjs_and_carries_oid_name_and_gender():
    c = contribution_from_bom_patientinfo(
        {
            "CPRNR": PNR_A,
            "PATIENTINFOOID": 12345,
            "FORNAVN": "Test",
            "EFTERNAVN": "Testesen",
            "KOEN": "K",
        }
    )
    assert c.kilde == SOURCE_IFDB
    assert _ids(c.patient, SYSTEM_CPR) == [(PNR_A, None)]
    assert [v for v, _ in _ids(c.patient, SYSTEM_BOM_IFDB_PATIENTINFO_OID)] == ["12345"]
    assert c.patient.name[0].use == "official" and c.patient.gender == "female"
    assert contribution_from_bom_patientinfo({"CPRNR": None}) is None


def test_other_epj_ids_are_added_as_secondary_or_old():
    c = stub_contribution(PNR_A, SOURCE_IFDB)
    c.patient.identifier.append(Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="P1"))
    c2 = with_additional_epj_ids(c, ["P2", "P3", "P1"], deleted_ids=["P3"])
    assert _ids(c2.patient, SYSTEM_IFDB_PATIENT_ID) == [
        ("P1", None),
        ("P2", USE_SECONDARY),
        ("P3", USE_OLD),
    ]


def test_only_epj_may_get_epj_ids():
    with pytest.raises(NotOwner):
        with_additional_epj_ids(stub_contribution(PNR_A, SOURCE_HAIBA), ["P2"])


# ------------------------------------------------------------------ the write point


def test_missing_epj_patient_is_built_with_all_its_ids_and_builder_provenance(monkeypatch):
    person = Person(
        key=PNR_A,
        cprs=(PNR_A,),
        epj_ids=("P1", "P2"),
        rows=(_row("P1", PNR_A), _row("P2", PNR_A, slettet="1")),
        fhir_id=None,
    )
    _opsaet(monkeypatch, [person])
    fhir = _FalskFhir()

    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, ifdb_patient_ids=["P2"]
    )

    assert res.written == 1 and res.existing == 0
    (skrevet,) = fhir.skrevet
    assert _ids(skrevet, SYSTEM_IFDB_PATIENT_ID) == [("P1", None), ("P2", USE_OLD)]
    assert skrevet.meta.source == SOURCE_PATIENT_BUILDER
    assert [t.code for t in skrevet.meta.tag] == [SOURCE_IFDB]
    assert skrevet.name[0].use == "official"
    # both identities get the new id
    assert res.by_ifdb_id == {"P1": "ny-0", "P2": "ny-0"}
    assert res.by_cpr == {PNR_A: "ny-0"}


def test_existing_person_is_untouched_but_gets_its_id_back(monkeypatch):
    person = Person(
        key=PNR_A,
        cprs=(PNR_A,),
        epj_ids=("P1",),
        rows=(_row("P1", PNR_A),),
        fhir_id="f-1",
        known_epj_ids=("P1",),
    )
    _opsaet(monkeypatch, [person])
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, ifdb_patient_ids=["P1"]
    )
    assert fhir.skrevet == [] and res.written == 0 and res.existing == 1
    assert res.by_ifdb_id == {"P1": "f-1"}


def test_existing_resource_without_the_duplicates_id_is_rebuilt_with_both(monkeypatch):
    """A resource missing one of the person's EPJ ids is rebuilt in full with both."""
    person = Person(
        key=PNR_A,
        cprs=(PNR_A,),
        epj_ids=("P1", "P2"),
        rows=(_row("P1", PNR_A), _row("P2", PNR_A)),
        fhir_id="f-1",
        known_epj_ids=("P1",),
    )
    _opsaet(monkeypatch, [person])
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, ifdb_patient_ids=["P2"]
    )
    (skrevet,) = fhir.skrevet
    assert skrevet.id == "f-1"  # same resource, not a new one
    assert _ids(skrevet, SYSTEM_IFDB_PATIENT_ID) == [("P1", None), ("P2", USE_SECONDARY)]
    assert res.written == 1 and res.rebuilt == 1 and res.existing == 0
    assert res.by_ifdb_id == {"P1": "f-1", "P2": "f-1"}


def test_error_message_carries_only_counts(monkeypatch):
    import dagster as dg

    person = Person(
        key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id=None
    )
    _opsaet(monkeypatch, [person])
    with pytest.raises(dg.Failure) as fejl:
        modul.ensure_patients(
            MagicMock(), mssql=object(), fhir_database=_FalskFhir(fejl=1), ifdb_patient_ids=["P1"]
        )
    assert PNR_A not in str(fejl.value) and "x" not in str(fejl.value).split("Patient")[0]
    assert "1 of 1" in str(fejl.value)


def test_unknown_personal_number_becomes_stub_with_the_sources_tag(monkeypatch):
    _opsaet(monkeypatch, [], unknown_cprs=[PNR_B])
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, cprs=[PNR_B], stub_source=SOURCE_HAIBA
    )
    (skrevet,) = fhir.skrevet
    assert _ids(skrevet, SYSTEM_CPR) == [(PNR_B, None)]
    assert skrevet.meta.source == SOURCE_PATIENT_BUILDER
    assert [t.code for t in skrevet.meta.tag] == [SOURCE_HAIBA]
    assert res.by_cpr == {PNR_B: "ny-0"} and res.unresolved_cprs == ()


def test_unknown_personal_number_with_bom_row_is_built_as_epj_contribution(monkeypatch):
    _opsaet(monkeypatch, [], unknown_cprs=[PNR_B])
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(),
        mssql=object(),
        fhir_database=fhir,
        cprs=[PNR_B],
        bom_rows_by_cpr={
            PNR_B: {"CPRNR": PNR_B, "PATIENTINFOOID": 7, "FORNAVN": "Bo", "KOEN": "M"}
        },
    )
    (skrevet,) = fhir.skrevet
    assert [t.code for t in skrevet.meta.tag] == [SOURCE_IFDB]
    assert [v for v, _ in _ids(skrevet, SYSTEM_BOM_IFDB_PATIENTINFO_OID)] == ["7"]
    assert res.by_cpr == {PNR_B: "ny-0"}


def test_unknown_personal_number_without_basis_becomes_nothing(monkeypatch):
    _opsaet(monkeypatch, [], unknown_cprs=[PNR_B], unknown_ifdb=["P404"])
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, cprs=[PNR_B], ifdb_patient_ids=["P404"]
    )
    assert fhir.skrevet == []
    assert res.unresolved_cprs == (PNR_B,) and res.unresolved_ifdb_ids == ("P404",)


def test_known_id_from_the_resolution_is_set_on_the_resource_before_writing(monkeypatch):
    """A person with no resource in ① gets a new id, but build_person sets the known one if any."""
    person = Person(
        key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id="f-9"
    )
    patient = modul.build_person(person, MagicMock())
    assert patient.id == "f-9"


def test_a_failed_write_is_a_hard_failure(monkeypatch):
    import dagster as dg

    person = Person(
        key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id=None
    )
    _opsaet(monkeypatch, [person])
    with pytest.raises(dg.Failure, match="checkpoint NOT advanced"):
        modul.ensure_patients(
            MagicMock(), mssql=object(), fhir_database=_FalskFhir(fejl=1), ifdb_patient_ids=["P1"]
        )


def test_epj_alone_without_duplicates_gives_exactly_the_transforms_resource(monkeypatch):
    """Via the write point, the EPJ contribution alone equals the transform's output plus meta."""
    from DSA.FHIR.Patient.patient_ifdb_contribution import _transform_patient_rows_to_fhir

    row = _row("P1", PNR_A)
    person = Person(key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(row,), fhir_id=None)
    bygget = modul.build_person(person, MagicMock())
    direkte = _transform_patient_rows_to_fhir([row], MagicMock())[0]
    uden_meta = bygget.__class__(**{**bygget.dict(exclude_none=True), "meta": None})
    assert uden_meta.json(exclude_none=True) == direkte.json(exclude_none=True)


# ------------------------------------------------------------------ the register's identity


def test_register_identity_contribution_only_when_there_are_previous_numbers():
    assert register_identity_contribution(PNR_A, []) is None
    assert register_identity_contribution(PNR_A, [PNR_A, " "]) is None
    reg = register_identity_contribution(PNR_A, [PNR_B])
    assert reg.kilde == SOURCE_CPR
    assert [(i.value, i.use) for i in reg.patient.identifier] == [(PNR_A, None), (PNR_B, USE_OLD)]


def test_previous_number_is_on_the_resource_as_old(monkeypatch):
    """A reference on the old number must resolve on the resource — not only in this run."""
    person = Person(
        key=PNR_A, cprs=(PNR_A, PNR_B), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id=None
    )
    _opsaet(monkeypatch, [person])
    fhir = _FalskFhir()
    modul.ensure_patients(MagicMock(), mssql=object(), fhir_database=fhir, ifdb_patient_ids=["P1"])
    (skrevet,) = fhir.skrevet
    assert _ids(skrevet, SYSTEM_CPR) == [(PNR_A, None), (PNR_B, USE_OLD)]
    assert sorted(t.code for t in skrevet.meta.tag) == sorted([SOURCE_IFDB, SOURCE_CPR])


# ------------------------------------------------------------------ the stub guard


def test_stub_is_not_written_if_the_person_has_appeared_meanwhile(monkeypatch):
    """The lookup just before the write finds a resource: the stub must not replace it."""
    _opsaet(monkeypatch, [], unknown_cprs=[PNR_B])

    def _findes_nu(**kw):
        for r in kw["resources"]:
            r.id = "f-imens"
        return kw["resources"]

    monkeypatch.setattr(modul, "prepare_resources_for_upsert", _findes_nu)
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, cprs=[PNR_B], stub_source=SOURCE_HAIBA
    )
    assert fhir.skrevet == []
    assert res.by_cpr == {PNR_B: "f-imens"}


def test_full_patient_is_written_even_if_the_lookup_finds_the_resource(monkeypatch):
    """The guard applies to stubs only: a person with EPJ rows must update the resource found."""
    person = Person(
        key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id=None
    )
    _opsaet(monkeypatch, [person])

    def _findes_nu(**kw):
        for r in kw["resources"]:
            r.id = "f-imens"
        return kw["resources"]

    monkeypatch.setattr(modul, "prepare_resources_for_upsert", _findes_nu)
    fhir = _FalskFhir()
    modul.ensure_patients(MagicMock(), mssql=object(), fhir_database=fhir, ifdb_patient_ids=["P1"])
    assert [p.id for p in fhir.skrevet] == ["f-imens"]


def test_create_only_must_match_in_count():
    with pytest.raises(ValueError):
        modul.write_patients(
            MagicMock(),
            _FalskFhir(),
            [MagicMock(id=None)],
            source_name="t",
            create_only=[True, True],
        )


def test_stub_policy_reaches_transaction_and_skipped_write_is_counted_correctly(monkeypatch):
    from core.fhir.direct_upsert import UpsertResult

    _opsaet(monkeypatch, [], unknown_cprs=[PNR_B])

    class RacedDatabase:
        mark_stale = MagicMock()
        safe_clear_identifier_caches_for_resources = MagicMock()

        def batch_upsert_resources(self, resources, resource_type, *, identity_safe, create_only):
            assert resource_type == "Patient" and identity_safe is True
            assert create_only == [True]
            resources[0].id = "created-after-preflight"
            return UpsertResult(0, 0, [], skipped=1)

    result = modul.ensure_patients(
        MagicMock(), object(), RacedDatabase(), cprs=[PNR_B], stub_source=SOURCE_HAIBA
    )
    assert result.by_cpr == {PNR_B: "created-after-preflight"}
    assert result.written == 0 and result.existing == 1
    RacedDatabase.mark_stale.assert_called_once_with("Patient")
    RacedDatabase.safe_clear_identifier_caches_for_resources.assert_called_once()


# ------------------------------------------------------------------ the producer path


def test_write_rows_builds_from_the_persons_live_row_not_the_deltas(monkeypatch):
    """The delta brought the old, deleted duplicate; the person's primary row is the live one."""
    levende = _row("P1", PNR_A)
    slettet = {**_row("P2", PNR_A, slettet="1"), "name_given": "Gammel"}
    person = Person(
        key=PNR_A, cprs=(PNR_A,), epj_ids=("P1", "P2"), rows=(levende, slettet), fhir_id="f-1"
    )
    _opsaet(monkeypatch, [person])
    fhir = _FalskFhir()
    skrevet = modul.write_rows(
        MagicMock(), mssql=object(), fhir_database=fhir, rows=[slettet], source_name="t"
    )
    assert len(skrevet) == 1 and skrevet[0].id == "f-1"
    assert skrevet[0].name[0].given == ["Test"]
    assert _ids(skrevet[0], SYSTEM_IFDB_PATIENT_ID) == [("P1", None), ("P2", USE_OLD)]


def test_rebuild_keeps_the_stub_sources_tag(monkeypatch):
    """A HAIBA stub that EPJ later rebuilds is still tagged with HAIBA."""
    person = Person(
        key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id="f-1"
    )
    _opsaet(monkeypatch, [person])
    monkeypatch.setattr(
        modul, "stub_sources_on_resources", lambda fhir_database, persons: {"f-1": (SOURCE_HAIBA,)}
    )
    skrevet = modul.write_rows(
        MagicMock(),
        mssql=object(),
        fhir_database=_FalskFhir(),
        rows=[_row("P1", PNR_A)],
        source_name="t",
    )
    assert [t.code for t in skrevet[0].meta.tag] == [SOURCE_IFDB, SOURCE_HAIBA]


class _TagFhir:
    """Answers with the given (id, meta.tag, meta.source) rows."""

    def __init__(self, raekker):
        self._raekker = raekker

    @contextmanager
    def get_connection(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = self._raekker
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        yield conn


def _tag(kode: str, system: str = SYSTEM_SOURCE_TAG) -> dict:
    return {"system": system, "code": kode}


def test_stub_sources_are_read_from_builder_tags_and_an_old_stubs_source():
    fhir = _TagFhir(
        [
            ("f-1", [_tag(SOURCE_CPR), _tag(SOURCE_HAIBA)], SOURCE_PATIENT_BUILDER),
            ("f-2", None, SOURCE_HAIBA),
            ("f-3", [_tag(SOURCE_IFDB)], SOURCE_PATIENT_BUILDER),
            ("f-4", [_tag(SOURCE_HAIBA, system="urn:andet")], None),
        ]
    )
    persons = [
        Person(key=str(n), cprs=(), epj_ids=(), rows=(), fhir_id=f"f-{n}") for n in range(1, 5)
    ]
    assert _stub_sources_on_resources(fhir, persons) == {
        "f-1": (SOURCE_HAIBA,),
        "f-2": (SOURCE_HAIBA,),
    }


def test_epj_contributions_are_built_once_per_batch(monkeypatch):
    kald: list[int] = []
    rigtig = modul.contributions_from_ifdb

    def _taellende(rows, context):
        kald.append(len(rows))
        return rigtig(rows, context)

    monkeypatch.setattr(modul, "contributions_from_ifdb", _taellende)
    persons = [
        Person(key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id=None),
        Person(key=PNR_B, cprs=(PNR_B,), epj_ids=("P2",), rows=(_row("P2", PNR_B),), fhir_id=None),
    ]
    _opsaet(monkeypatch, persons)
    fhir = _FalskFhir()
    modul.write_rows(
        MagicMock(),
        mssql=object(),
        fhir_database=fhir,
        rows=[_row("P1", PNR_A), _row("P2", PNR_B)],
        source_name="t",
    )
    assert kald == [2] and len(fhir.skrevet) == 2


def test_persons_with_several_resources_are_logged_as_counts(monkeypatch):
    person = Person(
        key=PNR_A, cprs=(PNR_A,), epj_ids=("P1",), rows=(_row("P1", PNR_A),), fhir_id="f-1"
    )
    res = IdentityResolution(
        persons=[person],
        by_ifdb_id={"P1": person},
        by_cpr={PNR_A: person},
        unknown_ifdb_ids=(),
        unknown_cprs=(),
        conflicts=3,
    )
    monkeypatch.setattr(
        modul, "resolve", lambda request, mssql, fhir_database, rows=(), members=None: res
    )
    monkeypatch.setattr(modul, "open_register", _uden_register)
    monkeypatch.setattr(modul, "prepare_resources_for_upsert", lambda **kw: kw["resources"])
    context = MagicMock()
    out = modul.ensure_patients(
        context, mssql=object(), fhir_database=_FalskFhir(), ifdb_patient_ids=["P1"]
    )
    assert out.conflicts == 3
    (advarsel,) = [c for c in context.log.warning.call_args_list]
    assert advarsel.args[-1] == 3 and PNR_A not in advarsel.args[0]


# ------------------------------------------------------------------ register contribution (2b)


def _med_register(monkeypatch, persons, registered, *, members=None, unknown_cprs=()):
    """Like _opsaet, but the register is published and answers with the given register persons."""
    from DSA.FHIR.Patient.patient_cpr_contribution import RegisterPerson

    res = IdentityResolution(
        persons=list(persons),
        by_ifdb_id={e: p for p in persons for e in p.epj_ids},
        by_cpr={c: p for p in persons for c in p.cprs},
        unknown_ifdb_ids=(),
        unknown_cprs=tuple(unknown_cprs),
    )
    set_members = set(members or ())

    class _Register:
        available = True

        def members(self, cprs):
            return set_members & set(cprs)

        def persons(self, persons):
            return {
                p.key: RegisterPerson(**registered[p.key]) for p in persons if p.key in registered
            }

    @contextmanager
    def _aabent(mssql):
        yield _Register()

    kald = []

    def _resolve(request, mssql, fhir_database, rows=(), members=None):
        kald.append(members)
        return res

    monkeypatch.setattr(modul, "resolve", _resolve)
    monkeypatch.setattr(modul, "open_register", _aabent)
    monkeypatch.setattr(modul, "prepare_resources_for_upsert", lambda **kw: kw["resources"])
    return kald


def _registerperson(pnr, **extra):
    return {
        "person": {"pnr": pnr, "status": "01", "koen": "K", "foed_dt": "1980-01-01"},
        "name": {"pnr": pnr, "fornvn": "Register", "melnvn": "", "efternvn": "Navn"},
        **extra,
    }


def test_register_contribution_is_included_in_the_producers_write(monkeypatch):
    person = Person(
        key=PNR_A,
        cprs=(PNR_A,),
        epj_ids=("P1",),
        rows=(_row("P1", PNR_A),),
        fhir_id="f-1",
        in_register=True,
    )
    kald = _med_register(monkeypatch, [person], {PNR_A: _registerperson(PNR_A)}, members=[PNR_A])
    fhir = _FalskFhir()
    (skrevet,) = modul.write_rows(
        MagicMock(), mssql=object(), fhir_database=fhir, rows=[_row("P1", PNR_A)], source_name="t"
    )
    assert skrevet.name[0].family == "Navn"  # the register wins the name
    assert _ids(skrevet, SYSTEM_IFDB_PATIENT_ID) == [("P1", None)]  # EPJ keeps its id
    assert [t.code for t in skrevet.meta.tag] == [SOURCE_CPR, SOURCE_IFDB]
    assert kald[0] is not None  # membership (rule C) is asked when the register is published


def test_ensure_builds_a_register_only_person_from_the_register_with_the_sources_tag(monkeypatch):
    person = Person(key=PNR_A, cprs=(PNR_A,), epj_ids=(), rows=(), fhir_id=None, in_register=True)
    _med_register(monkeypatch, [person], {PNR_A: _registerperson(PNR_A)}, members=[PNR_A])
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, cprs=[PNR_A], stub_source=SOURCE_HAIBA
    )
    (skrevet,) = fhir.skrevet
    assert skrevet.name[0].family == "Navn"
    assert [t.code for t in skrevet.meta.tag] == [SOURCE_CPR, SOURCE_HAIBA]
    assert res.written == 1 and res.from_register == 1 and res.unresolved_cprs == ()
    assert res.by_cpr == {PNR_A: "ny-0"}


def test_ensure_without_register_still_gives_the_stub(monkeypatch):
    _opsaet(monkeypatch, [], unknown_cprs=[PNR_A])
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, cprs=[PNR_A], stub_source=SOURCE_HAIBA
    )
    (skrevet,) = fhir.skrevet
    assert [t.code for t in skrevet.meta.tag] == [SOURCE_HAIBA] and res.from_register == 0


def test_existing_complete_resource_is_untouched_by_ensure_even_with_register(monkeypatch):
    person = Person(
        key=PNR_A,
        cprs=(PNR_A,),
        epj_ids=("P1",),
        rows=(_row("P1", PNR_A),),
        fhir_id="f-1",
        known_epj_ids=("P1",),
        in_register=True,
    )
    _med_register(monkeypatch, [person], {PNR_A: _registerperson(PNR_A)}, members=[PNR_A])
    fhir = _FalskFhir()
    res = modul.ensure_patients(MagicMock(), mssql=object(), fhir_database=fhir, cprs=[PNR_A])
    assert fhir.skrevet == [] and res.existing == 1


def test_write_register_persons_scope_known_skips_register_persons_without_epj_and_resource(
    monkeypatch,
):
    kendt = Person(
        key=PNR_A,
        cprs=(PNR_A,),
        epj_ids=("P1",),
        rows=(_row("P1", PNR_A),),
        fhir_id="f-1",
        in_register=True,
    )
    ukendt = Person(key=PNR_B, cprs=(PNR_B,), epj_ids=(), rows=(), fhir_id=None, in_register=True)
    _med_register(
        monkeypatch,
        [kendt, ukendt],
        {PNR_A: _registerperson(PNR_A), PNR_B: _registerperson(PNR_B)},
        members=[PNR_A, PNR_B],
        unknown_cprs=["0000000003"],
    )
    fhir = _FalskFhir()
    res = modul.write_register_persons(
        MagicMock(), object(), fhir, [PNR_A, PNR_B, "0000000003"], scope="known"
    )
    assert res.written == 1 and res.rebuilt == 1 and res.created == 0
    assert res.skipped_unknown == 1 and res.missing_in_register == 1
    (skrevet,) = fhir.skrevet
    assert skrevet.id == "f-1" and skrevet.name[0].family == "Navn"


def test_write_register_persons_scope_register_also_creates_the_registers_own(monkeypatch):
    ukendt = Person(key=PNR_B, cprs=(PNR_B,), epj_ids=(), rows=(), fhir_id=None, in_register=True)
    _med_register(monkeypatch, [ukendt], {PNR_B: _registerperson(PNR_B)}, members=[PNR_B])
    fhir = _FalskFhir()
    res = modul.write_register_persons(MagicMock(), object(), fhir, [PNR_B], scope="register")
    assert res.created == 1 and res.written == 1 and res.skipped_unknown == 0
    (skrevet,) = fhir.skrevet
    assert [t.code for t in skrevet.meta.tag] == [SOURCE_CPR]
    assert _ids(skrevet, SYSTEM_CPR) == [(PNR_B, None)]


def test_write_register_persons_without_published_register_is_a_hard_failure(monkeypatch):
    _opsaet(monkeypatch, [])
    with pytest.raises(dg.Failure):
        modul.write_register_persons(MagicMock(), object(), _FalskFhir(), [PNR_A])


def test_write_register_persons_rejects_unknown_scope():
    with pytest.raises(ValueError):
        modul.write_register_persons(MagicMock(), object(), _FalskFhir(), [PNR_A], scope="alle")


def test_bom_rows_can_be_given_as_list_and_all_oids_are_included(monkeypatch):
    _opsaet(monkeypatch, [], unknown_cprs=[PNR_B])
    fhir = _FalskFhir()
    rows = [
        {"CPRNR": PNR_B, "PATIENTINFOOID": 9},
        {"CPRNR": PNR_B, "PATIENTINFOOID": 7, "FORNAVN": "Bo", "EFTERNAVN": "Bom"},
    ]
    modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, cprs=[PNR_B], bom_rows_by_cpr={PNR_B: rows}
    )
    (skrevet,) = fhir.skrevet
    assert [v for v, _ in _ids(skrevet, SYSTEM_BOM_IFDB_PATIENTINFO_OID)] == ["7", "9"]
    assert skrevet.name[0].family == "Bom"


# ------------------------------------------------------------------ two guarantees


def test_reference_guarantee_looks_up_once_and_does_not_resolve(monkeypatch):
    """If the identity already exists there is nothing to decide — and no resolution is run."""
    kaldt: list = []
    monkeypatch.setattr(modul, "_already_known", lambda db, epj, cprs: ({"P1": "f-1"}, {}))
    monkeypatch.setattr(
        modul,
        "resolve",
        lambda *a, **k: kaldt.append(1) or (_ for _ in ()).throw(AssertionError("must not run")),
    )
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(),
        mssql=object(),
        fhir_database=fhir,
        ifdb_patient_ids=["P1"],
        need=modul.NEED_REFERENCE,
    )
    assert res.by_ifdb_id == {"P1": "f-1"} and res.existing == 1
    assert fhir.skrevet == [] and kaldt == []


def test_reference_guarantee_resolves_only_those_the_lookup_did_not_know(monkeypatch):
    """The known one is skipped; the unknown one goes the full path and is created."""
    person = Person(
        key=PNR_B, cprs=(PNR_B,), epj_ids=("P2",), rows=(_row("P2", PNR_B),), fhir_id=None
    )
    _opsaet(monkeypatch, [person])
    monkeypatch.setattr(modul, "_already_known", lambda db, epj, cprs: ({"P1": "f-1"}, {}))
    fhir = _FalskFhir()
    res = modul.ensure_patients(
        MagicMock(),
        mssql=object(),
        fhir_database=fhir,
        ifdb_patient_ids=["P1", "P2"],
        need=modul.NEED_REFERENCE,
    )
    assert res.by_ifdb_id == {"P1": "f-1", "P2": "ny-0"}
    assert len(fhir.skrevet) == 1


def test_person_guarantee_rebuilds_where_reference_guarantee_leaves_alone(monkeypatch):
    """Same person, two guarantees: the costly one sees the missing id, the cheap one does not."""
    person = Person(
        key=PNR_A,
        cprs=(PNR_A,),
        epj_ids=("P1", "P2"),
        rows=(_row("P1", PNR_A), _row("P2", PNR_A)),
        fhir_id="f-1",
        known_epj_ids=("P1",),
    )
    _opsaet(monkeypatch, [person])
    fhir = _FalskFhir()
    dyr = modul.ensure_patients(
        MagicMock(), mssql=object(), fhir_database=fhir, ifdb_patient_ids=["P1"]
    )
    assert dyr.rebuilt == 1 and len(fhir.skrevet) == 1

    monkeypatch.setattr(modul, "_already_known", lambda db, epj, cprs: ({"P1": "f-1"}, {}))
    billig = modul.ensure_patients(
        MagicMock(),
        mssql=object(),
        fhir_database=fhir,
        ifdb_patient_ids=["P1"],
        need=modul.NEED_REFERENCE,
    )
    assert billig.rebuilt == 0 and len(fhir.skrevet) == 1  # nothing new written


def test_an_unknown_guarantee_is_an_error():
    with pytest.raises(ValueError, match="NEED_PERSON"):
        modul.ensure_patients(
            MagicMock(), mssql=object(), fhir_database=object(), cprs=[PNR_A], need="hurtigst"
        )
