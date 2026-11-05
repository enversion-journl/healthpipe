# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Builder tests; EPJ alone gives the EPJ transform's resource byte for byte, except meta."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.Patient.patient_builder import assemble, build, contributions_from_ifdb
from DSA.FHIR.Patient.patient_ifdb_contribution import _transform_patient_rows_to_fhir
from DSA.FHIR.Patient.patient_ownership import (
    PATIENT_OWNERSHIP,
    SOURCE_CPR,
    SOURCE_DRG,
    SOURCE_HAIBA,
    SOURCE_IFDB,
)
from DSA.FHIR.utils.field_ownership import (
    Contribution,
    DuplicateContribution,
    NotOwner,
    UnknownField,
    field_keys,
)
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.period import Period
from utils.fhir.fhir_extensions import (
    EXTENSION_ADDRESS_KVHX,
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_DISTRICT,
    EXTENSION_DK_MUNICIPALITY_CODES,
    EXTENSION_PATIENT_CITIZENSHIP,
    EXTENSION_RESIDENCE_MUNICIPALITY,
)
from utils.fhir.identifier_systems import (
    SOURCE_PATIENT_BUILDER,
    SYSTEM_CPR,
    SYSTEM_CPR_PERSON_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOURCE_TAG,
)
from utils.fhir.protection import ADDRESS_PROTECTED_TEXT, NAME_PROTECTED_TEXT

pytestmark = pytest.mark.unit

PNR = "0000000001"
PNR_GAMMELT = "0000000002"


def _epj_raekke(**overstyr) -> dict:
    """An EPJ row with all the fields the transform reads."""
    raekke = {
        "patient_id": "999001",
        "identifier_cpr": PNR,
        "name_given": "Test Person",
        "name_family": "Testesen",
        "gender_code": "K",
        "birth_date": "1980-01-01",
        "address_street": "Testvej 1",
        "address_postal": "5000",
        "address_city": "Odense C",
        "address_municipality": "0461",
        "address_protection": "0",
        "person_status_code": "PAS_ks.ur.CPRStatus.01",  # the production form: with prefix
        "doctor_name": "Læge Test",
        "doctor_number": "123456",
        "modtidspunkt": "2026-01-02 03:04:05",
        "district_code": "D1",
        "district_text": "Distrikt 1",
        "social_district_code": "S1",
        "social_district_text": "Socialdistrikt 1",
        "is_deleted": "0",
    }
    raekke.update(overstyr)
    return raekke


def _uden_byggerens_egne_felter(patient: Patient) -> str:
    """The resource without meta and without the municipality of residence the builder adds."""
    felter = {**patient.dict(exclude_none=True), "meta": None}
    felter["extension"] = [
        e for e in felter.get("extension") or [] if e["url"] != EXTENSION_RESIDENCE_MUNICIPALITY
    ]
    return patient.__class__(**felter).json(exclude_none=True)


# ------------------------------------------------------ EPJ alone: byte identity


@pytest.mark.parametrize(
    "raekker",
    [
        [_epj_raekke()],
        [_epj_raekke(person_status_code="PAS_ks.ur.CPRStatus.90")],  # dead
        [_epj_raekke(person_status_code="PAS_ks.ur.CPRStatus.80")],  # emigrated
        [_epj_raekke(is_deleted="1", address_protection="1")],  # deleted, protected
        [_epj_raekke(doctor_name=None, doctor_number=None, district_code=None, district_text=None)],
        [_epj_raekke(), _epj_raekke(patient_id="999002", identifier_cpr="0000000003")],
    ],
)
def test_epj_contribution_alone_gives_exactly_the_current_resource(raekker):
    """Without meta the builder's output must be identical to the transform that runs today."""
    context = MagicMock()
    direkte = _transform_patient_rows_to_fhir(raekker, context)
    bygget = [assemble([b]) for b in contributions_from_ifdb(raekker, context)]

    assert len(direkte) == len(bygget) == len(raekker)
    for d, b in zip(direkte, bygget, strict=True):
        assert b.json(exclude_none=True) == d.json(exclude_none=True)


def test_everything_the_epj_transform_emits_has_an_owner():
    """The contract: the table covers every field EPJ actually supplies. Else the builder stops."""
    context = MagicMock()
    for bidrag in contributions_from_ifdb(
        [_epj_raekke(is_deleted="1", address_protection="1")], context
    ):
        assemble([bidrag])  # raises UnknownField or NotOwner if the contract is broken


def test_provenance_on_epj_contribution():
    context = MagicMock()
    patient = build(contributions_from_ifdb([_epj_raekke()], context))
    assert patient.meta.source == SOURCE_PATIENT_BUILDER
    assert [(t.system, t.code) for t in patient.meta.tag] == [(SYSTEM_SOURCE_TAG, SOURCE_IFDB)]
    # ...and otherwise no difference
    direkte = _transform_patient_rows_to_fhir([_epj_raekke()], context)[0]
    assert _uden_byggerens_egne_felter(patient) == direkte.json(exclude_none=True)


# ----------------------------------------------------- two sources, one person


def _cpr_bidrag() -> Contribution:
    """The CPR register's proposal for the same person: the fields the register owns or shares."""
    return Contribution(
        source=SOURCE_CPR,
        resource=Patient(
            identifier=[
                Identifier(system=SYSTEM_CPR, value=PNR),
                Identifier(system=SYSTEM_CPR, value=PNR_GAMMELT, use="old"),
                Identifier(system=SYSTEM_CPR_PERSON_ID, value="P-1"),
            ],
            name=[HumanName(use="official", family="Registersen", given=["Test", "Person"])],
            gender="female",
            birthDate="1980-01-02",
            address=[
                Address(
                    line=["Registervej 2"], postalCode="5000", period=Period(start="2015-03-01")
                )
            ],
            maritalStatus=CodeableConcept(coding=[Coding(code="G")]),
            extension=[
                Extension(
                    url=EXTENSION_PATIENT_CITIZENSHIP,
                    extension=[
                        Extension(
                            url="code",
                            valueCodeableConcept=CodeableConcept(coding=[Coding(code="5100")]),
                        )
                    ],
                ),
                Extension(url=EXTENSION_ADDRESS_PROTECTION, valueBoolean=True),
            ],
        ),
    )


def test_two_sources_win_per_field():
    context = MagicMock()
    (epj,) = contributions_from_ifdb([_epj_raekke()], context)
    patient = assemble([epj, _cpr_bidrag()])

    # The register wins the shared fields
    assert patient.name[0].family == "Registersen"
    assert patient.name[0].use == "official"
    assert str(patient.birthDate) == "1980-01-02"
    assert patient.address[0].line == ["Registervej 2"]
    assert str(patient.address[0].period.start) == "2015-03-01"
    assert patient.maritalStatus.coding[0].code == "G"

    # EPJ keeps what only EPJ has
    assert patient.generalPractitioner[0].identifier.value == "123456"
    assert any(e.url == EXTENSION_DISTRICT for e in patient.extension)

    # The register wins the personal number system — with the history — and EPJ's own id follows
    cpr_ids = [(i.value, i.use) for i in patient.identifier if i.system == SYSTEM_CPR]
    assert cpr_ids == [(PNR, None), (PNR_GAMMELT, "old")]
    assert [i.value for i in patient.identifier if i.system == SYSTEM_IFDB_PATIENT_ID] == ["999001"]
    assert [i.value for i in patient.identifier if i.system == SYSTEM_CPR_PERSON_ID] == ["P-1"]

    # Shared extension: the register wins, and there is only one
    beskyttelse = [e for e in patient.extension if e.url == EXTENSION_ADDRESS_PROTECTION]
    assert len(beskyttelse) == 1 and beskyttelse[0].valueBoolean is True


def test_list_order_is_the_canonical_source_order():
    """The register's elements first, then EPJ's — regardless of the order of the contributions."""
    context = MagicMock()
    (epj,) = contributions_from_ifdb([_epj_raekke()], context)
    a = assemble([epj, _cpr_bidrag()])
    b = assemble([_cpr_bidrag(), epj])
    assert a.json(exclude_none=True) == b.json(exclude_none=True)
    assert a.identifier[0].system == SYSTEM_CPR
    assert a.identifier[-1].system == SYSTEM_IFDB_PATIENT_ID


def test_provenance_names_all_contributing_sources_in_canonical_order():
    context = MagicMock()
    (epj,) = contributions_from_ifdb([_epj_raekke()], context)
    patient = build([epj, _cpr_bidrag()])
    assert [t.code for t in patient.meta.tag] == [SOURCE_CPR, SOURCE_IFDB]


def test_stub_contribution_gives_only_identifier_and_tag():
    """HAIBA/DRG only know a personal number. That is all they may supply."""
    stub = Contribution(
        source=SOURCE_HAIBA, resource=Patient(identifier=[Identifier(system=SYSTEM_CPR, value=PNR)])
    )
    patient = build([stub])
    assert [i.value for i in patient.identifier] == [PNR]
    assert patient.name is None
    assert [t.code for t in patient.meta.tag] == [SOURCE_HAIBA]


# ------------------------- municipality of residence and name and address protection


def _kommune(kode: str) -> Extension:
    return Extension(
        url=EXTENSION_DK_MUNICIPALITY_CODES,
        valueCodeableConcept=CodeableConcept(coding=[Coding(code=kode)]),
    )


def _registerbidrag(*, beskyttet: bool, med_aktuel_adresse: bool = True) -> Contribution:
    """The register's proposal: name, a historical address in Kolding, a current one in Odense."""
    adresser = [
        Address(
            use="old",
            country="DK",
            period=Period(start="2001-01-01", end="2015-03-01"),
            extension=[
                _kommune("0621"),
                Extension(url=EXTENSION_ADDRESS_KVHX, valueString="uuid-gammel"),
            ],
        )
    ]
    if med_aktuel_adresse:
        adresser.append(
            Address(
                use="home",
                line=["Registervej 2"],
                postalCode="5000",
                city="Odense C",
                country="DK",
                period=Period(start="2015-03-01"),
                extension=[
                    _kommune("0461"),
                    Extension(url=EXTENSION_ADDRESS_KVHX, valueString="uuid-ny"),
                ],
            )
        )
    return Contribution(
        source=SOURCE_CPR,
        resource=Patient(
            identifier=[Identifier(system=SYSTEM_CPR, value=PNR)],
            name=[
                HumanName(
                    use="official", family="Registersen", given=["Test"], text="Test Registersen"
                )
            ],
            address=adresser,
            extension=[Extension(url=EXTENSION_ADDRESS_PROTECTION, valueBoolean=beskyttet)],
        ),
    )


def _bopaelskommune(patient: Patient) -> str | None:
    koder = [
        e.valueCodeableConcept.coding[0].code
        for e in patient.extension or []
        if e.url == EXTENSION_RESIDENCE_MUNICIPALITY
    ]
    assert len(koder) <= 1
    return koder[0] if koder else None


def test_protected_person_is_written_without_name_and_address():
    patient = build([_registerbidrag(beskyttet=True)])

    tekst = patient.json(exclude_none=True)
    for hemmeligt in ("Registersen", "Registervej", "5000", "Odense", "uuid-", "2015", "0621"):
        assert hemmeligt not in tekst, hemmeligt
    assert [(n.given, n.family, n.text) for n in patient.name] == [
        (None, NAME_PROTECTED_TEXT, NAME_PROTECTED_TEXT)
    ]
    assert [(a.line, a.period) for a in patient.address] == [([ADDRESS_PROTECTED_TEXT], None)]
    assert _bopaelskommune(patient) == "0461"


def test_unprotected_person_keeps_name_and_addresses():
    patient = build([_registerbidrag(beskyttet=False)])

    assert patient.name[0].family == "Registersen"
    assert [a.use for a in patient.address] == ["old", "home"]
    # The municipality of the current address, not of the historical one.
    assert _bopaelskommune(patient) == "0461"


def test_person_without_current_address_has_no_municipality_of_residence():
    patient = build([_registerbidrag(beskyttet=False, med_aktuel_adresse=False)])
    assert _bopaelskommune(patient) is None


@pytest.mark.parametrize(
    ("kommunekode", "forventet"), [("461", "0461"), ("0461", "0461"), (None, None)]
)
def test_epj_municipality_code_becomes_four_digit_municipality_of_residence(kommunekode, forventet):
    bidrag = contributions_from_ifdb([_epj_raekke(address_municipality=kommunekode)], MagicMock())
    assert _bopaelskommune(build(bidrag)) == forventet


def test_epj_alone_with_protection_is_masked():
    patient = build(contributions_from_ifdb([_epj_raekke(address_protection="1")], MagicMock()))

    tekst = patient.json(exclude_none=True)
    for hemmeligt in ("Testesen", "Test Person", "Testvej", "5000", "Odense"):
        assert hemmeligt not in tekst, hemmeligt
    assert _bopaelskommune(patient) == "0461"


def test_a_yes_to_protection_wins_over_the_registers_no():
    """The register owns the flag first, but a yes from EPJ masks anyway."""
    (epj,) = contributions_from_ifdb([_epj_raekke(address_protection="1")], MagicMock())
    patient = build([epj, _registerbidrag(beskyttet=False)])

    tekst = patient.json(exclude_none=True)
    for hemmeligt in ("Registersen", "Registervej", "Testesen", "Testvej"):
        assert hemmeligt not in tekst, hemmeligt
    flag = [e.valueBoolean for e in patient.extension if e.url == EXTENSION_ADDRESS_PROTECTION]
    assert flag == [True]
    assert _bopaelskommune(patient) == "0461"


def test_the_registers_yes_stands_even_if_epj_says_no():
    (epj,) = contributions_from_ifdb([_epj_raekke(address_protection="0")], MagicMock())
    patient = build([epj, _registerbidrag(beskyttet=True)])
    assert patient.name[0].family == NAME_PROTECTED_TEXT


# ------------------------------------------------------------------ the contract


def test_field_without_owner_is_rejected():
    fremmed = Contribution(
        source=SOURCE_IFDB,
        resource=Patient(extension=[Extension(url="http://example.org/ukendt", valueString="x")]),
    )
    with pytest.raises(UnknownField):
        assemble([fremmed])


def test_contribution_supplying_something_it_does_not_own_is_rejected():
    for kilde in (SOURCE_HAIBA, SOURCE_DRG):
        stub_med_navn = Contribution(source=kilde, resource=Patient(name=[HumanName(family="X")]))
        with pytest.raises(NotOwner):
            assemble([stub_med_navn])


def test_two_contributions_from_the_same_source_are_rejected():
    with pytest.raises(DuplicateContribution):
        assemble([_cpr_bidrag(), _cpr_bidrag()])


def test_contribution_must_not_set_id_or_meta():
    with pytest.raises(NotOwner):
        assemble([Contribution(source=SOURCE_CPR, resource=Patient(id="abc"))])


def test_no_contributions_is_an_error():
    with pytest.raises(ValueError):
        assemble([])


def test_field_keys_treat_choice_types_as_one_field():
    p = Patient(deceasedBoolean=True, identifier=[Identifier(system=SYSTEM_CPR, value=PNR)])
    assert field_keys(PATIENT_OWNERSHIP, p) == {"deceased", f"identifier:{SYSTEM_CPR}"}


# --------------------------------------------------- EPJ alone: nothing else may move


def test_the_assets_identity_is_unchanged():
    """The builder keeps the EPJ producer's key, group and DFM checkpoint (fhir_server/Patient)."""
    from DSA.FHIR.Patient.Patient_IFDB_dfm import fhir_server_patient_dfm

    spec = next(iter(fhir_server_patient_dfm.specs))
    assert spec.key.to_user_string() == "fhir_server_patient_dfm"
    assert spec.group_name == "fhir_ifdb_dfm"
    # The union is a dependency: the write point reads it, and it must be
    # built before the producer writes. Consumers know the asset by its key
    # and group.
    assert {d.asset_key.to_user_string() for d in spec.deps} == {
        "arc_patient_ifdb",
        "ref_patient_identity",
    }


class _FalskDfm:
    """DFM without a database: the run is not recorded, and the checkpoint does not move."""

    def __init__(self, mssql, context):
        pass

    def start_dfm_run(self, *_, **__):
        return SimpleNamespace(log_id=1, checkpoint=None)

    def set_checkpoint(self, *_, **__):
        raise AssertionError("the checkpoint must not be moved with advance_checkpoint=False")

    def complete_dfm_run(self, *_, **__):
        pass

    def fail_dfm_run(self, *_, **__):
        pass


class _FalskCursor:
    """Answers the archive query with the given rows, in one chunk."""

    def __init__(self, raekker: list[dict]):
        self._raekker = list(raekker)
        self.description = [(k,) for k in raekker[0]] if raekker else []

    def execute(self, sql, parametre=None):
        pass

    def fetchone(self):
        # The run's start time (checkpoint candidate); the asset must not move it here.
        return ("2026-01-01 00:00:00",)

    def fetchmany(self, antal):
        klump, self._raekker = self._raekker[:antal], self._raekker[antal:]
        return [tuple(r.values()) for r in klump]

    def close(self):
        pass


class _FalskForbindelse:
    def __init__(self, raekker: list[dict]):
        self._raekker = raekker

    def cursor(self):
        return _FalskCursor(self._raekker)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _FalskMssql:
    def __init__(self, raekker: list[dict]):
        self._raekker = raekker

    def get_connection(self):
        return _FalskForbindelse(self._raekker)


def test_the_asset_writes_the_builders_provenance(monkeypatch):
    """What the asset writes carries the builder's meta and is otherwise the transform's resources."""
    from DSA.FHIR.Patient import Patient_IFDB_dfm as modul

    raekker = [_epj_raekke(), _epj_raekke(patient_id="999002", identifier_cpr="0000000003")]
    from DSA.FHIR.Patient import patient_service
    from DSA.FHIR.Patient.patient_identity import IdentityInputs, resolve_identities

    sendt: list[Patient] = []

    def _fang(context, fhir_database, patients, *, source_name):
        sendt.extend(patients)
        return patients

    # The identity resolution runs on the rows alone (no database, no register);
    # the write is captured.
    monkeypatch.setattr(
        patient_service,
        "resolve",
        lambda request, mssql, fhir_database, rows=(), members=None: resolve_identities(
            request, IdentityInputs(archive_rows=list(rows))
        ),
    )

    @contextmanager
    def _uden_register(mssql):
        yield SimpleNamespace(available=False, members=lambda c: set(), persons=lambda p: {})

    monkeypatch.setattr(patient_service, "open_register", _uden_register)
    monkeypatch.setattr(patient_service, "write_patients", _fang)
    monkeypatch.setattr(modul, "DfmManager", _FalskDfm)

    resultat = modul.fhir_server_patient_dfm(
        context=dg.build_asset_context(),
        mssql_target_staging=_FalskMssql(raekker),
        fhir_database=object(),
        config=RunModeConfig(mode="full_load", advance_checkpoint=False),
    )

    assert resultat.metadata["posted_count"] == len(raekker)
    assert [p.meta.source for p in sendt] == [SOURCE_PATIENT_BUILDER] * len(raekker)
    assert [[t.code for t in p.meta.tag] for p in sendt] == [[SOURCE_IFDB]] * len(raekker)
    direkte = _transform_patient_rows_to_fhir(raekker, MagicMock())
    assert [_uden_byggerens_egne_felter(p) for p in sendt] == [
        d.json(exclude_none=True) for d in direkte
    ]
