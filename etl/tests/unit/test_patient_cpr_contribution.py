# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Register contribution from synthetic archive rows, no database; DDL contract checked last."""

import re
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from DSA.FHIR.Patient import patient_cpr_contribution as modul
from DSA.FHIR.Patient.patient_builder import build, contributions_from_ifdb
from DSA.FHIR.Patient.patient_contributions import USE_OLD, stub_contribution
from DSA.FHIR.Patient.patient_cpr_contribution import (
    REGISTER_TABLES,
    RegisterPerson,
    RegisterRows,
    contribution_from_register,
    fetch_register_rows,
    register_person,
)
from DSA.FHIR.Patient.patient_identity import Person
from DSA.FHIR.Patient.patient_ownership import SOURCE_CPR, SOURCE_HAIBA, SOURCE_IFDB
from utils.fhir.fhir_extensions import (
    EXTENSION_ADDRESS_KVHX,
    EXTENSION_ADDRESS_OFFICIAL,
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_CPR_UPDATED,
    EXTENSION_DK_MUNICIPALITY_CODES,
    EXTENSION_FATHER_IDENTIFIER,
    EXTENSION_MOTHER_IDENTIFIER,
    EXTENSION_PATIENT_CITIZENSHIP,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_CPR_LANDEKODE,
    SYSTEM_CPR_PERSON_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_V3_MARITAL_STATUS,
)

pytestmark = pytest.mark.unit

PNR = "0000000001"
PNR_OLD = "0000000009"
PNR_MOR = "0000000002"


def _register(**overrides) -> RegisterPerson:
    """A full, synthetic register person; individual parts can be overridden or removed."""
    fields = {
        "person": {"pnr": PNR, "status": "01", "koen": "K", "foed_dt": "1980-01-01"},
        "address": {
            "pnr": PNR,
            "komkod": "0461",
            "husnr": "0012",
            "etage": "st",
            "sidedoer": "th",
            "tilflydto": "201503010000",
            "vejnvn": "Testvej",
            "adresse_uuid": "0a3f5089-0000-32b8-e044-0003ba298018",
        },
        "plain_address": {
            "pnr": PNR,
            "adrnvn": "Test Testesen",
            "standardadr": "Testvej 12, st. th.",
            "bynavn": "Odense C",
            "postnr": "5000",
            "postdisttxt": "Odense C",
        },
        "name": {"pnr": PNR, "fornvn": "Test Anna", "melnvn": "Mellem", "efternvn": "Testesen"},
        "citizenship": {"pnr": PNR, "landekode": "5100"},
        "marital": {"pnr": PNR, "civst": "G"},
        "parents": {"pnr": PNR, "pnrmor": PNR_MOR, "pnrfar": "0000000000"},
        "person_id": {"pnr": PNR, "person_id": "PID-1"},
        "protections": ({"pnr": PNR, "beskyttype": "0001", "slet_dt_beskyttelse": ""},),
        "history_addresses": (
            {
                "pnr": PNR,
                "annkor": "",
                "komkod": "0461",
                "tilflydto": "200001010000",
                "fraflydto": "201002280000",
                "adresse_uuid": "u-1",
            },
            {
                "pnr": PNR,
                "annkor": "",
                "komkod": "0461",
                "tilflydto": "201003010000",
                "fraflydto": "201502280000",
                "adresse_uuid": "u-2",
            },
            {  # cancelled: does not count
                "pnr": PNR,
                "annkor": "A",
                "komkod": "0461",
                "tilflydto": "199001010000",
                "fraflydto": "199912310000",
                "adresse_uuid": "u-x",
            },
        ),
    }
    fields.update(overrides)
    return RegisterPerson(**fields)


def _ids(patient, system):
    return [(i.value, i.use) for i in patient.identifier or [] if i.system == system]


def _ext(patient, url):
    return next((e for e in patient.extension or [] if e.url == url), None)


# ------------------------------------------------------------------ identity and status


def test_contribution_is_the_registers_and_carries_only_the_registers_fields():
    c = contribution_from_register(_register(), current_cpr=PNR, historical_cprs=[PNR_OLD])
    assert c.source == SOURCE_CPR
    p = c.resource
    assert p.id is None and p.meta is None
    assert _ids(p, SYSTEM_CPR) == [(PNR, None), (PNR_OLD, USE_OLD)]
    assert _ids(p, SYSTEM_CPR_PERSON_ID) == [("PID-1", None)]
    assert _ids(p, SYSTEM_IFDB_PATIENT_ID) == []
    assert p.generalPractitioner is None


def test_status_is_answered_explicitly_also_when_the_answer_is_no():
    levende = contribution_from_register(_register(), current_cpr=PNR).resource
    assert levende.active is True and levende.deceasedBoolean is False
    doed = contribution_from_register(
        _register(person={"pnr": PNR, "status": "90", "koen": "M", "foed_dt": "1950-05-05"}),
        current_cpr=PNR,
    ).resource
    assert doed.active is False and doed.deceasedBoolean is True and doed.gender == "male"


def test_previous_numbers_come_from_the_identity_and_are_not_duplicated():
    p = contribution_from_register(
        _register(), current_cpr=PNR, historical_cprs=[PNR_OLD, PNR, PNR_OLD, " "]
    ).resource
    assert _ids(p, SYSTEM_CPR) == [(PNR, None), (PNR_OLD, USE_OLD)]


# ------------------------------------------------------------------ demographics


def test_name_gender_birth_date_marital_status_and_citizenship():
    p = contribution_from_register(_register(), current_cpr=PNR).resource
    (navn,) = p.name
    assert navn.use == "official"
    assert navn.given == ["Test", "Anna", "Mellem"] and navn.family == "Testesen"
    assert navn.text == "Test Testesen"
    assert p.gender == "female" and str(p.birthDate) == "1980-01-01"
    (kodning,) = p.maritalStatus.coding
    assert (kodning.system, kodning.code, kodning.display) == (
        SYSTEM_V3_MARITAL_STATUS,
        "M",
        "Married",
    )
    assert p.maritalStatus.text == "gift"
    borger = _ext(p, EXTENSION_PATIENT_CITIZENSHIP)
    (kode,) = borger.extension
    assert kode.url == "code"
    assert kode.valueCodeableConcept.coding[0].system == SYSTEM_CPR_LANDEKODE
    assert kode.valueCodeableConcept.coding[0].code == "5100"


DK_MARITAL = "http://hl7.dk/fhir/core/CodeSystem/dk-marital-status"


@pytest.mark.parametrize(
    ("civst", "system", "kode", "tekst"),
    [
        ("U", SYSTEM_V3_MARITAL_STATUS, "U", "ugift"),
        ("G", SYSTEM_V3_MARITAL_STATUS, "M", "gift"),
        ("F", SYSTEM_V3_MARITAL_STATUS, "D", "fraskilt"),
        ("E", SYSTEM_V3_MARITAL_STATUS, "W", "enke eller enkemand"),
        ("P", DK_MARITAL, "P", "registreret partnerskab"),
        ("O", DK_MARITAL, "O", "ophævet partnerskab"),
        ("L", SYSTEM_V3_MARITAL_STATUS, "W", "længstlevende partner"),
    ],
)
def test_marital_status_is_mapped_as_dk_core(civst, system, kode, tekst):
    p = contribution_from_register(
        _register(marital={"pnr": PNR, "civst": civst}), current_cpr=PNR
    ).resource
    (kodning,) = p.maritalStatus.coding
    assert (kodning.system, kodning.code) == (system, kode)
    assert p.maritalStatus.text == tekst


def test_dead_is_not_a_marital_status():
    """dk-core: marital status D is recorded in Patient.deceased, not in maritalStatus."""
    p = contribution_from_register(
        _register(marital={"pnr": PNR, "civst": "D"}), current_cpr=PNR
    ).resource
    assert p.maritalStatus is None


def test_unknown_marital_status_and_zero_country_code_are_not_asserted():
    p = contribution_from_register(
        _register(
            marital={"pnr": PNR, "civst": "?"}, citizenship={"pnr": PNR, "landekode": "0000"}
        ),
        current_cpr=PNR,
    ).resource
    assert p.maritalStatus is None and _ext(p, EXTENSION_PATIENT_CITIZENSHIP) is None


def test_parents_only_when_the_number_is_filled_in():
    p = contribution_from_register(_register(), current_cpr=PNR).resource
    mor = _ext(p, EXTENSION_MOTHER_IDENTIFIER)
    assert mor.valueIdentifier.system == SYSTEM_CPR and mor.valueIdentifier.value == PNR_MOR
    assert _ext(p, EXTENSION_FATHER_IDENTIFIER) is None


def test_zero_date_unknown_gender_and_missing_name_become_nothing():
    p = contribution_from_register(
        _register(
            person={"pnr": PNR, "status": "01", "koen": "X", "foed_dt": "0000-00-00"},
            name={"pnr": PNR, "fornvn": "", "melnvn": "", "efternvn": ""},
        ),
        current_cpr=PNR,
    ).resource
    assert p.birthDate is None and p.name is None and p.gender is None


# ------------------------------------------------------------------ addresses


def test_all_address_periods_become_versions_with_the_current_one_last():
    p = contribution_from_register(_register(), current_cpr=PNR).resource
    perioder = [
        (str(a.period.start), str(a.period.end) if a.period.end else None) for a in p.address
    ]
    assert perioder == [
        ("2000-01-01", "2010-02-28"),
        ("2010-03-01", "2015-02-28"),
        ("2015-03-01", None),
    ]
    assert [a.use for a in p.address] == ["old", "old", "home"]
    aktuel = p.address[-1]
    assert aktuel.line == ["Testvej 12, st. th."]
    assert aktuel.postalCode == "5000" and aktuel.city == "Odense C" and aktuel.country == "DK"
    urls = [e.url for e in aktuel.extension]
    assert urls == [
        EXTENSION_ADDRESS_OFFICIAL,
        EXTENSION_DK_MUNICIPALITY_CODES,
        EXTENSION_ADDRESS_KVHX,
    ]
    assert aktuel.extension[1].valueCodeableConcept.coding[0].code == "0461"
    assert aktuel.extension[2].valueString.startswith("0a3f5089")
    # Historical addresses: no street name is invented; municipality and DAR key are carried.
    historisk = p.address[0]
    assert historisk.line is None and historisk.city is None
    assert [e.url for e in historisk.extension] == [
        EXTENSION_DK_MUNICIPALITY_CODES,
        EXTENSION_ADDRESS_KVHX,
    ]


def test_same_period_start_gives_one_version_and_the_current_one_wins():
    reg = _register(
        history_addresses=(
            {
                "pnr": PNR,
                "annkor": "",
                "komkod": "0461",
                "tilflydto": "200001010000",
                "fraflydto": "200501010000",
                "adresse_uuid": "kort",
            },
            {
                "pnr": PNR,
                "annkor": "",
                "komkod": "0461",
                "tilflydto": "200001010000",
                "fraflydto": "201002280000",
                "adresse_uuid": "lang",
            },
            {
                "pnr": PNR,
                "annkor": "",
                "komkod": "0461",
                "tilflydto": "201503010000",
                "fraflydto": "",
                "adresse_uuid": "som-aktuel",
            },
            {
                "pnr": PNR,
                "annkor": "",
                "komkod": "0461",
                "tilflydto": "000000000000",
                "fraflydto": "",
                "adresse_uuid": "uden-start",
            },
        )
    )
    p = contribution_from_register(reg, current_cpr=PNR).resource
    assert [str(a.period.start) for a in p.address] == ["2000-01-01", "2015-03-01"]
    assert p.address[0].extension[-1].valueString == "lang"
    assert p.address[1].use == "home"


def test_current_address_without_plain_text_is_composed_from_002():
    p = contribution_from_register(_register(plain_address=None), current_cpr=PNR).resource
    aktuel = p.address[-1]
    assert aktuel.line == ["Testvej 12 st th"] and aktuel.postalCode is None


def test_without_addresses_no_address_is_asserted():
    p = contribution_from_register(
        _register(address=None, plain_address=None, history_addresses=()), current_cpr=PNR
    ).resource
    assert p.address is None


# ------------------------------------------------------------------ protection


def _beskyttelse(*rows):
    return _ext(
        contribution_from_register(_register(protections=rows), current_cpr=PNR).resource,
        EXTENSION_ADDRESS_PROTECTION,
    ).valueBoolean


def _0001(start="", slet=""):
    return {
        "pnr": PNR,
        "beskyttype": "0001",
        "start_dt_beskyttelse": start,
        "slet_dt_beskyttelse": slet,
    }


def test_address_protection_only_type_0001():
    assert _beskyttelse() is False
    assert _beskyttelse({"pnr": PNR, "beskyttype": "0002", "slet_dt_beskyttelse": ""}) is False
    assert _beskyttelse(_0001()) is True


def test_address_protection_with_future_deletion_date_applies():
    """The deletion date is the planned end date; until then the citizen is protected."""
    assert _beskyttelse(_0001(start="2000-01-01", slet="2999-12-31")) is True


def test_address_protection_ends_on_the_deletion_date():
    assert _beskyttelse(_0001(start="2000-01-01", slet="2020-01-01")) is False


def test_address_protection_applies_only_from_the_start_date():
    assert _beskyttelse(_0001(start="2999-01-01")) is False


def test_address_protection_is_decided_on_the_day():
    from datetime import date

    from DSA.FHIR.Patient.patient_cpr_contribution import _address_protected

    reg = _register(protections=(_0001(start="2026-01-01", slet="2026-10-01"),))
    assert _address_protected(reg, idag=date(2026, 9, 30)) is True
    assert _address_protected(reg, idag=date(2026, 10, 1)) is False
    assert _address_protected(reg, idag=date(2025, 12, 31)) is False


# ------------------------------------------------------------------ through the builder


def _epj_row():
    return {
        "PATIENT_ID": "P1",
        "CPR": PNR,
        "FORNAVNE": "Gammel",
        "EFTERNAVN": "Navn",
        "KOEN_KODE": "M",
        "FOEDSELSDATO": "1979-12-31",
        "VEJ": "Epjvej 1",
        "POSTNR": "6000",
        "STEDNAVN": "Kolding",
        "KOMMUNEKODE": "0621",
        "LAEGENAVN": "Læge Lægesen",
        "LAEGENUMMER": "123456",
        "PERSON_STATUSKODE": "PAS_ks.ur.CPRStatus.90",
        "HAR_ADRESSEBESKYTTELSE": "1",
        "PERSON_DISTRIKTKODE": "D1",
        "SLETTET": "0",
    }


def test_register_wins_its_fields_and_epj_keeps_its_own():
    (epj,) = contributions_from_ifdb([{**_epj_row(), "HAR_ADRESSEBESKYTTELSE": "0"}], MagicMock())
    reg = contribution_from_register(
        _register(protections=()), current_cpr=PNR, historical_cprs=[PNR_OLD]
    )
    p = build([epj, reg])
    assert p.name[0].family == "Testesen" and p.gender == "female"
    assert str(p.birthDate) == "1980-01-01"
    assert p.active is True and p.deceasedBoolean is False  # EPJ's 90 loses to the register
    assert len(p.address) == 3 and p.address[-1].postalCode == "5000"
    assert _ext(p, EXTENSION_ADDRESS_PROTECTION).valueBoolean is False
    assert _ids(p, SYSTEM_CPR) == [(PNR, None), (PNR_OLD, USE_OLD)]
    assert _ids(p, SYSTEM_IFDB_PATIENT_ID) == [("P1", None)]
    assert p.generalPractitioner[0].display == "Læge Lægesen"
    assert [t.code for t in p.meta.tag] == [SOURCE_CPR, SOURCE_IFDB]


def test_stub_and_register_give_the_registers_person_with_the_sources_tag():
    p = build(
        [
            stub_contribution(PNR, SOURCE_HAIBA),
            contribution_from_register(_register(protections=()), current_cpr=PNR),
        ]
    )
    assert p.name[0].family == "Testesen"
    assert [t.code for t in p.meta.tag] == [SOURCE_CPR, SOURCE_HAIBA]


# ------------------------------------------------------------------ the adapter


def test_register_person_takes_the_person_record_under_the_current_number_and_gathers_history():
    rows = RegisterRows(
        single={
            "person": {
                PNR: {"pnr": PNR, "status": "01"},
                PNR_OLD: {"pnr": PNR_OLD, "status": "60"},
            },
            "name": {PNR_OLD: {"pnr": PNR_OLD, "fornvn": "Kun under gammelt"}},
        },
        multi={
            "history_addresses": {
                PNR: [{"pnr": PNR, "tilflydto": "2"}],
                PNR_OLD: [{"pnr": PNR_OLD, "tilflydto": "1"}],
            },
            "protections": {},
        },
    )
    reg = register_person(rows, (PNR, PNR_OLD))
    assert reg.person["status"] == "01"
    assert reg.name["fornvn"] == "Kun under gammelt"  # single row: fall back to another number
    assert [r["tilflydto"] for r in reg.history_addresses] == ["2", "1"]
    assert reg.protections == () and reg.address is None


def test_a_previous_numbers_address_does_not_become_the_persons_current():
    rows = RegisterRows(
        single={
            "person": {PNR: {"pnr": PNR, "status": "30"}},
            "address": {PNR_OLD: {"pnr": PNR_OLD, "komkod": "0621"}},
            "plain_address": {PNR_OLD: {"pnr": PNR_OLD, "standardadr": "Gammelvej 1"}},
        },
    )
    reg = register_person(rows, (PNR, PNR_OLD))
    assert reg.address is None and reg.plain_address is None


def test_historical_address_without_end_date_is_omitted_and_counted():
    modul.ufortolkede_vaerdier()
    reg = _register(
        history_addresses=(
            {
                "pnr": PNR,
                "annkor": "",
                "komkod": "0621",
                "tilflydto": "199001010000",
                "fraflydto": "000000000000",
                "adresse_uuid": "",
            },
        )
    )
    p = build([contribution_from_register(reg, current_cpr=PNR)])
    assert [a.use for a in p.address] == ["home"]
    assert modul.ufortolkede_vaerdier() == {"historisk_adresse_uden_slut": 1}


def test_register_person_without_person_record_is_no_register_person():
    assert register_person(RegisterRows(), (PNR,)) is None
    assert (
        register_person(RegisterRows(single={"person": {PNR_OLD: {"pnr": PNR_OLD}}}), (PNR,))
        is None
    )
    assert (
        register_person(
            RegisterRows(single={"person": {PNR_OLD: {"pnr": PNR_OLD}}}), (PNR, PNR_OLD)
        )
        is not None
    )


def test_fetch_register_rows_reads_each_table_on_pnr_without_deleted(monkeypatch):
    kald = []

    def _fetch(ids, *, table, columns, where_column, mssql, include_deleted=False, **_):
        kald.append((table, tuple(columns), where_column, include_deleted, tuple(ids)))
        return (
            [{"pnr": PNR, **{c: "x" for c in columns if c != "pnr"}}] if "023" not in table else []
        )

    monkeypatch.setattr(modul, "chunked_in_fetch", _fetch)
    rows = fetch_register_rows(object(), [PNR, " ", None, PNR])
    assert {k[0] for k in kald} == {t for t, _ in REGISTER_TABLES.values()}
    assert all(k[2] == "pnr" and k[3] is False and k[4] == (PNR,) for k in kald)
    assert rows.single["person"][PNR]["status"] == "x"
    assert rows.multi["history_addresses"] == {}


def test_fetch_register_rows_without_numbers_does_not_hit_the_database(monkeypatch):
    monkeypatch.setattr(modul, "chunked_in_fetch", lambda *a, **k: pytest.fail("called"))
    assert fetch_register_rows(object(), []).single == {}


def test_open_register_holds_the_publication_lock_and_answers_membership(monkeypatch):
    hændelser = []

    @contextmanager
    def _published(db):
        hændelser.append("åben")
        yield True
        hændelser.append("lukket")

    monkeypatch.setattr(modul, "published_cpr", _published)
    monkeypatch.setattr(modul, "register_members", lambda mssql, cprs: {PNR} & set(cprs))
    monkeypatch.setattr(
        modul,
        "fetch_register_rows",
        lambda mssql, cprs: RegisterRows(single={"person": {PNR: {"pnr": PNR, "status": "01"}}}),
    )
    with modul.open_register(object()) as register:
        assert register.available is True
        assert register.members({PNR, PNR_OLD}) == {PNR}
        person = Person(key=PNR, cprs=(PNR,), epj_ids=(), rows=(), fhir_id=None)
        uden = Person(key="epj:P9", cprs=(), epj_ids=("P9",), rows=(), fhir_id=None)
        found = register.persons([person, uden])
        assert set(found) == {PNR} and found[PNR].person["status"] == "01"
        assert hændelser == ["åben"]
    assert hændelser == ["åben", "lukket"]


def test_without_register_there_are_neither_members_nor_contributions(monkeypatch):
    @contextmanager
    def _published(db):
        yield False

    monkeypatch.setattr(modul, "published_cpr", _published)
    monkeypatch.setattr(modul, "register_members", lambda *a, **k: pytest.fail("read"))
    with modul.open_register(object()) as register:
        assert register.available is False
        assert register.members({PNR}) == set()
        assert (
            register.persons([Person(key=PNR, cprs=(PNR,), epj_ids=(), rows=(), fhir_id=None)])
            == {}
        )


# ------------------------------------------------------------------ the contract with the DDL

DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl" / "sources" / "cpr"


def test_every_column_the_contribution_reads_exists_in_the_archive_ddl():
    ddl = "\n".join(p.read_text(encoding="utf-8") for p in sorted(DDL_DIR.glob("*.sql")))
    for table, columns in REGISTER_TABLES.values():
        navn = table.split(".", 1)[1]
        blok = re.search(rf"CREATE TABLE \[arc\]\.\[{navn}\] \((.*?)\n    \);", ddl, re.S)
        assert blok, table
        for column in columns:
            assert f"[{column}]" in blok.group(1), f"{table} is missing {column}"


def test_technical_change_is_a_valid_period_corrected_and_withdrawn_are_not():
    """Æ is a technical change of a valid registration; K and A do not apply."""

    def _raekke(kode, start):
        return {
            "pnr": PNR,
            "annkor": kode,
            "komkod": "0461",
            "tilflydto": f"{start}010000",
            "fraflydto": f"{start}280000",
            "adresse_uuid": f"u-{kode}",
        }

    p = contribution_from_register(
        _register(
            history_addresses=(
                _raekke("Æ", "200502"),
                _raekke("K", "200602"),
                _raekke("A", "200702"),
                _raekke("æ", "200802"),
            )
        ),
        current_cpr=PNR,
    ).resource
    starter = sorted(str(a.period.start) for a in p.address if a.use == "old")
    assert starter == ["2005-02-01", "2008-02-01"]


def test_values_that_cannot_be_interpreted_are_counted_per_field():
    """An unknown code or an invalid date does not disappear silently."""
    from DSA.FHIR.Patient.patient_cpr_contribution import ufortolkede_vaerdier

    ufortolkede_vaerdier()
    reg = _register(marital={"pnr": PNR, "civst": "?"})
    reg.person["koen"] = "X"
    reg.person["foed_dt"] = "1980-13-45"
    contribution_from_register(reg, current_cpr=PNR)
    assert ufortolkede_vaerdier() == {"civilstand": 1, "koen": 1, "dato": 1}
    assert ufortolkede_vaerdier() == {}


def test_zero_dates_and_dead_are_not_counted_as_uninterpreted():
    from DSA.FHIR.Patient.patient_cpr_contribution import ufortolkede_vaerdier

    ufortolkede_vaerdier()
    reg = _register(marital={"pnr": PNR, "civst": "D"})
    reg.person["foed_dt"] = "0000-00-00"
    contribution_from_register(reg, current_cpr=PNR)
    assert ufortolkede_vaerdier() == {}


def test_update_date_is_the_latest_delivery_across_the_rows():
    reg = _register(
        person={"pnr": PNR, "status": "01", "koen": "K", "leverance_dato": date(2026, 6, 9)},
        marital={"pnr": PNR, "civst": "G", "leverance_dato": date(2026, 9, 29)},
        history_addresses=(
            {
                "pnr": PNR,
                "annkor": "",
                "tilflydto": "200001010000",
                "leverance_dato": date(2026, 8, 3),
            },
        ),
    )
    p = contribution_from_register(reg, current_cpr=PNR, historical_cprs=[]).resource
    assert _ext(p, EXTENSION_CPR_UPDATED).valueDate.isoformat() == "2026-09-29"


def test_without_delivery_date_no_update_date_is_asserted():
    p = contribution_from_register(_register(), current_cpr=PNR, historical_cprs=[]).resource
    assert _ext(p, EXTENSION_CPR_UPDATED) is None
