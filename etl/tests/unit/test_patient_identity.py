# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Identity resolution: rules A, B and D without a database.

Personal numbers in the tests are synthetic (day 00, month 00). EPJ ids are arbitrary.
"""

import pytest
from DSA.FHIR.Patient.patient_identity import (
    IdentityConflict,
    IdentityInputs,
    IdentityRequest,
    order_rows,
    resolve_identities,
    row_is_deleted,
)

pytestmark = pytest.mark.unit

PNR_A = "0000000001"
PNR_A_OLD = "0000000009"  # A's previous number
PNR_B = "0000000002"
PNR_C = "0000000003"


def _row(
    patient_id: str, cpr: str | None, slettet: str = "0", opdateret: str = "2026-01-01"
) -> dict:
    return {"PATIENT_ID": patient_id, "CPR": cpr, "SLETTET": slettet, "OPDATERET_TID": opdateret}


# ------------------------------------------------------------------ rule A


def test_epj_duplicates_with_same_personal_number_are_one_person():
    """Two EPJ ids, same personal number: one person with both ids, primary first."""
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        IdentityInputs(
            archive_rows=[
                _row("P1", PNR_A, slettet="1", opdateret="2026-03-01"),  # deleted duplicate
                _row("P2", PNR_A, slettet="0", opdateret="2025-01-01"),  # the active one
            ]
        ),
    )
    assert len(res.persons) == 1
    person = res.persons[0]
    assert person.key == PNR_A
    assert person.epj_ids == ("P2", "P1")  # non-deleted first, regardless of age
    assert person.primary_row["PATIENT_ID"] == "P2"
    assert res.by_ifdb_id["P1"] is person and res.by_ifdb_id["P2"] is person
    assert res.by_cpr[PNR_A] is person
    assert person.fhir_id is None


def test_primary_row_is_the_latest_updated_among_the_active():
    rows = [_row("P1", PNR_A, opdateret="2026-01-01"), _row("P2", PNR_A, opdateret="2026-06-01")]
    assert [r["PATIENT_ID"] for r in order_rows(rows)] == ["P2", "P1"]


def test_primary_row_follows_the_producers_dedup_rule():
    """The write point must pick the same row as the producer's _dedup_rows_by_cpr — otherwise
    a patient would get different demographics depending on who asked for her."""
    from DSA.FHIR.Patient.patient_ifdb_contribution import _dedup_rows_by_cpr

    rows = [
        _row("P1", PNR_A, slettet="1", opdateret="2026-09-01"),
        _row("P2", PNR_A, slettet="0", opdateret="2026-02-01"),
        _row("P3", PNR_A, slettet="0", opdateret="2026-05-01"),
    ]
    assert order_rows(rows)[0]["PATIENT_ID"] == _dedup_rows_by_cpr(rows)[0]["PATIENT_ID"] == "P3"


def test_row_without_update_time_is_not_the_newest():
    """The SQL's DESC puts NULL last; an empty time must not win over a real one."""
    rows = [_row("P1", PNR_A, opdateret=None), _row("P2", PNR_A, opdateret="2026-01-01")]
    assert [r["PATIENT_ID"] for r in order_rows(rows)] == ["P2", "P1"]
    rows = [_row("P1", PNR_A, opdateret=""), _row("P2", PNR_A, opdateret="2026-01-01")]
    assert [r["PATIENT_ID"] for r in order_rows(rows)] == ["P2", "P1"]


def test_the_longer_of_two_timestamps_with_same_start_is_newest():
    rows = [
        _row("P1", PNR_A, opdateret="2024-05-01"),
        _row("P2", PNR_A, opdateret="2024-05-01 10:00"),
    ]
    assert [r["PATIENT_ID"] for r in order_rows(rows)] == ["P2", "P1"]


def test_same_time_is_settled_by_lowest_patient_id():
    rows = [_row("P2", PNR_A, opdateret="2026-01-01"), _row("P1", PNR_A, opdateret="2026-01-01")]
    assert [r["PATIENT_ID"] for r in order_rows(rows)] == ["P1", "P2"]


@pytest.mark.parametrize(
    ("slettet", "forventet"),
    [("0", False), ("0.0", False), (None, False), (0, False), ("1", True), ("", True)],
)
def test_deleted_follows_the_sql_rule(slettet, forventet):
    """ISNULL(SLETTET, '0') IN ('0', '0.0') is live; an empty text is deleted."""
    assert row_is_deleted({"PATIENT_ID": "P1", "SLETTET": slettet}) is forventet


def test_row_without_personal_number_is_its_own_person():
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P9"]),
        IdentityInputs(archive_rows=[_row("P9", None)]),
    )
    assert len(res.persons) == 1
    assert res.persons[0].key == "epj:P9"
    assert res.persons[0].cprs == ()
    assert res.persons[0].current_cpr is None


def test_two_different_persons_are_kept_apart():
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1", "P2"]),
        IdentityInputs(archive_rows=[_row("P1", PNR_A), _row("P2", PNR_B)]),
    )
    assert [p.key for p in res.persons] == sorted([PNR_A, PNR_B])
    assert res.by_ifdb_id["P1"] is not res.by_ifdb_id["P2"]


# ------------------------------------------------------------------ rule B


def test_previous_personal_number_is_led_to_the_current_one():
    """An Encounter on the old number and the EPJ row on the new one are the same person."""
    res = resolve_identities(
        IdentityRequest.of(cprs=[PNR_A_OLD]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_A)],
            current_by_historical={PNR_A_OLD: PNR_A},
        ),
    )
    assert len(res.persons) == 1
    person = res.persons[0]
    assert person.key == PNR_A
    assert person.cprs == (PNR_A, PNR_A_OLD)  # current first
    assert res.by_cpr[PNR_A_OLD] is person
    assert person.epj_ids == ("P1",)


def test_a_lone_historical_number_becomes_a_person_on_the_current_one():
    res = resolve_identities(
        IdentityRequest.of(cprs=[PNR_A_OLD]),
        IdentityInputs(current_by_historical={PNR_A_OLD: PNR_A}, fhir_ids_by_cpr={PNR_A: "f-1"}),
    )
    assert res.persons[0].key == PNR_A
    assert res.persons[0].fhir_id == "f-1"
    assert res.unknown_cprs == ()


# ------------------------------------------------------------------ rule D


def test_existing_resource_is_reused():
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        IdentityInputs(archive_rows=[_row("P1", PNR_A)], fhir_ids_by_cpr={PNR_A: "f-1"}),
    )
    assert res.persons[0].fhir_id == "f-1"


def test_resource_found_on_epj_id_and_on_personal_number_is_the_same():
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_A)],
            fhir_ids_by_epj_id={"P1": "f-1"},
            fhir_ids_by_cpr={PNR_A: "f-1"},
        ),
    )
    assert res.persons[0].fhir_id == "f-1"


def test_known_epj_ids_on_the_resource_decide_whether_it_is_complete():
    """P1 is on the resource, P2 is not: the person exists, but the resource is incomplete."""
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P2"]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_A), _row("P2", PNR_A, slettet="1")],
            fhir_ids_by_epj_id={"P1": "f-1"},
            fhir_ids_by_cpr={PNR_A: "f-1"},
        ),
    )
    person = res.persons[0]
    assert person.fhir_id == "f-1"
    assert person.known_epj_ids == ("P1",)
    assert person.resource_is_complete is False
    komplet = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        IdentityInputs(archive_rows=[_row("P1", PNR_A)], fhir_ids_by_epj_id={"P1": "f-1"}),
    ).persons[0]
    assert komplet.resource_is_complete is True


def test_resource_carrying_two_identities_links_them():
    """The bridge from today: a resource with both P1 and P2 as EPJ id. Without archive rows
    for P2 they are linked anyway — the resource itself says they are the same person."""
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P2"]),
        IdentityInputs(fhir_ids_by_epj_id={"P1": "f-1", "P2": "f-1"}),
    )
    assert len(res.persons) == 1
    assert set(res.persons[0].epj_ids) == {"P1", "P2"}
    assert res.persons[0].fhir_id == "f-1"


def test_two_resources_for_one_person_are_split_per_resource_and_counted():
    """Same number on two EPJ rows that each have their own resource: not an error, but
    today's state — written per resource and counted, so the measurement can show it."""
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1", "P2"]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_A), _row("P2", PNR_A)],
            fhir_ids_by_epj_id={"P1": "f-1", "P2": "f-2"},
        ),
    )
    assert res.conflicts == 1
    assert {p.fhir_id: p.epj_ids for p in res.persons} == {"f-1": ("P1",), "f-2": ("P2",)}


def test_error_message_does_not_carry_the_personal_number():
    with pytest.raises(IdentityConflict) as fejl:
        resolve_identities(
            IdentityRequest.of(ifdb_patient_ids=["P1"]),
            IdentityInputs(
                archive_rows=[_row("P1", PNR_A), _row("P2", PNR_B)],
                fhir_ids_by_epj_id={"P1": "f-1", "P2": "f-1"},
            ),
        )
    assert PNR_A not in str(fejl.value) and PNR_B not in str(fejl.value)


def test_a_number_the_register_knows_supersedes_a_replacement_number():
    """A patient first registered under a replacement number, then identified.

    EPJ corrects the row to the real number while the resource still carries the replacement
    number. The register knows only the real one, so it is current and the other is previous.
    """
    resolution = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_B)],
            fhir_ids_by_epj_id={"P1": "f-1"},
            fhir_ids_by_cpr={PNR_A: "f-1"},
            register_cprs={PNR_B},
        ),
    )
    (person,) = resolution.persons
    assert person.cprs == (PNR_B, PNR_A) and person.key == PNR_B
    assert person.fhir_id == "f-1" and person.in_register
    assert resolution.superseded_numbers == 1


def test_two_current_numbers_both_known_to_the_register_is_still_a_hard_failure():
    with pytest.raises(IdentityConflict, match="several current personal numbers"):
        resolve_identities(
            IdentityRequest.of(cprs=[PNR_A]),
            IdentityInputs(
                fhir_ids_by_cpr={PNR_A: "f-1", PNR_B: "f-1"}, register_cprs={PNR_A, PNR_B}
            ),
        )


def test_two_current_numbers_on_one_person_is_a_hard_failure():
    """A resource carrying two different current personal numbers cannot be one person."""
    with pytest.raises(IdentityConflict, match="several current personal numbers"):
        resolve_identities(
            IdentityRequest.of(cprs=[PNR_A]),
            IdentityInputs(fhir_ids_by_cpr={PNR_A: "f-1", PNR_B: "f-1"}),
        )


# ------------------------------------------------------------------ unknown


def test_unknown_identities_do_not_become_persons():
    """Not in the archive, in 022 or in FHIR: the caller decides whether it becomes a stub."""
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P404"], cprs=[PNR_C]),
        IdentityInputs(),
    )
    assert res.persons == []
    assert res.unknown_ifdb_ids == ("P404",)
    assert res.unknown_cprs == (PNR_C,)


def test_personal_number_known_only_from_fhir_is_a_person_without_epj():
    """A HAIBA stub that already exists: person with a resource, but without rows."""
    res = resolve_identities(
        IdentityRequest.of(cprs=[PNR_B]), IdentityInputs(fhir_ids_by_cpr={PNR_B: "f-7"})
    )
    assert len(res.persons) == 1
    assert res.persons[0].fhir_id == "f-7"
    assert res.persons[0].has_epj is False
    assert res.persons[0].epj_ids == ()


def test_identities_are_normalised_for_whitespace():
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=[" P1 "]),
        IdentityInputs(archive_rows=[{"PATIENT_ID": "P1", "CPR": f" {PNR_A} ", "SLETTET": None}]),
    )
    assert res.persons[0].key == PNR_A
    assert res.by_ifdb_id["P1"].epj_ids == ("P1",)


def test_deleted_flag_is_read_as_the_producer_does():
    assert row_is_deleted({"SLETTET": "1"}) is True
    assert row_is_deleted({"SLETTET": "0.0"}) is False
    assert row_is_deleted({"SLETTET": None}) is False
    assert row_is_deleted({}) is False


# ------------------------------------------------------------------ several resources today


def test_old_and_new_number_on_separate_resources_stay_split_and_are_counted():
    """Today's state for a number change: both numbers have got their own resource.
    Nothing is merged — each resource keeps its identities — but it is counted."""
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_A_OLD), _row("P2", PNR_A)],
            current_by_historical={PNR_A_OLD: PNR_A},
            fhir_ids_by_epj_id={"P1": "f-gammel", "P2": "f-ny"},
            fhir_ids_by_cpr={PNR_A_OLD: "f-gammel", PNR_A: "f-ny"},
        ),
    )
    assert res.conflicts == 1
    assert {p.fhir_id: (p.cprs, p.epj_ids) for p in res.persons} == {
        "f-gammel": ((PNR_A_OLD,), ("P1",)),
        "f-ny": ((PNR_A,), ("P2",)),
    }
    assert res.by_ifdb_id["P1"].fhir_id == "f-gammel" and res.by_cpr[PNR_A].fhir_id == "f-ny"


def test_identities_without_resource_follow_their_nearest_when_split():
    """P3 (new number, no resource yet) belongs to the new number's resource; a
    historical number without a resource belongs to the current one's."""
    H2 = "0000000099"
    res = resolve_identities(
        IdentityRequest.of(cprs=[PNR_A]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_A_OLD), _row("P2", PNR_A), _row("P3", PNR_A, slettet="1")],
            current_by_historical={PNR_A_OLD: PNR_A, H2: PNR_A},
            fhir_ids_by_epj_id={"P1": "f-gammel", "P2": "f-ny"},
            fhir_ids_by_cpr={PNR_A_OLD: "f-gammel"},
        ),
    )
    assert res.conflicts == 1
    ny = res.by_cpr[PNR_A]
    assert ny.fhir_id == "f-ny" and ny.epj_ids == ("P2", "P3") and ny.cprs == (PNR_A, H2)
    assert res.by_cpr[PNR_A_OLD].epj_ids == ("P1",)
    assert res.unknown_cprs == ()


def test_one_resource_per_person_is_not_split():
    res = resolve_identities(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        IdentityInputs(
            archive_rows=[_row("P1", PNR_A_OLD), _row("P2", PNR_A)],
            current_by_historical={PNR_A_OLD: PNR_A},
            fhir_ids_by_epj_id={"P1": "f-1", "P2": "f-1"},
        ),
    )
    assert res.conflicts == 0 and len(res.persons) == 1


# ------------------------------------------------------------------ history


def test_history_with_two_current_numbers_is_a_hard_failure_in_any_order():
    from DSA.FHIR.Patient.patient_identity import add_history

    for edges in (
        [(PNR_A_OLD, PNR_A), (PNR_A_OLD, PNR_B)],
        [(PNR_A_OLD, PNR_B), (PNR_A_OLD, PNR_A)],
    ):
        history: dict[str, str] = {}
        with pytest.raises(IdentityConflict, match="two different current"):
            for h, c in edges:
                add_history(history, h, c)


def test_history_is_followed_through_the_chain_and_cycles_are_rejected():
    from DSA.FHIR.Patient.patient_identity import canonical_history

    assert canonical_history({"H": "C", "C": "D"}) == {"H": "D", "C": "D"}
    with pytest.raises(IdentityConflict, match="cycle"):
        canonical_history({"A": "B", "B": "A"})


def test_chain_of_numbers_becomes_one_person_with_the_last_as_current():
    res = resolve_identities(
        IdentityRequest.of(cprs=["H"]),
        IdentityInputs(current_by_historical={"H": "C", "C": "D"}, fhir_ids_by_cpr={"D": "f-1"}),
    )
    assert res.persons[0].cprs == ("D", "C", "H")
    assert res.persons[0].fhir_id == "f-1"


# ------------------------------------------------------------------ the adapter
#
# The path that computes the union in rounds (``union=False``), for an environment where
# ref.PatientIdentity is not built. The default lookup path is tested last.


class _Opslag:
    """Fake for archive, 022 and FHIR lookups; remembers what was asked for."""

    def __init__(
        self, archive_by_id, archive_by_cpr, history_by_pnr, history_by_pnrgaeld, fhir_epj, fhir_cpr
    ):
        self.kald: list[tuple] = []
        self._svar = {
            ("arc.ifdb_patient", "PATIENT_ID"): archive_by_id,
            ("arc.ifdb_patient", "CPR"): archive_by_cpr,
            ("arc.cpr_022_historisk_personnummer", "pnr"): history_by_pnr,
            ("arc.cpr_022_historisk_personnummer", "pnrgaeld"): history_by_pnrgaeld,
        }
        self._fhir = {"urn:source:ifdb:patient_id": fhir_epj, "urn:dk:cpr": fhir_cpr}

    def chunked_in_fetch(
        self, ids, *, table, columns, where_column, mssql, include_deleted=False, **_
    ):
        self.kald.append(("arkiv", table, where_column, tuple(ids), include_deleted))
        return [
            r for r in self._svar[(table, where_column)] if str(r[where_column]).strip() in set(ids)
        ]

    def fetch_fhir_ids(self, fhir_database, system, values):
        self.kald.append(("fhir", system, tuple(sorted(values))))
        return {k: v for k, v in self._fhir[system].items() if k in set(values)}


def _patch(monkeypatch, opslag):
    from DSA.FHIR.Patient import patient_identity as modul

    monkeypatch.setattr(modul, "chunked_in_fetch", opslag.chunked_in_fetch)
    monkeypatch.setattr(modul, "fetch_fhir_ids", opslag.fetch_fhir_ids)
    monkeypatch.setattr(
        modul,
        "fetch_fhir_aliases",
        lambda db, ids: tuple(
            {value: fid for value, fid in opslag._fhir[system].items() if fid in ids}
            for system in ("urn:source:ifdb:patient_id", "urn:dk:cpr")
        ),
    )
    monkeypatch.setattr(modul, "_archive_columns", lambda: ("PATIENT_ID", "CPR", "SLETTET"))
    return modul


def test_adapter_fetches_duplicates_and_previous_numbers_in_batch(monkeypatch):
    """Asked for P1 alone, P1's duplicate P2 and the old number must come along."""
    opslag = _Opslag(
        archive_by_id=[_row("P1", PNR_A)],
        archive_by_cpr=[_row("P1", PNR_A), _row("P2", PNR_A, slettet="1")],
        history_by_pnr=[],
        history_by_pnrgaeld=[{"pnr": PNR_A_OLD, "pnrgaeld": PNR_A}],
        fhir_epj={"P1": "f-1"},
        fhir_cpr={PNR_A: "f-1"},
    )
    modul = _patch(monkeypatch, opslag)

    res = modul.resolve(
        IdentityRequest.of(ifdb_patient_ids=["P1"]),
        mssql=object(),
        fhir_database=object(),
        union=False,
    )

    person = res.persons[0]
    assert person.epj_ids == ("P1", "P2")
    assert person.cprs == (PNR_A, PNR_A_OLD)
    assert person.fhir_id == "f-1"
    # the archive is looked up with deleted rows, 022 both ways, FHIR on both systems
    arkiv = [k for k in opslag.kald if k[0] == "arkiv"]
    assert all(k[4] for k in arkiv if k[1] == "arc.ifdb_patient")
    assert {(k[1], k[2]) for k in arkiv} >= {
        ("arc.cpr_022_historisk_personnummer", "pnr"),
        ("arc.cpr_022_historisk_personnummer", "pnrgaeld"),
        ("arc.ifdb_patient", "CPR"),
    }
    fhir = [k for k in opslag.kald if k[0] == "fhir"]
    assert {k[1] for k in fhir} == {"urn:source:ifdb:patient_id", "urn:dk:cpr"}
    # the old number, first found through 022, is also looked up in FHIR
    assert PNR_A_OLD in {v for k in fhir if k[1] == "urn:dk:cpr" for v in k[2]}


def test_adapter_closes_the_chain_from_an_old_number(monkeypatch):
    """Starting on H1, C, C's other historical number H2 and C's EPJ rows must come along."""
    H1, H2, C = "0000000011", "0000000012", "0000000013"
    opslag = _Opslag(
        archive_by_id=[],
        archive_by_cpr=[_row("P7", C), _row("P8", C, slettet="1")],
        history_by_pnr=[{"pnr": H1, "pnrgaeld": C}, {"pnr": H2, "pnrgaeld": C}],
        history_by_pnrgaeld=[{"pnr": H1, "pnrgaeld": C}, {"pnr": H2, "pnrgaeld": C}],
        fhir_epj={},
        fhir_cpr={},
    )
    modul = _patch(monkeypatch, opslag)
    res = modul.resolve(
        IdentityRequest.of(cprs=[H1]), mssql=object(), fhir_database=object(), union=False
    )
    person = res.persons[0]
    assert person.cprs == (C, H1, H2)
    assert person.epj_ids == ("P7", "P8")
    # and the result is the same when starting on the current number
    res2 = modul.resolve(
        IdentityRequest.of(cprs=[C]), mssql=object(), fhir_database=object(), union=False
    )
    assert (res2.persons[0].cprs, res2.persons[0].epj_ids) == (person.cprs, person.epj_ids)


def test_adapter_rejects_history_with_two_current_numbers(monkeypatch):
    H, C1, C2 = "0000000021", "0000000022", "0000000023"
    opslag = _Opslag([], [], [{"pnr": H, "pnrgaeld": C1}, {"pnr": H, "pnrgaeld": C2}], [], {}, {})
    modul = _patch(monkeypatch, opslag)
    with pytest.raises(IdentityConflict):
        modul.resolve(
            IdentityRequest.of(cprs=[H]), mssql=object(), fhir_database=object(), union=False
        )


def test_adapter_does_not_fetch_rows_it_already_has(monkeypatch):
    """The producer has its rows; they are not looked up again on EPJ id."""
    opslag = _Opslag([], [_row("P1", PNR_A)], [], [], {}, {})
    modul = _patch(monkeypatch, opslag)
    res = modul.resolve(
        IdentityRequest.of(),
        mssql=object(),
        fhir_database=object(),
        rows=[_row("P1", PNR_A)],
        union=False,
    )
    assert res.persons[0].epj_ids == ("P1",)
    assert not any(k[2] == "PATIENT_ID" for k in opslag.kald if k[0] == "arkiv")


def test_no_identities_gives_no_lookups(monkeypatch):
    opslag = _Opslag([], [], [], [], {}, {})
    modul = _patch(monkeypatch, opslag)
    res = modul.resolve(IdentityRequest.of(), mssql=object(), fhir_database=object(), union=False)
    assert res.persons == [] and opslag.kald == []


def _needle(value):
    return ("urn:source:ifdb:patient_id", value)


def test_fhir_lookup_reads_all_identifiers_and_rejects_two_resources():
    """The lookup must find the value on any identifier in the system — also
    when it is primary on one resource and alias on another — and reject two
    resources for one value."""
    from DSA.FHIR.Patient.patient_identity import fetch_fhir_ids

    class _Cur:
        def __init__(self, rows):
            self.rows, self.sql, self.params = rows, None, None
            self.alle = []

        def execute(self, sql, params=None):
            self.alle.append(repr(sql))
            if "enable_seqscan" in repr(sql):
                return
            self.sql, self.params = sql, params

        @property
        def indekstvang(self):
            return any("enable_seqscan = off" in s for s in self.alle)

        def fetchall(self):
            return self.rows

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    class _Conn:
        def __init__(self, cur):
            self._cur = cur

        def cursor(self):
            return self._cur

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    class _Db:
        def __init__(self, cur):
            self._cur = cur

        def get_connection(self):
            return _Conn(self._cur)

    cur = _Cur([("f-1", "P1"), ("f-1", "P2")])
    assert fetch_fhir_ids(_Db(cur), "urn:source:ifdb:patient_id", ["P1", "P2", " P1 "]) == {
        "P1": "f-1",
        "P2": "f-1",
    }
    # Each value is probed with containment through unnest — never "@> ANY", never only primary.
    assert "unnest" in repr(cur.sql) and "@> ANY" not in repr(cur.sql)
    assert cur.indekstvang, "the planner must not get to choose between index and scan"
    assert cur.params == (
        ["urn:source:ifdb:patient_id", "urn:source:ifdb:patient_id"],
        ["P1", "P2"],
    )
    with pytest.raises(IdentityConflict):
        fetch_fhir_ids(
            _Db(_Cur([("f-1", "P1"), ("f-2", "P1")])), "urn:source:ifdb:patient_id", ["P1"]
        )
    assert fetch_fhir_ids(_Db(cur), "urn:dk:cpr", []) == {}


def test_stored_alias_expands_to_archive_and_number_history(monkeypatch):
    # The old number and the EPJ id are only on the stored resource; they must still
    # lead to the archive and 022.
    opslag = _Opslag(
        archive_by_id=[_row("P1", PNR_A_OLD)],
        archive_by_cpr=[],
        history_by_pnr=[{"pnr": PNR_A_OLD, "pnrgaeld": PNR_A}],
        history_by_pnrgaeld=[],
        fhir_epj={"P1": "f-1"},
        fhir_cpr={PNR_A: "f-1", PNR_A_OLD: "f-1"},
    )
    modul = _patch(monkeypatch, opslag)
    result = modul.resolve(IdentityRequest.of(cprs=[PNR_A]), None, None, union=False)
    assert len(result.persons) == 1
    assert result.persons[0].cprs == (PNR_A, PNR_A_OLD)
    assert result.persons[0].epj_ids == ("P1",)
    assert result.persons[0].primary_row == _row("P1", PNR_A_OLD)


# ------------------------------------------------------------------ rule C


def test_a_number_the_register_knows_is_a_person_without_epj_row():
    res = resolve_identities(
        IdentityRequest.of(cprs=[PNR_A, PNR_B]), IdentityInputs(register_cprs={PNR_A})
    )
    (person,) = res.persons
    assert (
        person.key == PNR_A and person.in_register and person.rows == () and person.fhir_id is None
    )
    assert res.unknown_cprs == (PNR_B,)


def test_membership_also_follows_a_previous_number():
    res = resolve_identities(
        IdentityRequest.of(cprs=[PNR_A_OLD]),
        IdentityInputs(current_by_historical={PNR_A_OLD: PNR_A}, register_cprs={PNR_A}),
    )
    (person,) = res.persons
    assert person.cprs == (PNR_A, PNR_A_OLD) and person.in_register


def test_adapter_asks_for_membership_when_given_a_responder(monkeypatch):
    opslag = _Opslag(
        archive_by_id=[_row("P1", PNR_A)],
        archive_by_cpr=[],
        history_by_pnr=[],
        history_by_pnrgaeld=[],
        fhir_epj={},
        fhir_cpr={},
    )
    modul = _patch(monkeypatch, opslag)
    spurgt = []

    def _members(cprs):
        spurgt.append(set(cprs))
        return {PNR_A}

    result = modul.resolve(
        IdentityRequest.of(ifdb_patient_ids=["P1"]), None, None, members=_members, union=False
    )
    assert spurgt == [{PNR_A}] and result.persons[0].in_register
    uden = modul.resolve(IdentityRequest.of(ifdb_patient_ids=["P1"]), None, None, union=False)
    assert not uden.persons[0].in_register


# ------------------------------------------------------------------ the union is looked up


class _Forening:
    """ref.PatientIdentity as a fake: rows and what was asked for."""

    def __init__(self, raekker, tabel_findes=True):
        self.raekker = raekker
        self.tabel_findes = tabel_findes
        self.sql: list[str] = []
        self.indsat: list = []

    def get_connection(self):
        ydre = self

        class _Cur:
            def execute(self, sql, params=None):
                ydre.sql.append(sql)
                self._svar = (
                    [(1 if ydre.tabel_findes else 0,)]
                    if "INFORMATION_SCHEMA" in sql
                    else (ydre.raekker if "person_key IN" in sql else [])
                )

            def executemany(self, sql, params):
                ydre.indsat.extend(params)

            def fetchone(self):
                return self._svar[0] if self._svar else None

            def fetchall(self):
                return self._svar

            def close(self):
                pass

        class _Conn:
            def cursor(self):
                return _Cur()

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        return _Conn()


def test_union_is_looked_up_in_one_step_without_rounds():
    """The person's other numbers and ids come in one lookup; the chain is followed beforehand."""
    from DSA.FHIR.Patient.patient_identity import fetch_union

    db = _Forening(
        [
            ("urn:dk:cpr", PNR_A, PNR_A, True),
            ("urn:dk:cpr", PNR_A_OLD, PNR_A, False),
            ("urn:source:ifdb:patient_id", "P1", PNR_A, True),
            ("urn:source:ifdb:patient_id", "P2", PNR_A, False),
        ]
    )
    historik, epj, cprs = fetch_union(db, ["P1"], [])
    assert historik == {PNR_A_OLD: PNR_A}
    assert epj == {"P1", "P2"} and cprs == {PNR_A, PNR_A_OLD}
    # The identities are passed in a temporary table, not as a list of 2,000.
    assert any("#Identiteter" in s for s in db.sql)
    assert ("urn:source:ifdb:patient_id", "P1") in db.indsat


def test_no_identities_does_not_query():
    from DSA.FHIR.Patient.patient_identity import fetch_union

    db = _Forening([])
    assert fetch_union(db, [], []) == ({}, set(), set())
    assert db.sql == []


def test_missing_union_is_a_hard_failure_not_an_empty_answer():
    """Without the table an empty answer would look like "nobody belongs together"."""
    from DSA.FHIR.Patient.patient_identity import fetch_union

    with pytest.raises(IdentityConflict, match="ref.PatientIdentity"):
        fetch_union(_Forening([], tabel_findes=False), ["P1"], [])
