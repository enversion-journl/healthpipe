# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for SilkeborgData Practitioner / PractitionerRole transforms."""

from datetime import date
from unittest.mock import MagicMock

import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.Practitioner.practitioner_common import (
    build_practitioner_query,
    build_practitioner_role_query,
    cpr_birth_date,
    cpr_gender,
    disco_code,
    role_identifier_value,
    split_name,
    usable_date,
)
from DSA.FHIR.Practitioner.Practitioner_SilkeborgData_dfm import transform_practitioner_rows
from DSA.FHIR.Practitioner.PractitionerRole_SilkeborgData_dfm import (
    transform_practitioner_role_rows,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_SILKEBORGDATA_AFDELINGSKODE,
    EXTENSION_SILKEBORGDATA_AFLOEST_CPR,
    EXTENSION_SILKEBORGDATA_INST,
    EXTENSION_SILKEBORGDATA_JOBSTATUSKODE,
    EXTENSION_SILKEBORGDATA_OMRAADEKODE,
    EXTENSION_SILKEBORGDATA_STATUSDATO,
    EXTENSION_SILKEBORGDATA_STATUSKODE,
    EXTENSION_SILKEBORGDATA_TJENESTEKODE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_SILKEBORGDATA_DISCOKODE,
    SYSTEM_SILKEBORGDATA_LEDERKODE,
    SYSTEM_SILKEBORGDATA_SOURCE_TABLE,
    SYSTEM_SILKEBORGDATA_STILLINGSKODE,
    SYSTEM_SILKEBORGDATA_TJENESTENUMMER,
)
from utils.fhir.index_config import IDENTIFIER_UNIQUE_INDEX_SPECS
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

pytestmark = pytest.mark.unit

PERIOD = date(2026, 8, 26)


def _person_row(**overrides) -> dict:
    row = {
        "CprNr": "0101903456",
        "Navn": "Efternavn,Fornavn",
        "Udtraeksdato": PERIOD,
        "_alias_cpr": None,
        "_is_latest": 1,
    }
    row.update(overrides)
    return row


def _role_row(**overrides) -> dict:
    row = {
        "Inst": "OD",
        "Tjenestenummer": "10001",
        "CprNr": "0101903456",
        "_practitioner_cpr": "0101903456",
        "Udtraeksdato": PERIOD,
        "Ansaettelsesdato": date(2015, 3, 1),
        "Stillingskode": "1362",
        "Stilling": "Stillingsbetegnelse",
        "Lederkode": "36",
        "Discokode": "222110",
        "Afdelingskode": "6VFS",
        "Statuskode": "1",
        "Jobstatuskode": "9",
        "Statusdato": date(2015, 3, 1),
        "Tjenestekode": "G",
        "Omraadekode": "1",
        "_is_latest": 1,
        "_episode_key": "20150401",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# CPR validity gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cpr", "expected"),
    [
        # Century from digit 7 together with the two-digit year.
        ("0101903456", date(1990, 1, 1)),  # digit 3 -> 1900s
        ("1506054123", date(2005, 6, 15)),  # digit 4, yy<=36 -> 2000s
        ("1506854123", date(1985, 6, 15)),  # digit 4, yy>36 -> 1900s
        ("1506055123", date(2005, 6, 15)),  # digit 5, yy<=57 -> 2000s
        ("1506855123", date(1885, 6, 15)),  # digit 5, yy>57 -> 1800s
    ],
)
def test_cpr_birth_date_century_rule(cpr, expected):
    assert cpr_birth_date(cpr) == expected


@pytest.mark.parametrize(
    "cpr",
    [
        "0003901234",  # day 00 — the source's administrative form, 32 of 38 cases
        "0113901234",  # month 13
        "3201903456",  # day 32
        "2902013456",  # 29 February in a non-leap year
        "12345",  # too short
        "",
        None,
    ],
)
def test_cpr_birth_date_rejects_unusable(cpr):
    assert cpr_birth_date(cpr) is None
    assert cpr_gender(cpr) is None


def test_letter_in_serial_fails_the_gate_even_though_the_date_part_parses():
    """The gate is the whole number, not the date part.

    01019034X6 carries a perfectly parsable 1990-01-01, but the letter makes the
    number unusable — and its last digit, which carries sex, meaningless. An
    implementation that validated only the date part would emit both fields.
    """
    assert cpr_birth_date("01019034X6") is None
    assert cpr_gender("01019034X6") is None


@pytest.mark.parametrize(("cpr", "expected"), [("0101903456", "female"), ("1506054123", "male")])
def test_cpr_gender_from_last_digit_parity(cpr, expected):
    assert cpr_gender(cpr) == expected


def test_birth_date_suppressed_under_lower_age_bound():
    """A CPR implying age 3 must not produce a birthDate."""
    rows = [_person_row(CprNr="0101234456")]  # digit 4, yy 23 -> 2023
    practitioner = transform_practitioner_rows(rows)[0]
    assert practitioner.birthDate is None


def test_no_upper_age_bound():
    """Pins the retracted 15-75 rule.

    The archive holds real staff born as early as 1918; an upper bound would
    discard correct data about pension-age employees.
    """
    rows = [_person_row(CprNr="0101303456")]  # 1930
    practitioner = transform_practitioner_rows(rows)[0]
    assert practitioner.birthDate == date(1930, 1, 1)


def test_invalid_cpr_still_becomes_an_identifier_without_demographics():
    rows = [_person_row(CprNr="0003901234")]
    practitioner = transform_practitioner_rows(rows)[0]
    assert practitioner.identifier[0].value == "0003901234"
    assert practitioner.gender is None
    assert practitioner.birthDate is None


# ---------------------------------------------------------------------------
# Name
# ---------------------------------------------------------------------------


def test_split_name_on_comma():
    family, given, text = split_name("Efternavn,Fornavn Mellemnavn")
    assert family == "Efternavn"
    assert given == ["Fornavn", "Mellemnavn"]
    assert text == "Efternavn,Fornavn Mellemnavn"


def test_split_name_without_comma_keeps_only_text():
    """No split is guessed for the 31 comma-less source values."""
    family, given, text = split_name("Enkeltvaerdi")
    assert family is None
    assert given == []
    assert text == "Enkeltvaerdi"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_split_name_empty(value):
    assert split_name(value) == (None, [], None)


def test_missing_name_gives_no_humanname():
    practitioner = transform_practitioner_rows([_person_row(Navn=None)])[0]
    assert practitioner.name is None


def test_name_text_always_set_alongside_the_split():
    practitioner = transform_practitioner_rows([_person_row()])[0]
    name = practitioner.name[0]
    assert name.text == "Efternavn,Fornavn"
    assert name.family == "Efternavn"


# ---------------------------------------------------------------------------
# active and provenance
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("is_latest", "expected"), [(1, True), (0, False)])
def test_active_follows_latest_delivery(is_latest, expected):
    practitioner = transform_practitioner_rows([_person_row(_is_latest=is_latest)])[0]
    assert practitioner.active is expected


def test_meta_tag_carries_source_table():
    practitioner = transform_practitioner_rows([_person_row()])[0]
    tag = practitioner.meta.tag[0]
    assert tag.system == SYSTEM_SILKEBORGDATA_SOURCE_TABLE
    assert tag.code == "silkeborgdata_e26_pers"


def test_rows_without_cpr_are_skipped():
    assert transform_practitioner_rows([_person_row(CprNr=None)]) == []


# ---------------------------------------------------------------------------
# CPR correction: one person, two numbers, TWO resources
# ---------------------------------------------------------------------------


def test_superseded_cpr_is_never_a_second_identifier():
    """A second identifier of the same system is not representable.

    The identifier lookup reads only the first value per system
    (extract_identifier_value is LIMIT 1) and the unique index sits on that
    expression, so a second CPR identifier would make the index key depend on
    array order — and its collision partner would be the superseded number's own
    resource, which now exists.
    """
    rows = [_person_row(CprNr="1506055123", _alias_cpr="1506054123")]
    practitioner = transform_practitioner_rows(rows)[0]

    cpr_identifiers = [i for i in practitioner.identifier if i.system == SYSTEM_CPR]
    assert [i.value for i in cpr_identifiers] == ["1506055123"]


def test_superseded_cpr_travels_as_an_extension():
    rows = [_person_row(CprNr="1506055123", _alias_cpr="1506054123")]
    practitioner = transform_practitioner_rows(rows)[0]

    values = [
        e.valueString
        for e in practitioner.extension
        if e.url == EXTENSION_SILKEBORGDATA_AFLOEST_CPR
    ]
    assert values == ["1506054123"]


def test_person_without_a_correction_has_no_superseded_extension():
    practitioner = transform_practitioner_rows([_person_row()])[0]
    urls = {e.url for e in practitioner.extension}
    assert EXTENSION_SILKEBORGDATA_AFLOEST_CPR not in urls


def _is_deleted(resource) -> int:
    return next(e.valueInteger for e in resource.extension if e.url == EXTENSION_FHIR_IS_DELETED)


def test_superseded_identity_is_soft_deleted():
    """A retired identity is not a person to be counted.

    active=false says "not currently on the payroll"; dw_is_deleted says "do not
    count this row at all", which is what consumers already filter on.
    """
    survivor, superseded = transform_practitioner_rows(
        [
            _person_row(CprNr="1506055123", _alias_cpr="1506054123", _is_latest=1),
            _person_row(CprNr="1506054123", _is_latest=0, _is_superseded=1),
        ]
    )
    assert _is_deleted(survivor) == 0
    assert _is_deleted(superseded) == 1
    assert superseded.active is False


def test_a_person_who_merely_left_is_not_soft_deleted():
    """THE BUG THIS CATCHES: keying the soft delete on _is_latest.

    Some 15.000 persons are absent from the newest delivery because they left,
    not because their identity was retired. Both states carry _is_latest = 0, so
    deriving the marker from that flag would soft delete every former employee
    and hide staff that historical data still refers to.
    """
    former = transform_practitioner_rows([_person_row(_is_latest=0)])[0]
    assert _is_deleted(former) == 0
    assert former.active is False


def test_both_query_branches_state_the_superseded_flag():
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    survivor_branch, alias_branch = query.split("UNION ALL")
    assert "0 AS [_is_superseded]" in survivor_branch
    assert "1 AS [_is_superseded]" in alias_branch


def test_superseded_number_becomes_its_own_inactive_resource():
    """The two rows the query emits, through the transform.

    The alias row carries _is_latest = 0 and no _alias_cpr, so it needs no
    special case in the transform — active follows the flag.
    """
    rows = [
        _person_row(CprNr="1506055123", _alias_cpr="1506054123", _is_latest=1),
        _person_row(CprNr="1506054123", _alias_cpr=None, _is_latest=0),
    ]
    survivor, superseded = transform_practitioner_rows(rows)

    assert survivor.identifier[0].value == "1506055123"
    assert survivor.active is True
    assert superseded.identifier[0].value == "1506054123"
    assert superseded.active is False


def test_alias_rows_are_read_from_the_archive_not_the_delta_window():
    """The reason option (b) works in delta at all.

    A corrected number stops appearing in new deliveries, so its rows are older
    than the checkpoint and the delta window does not contain them. Selecting the
    alias row from `windowed` would work in full_load and silently do nothing in
    delta — the exact case the row exists to cover.
    """
    query = build_practitioner_query("2026-08-01 00:00:00", RunModeConfig(mode="delta"))

    # From the CTE's opening paren to the start of the main SELECT — splitting on
    # ")" would cut inside ROW_NUMBER(.
    alias_cte = query.split("alias_ranked AS (")[1].split("SELECT r.*")[0]
    assert "[arc].[silkeborgdata_e26_pers]" in alias_cte
    assert "FROM windowed" not in alias_cte


def test_delta_rebuilds_every_person_rather_than_only_the_new_delivery():
    """Why `active` can flip to false at all.

    The archive gains one delivery per month, so a delta window holds either
    nothing or the whole new delivery — never the people who stopped appearing.
    Ranking inside the window would leave every leaver stuck at active=true,
    because nothing would ever re-emit them.
    """
    query = build_practitioner_query("2026-08-01 00:00:00", RunModeConfig(mode="delta"))
    ranked_cte = query.split("ranked AS (")[1].split("alias_ranked AS (")[0]
    assert "[arc].[silkeborgdata_e26_pers] w" in ranked_cte
    assert "FROM windowed w" not in ranked_cte


def test_delta_is_a_no_op_when_no_delivery_has_landed():
    """The window survives as a trigger, not as a row filter.

    Without the guard the query would rebuild all 53.129 persons on each of the
    29 days a month that bring no delivery.
    """
    query = build_practitioner_query("2026-08-01 00:00:00", RunModeConfig(mode="delta"))
    survivor_branch, alias_branch = query.split("UNION ALL")
    for branch in (survivor_branch, alias_branch):
        assert "AND EXISTS (SELECT 1 FROM windowed)" in branch


def test_full_load_needs_no_guard():
    """Its window is the whole archive, so the guard could only ever pass."""
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    assert "AND EXISTS (SELECT 1 FROM windowed)" not in query


def test_targeted_modes_stay_inside_their_window():
    """`ids` re-posts a named subset and must not fan out to everyone.

    Reading the archive unconditionally would silently turn a targeted re-post
    into a full rebuild — and `active` is not this mode's to recompute.
    """
    config = RunModeConfig(mode="ids", id_filter_column="CprNr", id_filter_values=["1506055123"])
    query = build_practitioner_query(None, config)
    ranked_cte = query.split("ranked AS (")[1].split("alias_ranked AS (")[0]
    assert "FROM windowed w" in ranked_cte
    assert "AND EXISTS (SELECT 1 FROM windowed)" not in query


def test_role_query_shares_the_delta_contract():
    """A role whose employment ended must stop being active too."""
    query = build_practitioner_role_query("2026-08-01 00:00:00", RunModeConfig(mode="delta"))
    assert "[arc].[silkeborgdata_e26_pers] w" in query
    assert "FROM windowed w" not in query
    assert "AND EXISTS (SELECT 1 FROM windowed)" in query


def _mock_asset_deps(fetched_rows):
    """Resources for one pass through the asset body.

    `fetched_rows` is what the archive query yields — an empty list is the
    29-days-out-of-30 case where no delivery has landed.
    """
    cursor = MagicMock()
    cursor.description = [(name,) for name in _person_row()]
    cursor.fetchmany.side_effect = [fetched_rows, []] if fetched_rows else [[]]
    cursor.fetchone.return_value = ("2026-09-11 04:00:00",)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mssql = MagicMock()
    mssql.get_connection.return_value.__enter__.return_value = conn

    fhir_database = MagicMock()
    fhir_database.batch_upsert_resources.return_value = (len(fetched_rows), 0, [])

    context = MagicMock()
    context.run_id = "rid"
    dfm = MagicMock()
    dfm.start_dfm_run.return_value = MagicMock(
        checkpoint="2026-08-01 00:00:00", log_id=1, started_at="2026-09-11 04:00:00"
    )
    dfm.safe_reader_checkpoint.side_effect = lambda started_at: started_at
    return context, mssql, fhir_database, dfm


def _undecorated(asset_def):
    """The plain function inside a Dagster asset, so it can be called directly."""
    return asset_def.op.compute_fn.decorated_fn


def test_checkpoint_stays_put_when_no_delivery_has_landed(monkeypatch):
    """A run that moved no data must not claim it did.

    The no-op guard makes the query return nothing on a day without a delivery.
    If the checkpoint advanced anyway the run would look like work in DFMRunLog,
    and a reader could not tell a quiet day from a processed one.
    """
    import DSA.FHIR.Practitioner.Practitioner_SilkeborgData_dfm as module

    context, mssql, fhir_database, dfm = _mock_asset_deps([])
    monkeypatch.setattr(module, "DfmManager", lambda *a, **k: dfm)

    _undecorated(module.fhir_server_practitioner_dfm)(
        context, mssql, fhir_database, RunModeConfig(mode="delta")
    )

    dfm.set_checkpoint.assert_not_called()
    dfm.fail_dfm_run.assert_not_called()
    assert dfm.complete_dfm_run.call_args.kwargs["checkpoint_after"] is None
    assert dfm.complete_dfm_run.call_args.kwargs["rows_affected"] == 0
    fhir_database.batch_upsert_resources.assert_not_called()


def test_checkpoint_advances_once_a_delivery_is_processed(monkeypatch):
    """The other direction: real rows must move it, or the run repeats forever."""
    import DSA.FHIR.Practitioner.Practitioner_SilkeborgData_dfm as module

    row = tuple(_person_row().values())
    context, mssql, fhir_database, dfm = _mock_asset_deps([row])
    monkeypatch.setattr(module, "DfmManager", lambda *a, **k: dfm)
    monkeypatch.setattr(module, "prepare_resources_for_upsert", lambda **kw: kw["resources"])

    _undecorated(module.fhir_server_practitioner_dfm)(
        context, mssql, fhir_database, RunModeConfig(mode="delta")
    )

    dfm.set_checkpoint.assert_called_once()
    # The checkpoint is the run's read start, so rows written while it ran are
    # read next time instead of being skipped.
    dfm.safe_reader_checkpoint.assert_called_once_with("2026-09-11 04:00:00")
    assert dfm.complete_dfm_run.call_args.kwargs["checkpoint_after"] == "2026-09-11 04:00:00"


def test_alias_branch_forces_inactive_rather_than_deriving_it():
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    alias_branch = query.split("UNION ALL")[1]
    assert "0 AS [_is_latest]" in alias_branch
    assert "latest_period" not in alias_branch


def test_alias_persons_are_excluded_from_the_survivor_branch():
    """Otherwise the same person arrives twice on the active side."""
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    survivor_branch = query.split("UNION ALL")[0]
    assert "NOT EXISTS (SELECT 1 FROM cpr_pair s WHERE s.[alias_cpr] = w.[CprNr])" in (
        survivor_branch
    )


def test_practitioner_and_patient_share_the_cpr_system_without_colliding():
    """Most staff are also patients, so this is the normal path, not an edge.

    The unique index is per (resource_type, system); a single entry for CPR
    would make the two resource types collide.
    """
    cpr_specs = [rt for rt, system in IDENTIFIER_UNIQUE_INDEX_SPECS if system == SYSTEM_CPR]
    assert "Patient" in cpr_specs
    assert "Practitioner" in cpr_specs


# ---------------------------------------------------------------------------
# PractitionerRole
# ---------------------------------------------------------------------------


def test_role_identifier_is_composite_and_requires_inst():
    assert role_identifier_value("OD", "10001", "20150401") == "OD-10001-20150401"
    assert role_identifier_value(None, "10001", "20150401") is None
    assert role_identifier_value("OD", None, "20150401") is None


def test_role_identifier_requires_a_well_formed_episode_key():
    """Anything but CONVERT(..., 112) is a regression in the query, not a value
    to coerce: a silently reformatted key would move every role's id."""
    assert role_identifier_value("OD", "10001", None) is None
    assert role_identifier_value("OD", "10001", "") is None
    assert role_identifier_value("OD", "10001", "2015-04-01") is None
    assert role_identifier_value("OD", "10001", "20150401 00:00:00") is None
    assert role_identifier_value("OD", "10001", " 20150401 ") == "OD-10001-20150401"


def test_role_without_employment_key_is_skipped():
    assert transform_practitioner_role_rows([_role_row(Inst=None)]) == []


def test_role_codes_are_told_apart_by_system_not_position():
    """Both codes present: each must be findable by its own system.

    Order-independence is what makes the flat projection safe, so this test
    deliberately does not assert an order. The positional risk itself lives in
    the view JSON and is pinned in test_practitioner_pipeline_registration.
    """
    role = transform_practitioner_role_rows([_role_row()])[0]
    by_system = {c.coding[0].system: c.coding[0].code for c in role.code}
    assert by_system[SYSTEM_SILKEBORGDATA_STILLINGSKODE] == "1362"
    assert by_system[SYSTEM_SILKEBORGDATA_LEDERKODE] == "36"


def test_role_codes_still_resolve_when_lederkode_is_absent():
    """Lederkode is empty in 96,7 % of rows, so this is the common case."""
    role = transform_practitioner_role_rows([_role_row(Lederkode=None)])[0]
    by_system = {c.coding[0].system: c.coding[0].code for c in role.code}
    assert by_system == {SYSTEM_SILKEBORGDATA_STILLINGSKODE: "1362"}


def test_stilling_is_concept_text_not_coding_display():
    """Stilling is more granular than Stillingskode, so it is not its display."""
    role = transform_practitioner_role_rows([_role_row()])[0]
    concept = next(c for c in role.code if c.coding[0].system == SYSTEM_SILKEBORGDATA_STILLINGSKODE)
    assert concept.text == "Stillingsbetegnelse"
    assert concept.coding[0].display is None


def test_disco_goes_to_specialty_and_unknown_marker_is_absent():
    role = transform_practitioner_role_rows([_role_row()])[0]
    assert role.specialty[0].coding[0].system == SYSTEM_SILKEBORGDATA_DISCOKODE
    assert role.specialty[0].coding[0].code == "222110"

    unknown = transform_practitioner_role_rows([_role_row(Discokode="999999")])[0]
    assert unknown.specialty is None


@pytest.mark.parametrize("value", ["999999", "", None])
def test_disco_code_helper_rejects_unknown(value):
    assert disco_code(value) is None


def test_role_identifier_system_and_value():
    role = transform_practitioner_role_rows([_role_row()])[0]
    assert role.identifier[0].system == SYSTEM_SILKEBORGDATA_TJENESTENUMMER
    assert role.identifier[0].value == "OD-10001-20150401"


def test_role_carries_org_unit_as_extensions_not_references():
    """No SOR mapping exists, so organization/location must stay unset."""
    role = transform_practitioner_role_rows([_role_row()])[0]
    by_url = {e.url: e for e in role.extension}
    assert by_url[EXTENSION_SILKEBORGDATA_INST].valueCoding.code == "OD"
    assert by_url[EXTENSION_SILKEBORGDATA_AFDELINGSKODE].valueCoding.code == "6VFS"
    assert role.organization is None
    assert role.location is None


def test_role_carries_status_codes_verbatim():
    """Carried raw, not translated: consumers know the codes from payroll."""
    role = transform_practitioner_role_rows([_role_row()])[0]
    by_url = {e.url: e for e in role.extension}
    assert by_url[EXTENSION_SILKEBORGDATA_STATUSKODE].valueCoding.code == "1"
    assert by_url[EXTENSION_SILKEBORGDATA_JOBSTATUSKODE].valueCoding.code == "9"


def test_payroll_codes_are_carried_verbatim():
    role = transform_practitioner_role_rows([_role_row()])[0]
    by_url = {e.url: e for e in role.extension}
    assert by_url[EXTENSION_SILKEBORGDATA_TJENESTEKODE].valueCoding.code == "G"
    assert by_url[EXTENSION_SILKEBORGDATA_OMRAADEKODE].valueCoding.code == "1"


def test_zero_is_a_code_and_not_a_blank():
    """THE BUG THIS CATCHES: declaring "0" absent because it looks like noise.

    "0" is 86,8 % of Tjenestekode and 9,9 % of Omraadekode in the newest period,
    so a well-meant `and code != "0"` reads as a cleanup and would drop the most
    common value of both columns. Whether "0" means "none" is not known, and
    suppressing it asserts that it does. (A falsy check in _clean would not bite:
    a non-empty "0" is truthy either way — mutation-tested.)
    """
    role = transform_practitioner_role_rows([_role_row(Tjenestekode="0", Omraadekode="0")])[0]
    by_url = {e.url: e for e in role.extension}
    assert by_url[EXTENSION_SILKEBORGDATA_TJENESTEKODE].valueCoding.code == "0"
    assert by_url[EXTENSION_SILKEBORGDATA_OMRAADEKODE].valueCoding.code == "0"


def test_absent_payroll_codes_produce_no_extension():
    """Tjenestekode is missing on 10,1 % of the newest period."""
    role = transform_practitioner_role_rows([_role_row(Tjenestekode=None, Omraadekode="")])[0]
    urls = {e.url for e in role.extension}
    assert EXTENSION_SILKEBORGDATA_TJENESTEKODE not in urls
    assert EXTENSION_SILKEBORGDATA_OMRAADEKODE not in urls


def test_statusdato_is_carried_but_not_as_period_end():
    """period.end would assert that a status code means "left"."""
    role = transform_practitioner_role_rows([_role_row()])[0]
    by_url = {e.url: e for e in role.extension}
    assert by_url[EXTENSION_SILKEBORGDATA_STATUSDATO].valueDate == date(2015, 3, 1)
    assert role.period.end is None


def test_statusdato_sentinel_is_absent():
    """1.328 rows carry 0001-01-01 — the same gate as period.start rejects it."""
    role = transform_practitioner_role_rows([_role_row(Statusdato=date(1, 1, 1))])[0]
    urls = {e.url for e in role.extension}
    assert EXTENSION_SILKEBORGDATA_STATUSDATO not in urls


def test_status_codes_are_not_mapped_into_active():
    """active means "in the newest delivery" and nothing else.

    THE BUG THIS CATCHES: reading employment status out of the status codes and
    folding it into the flag. The codes are unmapped by decision, so a code the
    source has never sent before must not change active.
    """
    role = transform_practitioner_role_rows(
        [_role_row(Statuskode="3", Jobstatuskode="7", _is_latest=1)]
    )[0]
    assert role.active is True


def test_absent_status_codes_produce_no_extension():
    role = transform_practitioner_role_rows([_role_row(Statuskode=None, Jobstatuskode="")])[0]
    urls = {e.url for e in role.extension}
    assert EXTENSION_SILKEBORGDATA_STATUSKODE not in urls
    assert EXTENSION_SILKEBORGDATA_JOBSTATUSKODE not in urls


def test_unknown_status_code_is_not_warned_about(caplog):
    """Nothing maps them, so an unseen value is data rather than a defect."""
    import logging

    with caplog.at_level(logging.WARNING):
        transform_practitioner_role_rows([_role_row(Statuskode="Z", Jobstatuskode="Z")])
    assert "Z" not in caplog.text


def test_role_references_practitioner_by_surviving_cpr():
    """After a CPR correction the role must point at the number that survived."""
    role = transform_practitioner_role_rows(
        [_role_row(CprNr="1506054123", _practitioner_cpr="1506055123")]
    )[0]
    assert role.practitioner.identifier.system == SYSTEM_CPR
    assert role.practitioner.identifier.value == "1506055123"


def test_period_start_from_ansaettelsesdato():
    role = transform_practitioner_role_rows([_role_row()])[0]
    assert role.period.start == date(2015, 3, 1)


@pytest.mark.parametrize(
    "sentinel",
    [date(1, 1, 1), date(2050, 6, 1), date(2099, 12, 31), date(9999, 12, 31)],
)
def test_period_absent_for_missing_date_markers(sentinel):
    role = transform_practitioner_role_rows([_role_row(Ansaettelsesdato=sentinel)])[0]
    assert role.period is None


def test_period_absent_for_a_date_after_the_delivery_period():
    """The semantic rule, which catches markers not in the hardcoded set.

    2050-06-01 and a tail of 2081-2099 values are real source markers that no
    published list contains, so an enumeration alone leaks.
    """
    role = transform_practitioner_role_rows(
        [_role_row(Ansaettelsesdato=date(2027, 1, 1), Udtraeksdato=PERIOD)]
    )[0]
    assert role.period is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (date(2015, 3, 1), date(2015, 3, 1)),
        (date(9999, 12, 31), None),
        (date(1899, 1, 1), None),
        (None, None),
        ("2015-03-01", None),
    ],
)
def test_usable_date(value, expected):
    assert usable_date(value, PERIOD) == expected


def test_practitioner_snapshot_ordering_is_total():
    """The person snapshot must not let the query plan pick the winner.

    THE BUG THIS CATCHES: PARTITION BY CprNr ORDER BY Udtraeksdato DESC alone.
    2.066 persons hold several concurrent employments, so they have several rows
    in the newest period; with a partial ordering SQL Server may return either,
    and name_text/family/given would then differ between two runs over
    unchanged data. The role query used to be exempt, because its partition plus
    Udtraeksdato WAS the archive's primary key. With the episode in the partition
    that argument is gone, so it carries its own tie-break on CprNr.
    """
    from DSA.FHIR.Practitioner.practitioner_common import (
        build_practitioner_query,
        build_practitioner_role_query,
    )

    person_sql = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    person_order = person_sql[person_sql.index("PARTITION BY w.[CprNr]") :]
    person_order = person_order[: person_order.index(")")]
    for column in ("[Udtraeksdato] DESC", "[Inst]", "[Tjenestenummer]"):
        assert column in person_order, f"person snapshot ordering lacks {column}"

    role_sql = build_practitioner_role_query(None, RunModeConfig(mode="full_load"))
    role_order = role_sql[role_sql.index("PARTITION BY w.[Inst], w.[Tjenestenummer],") :]
    role_order = role_order[: role_order.index(") AS [_rn]")]
    assert "[Udtraeksdato] DESC" in role_order
    assert "w.[CprNr] DESC" in role_order, "the role snapshot ordering lacks its tie-break"


# ---------------------------------------------------------------------------
# Rollens grain: ansættelses-episoden
#
# 36 tjenestenumre er holdt af mere end én person over tid. Med grainet
# (Inst, Tjenestenummer) fandtes kun seneste indehavers rolle. Tredje led i
# nøglen er episodens første leverance-periode — ikke et CPR-derivat, fordi en
# hash af et CPR ikke er et pseudonym, og fordi CPR'et er det eneste i nøglen
# der er designet til at ændre sig.
# ---------------------------------------------------------------------------


def test_episode_key_survives_a_cpr_correction():
    """Én rolle, ikke to, og nøglen bærer den TIDLIGSTE af de to numres perioder.

    FEJLEN DEN FANGER: foldning på rå CprNr i stedet for på _practitioner_cpr.
    Så får en rettet ansættelse to roller, som begge peger på den samme
    Practitioner — K2's fan-out, genindført gennem nøglen i stedet for joinet.
    """
    rows = [
        _role_row(
            CprNr="0101903456",
            _practitioner_cpr="0202903456",
            _episode_key="20140101",
            _is_latest=1,
        )
    ]
    roles = transform_practitioner_role_rows(rows)
    assert len(roles) == 1
    assert roles[0].identifier[0].value == "OD-10001-20140101"
    assert (
        roles[0].practitioner.identifier.value == "0202903456"
    ), "referencen skal pege på den CPR der overlevede parringen"


def test_reused_tjenestenummer_yields_one_role_per_holder():
    """To indehavere af samme nummer giver to roller — det er hele udbyttet.

    Kun den nyeste episode er aktiv: den lukkede episodes nyeste række er ældre
    end seneste leverance, så `active` bliver falsk af sig selv.
    """
    rows = [
        _role_row(_practitioner_cpr="0101903456", _episode_key="20140101", _is_latest=0),
        _role_row(_practitioner_cpr="0202903456", _episode_key="20200601", _is_latest=1),
    ]
    roles = transform_practitioner_role_rows(rows)
    assert [r.identifier[0].value for r in roles] == [
        "OD-10001-20140101",
        "OD-10001-20200601",
    ]
    assert [r.active for r in roles] == [False, True]


def test_unresolved_episode_is_counted_apart_from_a_missing_key(caplog):
    """En manglende episode er en defekt i queryen, ikke kildedata.

    FEJLEN DEN FANGER: et INNER JOIN til `episode`, som ville slette rollen
    lydløst. Rækken skal tælles og logges, og den skal tælles for sig.
    """
    import logging

    logger = logging.getLogger("test_role_episode")
    with caplog.at_level(logging.WARNING, logger="test_role_episode"):
        roles = transform_practitioner_role_rows(
            [_role_row(_episode_key=None), _role_row(Inst=None)], logger=logger
        )
    assert roles == []
    tekst = caplog.text
    assert "1 rows whose employment episode did not resolve" in tekst
    assert "1 rows without Inst/Tjenestenummer" in tekst


def test_folded_cpr_expression_is_identical_in_partition_and_select():
    """Partition og select-liste skal folde på PRÆCIS samme udtryk.

    FEJLEN DEN FANGER: et udtryk der afviger ét tegn mellem `PARTITION BY` og
    select-listen. Så rangeres der på én person og joines på en anden, og intet
    nedenfor ville opdage det. Assertionen er grim; den er billigere end fejlen.
    """
    from DSA.FHIR.Practitioner.practitioner_common import _FOLDED_CPR_EXPR

    query = build_practitioner_role_query(None, RunModeConfig(mode="full_load"))
    assert (
        query.count(_FOLDED_CPR_EXPR) == 2
    ), "foldningsudtrykket skal stå ordret i både select-listen og PARTITION BY"
    assert f"{_FOLDED_CPR_EXPR} AS [_practitioner_cpr]" in query
    assert f"PARTITION BY w.[Inst], w.[Tjenestenummer], {_FOLDED_CPR_EXPR}" in query


def test_episode_join_is_left_so_a_missing_episode_cannot_delete_a_role():
    """FEJLEN DEN FANGER: INNER JOIN til `episode`, som fjerner rollen i stilhed."""
    query = build_practitioner_role_query(None, RunModeConfig(mode="full_load"))
    assert "LEFT JOIN episode e" in query
    assert "\n        JOIN episode e" not in query


def test_episode_cte_reads_the_archive_not_the_window():
    """En lukket episode optræder ikke i nye leverancer; dens startdato skal
    stadig kunne slås op. Samme grund som alias_ranked læser arkivet."""
    for mode in ("full_load", "delta"):
        query = build_practitioner_role_query(None, RunModeConfig(mode=mode))
        episode_del = query[query.index("role_cpr AS (") : query.index("latest_period AS (")]
        assert "[arc].[silkeborgdata_e26_pers] a" in episode_del
        assert "FROM windowed" not in episode_del


# ---------------------------------------------------------------------------
# cpr_pair fan-out
#
# The pair (keep_cpr, alias_cpr) is unique; neither side of it is. The
# fingerprint groups on (CprNr, Inst, Tjenestenummer), so a person with two
# employments — 2.066 of them hold several at once — can pair twice:
#
#   * two alias numbers under one keep_cpr  -> the person query fans out
#   * two keep numbers under one alias_cpr  -> the role query fans out
#
# Either one emits the same identifier twice in a single batch, and with the
# unique index on extract_identifier_value(...) that fails the whole asset. With
# 4 pairs it cannot happen today; the point of a rule over a list is that new
# cases arrive without a code change, and the count already went 3 -> 4 in three
# days. These tests pin the shape, not the count.
# ---------------------------------------------------------------------------


def test_person_join_takes_one_alias_per_surviving_cpr():
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    assert (
        "LEFT JOIN cpr_pair_by_keep p ON p.[keep_cpr] = r.[CprNr] AND p.[_rn] = 1" in query
    ), "the person join must not read cpr_pair directly — it fans out on keep_cpr"


def test_role_join_takes_one_successor_per_superseded_cpr():
    """The join moved into `ranked` when the episode entered the partition; the
    one-to-one CTE it reads is the point, and that has not changed."""
    query = build_practitioner_role_query(None, RunModeConfig(mode="full_load"))
    assert (
        "LEFT JOIN cpr_pair_by_alias p ON p.[alias_cpr] = w.[CprNr] AND p.[_rn] = 1" in query
    ), "the role join must not read cpr_pair directly — it fans out on alias_cpr"


@pytest.mark.parametrize(
    "cte, partition, ordering",
    [
        (
            "cpr_pair_by_keep",
            "PARTITION BY [keep_cpr]",
            "[alias_last_period] DESC, [alias_cpr] DESC",
        ),
        (
            "cpr_pair_by_alias",
            "PARTITION BY [alias_cpr]",
            "[keep_first_period] ASC, [keep_cpr] DESC",
        ),
    ],
)
def test_pair_ranking_is_total(cte, partition, ordering):
    """A partial ordering leaves the winner to the query plan.

    Both tie-breaks end on the CPR itself, so two runs over unchanged data
    resolve the same pair the same way.
    """
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    block = query[query.index(f"{cte} AS (") :]
    block = block[: block.index("FROM cpr_pair")]
    assert partition in block
    assert ordering in block


def test_semi_joins_still_read_the_raw_pair_table():
    """The two EXISTS predicates must NOT be narrowed to the ranked CTEs.

    They cannot fan out — a semi-join returns at most one row per probe — and
    they have to see every superseded number. Pointed at cpr_pair_by_keep, an
    alias that lost the ranking would fall out of the exclusion in `ranked` AND
    out of alias_ranked, and so reappear as an active person: the exact failure
    the alias branch exists to prevent.
    """
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    assert query.count("FROM cpr_pair s WHERE s.[alias_cpr]") == 2
    assert "cpr_pair_by_keep s" not in query
    assert "cpr_pair_by_alias s" not in query


def test_pair_table_keeps_one_row_per_pair():
    """GROUP BY replaced DISTINCT to expose the ordering keys.

    If it ever regresses to a plain projection, the pair table itself starts
    fanning out and the ranked CTEs rank duplicates.
    """
    query = build_practitioner_query(None, RunModeConfig(mode="full_load"))
    block = query[query.index("cpr_pair AS (") :]
    block = block[: block.index("cpr_pair_by_keep AS (")]
    assert "GROUP BY keep.[CprNr], old.[CprNr]" in block
    assert "SELECT DISTINCT" not in block
