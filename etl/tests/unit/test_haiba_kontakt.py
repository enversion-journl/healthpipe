# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the HAIBA -> FHIR contact relation.

The failures these guard against are all silent: SQL that returns nothing
because a soft-delete flag is NULL rather than 0, a non-deterministic winner
when several contacts qualify, a role wired to the wrong FHIR resource, or a
column named in the SQL but absent from the DDL. None of them raise — they just
produce an empty or arbitrary relation behind a green asset.
"""

import re
from pathlib import Path

import pytest
from DSA.SQL.dm.haiba_kontakt import (
    KONTAKT_TABELLER,
    MATCH_KVALITET,
    build_match_sql,
    build_stats_sql,
    haiba_kontakt_assets,
)

pytestmark = pytest.mark.unit

DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
DM_DDL = DDL_DIR / "tables" / "haiba" / "01_dm.sql"
MIGRATION = DDL_DIR / "migrations" / "20260820_dm_haiba_kontakt_relation.sql"

ROLLER = [(t, r) for t in KONTAKT_TABELLER for r in t.roller]
ROLLE_IDS = [f"{t.dm_table}.{r.navn}" for t, r in ROLLER]


def _ddl_columns(table: str) -> set[str]:
    sql = DM_DDL.read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE TABLE \[dm\]\.\[{re.escape(table)}\] \((.*?)\n    \);", sql, re.DOTALL
    )
    assert match, f"no CREATE TABLE block for dm.{table}"
    return set(re.findall(r"^\s{8}\[(\w+)\]\s+\w", match.group(1), re.MULTILINE))


# ------------------------------------------------------------------ columns ---


@pytest.mark.parametrize(("tabel", "rolle"), ROLLER, ids=ROLLE_IDS)
def test_every_role_column_exists_in_the_base_ddl(tabel, rolle):
    """A column named only in the UPDATE fails at runtime, not at import."""
    declared = _ddl_columns(tabel.dm_table)
    missing = sorted(set(rolle.columns) - declared)
    assert not missing, f"missing from dm DDL: {missing}"


@pytest.mark.parametrize(("tabel", "rolle"), ROLLER, ids=ROLLE_IDS)
def test_every_role_column_exists_in_the_migration(tabel, rolle):
    """Base DDL builds a fresh database; the migration fixes the existing one.
    A column in only one of them leaves the two environments diverged."""
    sql = MIGRATION.read_text(encoding="utf-8")
    for column in rolle.columns:
        assert f"[{column}]" in sql, f"{column} not added by the migration"


@pytest.mark.parametrize(("tabel", "rolle"), ROLLER, ids=ROLLE_IDS)
def test_quality_column_is_not_null_with_a_sentinel_default(tabel, rolle):
    """Without a default, a row whose relation has never been computed reads as
    NULL — indistinguishable from a genuine 'no match' by any consumer."""
    for path in (DM_DDL, MIGRATION):
        sql = path.read_text(encoding="utf-8")
        line = re.search(rf"^.*\[{re.escape(rolle.kvalitet_column)}\].*$", sql, re.MULTILINE)
        assert line, f"{rolle.kvalitet_column} not declared in {path.name}"
        window = sql[line.start() : line.start() + 400]
        assert "NOT NULL" in window
        assert "ikke_koert" in window


def test_only_the_location_role_stores_a_location_id():
    """A responsible unit is an Organization and has no location; storing one
    would imply a precision the match does not have."""
    for tabel in KONTAKT_TABELLER:
        for rolle in tabel.roller:
            if rolle.via == "lokation":
                assert rolle.location_column
            else:
                assert rolle.location_column is None


# ------------------------------------------------------------------- routing ---


def test_opholdsadresse_goes_to_encounter_location_and_the_rest_to_encounter():
    """The whole point of splitting the roles. Where the patient physically was
    is a Location; a responsible or performing unit is an Organization. Wiring
    both to Encounter would silently answer a different question."""
    infektion = KONTAKT_TABELLER[0]
    by_name = {r.navn: r for r in infektion.roller}
    assert by_name["opholdsadresse"].via == "lokation"
    assert by_name["kontaktansvar"].via == "organisation"
    assert all(r.via == "organisation" for r in KONTAKT_TABELLER[1].roller)


def test_location_role_queries_encounter_location_and_its_own_period():
    """The location period is narrower than the encounter period; matching a
    ward stay against the whole admission would over-report."""
    tabel = KONTAKT_TABELLER[0]
    rolle = next(r for r in tabel.roller if r.via == "lokation")
    sql = build_match_sql(tabel, rolle)

    assert "[fhir].[Encounter_Location]" in sql
    assert "[fhir].[Location]" in sql
    assert "location_period_start" in sql
    assert "[fhir].[Organization]" not in sql


def test_organisation_role_matches_the_service_provider():
    tabel = KONTAKT_TABELLER[0]
    rolle = next(r for r in tabel.roller if r.via == "organisation")
    sql = build_match_sql(tabel, rolle)

    assert "[service_provider_id]" in sql
    assert "[fhir].[Organization]" in sql
    assert "[fhir].[Encounter_Location]" not in sql


def test_reoperation_roles_match_against_the_reoperation_date():
    """The index operation and the reoperation are separate events. Matching a
    reoperation unit against the index date would link the wrong contact."""
    ssi = KONTAKT_TABELLER[1]
    by_name = {r.navn: r for r in ssi.roller}
    assert by_name["reo_producent"].date_column == "reo_date"
    assert by_name["reo_kontaktansvarlig"].date_column == "reo_date"
    assert by_name["producent"].date_column == "operation_date"
    assert "[reo_date]" in build_match_sql(ssi, by_name["reo_producent"])


# --------------------------------------------------------------- soft delete ---


def test_nullable_soft_delete_columns_are_filtered_null_safely():
    """dw_is_deleted is nullable on Encounter, Encounter_Location and Location,
    and the SoF views project NULL whenever the isDeleted extension is absent —
    the normal case. A bare `= 0` drops live rows, and it does so in the
    direction that under-reports a match rather than raising."""
    for tabel, rolle in ROLLER:
        sql = build_match_sql(tabel, rolle)
        for alias in ("e", "el", "l"):
            assert not re.search(
                rf"(?<![\w.]){alias}\.\[dw_is_deleted\]\s*=\s*0", sql
            ), f"{rolle.navn}: {alias}.dw_is_deleted is nullable — coalesce it"
        assert "ISNULL(e.[dw_is_deleted], 0) = 0" in sql


def test_the_sor_lookup_excludes_retracted_units():
    """The unit supplying the SOR code decides tier 1. A retracted Location or
    Organization would keep handing out its code and manufacture exact matches."""
    for tabel, rolle in ROLLER:
        sql = build_match_sql(tabel, rolle)
        if rolle.via == "organisation":
            # NOT NULL DEFAULT 0, and a bare `= 0` matches the filtered index.
            assert "o.[dw_is_deleted] = 0" in sql
        else:
            assert "ISNULL(l.[dw_is_deleted], 0) = 0" in sql


def test_encounter_excludes_retracted_contacts():
    """287,525 encounters are entered-in-error. Linking an infection to a
    retracted contact is worse than reporting no match."""
    for tabel, rolle in ROLLER:
        assert "'entered-in-error'" in build_match_sql(tabel, rolle)


# ------------------------------------------------------------------ matching ---


def test_timestamp_parsing_handles_the_offset_form():
    """period_start comes in two shapes; 128,271 rows carry a +00:00 offset.
    Truncating those to 19 chars reads UTC as local time and shifts the day."""
    sql = build_match_sql(*ROLLER[0])
    assert "datetimeoffset" in sql
    assert "AT TIME ZONE 'Romance Standard Time'" in sql
    assert "LEN(" in sql and "= 25" in sql


def test_blank_timestamps_cannot_become_1900():
    """TRY_CONVERT(datetime2, '') returns 1900-01-01, not NULL. 3,558,776
    Encounter_Location rows carry a blank start and a NULL end, so without a
    NULLIF each one becomes an interval from 1900 to forever and matches every
    case tested against it. It shipped once and the output looked plausible."""
    for tabel, rolle in ROLLER:
        sql = build_match_sql(tabel, rolle)
        for column in re.findall(r"TRY_CONVERT\(datetime2\(0\), LEFT\((.*?), 19\)\)", sql):
            assert "NULLIF" in column, f"unguarded blank-to-1900 conversion: {column}"
        assert "TRY_CONVERT(datetimeoffset, NULLIF" in sql


def test_location_rows_without_a_location_are_excluded():
    """A blank location_id is not a ward the patient occupied."""
    tabel = KONTAKT_TABELLER[0]
    rolle = next(r for r in tabel.roller if r.via == "lokation")
    sql = build_match_sql(tabel, rolle)
    assert "NULLIF(LTRIM(RTRIM(el.[location_id])), '') IS NOT NULL" in sql


def test_winner_is_fully_deterministic():
    """Several contacts can qualify. Without a total ordering the stamped
    encounter would change between runs on unchanged data, and every downstream
    diff would show churn."""
    sql = build_match_sql(*ROLLER[0])
    order = re.search(r"ORDER BY tier, ABS\(dage\), p_start DESC, \[encounter_id\]", sql)
    assert order, "ranking must end in a unique tiebreak column"


def test_match_days_are_signed_around_the_contact():
    """An infection dated before admission and one dated after discharge are
    different situations; an absolute gap would conflate them."""
    sql = build_match_sql(*ROLLER[0])
    assert "DATEDIFF(DAY, e.p_start, c.hai_ts)" in sql  # negative, case precedes
    assert "DATEDIFF(DAY, e.p_end, c.hai_ts)" in sql  # positive, case follows
    assert "ELSE 0" in sql


def test_day_gap_is_documented_as_calendar_days_not_elapsed_time():
    """DATEDIFF(DAY, ...) counts date boundaries, so a case two hours outside
    the contact reports 0 — same as one inside it. 269 rows land there. The
    contract only holds if the docstring says so and the quality column is read
    alongside; a reader who takes match_dage = 0 to mean "inside" is wrong."""
    from DSA.SQL.dm import haiba_kontakt

    doc = haiba_kontakt.__doc__
    assert "outside, by under a day" in doc
    assert "269" in doc


def test_no_tolerance_window_is_hardcoded():
    """Case definitions belong to infection control. The pipeline records the
    gap; it must not quietly decide that two days still counts as a match."""
    sql = build_match_sql(*ROLLER[0])
    assert not re.search(r"DATEADD\s*\(\s*DAY", sql, re.IGNORECASE)
    assert not re.search(r"ABS\(dage\)\s*<", sql)


def test_every_row_is_rewritten_so_a_rerun_cannot_leave_a_stale_relation():
    """The UPDATE joins with LEFT JOIN and covers the no-match branches, so a
    case that stops matching loses its old encounter instead of keeping it."""
    sql = build_match_sql(*ROLLER[0])
    assert "LEFT JOIN #match" in sql
    assert "'intet_match'" in sql
    assert "'ingen_kontakter'" in sql
    assert "'ingen_borger'" in sql


def test_unresolved_citizens_are_excluded_from_the_candidate_search():
    """dw_ek_Borger = -1 is a sentinel, not a key. Joining on it would link
    every unresolved case to whatever rows share the sentinel."""
    sql = build_match_sql(*ROLLER[0])
    assert "[dw_ek_Borger] <> -1" in sql


@pytest.mark.parametrize(("tabel", "rolle"), ROLLER, ids=ROLLE_IDS)
def test_every_emitted_quality_value_is_documented(tabel, rolle):
    """A value that appears in the SQL but not in MATCH_KVALITET reaches the
    asset metadata undocumented, and a consumer has to guess what it means."""
    sql = build_match_sql(tabel, rolle)
    emitted = set(re.findall(r"THEN '(\w+)'", sql))
    assert emitted <= set(MATCH_KVALITET), sorted(emitted - set(MATCH_KVALITET))


def test_statistics_are_broken_down_by_year():
    """A single overall match rate is misleading: it is 0% in 2020 and ~68%
    from 2022, because Cosmic was rolled out in between."""
    sql = build_stats_sql(*ROLLER[0])
    assert "YEAR(" in sql
    assert "GROUP BY YEAR(" in sql


# -------------------------------------------------------------------- assets ---


def test_assets_depend_on_the_dm_rebuild_they_update():
    """The rebuild truncates and reloads; running the stamp first would write
    a relation that the rebuild then discards."""
    names = {a.key.to_user_string() for a in haiba_kontakt_assets}
    assert names == {"dm_haiba_cases_infektion_kontakt", "dm_haiba_cases_ssi_kontakt"}

    for asset in haiba_kontakt_assets:
        deps = {k.to_user_string() for k in asset.asset_deps[next(iter(asset.keys))]}
        assert deps == {asset.key.to_user_string().removesuffix("_kontakt")}


def test_contact_assets_are_registered_in_the_dm_layer():
    from DSA.SQL.dm import all_assets

    for asset in haiba_kontakt_assets:
        assert asset in all_assets
