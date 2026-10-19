# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Parsing and mapping for the SilkeborgData E26PERS extract.

Every test here can fail on a plausible bug. The two worst bug classes for a
headerless, positional, Danish-notation flat file are:

  - separator confusion: "1.234,56" read as 1.23456 would scale every amount by
    1000 with nothing failing
  - positional shift: one extra or missing field makes every column after it
    wrong, also with nothing failing

Both are covered below. No tests for the DDL, for registration, or for asset
existence — those cannot fail on anything a real run would not also catch.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from DSI.csv_files.silkeborgdata import (
    LEVERANCE_COLUMNS,
    Delivery,
    parse_sd_date,
    parse_sd_decimal,
)
from DSI.csv_files.silkeborgdata_e26_pers import (
    DATE_COLUMNS,
    DECIMAL_COLUMNS,
    EXT_COLUMNS,
    FIELD_COUNT,
    FILE_COLUMNS,
    KEY_COLUMNS,
    _iter_ext_rows,
    _parse_line,
)

pytestmark = pytest.mark.unit

# Synthetic fixture value for the Navn column. Not a name: see _row_fields().
PROBE = "Testæøå-ÆØÅ"


class _StubLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def info(self, msg: str) -> None:
        self.infos.append(msg)


class _StubContext:
    def __init__(self) -> None:
        self.log = _StubLog()


def _stats() -> dict[str, int]:
    return {
        "rows_rejected_blank_key": 0,
        "duplicates_collapsed": 0,
        "duplicates_across_deliveries": 0,
        "unparsable_dates": 0,
        "unparsable_decimals": 0,
        "trailing_minus_values": 0,
    }


def _row_fields(**overrides: str) -> list[str]:
    """A valid 41-field line, with named overrides by archive column name."""
    values = {
        "Inst": "01",
        "Tjenestenummer": "12345",
        "CprNr": "0101901234",
        "Udtraeksdato": "2026-07-31",
        "GarantiloenBeloeb": "1.234,56",
        "Loendecimal": "1,0000",
        # Deliberately an encoding probe rather than a person's name. cp865 and
        # cp1252 disagree about æøå in BOTH cases, so this covers more than a
        # realistic name would — and a name-shaped fixture trips the repo's own
        # person-name-in-context PII rule in .gitleaks.toml, which has no
        # test-path allowlist (unlike danish-cpr).
        "Navn": PROBE,
    }
    values.update(overrides)
    return [values.get(column, "") for column in FILE_COLUMNS]


# ---------------------------------------------------------------------------
# parse_sd_decimal — COMMAX (comma decimal, period thousands)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1.234,56", Decimal("1234.56")),
        ("-0,5000", Decimal("-0.5000")),
        ("0", Decimal("0")),
        ("", None),
        (".", None),
        ("  12,25  ", Decimal("12.25")),
        ("1.000.000,00", Decimal("1000000.00")),
        # Trailing minus occurs in some mainframe extracts.
        ("123,45-", Decimal("-123.45")),
    ],
)
def test_parse_sd_decimal_handles_danish_notation(raw, expected):
    """A swapped thousands/decimal separator would scale every amount by 1000."""
    assert parse_sd_decimal(raw, column="Beloeb", line_no=1, tolerate=False) == expected


def test_parse_sd_decimal_counts_trailing_minus():
    stats = _stats()
    parse_sd_decimal("99,00-", column="Beloeb", line_no=7, tolerate=False, stats=stats)
    assert stats["trailing_minus_values"] == 1


def test_parse_sd_decimal_raises_on_garbage_by_default():
    """The error contract must not invert into silent NULLs in the archive."""
    with pytest.raises(ValueError, match="COMMAX"):
        parse_sd_decimal("tolv", column="Beloeb", line_no=42, tolerate=False)


def test_parse_sd_decimal_tolerates_garbage_when_asked():
    stats = _stats()
    result = parse_sd_decimal("tolv", column="Beloeb", line_no=42, tolerate=True, stats=stats)
    assert result is None
    assert stats["unparsable_decimals"] == 1


def test_parse_sd_decimal_error_message_carries_no_field_value():
    """The file holds CPR, name and address — values must never reach a log."""
    with pytest.raises(ValueError) as excinfo:
        parse_sd_decimal(PROBE, column="Beloeb", line_no=9, tolerate=False)
    assert PROBE not in str(excinfo.value)


# ---------------------------------------------------------------------------
# parse_sd_date
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-01-31", date(2026, 1, 31)),
        ("2026-12-01", date(2026, 12, 1)),
        ("", None),
        (".", None),
        ("0000-00-00", None),
        ("00000000", None),
    ],
)
def test_parse_sd_date_accepts_iso_and_missing_tokens(raw, expected):
    assert parse_sd_date(raw, column="Dato", line_no=1, tolerate=False) == expected


def test_parse_sd_date_rejects_danish_day_first_format():
    """Accepting dd-mm-yyyy would silently swap day and month for days 1-12."""
    with pytest.raises(ValueError, match="yyyy-mm-dd"):
        parse_sd_date("31-01-2026", column="Dato", line_no=3, tolerate=False)


def test_parse_sd_date_rejects_impossible_date():
    with pytest.raises(ValueError):
        parse_sd_date("2026-02-30", column="Dato", line_no=4, tolerate=False)


# ---------------------------------------------------------------------------
# _parse_line — field count is the only structural safety net
# ---------------------------------------------------------------------------


def test_parse_line_accepts_exactly_41_fields():
    row = _parse_line(_row_fields(), 1, _stats(), False, _StubContext())
    assert row is not None
    assert row["Inst"] == "01"
    assert row["Udtraeksdato"] == date(2026, 7, 31)
    assert row["GarantiloenBeloeb"] == Decimal("1234.56")
    # Empty text fields become NULL, not "".
    assert row["Stilling"] is None


@pytest.mark.parametrize("trailer", ["", " ", "   ", "\t", "\r", " \t "])
def test_parse_line_tolerates_one_blank_trailing_field(trailer):
    """A line ending in ';' is the same 41 fields, not a 42-field error.

    The real file ends every line with a delimiter followed by whitespace, so
    comparing the 42nd field with "" exactly rejected all 40,348 lines of the
    first delivery. Whitespace variants are parametrised because the exact
    padding is the source's business, not something we should depend on.
    """
    fields = [*_row_fields(), trailer]
    row = _parse_line(fields, 1, _stats(), False, _StubContext())
    assert row is not None
    assert row["Inst"] == "01"


@pytest.mark.parametrize("count", [40, 42, 46])
def test_parse_line_rejects_wrong_field_count(count):
    """Positional shift is the worst bug class here: every later column is wrong."""
    fields = (_row_fields() + ["x"] * 10)[:count]
    if count == 42:
        # A real 42nd field, not a trailing delimiter. It must still fail: we
        # cannot know where in the row the extra column belongs, and guessing
        # would shift every column after it without anything failing.
        fields[-1] = "not-empty"
    with pytest.raises(ValueError, match=f"expected {FIELD_COUNT} fields, found {count}"):
        _parse_line(fields, 17, _stats(), False, _StubContext())


def test_parse_line_field_count_error_names_the_line_not_the_content():
    fields = _row_fields(Navn=PROBE)[:40]
    with pytest.raises(ValueError) as excinfo:
        _parse_line(fields, 123, _stats(), False, _StubContext())
    message = str(excinfo.value)
    assert "line 123" in message
    assert PROBE not in message


@pytest.mark.parametrize("blank_key", ["Inst", "Tjenestenummer", "Udtraeksdato"])
def test_parse_line_rejects_blank_primary_key(blank_key):
    """A row with an unusable key would otherwise reach arc and raise a PK error."""
    stats = _stats()
    context = _StubContext()
    row = _parse_line(_row_fields(**{blank_key: ""}), 5, stats, False, context)
    assert row is None
    assert stats["rows_rejected_blank_key"] == 1
    assert any(blank_key in w and "line 5" in w for w in context.log.warnings)


def test_parse_line_rejection_warning_carries_no_field_value():
    context = _StubContext()
    _parse_line(_row_fields(Inst="", Navn=PROBE), 5, _stats(), False, context)
    assert all(PROBE not in w for w in context.log.warnings)


def test_blank_key_warnings_are_capped_but_the_count_is_not(tmp_path):
    """A file whose keys are systematically blank must not emit one Dagster event
    per line — a real delivery is 40k+ rows. The total still has to be exact,
    because the run metadata reports it."""
    stats = _stats()
    context = _StubContext()
    for line_no in range(1, 61):
        assert _parse_line(_row_fields(Inst=""), line_no, stats, False, context) is None

    assert stats["rows_rejected_blank_key"] == 60
    assert len(context.log.warnings) < 60
    assert any("no longer logging them individually" in w for w in context.log.warnings)


# ---------------------------------------------------------------------------
# _iter_ext_rows — dedup, provenance stamping, newest-delivery-wins
# ---------------------------------------------------------------------------


def _write_delivery(tmp_path, folder: str, rows: list[list[str]]) -> Delivery:
    directory = tmp_path / folder
    directory.mkdir()
    path = directory / "E26PERS.TXT"
    path.write_text("".join(";".join(row) + "\n" for row in rows), encoding="cp865")
    return Delivery(
        delivery_date=date(int(folder[:4]), int(folder[4:6]), int(folder[6:])),
        folder=folder,
        path=str(path),
    )


def test_iter_ext_rows_dedupes_within_one_file(tmp_path):
    """Without dedup, enable_bulk_insert_on_first_load breaks on the primary key."""
    delivery = _write_delivery(tmp_path, "20260731", [_row_fields(Navn="A"), _row_fields(Navn="B")])
    stats = _stats()
    rows = list(_iter_ext_rows([delivery], "cp865", stats, False, _StubContext(), {}, {}))
    assert len(rows) == 1
    assert rows[0]["Navn"] == "A"  # first occurrence wins
    assert stats["duplicates_collapsed"] == 1
    assert stats["duplicates_across_deliveries"] == 0


def test_iter_ext_rows_stamps_provenance_from_the_folder(tmp_path):
    delivery = _write_delivery(tmp_path, "20260731", [_row_fields()])
    rows = list(_iter_ext_rows([delivery], "cp865", _stats(), False, _StubContext(), {}, {}))
    assert rows[0]["dw_leverance_dato"] == date(2026, 7, 31)
    assert rows[0]["dw_leverance_mappe"] == "20260731"


def test_iter_ext_rows_lets_the_newest_delivery_win(tmp_path):
    """If the oldest folder won, arc would hold stale values under the wrong
    dw_leverance_dato — silently."""
    newest = _write_delivery(tmp_path, "20260731", [_row_fields(Navn="Ny")])
    oldest = _write_delivery(tmp_path, "20260630", [_row_fields(Navn="Gammel")])
    stats = _stats()
    # select_deliveries hands them over newest-first; that ordering is the
    # precondition this test protects.
    rows = list(_iter_ext_rows([newest, oldest], "cp865", stats, False, _StubContext(), {}, {}))
    assert len(rows) == 1
    assert rows[0]["Navn"] == "Ny"
    assert rows[0]["dw_leverance_mappe"] == "20260731"
    assert stats["duplicates_across_deliveries"] == 1


def test_iter_ext_rows_reads_cp865_danish_characters(tmp_path):
    """Reading cp865 as cp1252 yields wrong characters rather than an error."""
    delivery = _write_delivery(tmp_path, "20260731", [_row_fields(Navn=PROBE)])
    rows = list(_iter_ext_rows([delivery], "cp865", _stats(), False, _StubContext(), {}, {}))
    assert rows[0]["Navn"] == PROBE


def test_iter_ext_rows_reports_zero_usable_for_an_empty_file(tmp_path):
    """The extract fails when a selected delivery holds nothing loadable; that
    decision reads usable==0, so it has to be recorded even for an empty file."""
    delivery = _write_delivery(tmp_path, "20260731", [])
    per_delivery: dict[str, dict[str, int]] = {}
    rows = list(
        _iter_ext_rows([delivery], "cp865", _stats(), False, _StubContext(), per_delivery, {})
    )
    assert rows == []
    assert per_delivery == {"20260731": {"usable": 0, "kept": 0}}


def test_iter_ext_rows_reports_zero_usable_when_every_key_is_blank(tmp_path):
    """A file full of rows without a usable key is as unloadable as an empty one,
    and must be distinguishable from one whose rows were merely deduped away."""
    delivery = _write_delivery(
        tmp_path, "20260731", [_row_fields(Inst=""), _row_fields(Tjenestenummer="")]
    )
    per_delivery: dict[str, dict[str, int]] = {}
    rows = list(
        _iter_ext_rows([delivery], "cp865", _stats(), False, _StubContext(), per_delivery, {})
    )
    assert rows == []
    assert per_delivery["20260731"]["usable"] == 0


def test_a_fully_superseded_delivery_is_usable_but_keeps_nothing(tmp_path):
    """The distinction the 0-row guard turns on. A re-issued period during a
    backfill contributes no rows because a NEWER folder already claimed its keys.
    That is legitimate — the newer folder carries the newer dw_leverance_dato, so
    the checkpoint still advances. Testing 'kept' instead of 'usable' would turn
    every backfill over a re-issue into a hard failure."""
    newest = _write_delivery(tmp_path, "20260731", [_row_fields(Navn="Ny")])
    superseded = _write_delivery(tmp_path, "20260630", [_row_fields(Navn="Gammel")])
    per_delivery: dict[str, dict[str, int]] = {}
    rows = list(
        _iter_ext_rows(
            [newest, superseded], "cp865", _stats(), False, _StubContext(), per_delivery, {}
        )
    )
    assert len(rows) == 1
    assert per_delivery["20260731"] == {"usable": 1, "kept": 1}
    # usable > 0 proves the file was readable; kept == 0 is the legitimate outcome.
    assert per_delivery["20260630"] == {"usable": 1, "kept": 0}


# ---------------------------------------------------------------------------
# Column-list invariants — the lists must not drift apart
# ---------------------------------------------------------------------------


def test_file_columns_has_41_unique_entries():
    assert len(FILE_COLUMNS) == 41
    assert FIELD_COUNT == 41
    assert len(set(FILE_COLUMNS)) == 41


def test_key_columns_are_file_columns():
    assert set(KEY_COLUMNS) <= set(FILE_COLUMNS)


def test_ext_columns_appends_exactly_the_provenance_columns():
    """Forgetting these would leave checkpoint_query returning NULL forever, so
    every run would reload the whole history."""
    assert EXT_COLUMNS[:41] == FILE_COLUMNS
    assert tuple(EXT_COLUMNS[-2:]) == LEVERANCE_COLUMNS
    assert len(EXT_COLUMNS) == 43


def test_typed_columns_are_all_declared_file_columns():
    assert DATE_COLUMNS <= set(FILE_COLUMNS)
    assert set(DECIMAL_COLUMNS) <= set(FILE_COLUMNS)
    assert not DATE_COLUMNS & set(DECIMAL_COLUMNS)
