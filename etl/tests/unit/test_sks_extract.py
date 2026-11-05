# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for SKS register extraction: fixed-width parsing, deduplication,
hierarchy building, and ext row assembly from SKScomplete.txt."""

import gzip
from datetime import date

import pytest
from core.resources import HTTPDownloadResource
from dagster import build_asset_context
from DSI.csv_files.sks_register import (
    KNOWN_PREFIXES,
    SKSExtractConfig,
    _build_ext_rows,
    _build_hierarchy,
    _decode_sks_bytes,
    _deduplicate_newest,
    _parse_date,
    _parse_sks_lines,
    extract_sks_register,
)

pytestmark = pytest.mark.unit

_SKS_DOWNLOAD = HTTPDownloadResource(base_url="http://unused", user_agent="test")

TODAY = date(2026, 6, 11)
NO_END = date(2500, 1, 1)
NO_END_STR = "25000101"


def _make_line(
    full_code: str,
    valid_from: str = "20200101",
    update_date: str = "20200101",
    valid_to: str = NO_END_STR,
    display: str = "Some display text",
    with_marker: bool = True,
) -> str:
    """Build a fixed-width SKScomplete line.

    Layout: code(23) + dates(3x8) + display(120) + aux digits(6) +
    spaces(6) + "SKS" marker + status. Lines without the marker are
    right-trimmed after the display text (as in the real file).
    """
    line = f"{full_code:<23}{valid_from}{update_date}{valid_to}"
    if with_marker:
        line += f"{display:<120}001001      SKS     1                          "
    else:
        line += display
    return line


class TestKnownPrefixes:
    def test_all_ten_prefixes_present(self):
        expected = {"adm", "atc", "dia", "opr", "pro", "res", "spc", "til", "und", "uly"}
        assert set(KNOWN_PREFIXES) == expected


class TestParseSksLines:
    def test_full_length_line_with_marker(self):
        text = _make_line("diaDA01", display="Tyfus og paratyfus")
        records = _parse_sks_lines(text)
        assert len(records) == 1
        rec = records[0]
        assert rec["code"] == "DA01"
        assert rec["classification"] == "dia"
        assert rec["valid_from"] == date(2020, 1, 1)
        assert rec["valid_to"] == NO_END
        assert rec["display"] == "Tyfus og paratyfus"

    def test_short_line_without_marker(self):
        text = _make_line("atcA01", display="Kort tekst", with_marker=False)
        records = _parse_sks_lines(text)
        assert len(records) == 1
        assert records[0]["code"] == "A01"
        assert records[0]["classification"] == "atc"
        assert records[0]["display"] == "Kort tekst"

    def test_prefix_is_stripped_from_code(self):
        text = _make_line("oprKABC10", display="Operation")
        records = _parse_sks_lines(text)
        assert records[0]["code"] == "KABC10"
        assert records[0]["classification"] == "opr"

    def test_unknown_prefix_skipped_without_error(self):
        """Structurally valid lines with a new/unknown classification prefix
        are skipped (forward compatibility) — they are not format errors."""
        text = _make_line("xxxA01", display="Unknown classification")
        assert _parse_sks_lines(text) == []

    def test_blank_lines_ignored(self):
        text = "\n".join(["", "   ", _make_line("diaDA01")])
        records = _parse_sks_lines(text)
        assert len(records) == 1

    def test_multiple_lines(self):
        text = "\n".join(
            [
                _make_line("diaDA01", display="Tyfus"),
                _make_line("oprKABC10", display="Operation"),
                _make_line("tilZZ100", display="Tillaeg"),
            ]
        )
        records = _parse_sks_lines(text)
        assert len(records) == 3
        assert {r["classification"] for r in records} == {"dia", "opr", "til"}

    def test_display_padding_stripped(self):
        text = _make_line("diaDA01", display="Tyfus")
        assert _parse_sks_lines(text)[0]["display"] == "Tyfus"

    def test_aux_digit_fields_not_in_display(self):
        """The 3-digit auxiliary fields at pos 167-172 must not leak into display."""
        text = _make_line("diaDA01", display="Tyfus")
        rec = _parse_sks_lines(text)[0]
        assert "001" not in rec["display"]

    def test_full_width_display_preserved(self):
        long_display = "X" * 120
        text = _make_line("diaDA01", display=long_display)
        assert _parse_sks_lines(text)[0]["display"] == long_display

    def test_dates_are_date_objects(self):
        rec = _parse_sks_lines(_make_line("diaDA01"))[0]
        assert isinstance(rec["valid_from"], date)
        assert isinstance(rec["valid_to"], date)


class TestParseDate:
    """Two-pass date parsing: strict YYYYMMDD, then flexible fallback."""

    def test_strict_yyyymmdd(self):
        assert _parse_date("20200115") == date(2020, 1, 15)

    def test_no_end_sentinel(self):
        assert _parse_date("25000101") == date(2500, 1, 1)

    def test_flexible_fallback_with_warning(self, caplog):
        """Format drift (e.g. ISO dashes) still parses, but warns."""
        with caplog.at_level("WARNING", logger="DSI.csv_files.sks_register"):
            assert _parse_date("2020-01-15") == date(2020, 1, 15)
        assert "flexible fallback" in caplog.text

    def test_unparseable_returns_none_with_warning(self, caplog):
        with caplog.at_level("WARNING", logger="DSI.csv_files.sks_register"):
            assert _parse_date("NOTADATE") is None
        assert "could not be parsed" in caplog.text

    def test_calendar_invalid_returns_none(self):
        # Month 13 fails both strict and flexible parsing
        assert _parse_date("20201301") is None


class TestFormatValidation:
    """The parser must fail loudly when the source format has changed,
    so a malformed file never replaces the existing CodeSystem."""

    def test_non_date_field_raises(self):
        bad = f"{'diaDA01':<23}NOTADATE2020010125000101Some display"
        with pytest.raises(ValueError, match="format violation"):
            _parse_sks_lines(bad)

    def test_calendar_invalid_date_raises(self):
        # Month 13 passes the digit regex but is not a real date
        bad = _make_line("diaDA01", valid_from="20201301")
        with pytest.raises(ValueError, match="format violation"):
            _parse_sks_lines(bad)

    def test_garbage_lines_raise(self):
        text = "\n".join(["totally wrong format", _make_line("diaDA01")])
        with pytest.raises(ValueError, match="format violation"):
            _parse_sks_lines(text)

    def test_small_noise_fraction_tolerated_with_warning(self, caplog):
        # 1 bad line among 200 good ones (0.5%) stays under the 1% threshold,
        # but the skipped line is still surfaced as a warning
        lines = [_make_line(f"diaDA{i:03d}") for i in range(200)]
        lines.append("garbage line")
        with caplog.at_level("WARNING", logger="DSI.csv_files.sks_register"):
            records = _parse_sks_lines("\n".join(lines))
        assert len(records) == 200
        assert "skipped as format violations" in caplog.text

    def test_error_message_includes_samples(self):
        with pytest.raises(ValueError, match="garbage"):
            _parse_sks_lines("garbage line here")


class TestDeduplicateNewest:
    def test_single_version(self):
        records = [
            {
                "code": "DA01",
                "classification": "dia",
                "valid_from": date(2020, 1, 1),
                "valid_to": NO_END,
                "display": "Tyfus",
            },
        ]
        newest, classifications = _deduplicate_newest(records)
        assert newest["DA01"]["display"] == "Tyfus"
        assert classifications["DA01"] == {"dia"}

    def test_newest_version_wins(self):
        records = [
            {
                "code": "DA01",
                "classification": "dia",
                "valid_from": date(1994, 1, 1),
                "valid_to": date(2011, 12, 31),
                "display": "Old text",
            },
            {
                "code": "DA01",
                "classification": "dia",
                "valid_from": date(2012, 1, 1),
                "valid_to": NO_END,
                "display": "New text",
            },
        ]
        newest, _ = _deduplicate_newest(records)
        assert newest["DA01"]["display"] == "New text"
        assert newest["DA01"]["valid_to"] == NO_END

    def test_order_independence(self):
        records = [
            {
                "code": "DA01",
                "classification": "dia",
                "valid_from": date(2012, 1, 1),
                "valid_to": NO_END,
                "display": "New text",
            },
            {
                "code": "DA01",
                "classification": "dia",
                "valid_from": date(1994, 1, 1),
                "valid_to": date(2011, 12, 31),
                "display": "Old text",
            },
        ]
        newest, _ = _deduplicate_newest(records)
        assert newest["DA01"]["display"] == "New text"

    def test_classification_union_across_prefixes(self):
        records = [
            {
                "code": "ZZ100",
                "classification": "til",
                "valid_from": date(2010, 1, 1),
                "valid_to": NO_END,
                "display": "Via til",
            },
            {
                "code": "ZZ100",
                "classification": "pro",
                "valid_from": date(2015, 1, 1),
                "valid_to": NO_END,
                "display": "Via pro",
            },
        ]
        newest, classifications = _deduplicate_newest(records)
        assert classifications["ZZ100"] == {"til", "pro"}
        # display from the newest version overall
        assert newest["ZZ100"]["display"] == "Via pro"

    def test_tiebreak_on_valid_to(self):
        records = [
            {
                "code": "X",
                "classification": "dia",
                "valid_from": date(2020, 1, 1),
                "valid_to": date(2021, 12, 31),
                "display": "Closed",
            },
            {
                "code": "X",
                "classification": "dia",
                "valid_from": date(2020, 1, 1),
                "valid_to": NO_END,
                "display": "Open",
            },
        ]
        newest, _ = _deduplicate_newest(records)
        assert newest["X"]["display"] == "Open"


class TestBuildHierarchy:
    def test_empty_input(self):
        assert _build_hierarchy(set()) == {}

    def test_single_code_no_parent(self):
        assert _build_hierarchy({"A"}) == {}

    def test_simple_prefix_chain(self):
        parent_map = _build_hierarchy({"D", "DA", "DA01"})
        assert parent_map["DA"] == "D"
        assert parent_map["DA01"] == "DA"
        assert "D" not in parent_map

    def test_finds_nearest_parent(self):
        parent_map = _build_hierarchy({"D", "DA", "DA0", "DA01"})
        assert parent_map["DA01"] == "DA0"
        assert parent_map["DA0"] == "DA"
        assert parent_map["DA"] == "D"

    def test_siblings_share_parent(self):
        parent_map = _build_hierarchy({"D", "DA", "DB"})
        assert parent_map["DA"] == "D"
        assert parent_map["DB"] == "D"

    def test_unrelated_codes_no_parent(self):
        parent_map = _build_hierarchy({"ABC", "XYZ"})
        assert "ABC" not in parent_map
        assert "XYZ" not in parent_map


class _FakeContext:
    """Minimal stub for dg.AssetExecutionContext."""

    class log:
        @staticmethod
        def info(msg):
            pass

        @staticmethod
        def warning(msg):
            pass


class TestBuildExtRows:
    def test_empty_input(self):
        rows = _build_ext_rows({}, {}, TODAY, _FakeContext())
        assert rows == []

    def test_active_code(self):
        newest = {
            "DA01": {
                "code": "DA01",
                "classification": "dia",
                "valid_from": date(2012, 1, 1),
                "valid_to": NO_END,
                "display": "Tyfus",
            },
        }
        classifications = {"DA01": {"dia"}}
        rows = _build_ext_rows(newest, classifications, TODAY, _FakeContext())
        assert len(rows) == 1
        row = rows[0]
        assert set(row.keys()) == {"code", "display", "parent_code", "classification", "Inactive"}
        assert row["code"] == "DA01"
        assert row["classification"] == "dia"
        assert row["Inactive"] == "false"

    def test_inactive_when_valid_to_passed(self):
        newest = {
            "OLD1": {
                "code": "OLD1",
                "classification": "dia",
                "valid_from": date(2000, 1, 1),
                "valid_to": date(2010, 12, 31),
                "display": "Expired",
            },
        }
        rows = _build_ext_rows(newest, {"OLD1": {"dia"}}, TODAY, _FakeContext())
        assert rows[0]["Inactive"] == "true"

    def test_one_row_per_classification(self):
        newest = {
            "ZZ100": {
                "code": "ZZ100",
                "classification": "pro",
                "valid_from": date(2015, 1, 1),
                "valid_to": NO_END,
                "display": "Multi",
            },
        }
        classifications = {"ZZ100": {"til", "pro"}}
        rows = _build_ext_rows(newest, classifications, TODAY, _FakeContext())
        assert len(rows) == 2
        assert {r["classification"] for r in rows} == {"til", "pro"}
        # display/ParentCode/Inactive identical across the code's rows
        assert {r["display"] for r in rows} == {"Multi"}
        assert {r["Inactive"] for r in rows} == {"false"}

    def test_hierarchy_spans_classifications(self):
        # Clean codes are unique across classifications, so prefix-matching
        # links codes regardless of which classification they came from.
        newest = {
            "B": {
                "code": "B",
                "classification": "pro",
                "valid_from": date(2010, 1, 1),
                "valid_to": NO_END,
                "display": "Root",
            },
            "BA": {
                "code": "BA",
                "classification": "til",
                "valid_from": date(2010, 1, 1),
                "valid_to": NO_END,
                "display": "Child",
            },
        }
        classifications = {"B": {"pro"}, "BA": {"til"}}
        rows = _build_ext_rows(newest, classifications, TODAY, _FakeContext())
        ba_row = next(r for r in rows if r["code"] == "BA")
        assert ba_row["parent_code"] == "B"
        b_row = next(r for r in rows if r["code"] == "B")
        assert b_row["parent_code"] == ""


class TestEndToEndParsing:
    def test_parse_dedup_and_build(self):
        text = "\n".join(
            [
                _make_line("diaD", display="Diagnoser rod"),
                _make_line("diaDA01", "19940101", "20111028", "20111231", "Tyfus gammel"),
                _make_line("diaDA01", "20120101", "20111028", NO_END_STR, "Tyfus og paratyfus"),
                _make_line("tilZZ100", "20100101", "20100101", NO_END_STR, "Tillaegskode"),
                _make_line("proZZ100", "20150101", "20150101", NO_END_STR, "Plejekode"),
                _make_line("oprGAMMEL", "20000101", "20000101", "20051231", "Udgaaet operation"),
            ]
        )
        records = _parse_sks_lines(text)
        newest, classifications = _deduplicate_newest(records)
        rows = _build_ext_rows(newest, classifications, TODAY, _FakeContext())

        # 4 unique codes; ZZ100 yields 2 rows (til + pro)
        assert len(rows) == 5

        da01_rows = [r for r in rows if r["code"] == "DA01"]
        assert len(da01_rows) == 1
        assert da01_rows[0]["display"] == "Tyfus og paratyfus"
        assert da01_rows[0]["Inactive"] == "false"
        assert da01_rows[0]["parent_code"] == "D"

        zz_rows = [r for r in rows if r["code"] == "ZZ100"]
        assert {r["classification"] for r in zz_rows} == {"til", "pro"}
        assert {r["display"] for r in zz_rows} == {"Plejekode"}

        old_row = next(r for r in rows if r["code"] == "GAMMEL")
        assert old_row["Inactive"] == "true"


class TestSubsetGuard:
    """extract_sks_register must refuse any non-'all' classifications before
    touching the network or DB, because cs_sks uses a full-snapshot
    delete_stale MERGE."""

    def test_subset_classification_raises_before_network(self):
        """Passing classifications=['dia'] must raise RuntimeError immediately."""
        config = SKSExtractConfig(classifications=["dia"])
        # The guard fires before _fetch_sks_complete is called, so the
        # mssql resource is never touched — passing None is safe.
        with pytest.raises(RuntimeError, match="all") as exc_info:
            extract_sks_register(build_asset_context(), config, None, _SKS_DOWNLOAD)  # type: ignore[arg-type]
        assert "dia" in str(exc_info.value)

    def test_all_classifications_does_not_raise_guard(self):
        """The guard must not trigger when classifications=['all'].

        The function will proceed past the guard and attempt a network fetch,
        which raises a different exception — we only verify the guard message
        is absent.
        """
        config = SKSExtractConfig(classifications=["all"])
        with pytest.raises(Exception) as exc_info:
            extract_sks_register(build_asset_context(), config, None, _SKS_DOWNLOAD)  # type: ignore[arg-type]
        assert "only supports classifications" not in str(exc_info.value)

    def test_empty_list_raises_guard(self):
        config = SKSExtractConfig(classifications=[])
        with pytest.raises(RuntimeError, match="all"):
            extract_sks_register(build_asset_context(), config, None, _SKS_DOWNLOAD)  # type: ignore[arg-type]

    def test_multiple_subsets_raises_guard(self):
        config = SKSExtractConfig(classifications=["dia", "opr"])
        with pytest.raises(RuntimeError, match="all"):
            extract_sks_register(build_asset_context(), config, None, _SKS_DOWNLOAD)  # type: ignore[arg-type]


class TestDecodeSksBytes:
    """_decode_sks_bytes: plain latin-1 and gzip round-trips."""

    def test_plain_latin1_roundtrip(self):
        """Bytes with nordic characters (æ, ø, å) decode correctly via latin-1."""
        text = "Frisk \xe6blegrød med \xf8l og \xe5r"  # latin-1 encoded literals
        data = text.encode("latin-1")
        assert _decode_sks_bytes(data) == text

    def test_gzip_compressed_roundtrip(self):
        """Gzip-compressed latin-1 bytes are decompressed and decoded correctly."""
        text = "Tyfus og paratyfus: \xe6\xf8\xe5"
        compressed = gzip.compress(text.encode("latin-1"))
        assert _decode_sks_bytes(compressed) == text

    def test_short_bytes_not_treated_as_gzip(self):
        """Fewer than 3 bytes must not attempt gzip decompression."""
        data = b"\x1f"  # single gzip-magic byte, not a real gzip stream
        # Should decode without error (latin-1 of 0x1f is a control character)
        result = _decode_sks_bytes(data)
        assert isinstance(result, str)

    def test_non_gzip_bytes_decoded_as_latin1(self):
        """Bytes that don't start with the gzip magic are decoded as-is."""
        text = "Hello world"
        data = text.encode("latin-1")
        assert _decode_sks_bytes(data) == text
