# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the uploaded CSV file — the delivery itself, byte for byte."""

import csv
import io

import pytest
from DSO.export.os2mo.codesystems import OKOSTED_EXPORT, SOR_EXPORT
from utils.os2mo.csv_format import encode_csv, render_csv, write_csv

pytestmark = pytest.mark.unit

SOR_ROW = {"user_key": "6101000016001", "name": "Example Region", "_mappings": []}

OKOSTED_ROWS = [
    {"user_key": "OUH", "name": "OUH", "_mappings": [("12345", "SOR", "equivalent")]},
    {
        "user_key": "4711",
        "name": "Kardiologi",
        "_mappings": [("12345", "SOR", "narrower"), ("67890", "SOR", "narrower")],
    },
    {"user_key": "4712", "name": "Ingen match", "_mappings": []},
]


def _reparse(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(text.splitlines(), delimiter=";"))


def _reparse_rows(text: str) -> list[dict[str, str]]:
    """Reparse honouring quoted line breaks, as a CSV reader of the file would."""
    return list(csv.DictReader(io.StringIO(text, newline=""), delimiter=";"))


class TestRender:
    def test_the_header_uses_os2mo_names(self):
        header = render_csv(SOR_EXPORT, [SOR_ROW]).split("\r\n")[0]
        assert header == (
            '"user_key";"parent_user_key";"validity_from";"validity_to";"name";'
            '"org_unit_type";"active";"deleted";"created";"updated"'
        )

    def test_mappings_spread_into_numbered_columns(self):
        rows = {row["user_key"]: row for row in _reparse(render_csv(OKOSTED_EXPORT, OKOSTED_ROWS))}

        assert rows["4711"]["map_code_1"] == "12345"
        assert rows["4711"]["map_system_2"] == "SOR"
        assert rows["4711"]["map_relation_2"] == "narrower"
        assert rows["OUH"]["map_code_2"] == ""
        assert rows["4712"]["map_code_1"] == ""

    def test_every_field_is_quoted_even_when_empty(self):
        line = render_csv(SOR_EXPORT, [SOR_ROW]).split("\r\n")[1]
        assert line.startswith('"6101000016001";"";')

    def test_quotes_separators_and_line_breaks_survive(self):
        name = 'Afsnit "Nord"; 2. sal\nbygning 7'
        rows = _reparse_rows(render_csv(SOR_EXPORT, [{**SOR_ROW, "name": name}]))
        assert rows[0]["name"] == name
        # The quote is escaped by doubling it.
        assert '"Afsnit ""Nord""; 2. sal' in render_csv(SOR_EXPORT, [{**SOR_ROW, "name": name}])

    def test_the_internal_mapping_list_never_reaches_the_file(self):
        assert "_mappings" not in render_csv(OKOSTED_EXPORT, OKOSTED_ROWS)


class TestBytes:
    def test_lines_end_in_a_single_crlf(self):
        data = encode_csv(render_csv(SOR_EXPORT, [SOR_ROW]))
        assert b"\r\r\n" not in data
        assert data.count(b"\r\n") == 2  # header + one row

    def test_the_file_starts_with_the_bom(self):
        assert encode_csv(render_csv(SOR_EXPORT, [SOR_ROW])).startswith(b"\xef\xbb\xbf")

    def test_danish_letters_survive_the_round_trip(self):
        name = "Ærø Østergård Åbenrå — æøå"
        data = encode_csv(render_csv(SOR_EXPORT, [{**SOR_ROW, "name": name}]))

        # UTF-8: each letter is its two-byte sequence, never a "?" or a dropped byte.
        assert "Æ".encode() in data and "ø".encode() in data and "å".encode() in data
        rows = _reparse_rows(data.decode("utf-8-sig"))
        assert rows[0]["name"] == name

    def test_a_utf8_sig_reader_sees_a_clean_first_header(self):
        data = encode_csv(render_csv(SOR_EXPORT, [SOR_ROW]))
        assert _reparse_rows(data.decode("utf-8-sig"))[0]["user_key"] == "6101000016001"

    def test_the_written_file_is_the_uploaded_bytes(self, tmp_path):
        text = render_csv(OKOSTED_EXPORT, OKOSTED_ROWS)
        path = write_csv(text, tmp_path / "nested" / "rsyd_okosted_20261006_013000.csv")
        assert path.read_bytes() == encode_csv(text)
