# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the uploaded CSV file — the delivery itself, byte for byte."""

import csv
import io

import pytest
from utils.os2mo.codesystem_export import CodesystemExportSpec
from utils.os2mo.csv_format import encode_csv, render_csv, write_csv

pytestmark = pytest.mark.unit

# Simple test spec with no optional fields
_SIMPLE_SPEC = CodesystemExportSpec(
    name="Test",
    asset_prefix="test",
    source_deps=(),
    from_property="valid_from",
    to_property="valid_to",
)

# Test rows keyed by BASE_COLUMNS from Task 1
SOR_ROW = {
    "kode": "6101000016001",
    "kode_parent": "",
    "gyldig_fra": "",
    "gyldig_til": "",
    "navn": "Example Region",
    "type": "",
    "niveau": "1",
    "aktiv": "",
    "dw_is_deleted": "",
    "dw_timestamp_insert": "",
    "dw_timestamp_update": "",
}

OKOSTED_ROWS = [
    {
        "kode": "OUH",
        "kode_parent": "",
        "gyldig_fra": "",
        "gyldig_til": "",
        "navn": "OUH",
        "type": "",
        "niveau": "1",
        "aktiv": "",
        "dw_is_deleted": "",
        "dw_timestamp_insert": "",
        "dw_timestamp_update": "",
    },
    {
        "kode": "4711",
        "kode_parent": "",
        "gyldig_fra": "",
        "gyldig_til": "",
        "navn": "Kardiologi",
        "type": "",
        "niveau": "2",
        "aktiv": "",
        "dw_is_deleted": "",
        "dw_timestamp_insert": "",
        "dw_timestamp_update": "",
    },
    {
        "kode": "4712",
        "kode_parent": "",
        "gyldig_fra": "",
        "gyldig_til": "",
        "navn": "Ingen match",
        "type": "",
        "niveau": "2",
        "aktiv": "",
        "dw_is_deleted": "",
        "dw_timestamp_insert": "",
        "dw_timestamp_update": "",
    },
]


def _reparse(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(text.splitlines(), delimiter=";"))


def _reparse_rows(text: str) -> list[dict[str, str]]:
    """Reparse honouring quoted line breaks, as a CSV reader of the file would."""
    return list(csv.DictReader(io.StringIO(text, newline=""), delimiter=";"))


class TestRender:
    EXPECTED_HEADER = (
        '"kode";"kode_parent";"gyldig_fra";"gyldig_til";"navn";"type";'
        '"niveau";"aktiv";"dw_is_deleted";"dw_timestamp_insert";"dw_timestamp_update"'
    )

    def test_header_is_always_the_eleven_agreed_columns(self):
        # The header must not depend on the data: no dynamic map-column width.
        text = render_csv(_SIMPLE_SPEC, [])
        assert text.split("\r\n")[0] == self.EXPECTED_HEADER

    def test_every_field_is_quoted_even_when_empty(self):
        line = render_csv(_SIMPLE_SPEC, [SOR_ROW]).split("\r\n")[1]
        assert line.startswith('"6101000016001";"";')

    def test_quotes_separators_and_line_breaks_survive(self):
        name = 'Afsnit "Nord"; 2. sal\nbygning 7'
        row_with_name = {**SOR_ROW, "navn": name}
        rows = _reparse_rows(render_csv(_SIMPLE_SPEC, [row_with_name]))
        assert rows[0]["navn"] == name
        # The quote is escaped by doubling it.
        assert '"Afsnit ""Nord""; 2. sal' in render_csv(_SIMPLE_SPEC, [row_with_name])


class TestBytes:
    def test_lines_end_in_a_single_crlf(self):
        data = encode_csv(render_csv(_SIMPLE_SPEC, [SOR_ROW]))
        assert b"\r\r\n" not in data
        assert data.count(b"\r\n") == 2  # header + one row

    def test_the_file_starts_with_the_bom(self):
        assert encode_csv(render_csv(_SIMPLE_SPEC, [SOR_ROW])).startswith(b"\xef\xbb\xbf")

    def test_danish_letters_survive_the_round_trip(self):
        name = "Ærø Østergård Åbenrå — æøå"
        data = encode_csv(render_csv(_SIMPLE_SPEC, [{**SOR_ROW, "navn": name}]))

        # UTF-8: each letter is its two-byte sequence, never a "?" or a dropped byte.
        assert "Æ".encode() in data and "ø".encode() in data and "å".encode() in data
        rows = _reparse_rows(data.decode("utf-8-sig"))
        assert rows[0]["navn"] == name

    def test_a_utf8_sig_reader_sees_a_clean_first_header(self):
        data = encode_csv(render_csv(_SIMPLE_SPEC, [SOR_ROW]))
        assert _reparse_rows(data.decode("utf-8-sig"))[0]["kode"] == "6101000016001"

    def test_the_written_file_is_the_uploaded_bytes(self, tmp_path):
        text = render_csv(_SIMPLE_SPEC, OKOSTED_ROWS)
        path = write_csv(text, tmp_path / "nested" / "rsyd_okosted_20261006_013000.csv")
        assert path.read_bytes() == encode_csv(text)
