# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the SHAK/SOR map extraction helpers (ext.shaksor_map)."""

import pytest
from DSI.csv_files.shaksor_map import (
    EXT_COLUMNS,
    _clean_excel,
    _is_direct,
    select_latest_direct,
    transform_to_ext_format,
)

pytestmark = pytest.mark.unit


def _row(shak, sor, *, direct=True, from_date="2020-01-01", to_date="", updated="", name="Afd"):
    return {
        "ShakId": shak,
        "SorId": sor,
        "IsDirectSorShakMapping": "True" if direct else "False",
        "FromDate": from_date,
        "ToDate": to_date,
        "UpdatedAt": updated,
        "ShakName": name,
    }


class TestCleanExcel:
    def test_strips_excel_formula_wrapping(self):
        assert _clean_excel('="7951000016004"') == "7951000016004"

    def test_plain_value_untouched(self):
        assert _clean_excel("4202011") == "4202011"

    def test_blank(self):
        assert _clean_excel("") == ""
        assert _clean_excel(None) == ""


class TestIsDirect:
    def test_true_false(self):
        assert _is_direct({"IsDirectSorShakMapping": "True"}) is True
        assert _is_direct({"IsDirectSorShakMapping": "false"}) is False
        assert _is_direct({}) is False


class TestSelectLatestDirect:
    def test_one_row_per_shak(self):
        rows = [
            _row("2000", '="111"', from_date="2008-01-01", to_date="2011-01-01"),
            _row("2000", '="222"', from_date="2012-01-01", to_date=""),
        ]
        selected = select_latest_direct(rows)
        assert len(selected) == 1
        # Newest open mapping wins (empty ToDate, later FromDate).
        assert _clean_excel(selected[0]["SorId"]) == "222"

    def test_direct_preferred_over_indirect(self):
        rows = [
            _row("3000", '="999"', direct=False, from_date="2025-01-01", to_date=""),
            _row("3000", '="111"', direct=True, from_date="2010-01-01", to_date="2012-01-01"),
        ]
        selected = select_latest_direct(rows)
        assert len(selected) == 1
        assert _clean_excel(selected[0]["SorId"]) == "111"

    def test_indirect_only_falls_back_and_is_flagged(self):
        # A SHAK code with only indirect candidates keeps its newest indirect
        # mapping (not dropped) and is flagged isDirect=False in the ext row.
        rows = [
            _row("4000", '="111"', direct=False, from_date="2010-01-01", to_date="2012-01-01"),
            _row("4000", '="222"', direct=False, from_date="2020-01-01", to_date=""),
        ]
        selected = select_latest_direct(rows)
        assert len(selected) == 1
        assert _clean_excel(selected[0]["SorId"]) == "222"
        ext = transform_to_ext_format(selected)
        assert ext[0]["isDirect"] == "False"

    def test_blank_shak_skipped(self):
        assert select_latest_direct([_row("", '="1"')]) == []


class TestTransformToExtFormat:
    def test_basic_mapping(self):
        rows = [
            {
                "ShakId": "4202011",
                "SorId": '="215811000016008"',
                "IsDirectSorShakMapping": "True",
                "ShakName": "Medicinsk Afd",
                "SorEntityName": "Sygehus Lillebaelt",
                "ShakInstitutionTypeId": "102",
                "ShakInstitutionTypeName": "Hovedsygehuse II",
                "ShakDateFrom": "2007-01-01 00:00:00",
            }
        ]
        ext = transform_to_ext_format(rows)
        assert len(ext) == 1
        r = ext[0]
        assert r["code"] == "4202011"
        assert r["SorCode"] == "215811000016008"
        assert r["display"] == "Medicinsk Afd"
        assert r["SorDisplay"] == "Sygehus Lillebaelt"
        assert r["isDirect"] == "True"
        assert r["institutionTypeId"] == "102"
        assert r["institutionType"] == "Hovedsygehuse II"
        assert r["validFrom"] == "2007-01-01 00:00:00"
        # every declared ext column is present
        assert set(r.keys()) == set(EXT_COLUMNS)

    def test_display_falls_back_to_code(self):
        rows = [{"ShakId": "1234", "SorId": '="1"', "ShakName": ""}]
        ext = transform_to_ext_format(rows)
        assert ext[0]["display"] == "1234"

    def test_rows_without_code_or_target_dropped(self):
        rows = [
            {"ShakId": "", "SorId": '="1"'},
            {"ShakId": "1234", "SorId": ""},
            {"ShakId": "5678", "SorId": '="9"'},
        ]
        ext = transform_to_ext_format(rows)
        assert [r["code"] for r in ext] == ["5678"]
