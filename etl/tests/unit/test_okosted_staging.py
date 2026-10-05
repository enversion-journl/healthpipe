# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for Organization shared dimensions staging and supporting utilities.

Tests validate:
- SOR helpers (parse_sor_date + determine_status from Organization_SOR_dfm,
  clean_sor_id + version-grain transform from the sor_register extract)
- stg_Okosted helpers (hierarchy/level computation, properties lookup, aktiv parsing)
- Postering omkostningsstednummer derivation (COALESCE over all 20 hierarchy levels)
"""

import re
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _mock_context() -> MagicMock:
    """Create a mock Dagster AssetExecutionContext with a logger."""
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


# =============================================================================
# TestStgOkostedHelpers
# =============================================================================


@pytest.mark.unit
class TestOkostedHierarchyLookup:
    """Tests for hierarchy and properties helpers in stg_Okosted."""

    def test_compute_levels_single_root(self):
        from DSA.SQL.staging.stg_Okosted import _compute_levels

        hierarchy = {"CHILD": "ROOT"}
        levels = _compute_levels(hierarchy)
        assert levels["CHILD"] == 2
        assert levels["ROOT"] == 1

    def test_compute_levels_deep_hierarchy(self):
        from DSA.SQL.staging.stg_Okosted import _compute_levels

        hierarchy = {"L3": "L2", "L2": "L1"}
        levels = _compute_levels(hierarchy)
        assert levels["L3"] == 3
        assert levels["L2"] == 2
        assert levels["L1"] == 1

    def test_compute_levels_handles_cycle(self):
        """A cyclic parent chain must terminate (visited-guard), not loop forever."""
        from DSA.SQL.staging.stg_Okosted import _compute_levels

        hierarchy = {"A": "B", "B": "A"}
        levels = _compute_levels(hierarchy)
        # Every code resolves to a finite depth despite the cycle.
        assert set(levels) == {"A", "B"}
        assert all(isinstance(d, int) and d >= 1 for d in levels.values())


# =============================================================================
# TestStgOkostedLookups (real-helper tests, mocked cursor)
# =============================================================================


@pytest.mark.unit
class TestStgOkostedLookups:
    """Tests that exercise the real stg_Okosted helper functions."""

    @staticmethod
    def _cursor(rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        return cursor

    def test_build_hierarchy_lookup_trims_whitespace(self):
        from DSA.SQL.staging.stg_Okosted import _build_hierarchy_lookup

        cursor = self._cursor([("  CHILD  ", "  PARENT  "), ("C2", "P2")])
        result = _build_hierarchy_lookup(cursor)
        assert result == {"CHILD": "PARENT", "C2": "P2"}

    def test_build_properties_lookup_groups_by_code(self):
        from DSA.SQL.staging.stg_Okosted import _build_properties_lookup

        cursor = self._cursor(
            [
                ("C1", "aktiv", "true"),
                ("C1", "other", "x"),
                ("C2", "aktiv", "false"),
            ]
        )
        result = _build_properties_lookup(cursor)
        assert result == {
            "C1": {"aktiv": "true", "other": "x"},
            "C2": {"aktiv": "false"},
        }

    def test_build_properties_lookup_handles_null_value(self):
        from DSA.SQL.staging.stg_Okosted import _build_properties_lookup

        cursor = self._cursor([("C1", "aktiv", None)])
        result = _build_properties_lookup(cursor)
        assert result == {"C1": {"aktiv": ""}}

    def test_parse_aktiv(self):
        from DSA.SQL.staging.stg_Okosted import _parse_aktiv

        assert _parse_aktiv("true") is True
        assert _parse_aktiv("TRUE") is True
        assert _parse_aktiv("false") is False
        assert _parse_aktiv("") is True  # missing property defaults to active
        assert _parse_aktiv(None) is True


# =============================================================================
# TestPosteringOkostedDerivation (postering cross-file contract)
# =============================================================================


@pytest.mark.unit
class TestPosteringOkostedDerivation:
    """omkostningsstednummer keys fct.Postering against dim.Okosted."""

    def test_coalesce_covers_all_20_levels_deepest_first(self):
        src = (
            Path(__file__).resolve().parents[2]
            / "DSA"
            / "SQL"
            / "staging"
            / "pre_stg_Postering_sydax.py"
        ).read_text(encoding="utf-8")
        match = re.search(r"COALESCE\((.*?)\)\s+AS\s+omkostningsstednummer", src, re.DOTALL)
        assert match, "omkostningsstednummer must be a COALESCE over omk_hier.[nummerN]"

        chain = match.group(1)
        positions = []
        for level in range(20, 0, -1):
            pos = chain.find(f"[nummer{level}]")
            assert pos != -1, (
                f"omk_hier.[nummer{level}] missing from the COALESCE chain — rows at "
                f"that hierarchy level silently derive a wrong omkostningsstednummer"
            )
            positions.append(pos)
        assert positions == sorted(
            positions
        ), "COALESCE must run nummer20..nummer1 so the deepest level wins"


# =============================================================================
# TestSorDateParsing
# =============================================================================


@pytest.mark.unit
class TestSorDateParsing:
    """Tests for parse_sor_date() from SOR staging."""

    def test_full_datetime(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import parse_sor_date

        result = parse_sor_date("2024-01-15 10:30:00")
        assert result == datetime(2024, 1, 15, 10, 30, 0)

    def test_date_only(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import parse_sor_date

        result = parse_sor_date("2024-01-15")
        assert result == datetime(2024, 1, 15)

    def test_empty_string_returns_none(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import parse_sor_date

        assert parse_sor_date("") is None
        assert parse_sor_date("   ") is None

    def test_none_returns_none(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import parse_sor_date

        assert parse_sor_date(None) is None

    def test_invalid_format_returns_none(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import parse_sor_date

        assert parse_sor_date("15/01/2024") is None
        assert parse_sor_date("not-a-date") is None

    def test_whitespace_trimmed(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import parse_sor_date

        result = parse_sor_date("  2024-01-15 10:30:00  ")
        assert result == datetime(2024, 1, 15, 10, 30, 0)


# =============================================================================
# TestCleanSorId
# =============================================================================


@pytest.mark.unit
class TestCleanSorId:
    """Tests for clean_sor_id() from SOR staging."""

    def test_excel_formula_format(self):
        from DSI.csv_files.sor_register import clean_sor_id

        assert clean_sor_id('="21000016006"') == "21000016006"

    def test_plain_value(self):
        from DSI.csv_files.sor_register import clean_sor_id

        assert clean_sor_id("21000016006") == "21000016006"

    def test_whitespace_trimmed(self):
        from DSI.csv_files.sor_register import clean_sor_id

        assert clean_sor_id("  21000016006  ") == "21000016006"

    def test_empty_returns_empty(self):
        from DSI.csv_files.sor_register import clean_sor_id

        assert clean_sor_id("") == ""

    def test_none_returns_empty(self):
        from DSI.csv_files.sor_register import clean_sor_id

        assert clean_sor_id(None) == ""

    def test_excel_with_whitespace(self):
        from DSI.csv_files.sor_register import clean_sor_id

        assert clean_sor_id(' ="21000016006" ') == "21000016006"


# =============================================================================
# TestDetermineStatus
# =============================================================================


@pytest.mark.unit
class TestDetermineStatus:
    """Tests for determine_status() from SOR staging."""

    def test_empty_to_date_is_active(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import determine_status

        assert determine_status("") == "active"
        assert determine_status("   ") == "active"

    def test_none_to_date_is_active(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import determine_status

        assert determine_status(None) == "active"

    def test_future_to_date_is_active(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import determine_status

        assert determine_status("2099-12-31 23:59:59") == "active"

    def test_past_to_date_is_closed(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import determine_status

        assert determine_status("2020-01-01 00:00:00") == "Closed"

    def test_invalid_to_date_is_active(self):
        from DSA.FHIR.Organization.Organization_SOR_dfm import determine_status

        assert determine_status("not-a-date") == "active"


# =============================================================================
# TestTransformToExtFormat (SOR version-grain extract)
# =============================================================================


@pytest.mark.unit
class TestTransformToExtFormat:
    """Tests for _transform_to_ext_format() — the extract keeps ALL versions
    (arc.sor_register is version-grained) and only collapses exact
    (SorId, FromDate) duplicates so the archive MERGE never sees duplicate keys."""

    def test_all_versions_of_a_code_are_kept(self):
        from DSI.csv_files.sor_register import _transform_to_ext_format

        rows = [
            {"SorId": "100", "FromDate": "2024-01-01", "EntityName": "V1"},
            {"SorId": "100", "FromDate": "2024-06-01", "EntityName": "V2"},
        ]
        result = _transform_to_ext_format(rows, _mock_context())
        assert len(result) == 2
        assert {r["from_date"] for r in result} == {"2024-01-01", "2024-06-01"}

    def test_duplicate_version_key_collapsed_to_one_row(self):
        from DSI.csv_files.sor_register import _transform_to_ext_format

        rows = [
            {"SorId": "100", "FromDate": "2024-01-01", "EntityName": "First"},
            {"SorId": "100", "FromDate": "2024-01-01", "EntityName": "Second"},
        ]
        result = _transform_to_ext_format(rows, _mock_context())
        assert len(result) == 1
        assert result[0]["display"] == "First"

    def test_excel_format_ids_cleaned(self):
        from DSI.csv_files.sor_register import _transform_to_ext_format

        rows = [
            {"SorId": '="100"', "FromDate": "2024-01-01", "ParentSorId": '="200"'},
        ]
        result = _transform_to_ext_format(rows, _mock_context())
        assert result[0]["code"] == "100"
        assert result[0]["parent_code"] == "200"

    def test_empty_sor_id_skipped(self):
        from DSI.csv_files.sor_register import _transform_to_ext_format

        rows = [
            {"SorId": "", "FromDate": "2024-01-01"},
            {"SorId": "100", "FromDate": "2024-01-01"},
        ]
        result = _transform_to_ext_format(rows, _mock_context())
        assert len(result) == 1
        assert result[0]["code"] == "100"

    def test_csv_columns_mapped_to_ext_names(self):
        from DSI.csv_files.sor_register import _transform_to_ext_format

        rows = [
            {
                "SorId": "100",
                "FromDate": "2024-01-01",
                "ToDate": "2025-01-01",
                "EntityName": "Afdeling X",
                "EntityTypeName": "klinisk enhed",
                "FirstFromDate": "2020-01-01",
            }
        ]
        result = _transform_to_ext_format(rows, _mock_context())
        row = result[0]
        assert row["to_date"] == "2025-01-01"
        assert row["display"] == "Afdeling X"
        assert row["entityType"] == "klinisk enhed"
        assert row["openingDate"] == "2020-01-01"
