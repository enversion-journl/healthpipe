# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for Organization shared dimensions staging and supporting utilities.

Tests validate:
- SOR staging helpers (parse_sor_date, clean_sor_id, determine_status, deduplication)
- DSA ownership contract (staging assets registered in DSA, correct keys)
- stg_okosted helpers (hierarchy/level computation, properties lookup, aktiv parsing)
- Postering COALESCE completeness (cross-file validation)
"""

import os
from datetime import datetime
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
    """Tests for hierarchy and properties helpers in stg_okosted."""

    def test_compute_levels_single_root(self):
        from DSA.SQL.staging.stg_okosted import _compute_levels

        hierarchy = {"CHILD": "ROOT"}
        levels = _compute_levels(hierarchy)
        assert levels["CHILD"] == 2
        assert levels["ROOT"] == 1

    def test_compute_levels_deep_hierarchy(self):
        from DSA.SQL.staging.stg_okosted import _compute_levels

        hierarchy = {"L3": "L2", "L2": "L1"}
        levels = _compute_levels(hierarchy)
        assert levels["L3"] == 3
        assert levels["L2"] == 2
        assert levels["L1"] == 1

    def test_compute_levels_handles_cycle(self):
        """A cyclic parent chain must terminate (visited-guard), not loop forever."""
        from DSA.SQL.staging.stg_okosted import _compute_levels

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
    """Tests that exercise the real stg_okosted helper functions."""

    @staticmethod
    def _cursor(rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        return cursor

    def test_build_hierarchy_lookup_trims_whitespace(self):
        from DSA.SQL.staging.stg_okosted import _build_hierarchy_lookup

        cursor = self._cursor([("  CHILD  ", "  PARENT  "), ("C2", "P2")])
        result = _build_hierarchy_lookup(cursor)
        assert result == {"CHILD": "PARENT", "C2": "P2"}

    def test_build_properties_lookup_groups_by_code(self):
        from DSA.SQL.staging.stg_okosted import _build_properties_lookup

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
        from DSA.SQL.staging.stg_okosted import _build_properties_lookup

        cursor = self._cursor([("C1", "aktiv", None)])
        result = _build_properties_lookup(cursor)
        assert result == {"C1": {"aktiv": ""}}

    def test_parse_aktiv(self):
        from DSA.SQL.staging.stg_okosted import _parse_aktiv

        assert _parse_aktiv("true") is True
        assert _parse_aktiv("TRUE") is True
        assert _parse_aktiv("false") is False
        assert _parse_aktiv("") is True  # missing property defaults to active
        assert _parse_aktiv(None) is True


# =============================================================================
# TestOkostedCompleteness (postering cross-file check)
# =============================================================================


@pytest.mark.unit
class TestOkostedCompleteness:
    """Cross-file validation for postering COALESCE chain."""

    def test_postering_coalesce_includes_all_20_levels(self):
        """The COALESCE chain in sydax_enriched.py must include nummer20..nummer1."""
        sql_file_path = os.path.join(
            os.path.dirname(__file__),
            "../../DSI/mssql/sydax_enriched.py",
        )

        with open(sql_file_path, encoding="utf-8") as f:
            content = f.read()

        for i in range(1, 21):
            found = f"omk_hier.nummer{i}" in content or f"omk_hier.[nummer{i}]" in content
            assert found, (
                f"omk_hier.nummer{i} not found in sydax_enriched.py COALESCE chain. "
                f"All 20 levels must be included in the Omkostningsstednummer derivation."
            )


# =============================================================================
# TestSorDateParsing
# =============================================================================


@pytest.mark.unit
class TestSorDateParsing:
    """Tests for parse_sor_date() from SOR staging."""

    def test_full_datetime(self):
        from DSA.SQL.staging.stg_organisation_sor import parse_sor_date

        result = parse_sor_date("2024-01-15 10:30:00")
        assert result == datetime(2024, 1, 15, 10, 30, 0)

    def test_date_only(self):
        from DSA.SQL.staging.stg_organisation_sor import parse_sor_date

        result = parse_sor_date("2024-01-15")
        assert result == datetime(2024, 1, 15)

    def test_empty_string_returns_none(self):
        from DSA.SQL.staging.stg_organisation_sor import parse_sor_date

        assert parse_sor_date("") is None
        assert parse_sor_date("   ") is None

    def test_none_returns_none(self):
        from DSA.SQL.staging.stg_organisation_sor import parse_sor_date

        assert parse_sor_date(None) is None

    def test_invalid_format_returns_none(self):
        from DSA.SQL.staging.stg_organisation_sor import parse_sor_date

        assert parse_sor_date("15/01/2024") is None
        assert parse_sor_date("not-a-date") is None

    def test_whitespace_trimmed(self):
        from DSA.SQL.staging.stg_organisation_sor import parse_sor_date

        result = parse_sor_date("  2024-01-15 10:30:00  ")
        assert result == datetime(2024, 1, 15, 10, 30, 0)


# =============================================================================
# TestCleanSorId
# =============================================================================


@pytest.mark.unit
class TestCleanSorId:
    """Tests for clean_sor_id() from SOR staging."""

    def test_excel_formula_format(self):
        from DSA.SQL.staging.stg_organisation_sor import clean_sor_id

        assert clean_sor_id('="21000016006"') == "21000016006"

    def test_plain_value(self):
        from DSA.SQL.staging.stg_organisation_sor import clean_sor_id

        assert clean_sor_id("21000016006") == "21000016006"

    def test_whitespace_trimmed(self):
        from DSA.SQL.staging.stg_organisation_sor import clean_sor_id

        assert clean_sor_id("  21000016006  ") == "21000016006"

    def test_empty_returns_empty(self):
        from DSA.SQL.staging.stg_organisation_sor import clean_sor_id

        assert clean_sor_id("") == ""

    def test_none_returns_empty(self):
        from DSA.SQL.staging.stg_organisation_sor import clean_sor_id

        assert clean_sor_id(None) == ""

    def test_excel_with_whitespace(self):
        from DSA.SQL.staging.stg_organisation_sor import clean_sor_id

        assert clean_sor_id(' ="21000016006" ') == "21000016006"


# =============================================================================
# TestDetermineStatus
# =============================================================================


@pytest.mark.unit
class TestDetermineStatus:
    """Tests for determine_status() from SOR staging."""

    def test_empty_to_date_is_active(self):
        from DSA.SQL.staging.stg_organisation_sor import determine_status

        assert determine_status("") == "Active"
        assert determine_status("   ") == "Active"

    def test_none_to_date_is_active(self):
        from DSA.SQL.staging.stg_organisation_sor import determine_status

        assert determine_status(None) == "Active"

    def test_future_to_date_is_active(self):
        from DSA.SQL.staging.stg_organisation_sor import determine_status

        assert determine_status("2099-12-31 23:59:59") == "Active"

    def test_past_to_date_is_closed(self):
        from DSA.SQL.staging.stg_organisation_sor import determine_status

        assert determine_status("2020-01-01 00:00:00") == "Closed"

    def test_invalid_to_date_is_active(self):
        from DSA.SQL.staging.stg_organisation_sor import determine_status

        assert determine_status("not-a-date") == "Active"


# =============================================================================
# TestDeduplicateSorByLatest
# =============================================================================


@pytest.mark.unit
class TestDeduplicateSorByLatest:
    """Tests for deduplicate_sor_by_latest() from SOR staging."""

    def test_no_duplicates(self):
        from DSA.SQL.staging.stg_organisation_sor import deduplicate_sor_by_latest

        rows = [
            {"SorId": "100", "FromDate": "2024-01-01"},
            {"SorId": "200", "FromDate": "2024-02-01"},
        ]
        result = deduplicate_sor_by_latest(rows, _mock_context(), "test")
        assert len(result) == 2

    def test_duplicate_keeps_latest(self):
        from DSA.SQL.staging.stg_organisation_sor import deduplicate_sor_by_latest

        rows = [
            {"SorId": "100", "FromDate": "2024-01-01", "EntityName": "Old"},
            {"SorId": "100", "FromDate": "2024-06-01", "EntityName": "New"},
        ]
        result = deduplicate_sor_by_latest(rows, _mock_context(), "test")
        assert len(result) == 1
        assert result[0]["EntityName"] == "New"

    def test_excel_format_sor_ids_deduplicated(self):
        from DSA.SQL.staging.stg_organisation_sor import deduplicate_sor_by_latest

        rows = [
            {"SorId": '="100"', "FromDate": "2024-01-01", "EntityName": "Old"},
            {"SorId": '="100"', "FromDate": "2024-06-01", "EntityName": "New"},
        ]
        result = deduplicate_sor_by_latest(rows, _mock_context(), "test")
        assert len(result) == 1

    def test_empty_sor_id_skipped(self):
        from DSA.SQL.staging.stg_organisation_sor import deduplicate_sor_by_latest

        rows = [
            {"SorId": "", "FromDate": "2024-01-01"},
            {"SorId": "100", "FromDate": "2024-01-01"},
        ]
        result = deduplicate_sor_by_latest(rows, _mock_context(), "test")
        assert len(result) == 1

    def test_three_versions_keeps_latest(self):
        from DSA.SQL.staging.stg_organisation_sor import deduplicate_sor_by_latest

        rows = [
            {"SorId": "100", "FromDate": "2024-01-01", "EntityName": "V1"},
            {"SorId": "100", "FromDate": "2024-03-01", "EntityName": "V2"},
            {"SorId": "100", "FromDate": "2024-06-01", "EntityName": "V3"},
        ]
        result = deduplicate_sor_by_latest(rows, _mock_context(), "test")
        assert len(result) == 1
        assert result[0]["EntityName"] == "V3"


# =============================================================================
# TestDsaOwnershipContract
# =============================================================================


@pytest.mark.unit
class TestDsaOwnershipContract:
    """Verify that organization staging assets are owned by DSA, not DSI."""

    def test_staging_assets_importable_from_dsa(self):
        from DSA.SQL.staging import staging_assets

        assert len(staging_assets) >= 3

    def test_staging_assets_have_correct_keys(self):
        from DSA.SQL.staging import staging_assets

        keys = {str(a.key) for a in staging_assets}
        assert "AssetKey(['stg_organisation_sor'])" in keys
        assert "AssetKey(['stg_okosted'])" in keys

    def test_all_assets_includes_staging(self):
        from DSA.SQL.staging import all_assets

        keys = {str(a.key) for a in all_assets}
        assert "AssetKey(['stg_organisation_sor'])" in keys
        assert "AssetKey(['stg_okosted'])" in keys

    def test_dsi_stg_no_longer_importable(self):
        """DSI.stg.shared_dimensions must not exist after the move."""
        import importlib

        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("DSI.stg.shared_dimensions")
