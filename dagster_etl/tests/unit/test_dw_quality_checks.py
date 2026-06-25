# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from unittest.mock import MagicMock, patch

from DSA.SQL.checks.dw_quality_checks import (
    dq_dim_map_referential_check,
    dq_fact_dim_referential_check,
    dq_null_check,
)


def _make_mssql_resource(cursor):
    """Create a mock MSSQLResource whose get_connection() context yields a conn."""
    resource = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    resource.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    resource.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    return resource


class TestDqNullCheck:
    @patch("DSA.SQL.checks.dw_quality_checks.discover_dw_ek_columns")
    def test_passes_when_no_nulls(self, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "fct",
                "table": "Postering",
                "column": "DW_EK_Kreditor",
                "dim_name": "Kreditor",
                "role_suffix": None,
                "data_type": "int",
            },
        ]
        cursor = MagicMock()
        # TOP 1 existence check returns no rows -> no nulls
        cursor.fetchone.return_value = None
        resource = _make_mssql_resource(cursor)

        result = dq_null_check(mssql_target_staging=resource)
        assert result.passed is True

    @patch("DSA.SQL.checks.dw_quality_checks.discover_dw_ek_columns")
    def test_fails_when_nulls_found(self, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "fct",
                "table": "Postering",
                "column": "DW_EK_Kreditor",
                "dim_name": "Kreditor",
                "role_suffix": None,
                "data_type": "int",
            },
        ]
        cursor = MagicMock()
        # TOP 1 returns a row -> nulls exist; then CASE aggregation returns 5
        cursor.fetchone.side_effect = [(1,), (5,)]
        resource = _make_mssql_resource(cursor)

        result = dq_null_check(mssql_target_staging=resource)
        assert result.passed is False


_REF_CHECK_MODULE = "DSA.SQL.checks.dw_quality_checks"


class TestDqFactDimReferentialCheck:
    @patch(f"{_REF_CHECK_MODULE}.discover_dw_ek_columns")
    @patch(f"{_REF_CHECK_MODULE}.get_existing_tables")
    @patch(f"{_REF_CHECK_MODULE}.get_primary_key_columns")
    def test_passes_when_all_fks_resolve(self, mock_pk, mock_tables, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "fct",
                "table": "Postering",
                "column": "DW_EK_Kreditor",
                "dim_name": "Kreditor",
                "role_suffix": None,
                "data_type": "int",
            },
        ]
        mock_tables.return_value = {"Kreditor", "Vare"}
        mock_pk.return_value = {"ref_LedgertransId"}  # DW_EK_Kreditor is not PK
        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)  # 0 orphans
        resource = _make_mssql_resource(cursor)

        result = dq_fact_dim_referential_check(mssql_target_staging=resource)
        assert result.passed is True

    @patch(f"{_REF_CHECK_MODULE}.discover_dw_ek_columns")
    @patch(f"{_REF_CHECK_MODULE}.get_existing_tables")
    @patch(f"{_REF_CHECK_MODULE}.get_primary_key_columns")
    def test_fails_when_orphans_found(self, mock_pk, mock_tables, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "fct",
                "table": "Postering",
                "column": "DW_EK_Kreditor",
                "dim_name": "Kreditor",
                "role_suffix": None,
                "data_type": "int",
            },
        ]
        mock_tables.return_value = {"Kreditor"}
        mock_pk.return_value = {"ref_LedgertransId"}
        cursor = MagicMock()
        cursor.fetchone.return_value = (12,)  # 12 orphans
        resource = _make_mssql_resource(cursor)

        result = dq_fact_dim_referential_check(mssql_target_staging=resource)
        assert result.passed is False

    @patch(f"{_REF_CHECK_MODULE}.discover_dw_ek_columns")
    @patch(f"{_REF_CHECK_MODULE}.get_existing_tables")
    @patch(f"{_REF_CHECK_MODULE}.get_primary_key_columns")
    def test_skips_pk_when_no_matching_dim(self, mock_pk, mock_tables, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "fct",
                "table": "Fakturalinje",
                "column": "DW_EK_Fakturalinje",
                "dim_name": "Fakturalinje",
                "role_suffix": None,
                "data_type": "int",
            },
        ]
        mock_tables.return_value = set()  # No dim.Fakturalinje
        mock_pk.return_value = {"DW_EK_Fakturalinje"}  # It's the PK
        cursor = MagicMock()
        resource = _make_mssql_resource(cursor)

        result = dq_fact_dim_referential_check(mssql_target_staging=resource)
        assert result.passed is True  # Skipped, no failures
        assert "skipped_pk_columns" in result.metadata

    @patch(f"{_REF_CHECK_MODULE}.discover_dw_ek_columns")
    @patch(f"{_REF_CHECK_MODULE}.get_existing_tables")
    @patch(f"{_REF_CHECK_MODULE}.get_primary_key_columns")
    def test_warns_on_unresolvable_dim(self, mock_pk, mock_tables, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "fct",
                "table": "Postering",
                "column": "DW_EK_Dato_Lukket",
                "dim_name": "Date",
                "role_suffix": "Lukket",
                "data_type": "int",
            },
        ]
        mock_tables.return_value = set()  # No dim.Date
        mock_pk.return_value = {"ref_LedgertransId"}  # Not a PK
        cursor = MagicMock()
        resource = _make_mssql_resource(cursor)

        result = dq_fact_dim_referential_check(mssql_target_staging=resource)
        assert result.passed is False
        failed = result.metadata["failed"].value
        assert len(failed["unresolvable_columns"]) > 0


class TestDqDimMapReferentialCheck:
    @patch(f"{_REF_CHECK_MODULE}.discover_dw_ek_columns")
    @patch(f"{_REF_CHECK_MODULE}.get_existing_tables")
    @patch(f"{_REF_CHECK_MODULE}.get_primary_key_columns")
    def test_passes_when_all_dim_keys_in_map(self, mock_pk, mock_tables, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "dim",
                "table": "Kreditor",
                "column": "DW_EK_Kreditor",
                "dim_name": "Kreditor",
                "role_suffix": None,
                "data_type": "int",
            },
        ]
        mock_tables.return_value = {"Kreditor"}
        mock_pk.return_value = {"DW_EK_Kreditor"}
        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)
        resource = _make_mssql_resource(cursor)

        result = dq_dim_map_referential_check(mssql_target_staging=resource)
        assert result.passed is True

    @patch(f"{_REF_CHECK_MODULE}.discover_dw_ek_columns")
    @patch(f"{_REF_CHECK_MODULE}.get_existing_tables")
    @patch(f"{_REF_CHECK_MODULE}.get_primary_key_columns")
    def test_fails_when_orphans_found(self, mock_pk, mock_tables, mock_discover):
        mock_discover.return_value = [
            {
                "schema": "dim",
                "table": "Kreditor",
                "column": "DW_EK_Kreditor",
                "dim_name": "Kreditor",
                "role_suffix": None,
                "data_type": "int",
            },
        ]
        mock_tables.return_value = {"Kreditor"}
        mock_pk.return_value = {"DW_EK_Kreditor"}
        cursor = MagicMock()
        cursor.fetchone.return_value = (3,)
        resource = _make_mssql_resource(cursor)

        result = dq_dim_map_referential_check(mssql_target_staging=resource)
        assert result.passed is False


from DSA.SQL.checks.dw_quality_checks import dq_faktura_postering_match_check


class TestDqFakturaPosteringMatchCheck:
    def test_passes_and_reports_zero_collisions(self):
        cursor = MagicMock()
        # (total_invoices, multi_header_invoices, max_headers_per_invoice)
        cursor.fetchone.return_value = (13_262_008, 0, 1)
        resource = _make_mssql_resource(cursor)

        result = dq_faktura_postering_match_check(mssql_target_staging=resource)

        assert result.passed is True
        meta = result.metadata
        assert meta["total_invoices"].value == 13_262_008
        assert meta["multi_header_invoices"].value == 0
        assert meta["max_headers_per_invoice"].value == 1

    def test_passes_but_reports_collisions(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (13_262_008, 504_066, 2_599)
        resource = _make_mssql_resource(cursor)

        result = dq_faktura_postering_match_check(mssql_target_staging=resource)

        # Non-failing by design: collapse is expected/observable, not an error.
        assert result.passed is True
        assert result.metadata["multi_header_invoices"].value == 504_066
        assert result.metadata["max_headers_per_invoice"].value == 2_599

    def test_handles_empty_staging(self):
        cursor = MagicMock()
        # Empty staging: aggregation over zero rows yields NULLs from MAX/SUM.
        cursor.fetchone.return_value = (0, None, None)
        resource = _make_mssql_resource(cursor)

        result = dq_faktura_postering_match_check(mssql_target_staging=resource)

        assert result.passed is True
        assert result.metadata["total_invoices"].value == 0
        assert result.metadata["multi_header_invoices"].value == 0
        assert result.metadata["max_headers_per_invoice"].value == 0
