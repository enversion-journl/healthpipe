# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from unittest.mock import MagicMock

from DSA.SQL.checks.discovery import (
    discover_dw_ek_columns,
    get_existing_tables,
    get_primary_key_columns,
    parse_dim_name,
    sentinel_literal,
)


class TestParseDimName:
    def test_simple_column(self):
        assert parse_dim_name("DW_EK_Kreditor") == ("Kreditor", None)

    def test_role_playing_column(self):
        assert parse_dim_name("DW_EK_Dato_Godkendelses") == ("Dato", "Godkendelses")

    def test_multi_word_role(self):
        assert parse_dim_name("DW_EK_Org_MarkoerA") == ("Org", "MarkoerA")

    def test_date_role_playing(self):
        assert parse_dim_name("DW_EK_Dato_Faktura") == ("Dato", "Faktura")

    def test_invalid_column_name(self):
        assert parse_dim_name("SomeOtherColumn") is None

    def test_ek_only(self):
        assert parse_dim_name("DW_EK_") is None


class TestDiscoverDwEkColumns:
    def test_discovers_columns_across_schemas(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("fct", "Postering", "DW_EK_Kreditor", "int"),
            ("fct", "Postering", "DW_EK_Dato_Godkendelse", "int"),
            ("dim", "Kreditor", "DW_EK_Kreditor", "int"),
        ]
        result = discover_dw_ek_columns(cursor, ["fct", "dim", "map"])
        assert len(result) == 3
        assert result[0]["dim_name"] == "Kreditor"
        assert result[0]["role_suffix"] is None
        assert result[1]["dim_name"] == "Dato"
        assert result[1]["role_suffix"] == "Godkendelse"

    def test_skips_non_matching_columns(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("fct", "Postering", "ref_LedgertransId", "bigint"),
        ]
        result = discover_dw_ek_columns(cursor, ["fct"])
        assert len(result) == 0


class TestGetPrimaryKeyColumns:
    def test_returns_pk_columns(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("DW_EK_Fakturalinje",)]
        result = get_primary_key_columns(cursor, "fct", "Fakturalinje")
        assert result == {"DW_EK_Fakturalinje"}

    def test_returns_empty_for_no_pk(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        result = get_primary_key_columns(cursor, "fct", "SomeTable")
        assert result == set()


class TestGetExistingTables:
    def test_returns_table_names(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("Kreditor",), ("Vare",)]
        result = get_existing_tables(cursor, "dim")
        assert result == {"Kreditor", "Vare"}


class TestSentinelLiteral:
    def test_int_sentinel(self):
        assert sentinel_literal("int") == "-1"

    def test_bigint_sentinel(self):
        assert sentinel_literal("bigint") == "-1"

    def test_varchar_sentinel(self):
        assert sentinel_literal("varchar") == "'-1'"

    def test_nvarchar_sentinel(self):
        assert sentinel_literal("nvarchar") == "'-1'"
