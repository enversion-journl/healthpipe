# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for ext.vare_enriched_ils new columns + concat fix (task 28149)."""

from DSI.mssql import ils_enriched as mod

NEW_SOURCE_COLS = [
    "faelvnr", "ean_vnr", "billed", "oprbruger",
    "oprbruger_navn", "billedli", "ejlukkes", "steril",
]


class TestSourceQuery:
    def test_source_query_selects_new_columns(self):
        for col in ("v.faelvnr", "v.ean_vnr", "v.billed", "v.oprbruger",
                    "v.billedli", "v.ejlukkes", "v.steril"):
            assert col in mod.SOURCE_QUERY, col

    def test_source_query_joins_zz_oper_for_full_name(self):
        assert "LEFT JOIN dbo.zz_oper zz ON zz.bd_init = v.oprbruger" in mod.SOURCE_QUERY
        assert "zz.bd_navn AS oprbruger_navn" in mod.SOURCE_QUERY


class TestColumnLists:
    def test_columns_contains_new_names(self):
        for col in NEW_SOURCE_COLS:
            assert col in mod.COLUMNS, col

    def test_insert_columns_contains_new_names(self):
        for col in NEW_SOURCE_COLS:
            assert col in mod.INSERT_COLUMNS, col

    def test_column_counts(self):
        assert len(mod.COLUMNS) == 30
        assert len(mod.INSERT_COLUMNS) == 31

    def test_varenavn_concat_removed(self):
        # Redundant computed column dropped — no longer extracted or inserted.
        assert "varenavn_concat" not in mod.COLUMNS
        assert "varenavn_concat" not in mod.INSERT_COLUMNS
        assert not hasattr(mod, "build_varenavn_concat")


class TestEnrichedTableDdl:
    def test_ddl_declares_new_columns(self):
        for frag in ("[faelvnr]", "[ean_vnr]", "[billed]", "[oprbruger]",
                     "[oprbruger_navn]", "[billedli]", "[ejlukkes]", "[steril]"):
            assert frag in mod.ENRICHED_TABLE_DDL, frag

    def test_ddl_omits_varenavn_concat(self):
        assert "[varenavn_concat]" not in mod.ENRICHED_TABLE_DDL
