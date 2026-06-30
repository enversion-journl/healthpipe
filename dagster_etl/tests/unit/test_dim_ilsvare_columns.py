# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for DimUpd_ilsvare new columns + swap rename."""

import sys

import DSA.SQL.dim.DimUpd_ilsvare  # noqa: F401

_m = sys.modules["DSA.SQL.dim.DimUpd_ilsvare"]
RESOLVE_SQL = _m.RESOLVE_SQL
MERGE_SQL = _m.MERGE_SQL


class TestResolve:
    def test_swap_aliases(self):
        assert "stg.vareart AS Gruppering" in RESOLVE_SQL
        assert "stg.vareart_navn AS Gruppering_Navn" in RESOLVE_SQL
        assert "stg.varegrp AS Art" in RESOLVE_SQL
        assert "stg.varegrp_navn AS Art_Navn" in RESOLVE_SQL

    def test_old_aliases_gone(self):
        assert " AS Vareart," not in RESOLVE_SQL
        assert " AS Varegruppe," not in RESOLVE_SQL

    def test_new_columns_resolved_with_nullif(self):
        assert "NULLIF(stg.faelvnr, '') AS FaellesVarenummer" in RESOLVE_SQL
        assert "NULLIF(stg.ean_vnr, '') AS EAN_Varenummer" in RESOLVE_SQL
        assert "NULLIF(stg.billed, '') AS Billede" in RESOLVE_SQL
        assert "NULLIF(stg.billedli, '') AS Weblink" in RESOLVE_SQL
        assert "NULLIF(stg.ejlukkes, '') AS MaaIkkeLukkes" in RESOLVE_SQL
        assert "NULLIF(stg.steril, '') AS Hygiejne" in RESOLVE_SQL

    def test_oprbruger_uppercased_and_name_resolved(self):
        assert "UPPER(NULLIF(stg.oprbruger, '')) AS OprBruger" in RESOLVE_SQL
        assert "NULLIF(stg.oprbruger_navn, '') AS OprBruger_Navn" in RESOLVE_SQL


class TestMerge:
    def test_update_sets_renamed_and_new(self):
        for frag in ("tgt.Gruppering = src.Gruppering",
                     "tgt.Art = src.Art",
                     "tgt.FaellesVarenummer = src.FaellesVarenummer",
                     "tgt.OprBruger = src.OprBruger",
                     "tgt.OprBruger_Navn = src.OprBruger_Navn",
                     "tgt.Hygiejne = src.Hygiejne"):
            assert frag in MERGE_SQL, frag

    def test_insert_lists_new_columns(self):
        for col in ("Gruppering", "Art", "FaellesVarenummer", "EAN_Varenummer",
                    "Billede", "OprBruger", "OprBruger_Navn", "Weblink",
                    "MaaIkkeLukkes", "Hygiejne"):
            assert col in MERGE_SQL, col
        assert "src.Gruppering" in MERGE_SQL
        assert "src.Hygiejne" in MERGE_SQL

    def test_old_names_gone_from_merge(self):
        assert "tgt.Vareart " not in MERGE_SQL
        assert "tgt.Varegruppe " not in MERGE_SQL
