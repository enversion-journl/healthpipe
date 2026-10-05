# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim_ILSVare new columns + swap rename (task 28149)."""

import sys

import DSA.SQL.dim.dim_ILSVare  # noqa: F401
import pytest

pytestmark = pytest.mark.unit

_m = sys.modules["DSA.SQL.dim.dim_ILSVare"]
RESOLVE_SQL = _m.RESOLVE_SQL
MERGE_SQL = _m.MERGE_SQL


class TestResolve:
    def test_swap_aliases(self):
        assert "stg.vareart AS gruppering" in RESOLVE_SQL
        assert "stg.vareart_navn AS gruppering_navn" in RESOLVE_SQL
        assert "stg.varegrp AS art" in RESOLVE_SQL
        assert "stg.varegrp_navn AS art_navn" in RESOLVE_SQL

    def test_old_aliases_gone(self):
        assert " AS Vareart," not in RESOLVE_SQL
        assert " AS Varegruppe," not in RESOLVE_SQL

    def test_new_columns_resolved_with_nullif(self):
        assert "NULLIF(stg.faelvnr, '') AS faelles_varenummer" in RESOLVE_SQL
        assert "NULLIF(stg.ean_vnr, '') AS ean_varenummer" in RESOLVE_SQL
        assert "NULLIF(stg.billed, '') AS billede" in RESOLVE_SQL
        assert "NULLIF(stg.billedli, '') AS weblink" in RESOLVE_SQL
        assert "NULLIF(stg.ejlukkes, '') AS maa_ikke_lukkes" in RESOLVE_SQL
        assert "NULLIF(stg.steril, '') AS hygiejne" in RESOLVE_SQL

    def test_oprbruger_uppercased_and_name_resolved(self):
        assert "UPPER(NULLIF(stg.oprbruger, '')) AS opr_bruger" in RESOLVE_SQL
        assert "NULLIF(stg.oprbruger_navn, '') AS opr_bruger_navn" in RESOLVE_SQL


class TestMerge:
    def test_update_sets_renamed_and_new(self):
        for frag in (
            "tgt.gruppering = src.gruppering",
            "tgt.art = src.art",
            "tgt.faelles_varenummer = src.faelles_varenummer",
            "tgt.opr_bruger = src.opr_bruger",
            "tgt.opr_bruger_navn = src.opr_bruger_navn",
            "tgt.hygiejne = src.hygiejne",
        ):
            assert frag in MERGE_SQL, frag

    def test_insert_lists_new_columns(self):
        for col in (
            "gruppering",
            "art",
            "faelles_varenummer",
            "ean_varenummer",
            "billede",
            "opr_bruger",
            "opr_bruger_navn",
            "weblink",
            "maa_ikke_lukkes",
            "hygiejne",
        ):
            assert col in MERGE_SQL, col
        assert "src.gruppering" in MERGE_SQL
        assert "src.hygiejne" in MERGE_SQL

    def test_old_names_gone_from_merge(self):
        assert "tgt.Vareart " not in MERGE_SQL
        assert "tgt.Varegruppe " not in MERGE_SQL
