# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tripwires: these sites previously failed SILENTLY GREEN when the key-column
grammar changed (naming overhaul, dw_ek_<PascalTable> grammar). If any assertion
here fails, DWH data-quality coverage or the FK-annotation gate has silently
disappeared — do NOT weaken these to make them pass; fix the regression.
"""

from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


class TestDqaLineageResolvesNewGrammar:
    def test_dw_ek_columns_non_empty_for_fct_fakturalinje(self):
        from dqa.references.lineage import dw_ek_columns

        cols = dw_ek_columns("fct.Fakturalinje")
        assert cols, "dw_ek columns no longer parsed from lineage — DWH DQA dead"
        assert all(c.startswith("dw_ek_") for c in cols)
        assert "dw_ek_Kreditor" in cols

    def test_referential_specs_non_empty_for_fct_fakturalinje(self):
        from dqa.references.lineage import referential_specs

        specs = referential_specs("fct.Fakturalinje")
        assert specs, "RI specs empty — lineage convention parser dead"


class TestLineageCheckerFlagsUnannotatedDwEk:
    def test_fk_column_is_relationship(self):
        from data_lineage.tools.lineage_ddl_check.checker import (
            _is_relationship_column,
        )

        assert _is_relationship_column(
            "fct", "Faktura", "dw_ek_Kreditor"
        ), "dw_ek_ FK columns no longer recognised — FK-annotation gate dead"

    def test_own_pk_is_exempt(self):
        from data_lineage.tools.lineage_ddl_check.checker import (
            _is_relationship_column,
        )

        assert not _is_relationship_column("fct", "Faktura", "dw_ek_Faktura")

    def test_old_grammar_no_longer_matches(self):
        """The pre-overhaul spelling must NOT be treated as an FK column."""
        from data_lineage.tools.lineage_ddl_check.checker import (
            _is_relationship_column,
        )

        assert not _is_relationship_column("fct", "Faktura", "DW_EK_Kreditor")


class TestDefinitionsContainsDqaSensors:
    def test_all_three_dqa_sensors_present(self):
        import definitions

        names = {s.name for s in definitions.defs.sensors}
        for expected in (
            "dqa_fhir_after_etl",
            "dqa_erp_daily_after_dfm",
            "dqa_arc_ifdb_after_archive",
        ):
            assert expected in names, f"sensor {expected} silently dropped"


class TestDiscoveryParsesNewGrammar:
    def test_parse_dim_name_new_grammar(self):
        from DSA.SQL.checks.discovery import parse_dim_name

        assert parse_dim_name("dw_ek_Kreditor") == ("Kreditor", None)
        assert parse_dim_name("dw_ek_Dato_faktura") == ("Dato", "faktura")

    def test_parse_dim_name_rejects_old_grammar(self):
        from DSA.SQL.checks.discovery import parse_dim_name

        assert parse_dim_name("DW_EK_Kreditor") is None

    def test_discover_finds_real_style_columns(self):
        """discover_dw_ek_columns must both LIKE-match and parse the new names."""
        from DSA.SQL.checks.discovery import discover_dw_ek_columns

        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("fct", "Fakturalinje", "dw_ek_Kreditor", "int"),
            ("fct", "Postering", "dw_ek_Dato_lukket", "int"),
        ]
        result = discover_dw_ek_columns(cursor, ["fct"])
        assert (
            len(result) == 2
        ), "discovery no longer parses dw_ek_ columns — DW quality checks dead"
        assert result[0]["dim_name"] == "Kreditor"
        assert result[1] == {
            "schema": "fct",
            "table": "Postering",
            "column": "dw_ek_Dato_lukket",
            "dim_name": "Dato",
            "role_suffix": "lukket",
            "data_type": "int",
        }
        # the SQL sent to the DB must still LIKE-match the lowercase grammar
        sql = cursor.execute.call_args[0][0]
        assert "dw[_]ek[_]%" in sql
