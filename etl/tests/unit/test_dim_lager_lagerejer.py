# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dim.Lager: the warehouse owner plus the four columns that were never filled.

Test feedback from DOL asked for `lgrejer`, which is used to look up a
requisitioner number. Strategisk Indkoeb describes it as "a requisitioner used for
goods movements into the warehouse", and highlights `rekvsvind` ("a requisitioner
used to record shrinkage") and `deflevdage` ("a default delivery time for the
warehouse") in the same list — both likewise absent.

Along the way it turned out that dim.Lager.adr/land/postdi/tlf were ALWAYS NULL:
stg_dim_Lager pivoted seven property keys, but cs_lager wrote only four. The
property parity test below is the most important one in this file — it catches
exactly that defect, and would have caught it years ago.

The split between the two paths follows one rule: details belong in the
CodeSystem as properties — except address details, which are populated outside it.

  CodeSystem : lagerejer_rekvirentnummer, svind_rekvirentnummer,
               standard_leveringsdage, aktiv
  joined in  : adresse, land, postnummer, telefon
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import DSA.SQL.staging.stg_dim_Lager  # noqa: F401
import DSA.SQL.staging.stg_dim_Rekvirent  # noqa: F401
import pytest
import yaml
from DSA.CodeSystem.lager.lager import build_lager_hierarchy
from DSA.CodeSystem.rekvirent.rekvirent import build_rekvirent_hierarchy

pytestmark = pytest.mark.unit

_REPO_ETL = Path(__file__).resolve().parents[2]
_DIMS_DDL = _REPO_ETL / "infrastructure" / "ddl" / "tables" / "erp" / "02_dims.sql"
_LINEAGE = _REPO_ETL / "data_lineage" / "dim" / "dim_lager.yml"
_MIGRATIONS = _REPO_ETL / "infrastructure" / "ddl" / "migrations"

# Column in dim.Lager -> source column in arc.ils_lgr. Spelled out, not the ILS
# abbreviations (per the naming standard); provenance is recorded in
# data_lineage/dim/dim_lager.yml.
LAGER_SOURCE_COLUMNS = {
    "adresse": "adr",
    "land": "land",
    "postnummer": "postdi",
    "telefon": "tlf",
    "lagerejer_rekvirentnummer": "lgrejer",
    "svind_rekvirentnummer": "rekvsvind",
    "standard_leveringsdage": "deflevdage",
    "aktiv": "aktiv",
}

# Carried by the CodeSystem: property key -> dim column.
CODESYSTEM_BORNE = {
    "lagerejerRekvirentnummer": "lagerejer_rekvirentnummer",
    "svindRekvirentnummer": "svind_rekvirentnummer",
    "standardLeveringsdage": "standard_leveringsdage",
    "aktiv": "aktiv",
}

# Populated outside the CodeSystem: address details, joined from arc.ils_lgr.
JOINED_COLUMNS = ("adresse", "land", "postnummer", "telefon")

# The four that were declared, pivoted — and never written.
PREVIOUSLY_DEAD_COLUMNS = ("adresse", "land", "postnummer", "telefon")

_LGR_COLUMNS = [
    "sgh",
    "lgr",
    "navn",
    "adr",
    "land",
    "postdi",
    "tlf",
    "lgrejer",
    "rekvsvind",
    "deflevdage",
    "aktiv",
]
_LGR_ROW = [
    "6000",
    "0100",
    "Centrallager",
    "Vejen 1",
    "DK",
    "6000",
    "12345678",
    "0042",
    "0099",
    3,
    True,
]
_SGH_ROW = ["6000", "Sygehus Lillebælt"]


def _property_column_map(name: str) -> dict[str, str]:
    """The package __init__ re-exports the asset under the module's own name."""
    return sys.modules[f"DSA.SQL.staging.{name}"].PROPERTY_COLUMN_MAP


def _dim_module(name: str):
    __import__(f"DSA.SQL.dim.{name}")
    return sys.modules[f"DSA.SQL.dim.{name}"]


def _lager_properties() -> dict[str, str]:
    """Run the real builder over one hospital + one warehouse; return the lager's props."""
    _, properties, _ = build_lager_hierarchy([_SGH_ROW], [_LGR_ROW], ["sgh", "navn"], _LGR_COLUMNS)
    lgr_code = "SGH_6000_LGR_0100"
    return {p["KEY"]: p["VALUE"] for p in properties if p["CODE"] == lgr_code}


# ---------------------------------------------------------------------------
# The guardrail: a pivoted property that nobody writes is a column of NULLs.
# ---------------------------------------------------------------------------


def test_every_pivoted_lager_property_is_actually_written():
    """dim.Lager.adr/land/postdi/tlf were NULL for exactly this reason."""
    written = set(_lager_properties())
    pivoted = set(_property_column_map("stg_dim_Lager"))
    missing = sorted(pivoted - written)
    assert not missing, (
        f"stg_dim_Lager pivots {missing} out of codesystem.LagerProperties, but cs_lager "
        f"never writes them — every one of those dim.Lager columns is silently all-NULL."
    )


def test_every_pivoted_rekvirent_property_is_actually_written():
    """Same invariant for the sister dimension, so neither can drift."""
    _, properties, _ = build_rekvirent_hierarchy(
        [["6000", "Sygehus Lillebælt"]],
        [["6000", "A1", "AFD", "0042", "Afdeling A"]],
        [["A1", "Gruppe A1"]],
        ["sgh", "navn"],
        ["sgh", "rgrp", "rtyp", "rnr", "navn"],
        ["rgrp", "navn"],
    )
    written = {p["KEY"] for p in properties}
    pivoted = set(_property_column_map("stg_dim_Rekvirent"))
    missing = sorted(pivoted - written)
    assert not missing, f"stg_dim_Rekvirent pivots unwritten properties: {missing}"


# ---------------------------------------------------------------------------
# cs_lager must publish the warehouse attributes
# ---------------------------------------------------------------------------


class TestCodeSystemProperties:
    def test_lagerejer_carries_the_requisitioner_number(self):
        assert _lager_properties().get("lagerejerRekvirentnummer") == "0042"

    def test_svind_requisitioner_and_delivery_days_are_published(self):
        props = _lager_properties()
        assert props.get("svindRekvirentnummer") == "0099"
        assert props.get("standardLeveringsdage") == "3"

    def test_aktiv_flag_is_published_and_false_is_kept(self):
        """BIT arrives as bool; False must be written as '0', not dropped as blank."""
        assert _lager_properties().get("aktiv") == "1"
        inactive = [*_LGR_ROW[:-1], False]
        _, properties, _ = build_lager_hierarchy(
            [_SGH_ROW], [inactive], ["sgh", "navn"], _LGR_COLUMNS
        )
        props = {p["KEY"]: p["VALUE"] for p in properties if p["CODE"] == "SGH_6000_LGR_0100"}
        assert props.get("aktiv") == "0"

    def test_duplicate_rows_dedup_by_newest_pro2modified_then_prrowid(self):
        """Same rule as build_adresse_update_sql (pro2modified DESC, prrowid ASC),
        so display/properties and the address enrichment come from the same
        source row when (sgh, lgr) has duplicates."""
        columns = [*_LGR_COLUMNS, "pro2modified", "prrowid"]
        older = [*_LGR_ROW[:2], "Gammelt navn", *_LGR_ROW[3:], datetime(2024, 1, 1), "a"]
        newer = [*_LGR_ROW[:2], "Nyt navn", *_LGR_ROW[3:], datetime(2026, 1, 1), "b"]
        concepts, _, _ = build_lager_hierarchy([_SGH_ROW], [older, newer], ["sgh", "navn"], columns)
        display = {c["CODE"]: c["DISPLAY"] for c in concepts}["SGH_6000_LGR_0100"]
        assert display == "Nyt navn"

    def test_address_attributes_are_NOT_published_as_properties(self):
        """Address details are populated outside the CodeSystem — including for
        warehouses, where they are not identifying. The rule is phrased in terms
        of the field, not a judgement made per table."""
        props = _lager_properties()
        for key in ("adresse", "land", "postnummer", "telefon", "adr", "postdi", "tlf"):
            assert key not in props, key

    def test_blank_values_are_not_written_as_empty_property_rows(self):
        """A warehouse with no owner must not get a blank lagerejer row —
        codesystem.LagerProperties syncs to DSO, so empty rows are pure noise."""
        sparse = ["6000", "0200", "Tomt lager", "", "", "", "", "", "", None, None]
        _, properties, _ = build_lager_hierarchy(
            [_SGH_ROW], [sparse], ["sgh", "navn"], _LGR_COLUMNS
        )
        keys = {p["KEY"] for p in properties if p["CODE"] == "SGH_6000_LGR_0200"}
        for optional in ("lagerejerRekvirentnummer", "standardLeveringsdage"):
            assert optional not in keys, f"{optional} written with a blank value"


# ---------------------------------------------------------------------------
# Staging + dim carry them through
# ---------------------------------------------------------------------------


class TestStagingAndDim:
    def test_staging_pivots_the_codesystem_borne_columns(self):
        pivoted = _property_column_map("stg_dim_Lager")
        for key, column in CODESYSTEM_BORNE.items():
            assert pivoted.get(key) == column, key

    def test_address_columns_are_not_pivoted_from_the_codesystem(self):
        pivoted = set(_property_column_map("stg_dim_Lager").values())
        for col in JOINED_COLUMNS:
            assert col not in pivoted, col

    def test_address_columns_are_joined_in_from_the_archive(self):
        stg = sys.modules["DSA.SQL.staging.stg_dim_Lager"]
        sql = stg.build_adresse_update_sql()
        assert "[arc].[ils_lgr]" in sql
        assert "LagerProperties" not in sql
        for col in JOINED_COLUMNS:
            source = LAGER_SOURCE_COLUMNS[col]
            assert f"stg.[{col}] = NULLIF(LTRIM(RTRIM(r.[{source}])), '')" in sql, col

    def test_address_join_dedupes_and_matches_on_sgh_and_lgr(self):
        """arc.ils_lgr has prrowid as its PK but (sgh, lgr) as the logical key."""
        stg = sys.modules["DSA.SQL.staging.stg_dim_Lager"]
        sql = stg.build_adresse_update_sql()
        assert "ROW_NUMBER() OVER" in sql
        assert "rn = 1" in sql
        from DSA.SQL.common.ils_clean_code_shared import sql_norm_code

        assert sql_norm_code("stg.[sgh]") in sql
        assert sql_norm_code("stg.[lgr]") in sql

    def test_staging_insert_names_its_columns(self):
        """The joined columns are filled in afterwards; without an explicit column
        list, INSERT ... VALUES would miscount."""
        stg = sys.modules["DSA.SQL.staging.stg_dim_Lager"]
        insert_sql = stg.build_staging_insert_sql()
        assert "INSERT INTO [stg].[dim_Lager] (" in insert_sql
        for col in CODESYSTEM_BORNE.values():
            assert f"[{col}]" in insert_sql, col
        for col in JOINED_COLUMNS:
            assert f"[{col}]" not in insert_sql, col

    def test_no_raw_ils_abbreviation_survives_as_a_dim_column(self):
        columns = set(_property_column_map("stg_dim_Lager").values())
        for abbrev in ("adr", "postdi", "tlf", "lgrejer", "rekvsvind", "deflevdage"):
            assert abbrev not in columns, abbrev

    def test_resolve_selects_every_column(self):
        sql = _dim_module("dim_Lager").build_resolve_sql()
        for col in LAGER_SOURCE_COLUMNS:
            assert f"stg.[{col}]" in sql, col

    def test_requisitioners_resolve_to_rekvirent_entity_keys(self):
        """A rekvirent reference belongs in the model as a DW_EK, not only as a
        loose string (dw_ek_* resolved via map tables, -1 = unknown)."""
        sql = _dim_module("dim_Lager").build_resolve_sql()
        assert "[map].[Rekvirent]" in sql
        assert "dw_ek_Rekvirent_lagerejer" in sql
        assert "COALESCE(m_rekv.dw_ek_Rekvirent, -1)" in sql
        assert "dw_ek_Rekvirent_svind" in sql
        assert "COALESCE(m_svind.dw_ek_Rekvirent, -1)" in sql

    def test_requisitioner_joins_are_guarded_against_the_hospital_fallback(self):
        """sql_clean_code_rekvirent falls back to the hospital-level member when the
        child segment is absent. Without a guard, every warehouse WITHOUT the
        requisitioner stated would silently resolve to its hospital's rekvirent
        instead of to -1."""
        sql = _dim_module("dim_Lager").build_resolve_sql()
        from DSA.SQL.common.ils_clean_code_shared import sql_norm_code

        for col in ("lagerejer_rekvirentnummer", "svind_rekvirentnummer"):
            guard = f"{sql_norm_code(f'stg.[{col}]')} IS NOT NULL"
            assert guard in sql, f"join must require a stated {col}, not merely a non-NULL one"

    def test_typed_attributes_are_try_cast_in_resolve(self):
        """Property values are NVARCHAR in staging; TRY_CAST types the temp table
        and turns a malformed value into NULL instead of failing the MERGE."""
        sql = _dim_module("dim_Lager").build_resolve_sql()
        assert "TRY_CAST(stg.[standard_leveringsdage] AS INT)" in sql
        assert "TRY_CAST(stg.[aktiv] AS BIT)" in sql

    def test_merge_writes_every_column(self):
        sql = _dim_module("dim_Lager").MERGE_SQL
        fks = ("dw_ek_Rekvirent_lagerejer", "dw_ek_Rekvirent_svind")
        for col in (*LAGER_SOURCE_COLUMNS, *fks):
            assert f"[{col}] = source.[{col}]" in sql, col
        insert_branch = sql[sql.index("WHEN NOT MATCHED BY TARGET") :]
        for col in (*LAGER_SOURCE_COLUMNS, *fks):
            assert f"source.[{col}]" in insert_branch, col


# ---------------------------------------------------------------------------
# DDL, migration, lineage
# ---------------------------------------------------------------------------


class TestDdlAndMigration:
    @pytest.fixture(scope="class")
    def dim_block(self):
        ddl = _DIMS_DDL.read_text()
        block = ddl[ddl.index("CREATE TABLE [dim].[Lager]") :]
        return block[: block.index("GO")]

    def test_declares_the_new_columns(self, dim_block):
        for col in (*LAGER_SOURCE_COLUMNS, "dw_ek_Rekvirent_lagerejer", "dw_ek_Rekvirent_svind"):
            assert f"[{col}]" in dim_block, col

    def test_old_abbreviations_are_gone(self, dim_block):
        for abbrev in ("[adr]", "[postdi]", "[tlf]"):
            assert abbrev not in dim_block, abbrev

    def test_delivery_days_is_an_integer_not_a_string(self, dim_block):
        assert "[standard_leveringsdage] [int]" in dim_block.lower().replace("  ", " ")

    def test_migration_renames_and_adds_guarded(self):
        texts = [p.read_text() for p in _MIGRATIONS.glob("20260917_02_*.sql")]
        assert texts, "no 29731 migration found"
        text = "\n".join(texts)
        for old, new in (("adr", "adresse"), ("postdi", "postnummer"), ("tlf", "telefon")):
            assert f"'dim.Lager.{old}', '{new}'" in text, f"{old} -> {new} rename missing"
        for col in (
            "lagerejer_rekvirentnummer",
            "svind_rekvirentnummer",
            "standard_leveringsdage",
            "aktiv",
        ):
            assert f"ADD [{col}]" in text, col
        assert "dw_ek_Rekvirent_lagerejer" in text
        assert "dw_ek_Rekvirent_svind" in text


class TestLineage:
    @pytest.fixture(scope="class")
    def columns(self):
        return yaml.safe_load(_LINEAGE.read_text())["dso_lineage"]["Columns"]

    def test_every_column_declares_its_ils_source(self, columns):
        for dim_col, source_col in LAGER_SOURCE_COLUMNS.items():
            if dim_col == "aktiv":
                continue  # both levels carry it — see the test below
            assert dim_col in columns, dim_col
            assert columns[dim_col]["Source"] == [f"ils.dbo.lgr.{source_col}"], dim_col

    def test_aktiv_comes_from_the_warehouse_and_the_hospital(self, columns):
        """A lager row's flag is dbo.lgr.aktiv, a sygehus row's is dbo.sgh.aktiv."""
        assert columns["aktiv"]["Source"] == ["ils.dbo.lgr.aktiv", "ils.dbo.sgh.aktiv"]

    def test_previously_dead_columns_no_longer_claim_to_come_from_sgh(self, columns):
        """They were documented as ils.dbo.sgh.* while being populated by nothing."""
        for col in PREVIOUSLY_DEAD_COLUMNS:
            assert "sgh" not in str(columns[col]["Source"]), col

    def test_requisitioner_fks_are_annotated(self, columns):
        for col in ("dw_ek_Rekvirent_lagerejer", "dw_ek_Rekvirent_svind"):
            fk = columns[col]
            assert fk["Type"] == "FK", col
            assert fk["FK-Target"] == "dim.Rekvirent", col
