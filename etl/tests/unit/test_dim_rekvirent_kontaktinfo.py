# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the requisitioner address and contact columns.

Test feedback from DOL asked for altrnr, adr, adr2, postdi, kontakt, ktosted,
land and by in dim.Rekvirent. The first seven come from arc.ils_rekv; "by" comes
from the new arc.ils_postdi.

Every column takes the same path: joined into stg_dim_Rekvirent from the
archive after the bulk insert, from ONE physical source row per requisitioner.
None of them may be a CodeSystem property — codesystem.RekvirentProperties
syncs to DSO WITHOUT a masking view (adr/adr2/kontakt hold home-patient PII),
and its merge keeps stale rows, so a value the source clears would survive
there forever.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

import DSA.SQL.staging.stg_dim_Rekvirent  # noqa: F401
import pytest
from DSA.CodeSystem.rekvirent.rekvirent import build_rekvirent_hierarchy
from DSA.SQL.common.rekvirent_kontaktinfo import (
    KONTAKTINFO_COLUMNS as _CONTRACT_COLUMNS,
)
from DSA.SQL.common.rekvirent_kontaktinfo import (
    REKVIRENT_ATTRIBUTES,
)

pytestmark = pytest.mark.unit

_DDL_ROOT = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_ARCHIVE_DDL = _DDL_ROOT / "sources" / "ils" / "ils_postdi.sql"
_DIMS_DDL = _DDL_ROOT / "tables" / "erp" / "02_dims.sql"
_MIGRATION_SQL = _DDL_ROOT / "migrations" / "20260917_01_dim_rekvirent_adresse_kontakt.sql"
_LINEAGE_YML = Path(__file__).resolve().parents[2] / "data_lineage" / "dim" / "dim_rekvirent.yml"

# The columns as they are named all the way from stg to dim. Spelled out, not
# the ILS abbreviations: the naming standard says "use only well-known
# abbreviations ... and avoid abbreviations like dg for dage or kd for kode".
# Where a column comes from is recorded in data_lineage/dim/dim_rekvirent.yml,
# not in its name.
KONTAKTINFO_COLUMNS = (
    "alternativt_rekvirentnummer",
    "adresse",
    "adresse2",
    "land",
    "postnummer",
    "bynavn",
    "kontaktperson",
    "kontosted",
)

# Dimension column -> source column in arc.ils_rekv. bynavn is deliberately
# absent: it is looked up in arc.ils_postdi.
EXPECTED_SOURCE_COLUMNS = {
    "alternativt_rekvirentnummer": "altrnr",
    "adresse": "adr",
    "adresse2": "adr2",
    "land": "land",
    "postnummer": "postdi",
    "kontaktperson": "kontakt",
    "kontosted": "ktosted",
}

# The subset that is identifying for rtyp HP/OHP.
PII_COLUMNS = ("adresse", "adresse2", "kontaktperson")

_REKV_COLUMNS = ["sgh", "rgrp", "rtyp", "rnr", "navn", "altrnr", "ktosted", "adr", "kontakt"]
_REKV_ROW = ["6000", "A1", "AFD", "0042", "Afdeling A", "ALT42", "KS-9", "Hjemmevej 3", "Anna"]


def _rekvirent_properties(rekv_row=None) -> dict[str, str]:
    """Run the real builder; return the leaf requisitioner concept's properties."""
    _, properties, _ = build_rekvirent_hierarchy(
        [["6000", "Sygehus Lillebælt"]],
        [rekv_row or _REKV_ROW],
        [["A1", "Gruppe A1"]],
        ["sgh", "navn"],
        _REKV_COLUMNS,
        ["rgrp", "navn"],
    )
    return {p["KEY"]: p["VALUE"] for p in properties if p["CODE"] == "RNR_6000_0042"}


def test_every_pivoted_property_is_actually_written():
    """A pivoted property the CodeSystem never writes yields a column that is
    always NULL — exactly the defect dim.Lager.adr/postdi/tlf suffered from."""
    stg = sys.modules["DSA.SQL.staging.stg_dim_Rekvirent"]
    written = set(_rekvirent_properties())
    missing = sorted(set(stg.PROPERTY_COLUMN_MAP) - written)
    assert not missing, f"stg_dim_Rekvirent pivots properties nobody writes: {missing}"


class TestKontaktinfoStaysOutsideTheCodeSystem:
    """codesystem.RekvirentProperties syncs to DSO unmasked and its merge keeps
    stale rows — no address or contact detail may live there."""

    def test_builder_writes_no_kontaktinfo_property(self):
        props = _rekvirent_properties()
        assert set(props) == {"entityType", "sgh", "rgrp", "rtyp", "rnr", "mapCode"}

    def test_no_kontaktinfo_column_is_pivoted_from_a_property(self):
        stg = sys.modules["DSA.SQL.staging.stg_dim_Rekvirent"]
        pivoted_columns = set(stg.PROPERTY_COLUMN_MAP.values())
        for col in KONTAKTINFO_COLUMNS:
            assert col not in pivoted_columns, (
                f"{col} must be joined in from the archive, not carried as a "
                "codesystem property — codesystem.RekvirentProperties syncs to "
                "DSO unmasked and never stale-deletes."
            )

    def test_no_pii_column_can_ever_reach_the_codesystem(self):
        """The hard version of the above: PII must never sit in a table without a
        masking view, however the property map may change later."""
        written = set(_rekvirent_properties())
        stg = sys.modules["DSA.SQL.staging.stg_dim_Rekvirent"]
        for col in PII_COLUMNS:
            assert col not in written, col
            assert col not in set(stg.PROPERTY_COLUMN_MAP), col


class TestOneSourceRowPerRequisitioner:
    """arc.ils_rekv has prrowid as its PK but (sgh, rnr) as the logical key.
    The builder must dedupe with the SAME rule as build_kontaktinfo_update_sql's
    ROW_NUMBER (pro2modified DESC, prrowid ASC) — otherwise one dim row could
    take its display from one physical row and its address from another."""

    _COLUMNS = ["sgh", "rgrp", "rtyp", "rnr", "navn", "pro2modified", "prrowid"]

    def _leaf_displays(self, rekv_rows) -> list[str]:
        concepts, _, _ = build_rekvirent_hierarchy(
            [["6000", "Sygehus Lillebælt"]],
            rekv_rows,
            [["A1", "Gruppe A1"]],
            ["sgh", "navn"],
            self._COLUMNS,
            ["rgrp", "navn"],
        )
        return [c["DISPLAY"] for c in concepts if c["CODE"] == "RNR_6000_0042"]

    def test_newest_pro2modified_wins_regardless_of_input_order(self):
        rows = [
            ["6000", "A1", "AFD", "0042", "Gammelt navn", datetime(2026, 1, 1), "aaa"],
            ["6000", "A1", "AFD", "0042", "Nyt navn", datetime(2026, 6, 1), "bbb"],
        ]
        assert self._leaf_displays(rows) == ["Nyt navn"]
        assert self._leaf_displays(list(reversed(rows))) == ["Nyt navn"]

    def test_lowest_prrowid_wins_a_timestamp_tie(self):
        rows = [
            ["6000", "A1", "AFD", "0042", "Række b", datetime(2026, 1, 1), "b"],
            ["6000", "A1", "AFD", "0042", "Række a", datetime(2026, 1, 1), "a"],
        ]
        assert self._leaf_displays(rows) == ["Række a"]

    def test_a_dated_row_beats_an_undated_one(self):
        rows = [
            ["6000", "A1", "AFD", "0042", "Uden dato", None, "a"],
            ["6000", "A1", "AFD", "0042", "Med dato", datetime(2026, 1, 1), "b"],
        ]
        assert self._leaf_displays(rows) == ["Med dato"]

    def test_distinct_requisitioners_are_untouched(self):
        rows = [
            ["6000", "A1", "AFD", "0042", "Rekvirent 42", datetime(2026, 1, 1), "a"],
            ["6000", "A1", "AFD", "0043", "Rekvirent 43", datetime(2026, 1, 1), "b"],
        ]
        concepts, _, _ = build_rekvirent_hierarchy(
            [["6000", "Sygehus Lillebælt"]],
            rows,
            [["A1", "Gruppe A1"]],
            ["sgh", "navn"],
            self._COLUMNS,
            ["rgrp", "navn"],
        )
        leaf_codes = {c["CODE"] for c in concepts if c["CODE"].startswith("RNR_")}
        assert leaf_codes == {"RNR_6000_0042", "RNR_6000_0043"}


def test_contract_module_matches_this_test_s_expectations():
    """Hand-written list here, shared constant in the production code — if the two
    drift apart the test should fail rather than adapt."""
    assert _CONTRACT_COLUMNS == KONTAKTINFO_COLUMNS


def test_no_raw_ils_abbreviation_leaks_into_the_dim_column_names():
    """The naming standard forbids abbreviations like adr/postdi/ktosted in the
    model's column names — provenance is recorded in data_lineage instead."""
    for abbrev in ("altrnr", "adr", "adr2", "postdi", "kontakt", "ktosted"):
        assert abbrev not in _CONTRACT_COLUMNS, abbrev


@pytest.fixture
def staging_module():
    # The package __init__ re-exports the *asset* under the module's own name,
    # so go through sys.modules to reach the module (as test_dim_ilsvare_columns does).
    return sys.modules["DSA.SQL.staging.stg_dim_Rekvirent"]


@pytest.fixture
def dim_module():
    import DSA.SQL.dim.dim_Rekvirent  # noqa: F401

    return sys.modules["DSA.SQL.dim.dim_Rekvirent"]


class TestStagingContract:
    """stg.dim_Rekvirent must carry the columns and fill them from the archive."""

    def test_declares_every_kontaktinfo_column_with_a_type(self, staging_module):
        declared = staging_module.KONTAKTINFO_COLUMN_TYPES
        assert tuple(declared) == KONTAKTINFO_COLUMNS
        for col, sql_type in declared.items():
            assert sql_type.startswith("NVARCHAR("), f"{col}: {sql_type}"

    def test_staging_table_ddl_includes_every_kontaktinfo_column(self, staging_module):
        ddl = staging_module.build_kontaktinfo_column_ddl()
        for col in KONTAKTINFO_COLUMNS:
            assert f"[{col}]" in ddl, col

    def test_insert_names_its_columns_explicitly(self, staging_module):
        """Without an explicit column list, INSERT ... VALUES breaks as soon as the
        table has more columns than the tuple has values."""
        insert_sql = staging_module.build_staging_insert_sql()
        assert "INSERT INTO [stg].[dim_Rekvirent] (" in insert_sql
        assert "[clean_code_Rekvirent]" in insert_sql
        # The kontaktinfo columns are filled in by the UPDATE afterwards,
        # never by the bulk insert.
        for col in KONTAKTINFO_COLUMNS:
            assert f"[{col}]" not in insert_sql, col


class TestStagingEnrichment:
    """UPDATE'et der henter adresse-/kontaktoplysninger ind fra arkivet."""

    @pytest.fixture
    def sql(self, staging_module):
        return staging_module.build_kontaktinfo_update_sql()

    def test_reads_from_the_ils_archive_not_the_codesystem(self, sql):
        assert "[arc].[ils_rekv]" in sql
        assert "[arc].[ils_postdi]" in sql
        assert "RekvirentProperties" not in sql

    def test_sets_every_kontaktinfo_column(self, sql):
        for col in KONTAKTINFO_COLUMNS:
            assert f"stg.[{col}] =" in sql, col

    def test_maps_each_column_to_its_ils_source_column(self, sql):
        """The dimension uses spelled-out names; the translation to the ILS
        abbreviations happens here and is documented in data_lineage."""
        for dim_col, source_col in EXPECTED_SOURCE_COLUMNS.items():
            assert f"stg.[{dim_col}] = NULLIF(LTRIM(RTRIM(r.[{source_col}])), '')" in sql, dim_col

    def test_joins_on_trim_normalised_sgh_and_rnr(self, sql):
        """The same normalisation as the ref.RekvirentUndtagelse join in
        dim_Rekvirent — otherwise the ILS keys drift apart again."""
        from DSA.SQL.common.ils_clean_code_shared import sql_norm_code

        assert sql_norm_code("stg.[sgh]") in sql
        assert sql_norm_code("stg.[rnr]") in sql

    def test_dedupes_arc_ils_rekv_on_sgh_rnr(self, sql):
        """(sgh, rnr) is the logical key but the arc PK is prrowid — without
        deduplication, UPDATE ... FROM would pick an arbitrary row."""
        assert "ROW_NUMBER() OVER" in sql
        assert "rn = 1" in sql

    def test_dedup_rule_matches_the_codesystem_builder(self, sql):
        """One rule on both paths: newest pro2modified, lowest prrowid on ties.
        The Python side is exercised in TestOneSourceRowPerRequisitioner."""
        assert "ORDER BY [pro2modified] DESC, [prrowid] ASC" in sql

    def test_bynavn_resolves_alphabetically_first_city_per_land_postdi(self, sql):
        """dbo.postdi has duplicates on (land, postdi), e.g. (DK, 1172).
        Strategisk Indkoeb resolves it with MIN(navn); the same rule is applied here."""
        assert "MIN(" in sql
        assert "GROUP BY" in sql

    def test_blank_source_values_become_null(self, sql):
        """ILS stores empty strings; the warehouse must show NULL (as dim.ILSVare does)."""
        assert sql.count("NULLIF(") >= len(KONTAKTINFO_COLUMNS)


class TestDimResolveAndMerge:
    """dim.Rekvirent must carry the columns through resolve and MERGE."""

    def test_resolve_selects_every_kontaktinfo_column(self, dim_module):
        sql = dim_module.build_resolve_sql()
        for col in KONTAKTINFO_COLUMNS:
            assert f"stg.[{col}]" in sql, col

    def test_merge_updates_every_kontaktinfo_column(self, dim_module):
        sql = dim_module.MERGE_SQL
        for col in KONTAKTINFO_COLUMNS:
            assert f"[{col}] = source.[{col}]" in sql, col

    def test_merge_inserts_every_kontaktinfo_column(self, dim_module):
        sql = dim_module.MERGE_SQL
        insert_branch = sql[sql.index("WHEN NOT MATCHED BY TARGET") :]
        for col in KONTAKTINFO_COLUMNS:
            assert f"[{col}]" in insert_branch, col
            assert f"source.[{col}]" in insert_branch, col


def _declared_type(sql_text: str, column: str) -> str:
    """Extract the declared type of a column from DDL, normalised to NVARCHAR(n)."""
    match = re.search(rf"\[{column}\]\s*\[?(nvarchar)\]?\s*\((\d+)\)", sql_text, re.IGNORECASE)
    assert match, f"no NVARCHAR declaration found for [{column}]"
    return f"{match.group(1).upper()}({match.group(2)})"


class TestDimTableDdl:
    @staticmethod
    def _dim_rekvirent_block() -> str:
        ddl = _DIMS_DDL.read_text()
        block = ddl[ddl.index("CREATE TABLE [dim].[Rekvirent]") :]
        return block[: block.index("ON [PRIMARY]")]

    def test_dim_rekvirent_declares_the_columns(self):
        block = self._dim_rekvirent_block()
        for col in KONTAKTINFO_COLUMNS:
            assert f"[{col}]" in block, col

    def test_dim_ddl_width_matches_the_contract(self):
        """sql_type generates the staging DDL; the hand-written dim DDL must
        declare the same width, or the contract silently lies about types."""
        block = self._dim_rekvirent_block()
        for attr in REKVIRENT_ATTRIBUTES:
            assert _declared_type(block, attr.name) == attr.sql_type, attr.name

    def test_migration_width_matches_the_contract(self):
        """The migration adds the same columns on existing databases — same
        width contract as the base DDL."""
        sql = _MIGRATION_SQL.read_text()
        for attr in REKVIRENT_ATTRIBUTES:
            assert _declared_type(sql, attr.name) == attr.sql_type, attr.name


class TestLineage:
    """The source column is recorded in the lineage file — which is why the
    dimension's column names need not carry the ILS abbreviations."""

    def test_every_column_declares_its_ils_source(self):
        import yaml

        lineage = yaml.safe_load(_LINEAGE_YML.read_text())["dso_lineage"]["Columns"]
        for dim_col, source_col in EXPECTED_SOURCE_COLUMNS.items():
            assert dim_col in lineage, dim_col
            assert lineage[dim_col]["Source"] == [f"ils.dbo.rekv.{source_col}"], dim_col

    def test_bynavn_points_at_the_postdi_lookup(self):
        import yaml

        lineage = yaml.safe_load(_LINEAGE_YML.read_text())["dso_lineage"]["Columns"]
        assert lineage["bynavn"]["Source"] == ["ils.dbo.postdi.navn"]


class TestPostdiArchive:
    """dbo.postdi was not among the extracted ILS tables before this change."""

    def test_postdi_is_a_full_load_extract(self):
        from DSI.mssql.extract.ils_extract_configs import ILS_DFM_EXTRACT_CONFIGS

        cfg = next(c for c in ILS_DFM_EXTRACT_CONFIGS if c.target_table == "ils_postdi")
        assert cfg.source_schema == "dbo"
        assert cfg.source_table == "postdi"
        assert cfg.is_full_load is True
        assert cfg.checkpoint_from is None

    def test_postdi_archive_upserts_on_prrowid(self):
        from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_configs

        cfg = next(c for c in ils_dfm_archive_configs if c.target_table == "ils_postdi")
        assert cfg.source_schema == "ext"
        assert cfg.target_schema == "arc"
        assert cfg.merge_keys == ("prrowid",)
        assert cfg.upstream_is_full_load is True

    def test_postdi_archives_the_columns_the_city_lookup_needs(self):
        from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_configs

        cfg = next(c for c in ils_dfm_archive_configs if c.target_table == "ils_postdi")
        for col in ("land", "postdi", "navn"):
            assert col in cfg.non_key_columns, col

    def test_arc_ils_postdi_table_is_created(self):
        ddl = _ARCHIVE_DDL.read_text()
        assert "CREATE TABLE [arc].[ils_postdi]" in ddl
        block = ddl[ddl.index("CREATE TABLE [arc].[ils_postdi]") :]
        block = block[: block.index("GO")]
        assert "[prrowid]" in block
        assert "PK_arc_ils_postdi" in block
        for col in ("[land]", "[postdi]", "[navn]"):
            assert col in block, col
