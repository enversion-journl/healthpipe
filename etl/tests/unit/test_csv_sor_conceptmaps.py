# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the CSV-only *-to-SOR ConceptMap builders.

The four ERP conceptmaps (ean/lager/rekvirent/okosted -> SOR) are built
exclusively from the curated CSV files in DSI/csv_files/data/ — no fuzzy
matching, no SOR-property lookups, no hierarchical fallback.
"""

import dagster as dg
import pytest
from DSA.ConceptMap.csv_to_sor import csv_sor_conceptmap_assets
from DSA.ConceptMap.utils import conceptmap_rows_from_csv

pytestmark = pytest.mark.unit


class TestConceptmapRowsFromCsv:
    def test_builds_row_per_csv_line(self):
        csv_rows = [
            {
                "source_code": "SGH_6000",
                "source_display": "Sygehus Lillebælt",
                "target_code": "8061000016004",
                "target_display": "Sygehus Lillebælt",
                "relationship": "equal",
                "note": "Hospital mapping",
            }
        ]
        assert conceptmap_rows_from_csv(csv_rows) == [
            ("SGH_6000", "Sygehus Lillebælt", "equal", "8061000016004", "Sygehus Lillebælt")
        ]

    def test_strips_whitespace_and_blank_display_becomes_none(self):
        csv_rows = [
            {
                "source_code": " RSD ",
                "source_display": "  ",
                "target_code": " 6101000016001 ",
                "target_display": "",
                "relationship": " equal ",
            }
        ]
        assert conceptmap_rows_from_csv(csv_rows) == [("RSD", None, "equal", "6101000016001", None)]

    def test_skips_rows_missing_required_fields(self):
        csv_rows = [
            {"source_code": "", "target_code": "1", "relationship": "equal"},
            {"source_code": "A", "target_code": "", "relationship": "equal"},
            {"source_code": "B", "target_code": "2", "relationship": ""},
            {"source_code": "C", "target_code": "3", "relationship": "equivalent"},
        ]
        rows = conceptmap_rows_from_csv(csv_rows)
        assert [r[0] for r in rows] == ["C"]

    def test_deduplicates_source_target_pairs_keeping_first(self):
        # conceptmap tables have PK (source_code, target_code); a duplicate CSV
        # line must not break the temp-table insert.
        csv_rows = [
            {"source_code": "A", "target_code": "1", "relationship": "equal"},
            {"source_code": "A", "target_code": "1", "relationship": "narrower"},
            {"source_code": "A", "target_code": "2", "relationship": "equivalent"},
        ]
        rows = conceptmap_rows_from_csv(csv_rows)
        assert [(r[0], r[3], r[2]) for r in rows] == [
            ("A", "1", "equal"),
            ("A", "2", "equivalent"),
        ]


class TestCsvSorConceptmapAssets:
    _EXPECTED = {
        "cm_ean_to_sor": "ext_csv_ean_to_sor",
        "cm_lager_to_sor": "ext_csv_lager_to_sor",
        "cm_rekvirent_to_sor": "ext_csv_rekvirent_to_sor",
        "cm_okosted_to_sor": "ext_csv_okosted_to_sor",
    }

    def test_all_four_assets_exist(self):
        names = {a.key.to_user_string() for a in csv_sor_conceptmap_assets}
        assert names == set(self._EXPECTED)

    def test_only_upstream_is_the_csv_extract(self):
        # CSV-only: no deps on cs_* codesystem assets may remain.
        for asset_def in csv_sor_conceptmap_assets:
            name = asset_def.key.to_user_string()
            deps = asset_def.asset_deps[asset_def.key]
            assert deps == {dg.AssetKey(self._EXPECTED[name])}, name

    def test_registered_in_conceptmap_aggregate(self):
        from DSA.ConceptMap import conceptmap_dsa_assets

        names = {a.key.to_user_string() for a in conceptmap_dsa_assets}
        assert set(self._EXPECTED) <= names
        # SHAK-varianterne er autoritative crosswalks og skal bestå uændret.
        assert {"cm_shak_to_sor", "cm_shakalike_to_sor"} <= names

    def test_empty_csv_fails_instead_of_wiping_table(self):
        # delete_stale=True means an empty mapping set would DELETE every row in
        # the conceptmap table. A missing/empty/misread CSV must fail the run
        # before any SQL touches the database.
        from unittest.mock import MagicMock

        from DSA.ConceptMap.csv_to_sor import cm_ean_to_sor

        resource = MagicMock()
        with pytest.raises(dg.Failure, match="ean_to_sor"):
            cm_ean_to_sor(
                dg.build_asset_context(),
                csv_rows=[],
                mssql_target_staging=resource,
            )
        resource.get_connection.assert_not_called()
