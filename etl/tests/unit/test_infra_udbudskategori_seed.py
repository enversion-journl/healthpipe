# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for UNSPSC udbudskategori infra archive + seed (#28710)."""

import sys
from pathlib import Path

import infrastructure.assets.udbudskategori_infrastructure  # noqa: F401
import pytest

pytestmark = pytest.mark.unit

_module = sys.modules["infrastructure.assets.udbudskategori_infrastructure"]
REPO = Path(__file__).resolve().parents[2]  # etl/


class TestArchiveDdl:
    def test_ddl_file_creates_arc_table(self):
        ddl = (
            REPO / "infrastructure" / "ddl" / "tables" / "udbudskategori" / "00_archive.sql"
        ).read_text(encoding="utf-8")
        assert "[arc].[unspsc_udbudskategori]" in ddl
        assert "[UNSPSC_Nr]             NVARCHAR(50)  NOT NULL" in ddl

    def test_ddl_asset_key_and_group(self):
        asset = _module.staging_infra_udbudskategori
        assert asset.key.path == ["staging_infra_udbudskategori"]
        assert "staging_infrastructure" in asset.group_names_by_key.values()

    def test_ddl_asset_registered(self):
        from infrastructure.assets import staging_infra_udbudskategori  # noqa: F401

    def test_ddl_has_update_timestamp(self):
        ddl = (
            REPO / "infrastructure" / "ddl" / "tables" / "udbudskategori" / "00_archive.sql"
        ).read_text(encoding="utf-8")
        assert "[dw_timestamp_update]" in ddl


class TestSeedAsset:
    def test_constants(self):
        assert _module.UDBUD_ARC_TABLE == "arc.unspsc_udbudskategori"
        assert _module.UDBUD_CSV_DELIMITER == ";"
        assert _module.UDBUD_COLUMNS == [
            "UNSPSC_Nr",
            "UNSPSC_Tekst",
            "Udbudstype",
            "Udbudskategori_niv1",
            "Udbudskategori_niv2",
            "Udbudskategori_niv3",
            "Udbudskategori_niv4",
        ]

    def test_seed_asset_key_and_group(self):
        asset = _module.staging_infra_udbudskategori_seed
        assert asset.key.path == ["staging_infra_udbudskategori_seed"]
        assert "staging_infrastructure" in asset.group_names_by_key.values()

    def test_seed_registered_in_all_infrastructure_assets(self):
        from infrastructure import all_infrastructure_assets

        keys = {a.key.path[-1] for a in all_infrastructure_assets}
        assert "staging_infra_udbudskategori" in keys
        assert "staging_infra_udbudskategori_seed" in keys

    def test_csv_present_with_expected_header(self):
        header = _module.UDBUD_CSV_PATH.read_text(encoding="utf-8").splitlines()[0]
        assert header.split(";")[:5] == [
            "UNSPSC_Nr",
            "UNSPSC_Tekst",
            "Udbudstype",
            "Udbudskategori_niv1",
            "Udbudskategori_niv2",
        ]


class TestSeedGuard:
    """The seed body must short-circuit when the table already has rows."""

    def test_skips_when_table_not_empty(self):
        from contextlib import contextmanager
        from unittest.mock import MagicMock

        fn = _module.staging_infra_udbudskategori_seed.op.compute_fn.decorated_fn
        cur = MagicMock()
        cur.fetchone.return_value = (42,)
        conn = MagicMock()
        conn.cursor.return_value = cur
        res = MagicMock()

        @contextmanager
        def _g():
            yield conn

        res.get_connection = _g
        ctx = MagicMock()

        result = fn(ctx, res)
        assert result.metadata["skipped"] is True
        cur.executemany.assert_not_called()
