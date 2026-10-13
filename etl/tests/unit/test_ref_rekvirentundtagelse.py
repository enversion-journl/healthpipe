# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for ref.RekvirentUndtagelse CleanCode + rebuild SQL contracts."""

from pathlib import Path

import pytest
from DSA.SQL.ref.rekvirent_undtagelse import ref_RekvirentUndtagelse
from DSA.SQL.ref.rekvirent_undtagelse_sql import CLEAN_CODE_EXPR, REBUILD_SQL
from DSO.load.sync_configs import SHARED_DIMS_REF_CONFIGS

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "infrastructure" / "ddl" / "migrations" / "20260811_ref_rekvirentundtagelse.sql"
BASE_DDL = ROOT / "infrastructure" / "ddl" / "tables" / "ref" / "01_tables.sql"


def _raw_clean_code(sgh: str, rnr: str) -> str:
    """Python mirror of the raw SQL CleanCode used by the rebuild.

    Trim-only, matching sql_norm_code: the key has to be spelled the way
    map.Rekvirent minted it, and the minting side only strips.
    """
    return f"SGH_{sgh.strip()}_RNR_{rnr.strip()}"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("sgh", "rnr", "expected"),
    [
        ("4200", "0840", "SGH_4200_RNR_0840"),
        ("4200", "840", "SGH_4200_RNR_840"),
        ("4200", "97000", "SGH_4200_RNR_97000"),
        ("9999", "OUHLP2335", "SGH_9999_RNR_OUHLP2335"),
        ("9999", "S001", "SGH_9999_RNR_S001"),
        ("55", "01", "SGH_55_RNR_01"),
        ("4200", "00", "SGH_4200_RNR_00"),
        # Trim-only: an internal space is part of the key, because it is part
        # of the map member (SGH_6000_RNR_SLB TELE, dw_ek 26242).
        ("6000", "SLB TELE", "SGH_6000_RNR_SLB TELE"),
        ("6000", "  840  ", "SGH_6000_RNR_840"),
    ],
)
def test_raw_clean_code_keeps_rnr_verbatim(sgh, rnr, expected):
    """Neither padded nor truncated: 840 stays 840, and 00 stays 00.

    Padding collapsed 00 and a blank rnr onto the same key; truncation would
    corrupt OUHLP2335 -> 2335.
    """
    assert _raw_clean_code(sgh, rnr) == expected


@pytest.mark.unit
def test_rebuild_sql_uses_raw_rnr():
    assert "'SGH_' + sgh_n + '_RNR_' + rnr_n" in CLEAN_CODE_EXPR
    assert "REPLICATE" not in CLEAN_CODE_EXPR
    assert "RIGHT(" not in REBUILD_SQL
    # Trim-only, like every other derivation of this key: de-spacing or
    # case-folding would build a key map.Rekvirent cannot hold.
    assert "REPLACE(" not in REBUILD_SQL
    assert "UPPER(" not in REBUILD_SQL
    assert "COALESCE(r.navn, u.navn)" in REBUILD_SQL
    assert "[arc].[ils_rekvundtagne]" in REBUILD_SQL
    assert "[map].[Rekvirent]" in REBUILD_SQL


@pytest.mark.unit
def test_rebuild_sql_deletes_then_inserts_full_snapshot():
    assert REBUILD_SQL.strip().startswith("DELETE FROM [ref].[RekvirentUndtagelse]")
    assert "INSERT INTO [ref].[RekvirentUndtagelse]" in REBUILD_SQL


@pytest.mark.unit
def test_asset_wiring():
    spec = ref_RekvirentUndtagelse.get_asset_spec()
    assert spec.group_name == "erp_ref"
    dep_keys = {k.to_user_string() for k in ref_RekvirentUndtagelse.dependency_keys}
    assert "arc_ils_rekvundtagne" in dep_keys
    assert "arc_ils_rekv" in dep_keys
    assert "map_Rekvirent" in dep_keys


@pytest.mark.unit
def test_dso_sync_config_registered():
    cfg = SHARED_DIMS_REF_CONFIGS["ref_RekvirentUndtagelse"]
    assert cfg.schema == "ref"
    assert cfg.table == "RekvirentUndtagelse"
    assert cfg.primary_key == "clean_code_Rekvirent"
    assert cfg.mode == "truncate_insert"


@pytest.mark.unit
def test_ddl_and_migration_define_same_table():
    for path in (BASE_DDL, MIGRATION):
        text = path.read_text(encoding="utf-8")
        assert "[ref].[RekvirentUndtagelse]" in text
        assert "[clean_code_Rekvirent]" in text
        assert "[navn_manuelt]" in text
        assert "UQ_Ref_RekvirentUndtagelse_SghRnr" in text
