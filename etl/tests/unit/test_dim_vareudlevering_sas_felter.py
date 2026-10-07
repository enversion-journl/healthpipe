# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for [internal ticket] dim.Vareudlevering SAS Forbrug parity fields.

This tests that the SAS parity fields are correctly added to the dim.Vareudlevering table.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_LINEAGE = (_REPO / "data_lineage" / "dim" / "dim_vareudlevering.yml").read_text(encoding="utf-8")
_DIM_DDL = (_REPO / "infrastructure" / "ddl" / "tables" / "erp" / "02_dims.sql").read_text(
    encoding="utf-8"
)
_MIGRATION = (
    _REPO
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260828_01_dim_vareudlevering_sas_felter.sql"
).read_text(encoding="utf-8")
_PRE_STG = (_REPO / "DSA" / "SQL" / "staging" / "pre_stg_Vareudlevering_ils.py").read_text(
    encoding="utf-8"
)
_STG_DIM = (_REPO / "DSA" / "SQL" / "staging" / "stg_dim_Vareudlevering_ils.py").read_text(
    encoding="utf-8"
)
_DIM = (_REPO / "DSA" / "SQL" / "dim" / "dim_Vareudlevering.py").read_text(encoding="utf-8")


EXPECTED_COLS = (
    "lgrlok",
    "ekspeditionsnummer",
    "projektnavn",
    "lager_varetype",
    "lager_varetype_navn",
)


def test_lineage_documents_sas_parity_columns_and_ils_varetype2():
    for col in EXPECTED_COLS:
        assert col in _LINEAGE, col
    assert "ils.dbo.lgrvare.varetype2" in _LINEAGE
    assert "ils.dbo.varetype2.navn" in _LINEAGE
    assert "ILS varetype2" in _LINEAGE
    assert "ils.dbo.udlev.lgrlok" in _LINEAGE
    assert "ils.dbo.udlev.udtraknr3" in _LINEAGE
    assert "ils.dbo.projekt.navn" in _LINEAGE


def test_dim_ddl_and_migration_add_five_columns():
    for col in EXPECTED_COLS:
        assert f"[{col}]" in _DIM_DDL, col
        assert f"[{col}]" in _MIGRATION, col


def test_migration_resets_dim_and_fct_checkpoints_to_null():
    """QA deploy must NULL both owners — pre_stg takes MIN; early date is still delta."""
    assert "[meta].[usp_dfm_reset]" in _MIGRATION
    assert "@new_value = NULL" in _MIGRATION
    assert "@schema_name = 'dim'" in _MIGRATION or "@schema_name = ''dim''" in _MIGRATION
    assert "@schema_name = 'fct'" in _MIGRATION or "@schema_name = ''fct''" in _MIGRATION
    assert "Vareudlevering" in _MIGRATION
    # Must not use an early-date sentinel (still a delta path for pre_stg).
    assert "1900-01-01" not in _MIGRATION


def test_pre_stg_passes_lgrlok_and_udtraknr3():
    assert "lgrlok" in _PRE_STG
    assert "udtraknr3" in _PRE_STG


def test_pre_stg_full_load_bypasses_checkpoint():
    """full_load must ignore dim/fct checkpoints (same pattern as Finanskladdelinje)."""
    assert 'config.mode == "full_load"' in _PRE_STG
    assert "min_checkpoint = None" in _PRE_STG


def test_stg_dim_joins_projekt_lgrvare_varetype2():
    assert "[arc].[ils_projekt]" in _STG_DIM
    assert "[arc].[ils_lgrvare]" in _STG_DIM
    assert "[arc].[ils_varetype2]" in _STG_DIM
    assert "projnr = p.projnr" in _STG_DIM
    assert "lg.sgh = p.sgh" in _STG_DIM
    assert "lg.lgr = p.lgr" in _STG_DIM
    assert "lg.vnr = p.vnr" in _STG_DIM
    assert "UPPER(LTRIM(RTRIM(vt2.varetype2)))" in _STG_DIM
    for col in EXPECTED_COLS:
        assert col in _STG_DIM, col


def test_dim_merge_includes_sas_parity_columns():
    for col in EXPECTED_COLS:
        assert f"src.{col}" in _DIM, col
        assert f"tgt.{col}" in _DIM, col
