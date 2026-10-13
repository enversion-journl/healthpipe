# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the ILS sgh/lgr/rnr CleanCode contracts.

Both maps hold members at two levels — ``SGH_{sgh}`` for the hospital and
``SGH_{sgh}_LGR_{lgr}`` / ``SGH_{sgh}_RNR_{rnr}`` below it — and both mint the
child segment raw.  So the fact-side key is raw — trim-only, never de-spaced
or case-folded — falls back to the hospital-level member when the child code is
not stated, and is NULL when there is no hospital to resolve against.
"""

from pathlib import Path

import pytest
from DSA.SQL.common.ils_clean_code_shared import (
    sql_clean_code_lager,
    sql_clean_code_rekvirent,
    sql_norm_code,
)
from DSA.SQL.staging.pre_stg_AfdelingLagerbeholdning_ils import (
    LAGER_CLEAN_CODE_SQL as AFD_LAGERBEH_LAGER_SQL,
)
from DSA.SQL.staging.pre_stg_AfdelingLagerbeholdning_ils import (
    REKVIRENT_CLEAN_CODE_SQL as AFD_LAGERBEH_REKVIRENT_SQL,
)
from DSA.SQL.staging.pre_stg_Lagerbeholdning_ils import (
    LAGER_CLEAN_CODE_SQL as LAGERBEH_LAGER_SQL,
)
from DSA.SQL.staging.pre_stg_Vareudlevering_ils import (
    LAGER_CLEAN_CODE_SQL,
    REKVIRENT_CLEAN_CODE_SQL,
)

ROOT = Path(__file__).resolve().parents[2]
CHECKPOINT_MIGRATION = (
    ROOT
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260903_01_reset_vareudlevering_checkpoints.sql"
)
LAGERBEH_MIGRATION = (
    ROOT
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260904_01_reset_lagerbeholdning_checkpoints.sql"
)
LINEAGE = ROOT / "data_lineage" / "fct" / "fct_vareudlevering.yml"


def _clean_code(sgh: str | None, child: str | None, prefix: str) -> str | None:
    """Python mirror of the shared SQL expression, for the semantic cases.

    Normalisation is trim-only — the same thing the codesystems do when they
    mint the segment — blank collapses to NULL, an unstated child falls back to
    the hospital-level member, and no hospital means no key at all.
    """
    sgh_n = sgh.strip() if sgh else None
    child_n = child.strip() if child else None
    if not sgh_n:
        return None
    if not child_n:
        return f"SGH_{sgh_n}"
    return f"SGH_{sgh_n}_{prefix}_{child_n}"


# ---------------------------------------------------------------------------
# Shared SQL expression builders
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_norm_code_trims_only_and_nulls_blanks():
    expr = sql_norm_code("src.lgr")
    assert "LTRIM(RTRIM(CAST(src.lgr AS NVARCHAR(50))))" in expr
    # A blank code is "not stated", not a key segment.
    assert expr.startswith("NULLIF(")
    assert expr.endswith(", '')")


@pytest.mark.unit
def test_norm_code_never_reshapes_the_value():
    """The map mints the segment strip-only; anything stricter fabricates a key.

    REPLACE(v,' ','') made the genuine map.Rekvirent member
    SGH_6000_RNR_SLB TELE unreachable, and UPPER is redundant under the
    Danish_Norwegian_CI_AS collation.
    """
    for expr in (sql_norm_code("sgh"), sql_clean_code_lager(), sql_clean_code_rekvirent()):
        assert "REPLACE(" not in expr
        assert "UPPER(" not in expr


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expr", "prefix"),
    [(sql_clean_code_lager(), "LGR"), (sql_clean_code_rekvirent(), "RNR")],
)
def test_keys_are_raw_never_padded_or_truncated(expr, prefix):
    """Padding fabricates keys the maps never hold; truncation corrupts long codes."""
    assert f"'_{prefix}_' + " in expr
    assert "REPLICATE" not in expr
    assert "RIGHT(" not in expr
    assert "LEN(" not in expr


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expr", "prefix"),
    [(sql_clean_code_lager(), "LGR"), (sql_clean_code_rekvirent(), "RNR")],
)
def test_keys_fall_back_to_the_hospital_level_member(expr, prefix):
    """map.Rekvirent/map.Lager hold SGH_{sgh}; an unstated child resolves there."""
    sgh_only = expr.split(f"'_{prefix}_'")[1]
    assert "THEN 'SGH_' + " in sgh_only
    # ...and without a hospital there is nothing to resolve at all.
    assert "ELSE NULL" in expr
    assert f"'{prefix}_' + " not in expr.replace(f"'_{prefix}_' + ", "")


@pytest.mark.unit
def test_expression_builders_accept_qualified_columns():
    expr = sql_clean_code_lager(sgh="src.sgh", lgr="src.lgr")
    assert "CAST(src.sgh AS NVARCHAR(50))" in expr
    assert "CAST(src.lgr AS NVARCHAR(50))" in expr


# ---------------------------------------------------------------------------
# pre_stg_Vareudlevering_ils constants
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_vareudlevering_constants_come_from_the_shared_builders():
    assert LAGER_CLEAN_CODE_SQL == sql_clean_code_lager()
    assert REKVIRENT_CLEAN_CODE_SQL == sql_clean_code_rekvirent()


@pytest.mark.unit
def test_lagerbeholdning_assets_share_the_same_expression():
    """Hand-copied CASE expressions are what let the keys drift apart."""
    assert LAGERBEH_LAGER_SQL == sql_clean_code_lager("src.sgh", "src.lgr")
    assert AFD_LAGERBEH_LAGER_SQL == sql_clean_code_lager("src.sgh", "src.lgr")
    assert AFD_LAGERBEH_REKVIRENT_SQL == sql_clean_code_rekvirent("src.sgh", "src.rnr")


@pytest.mark.unit
def test_afdelinglagerbeholdning_rekvirent_key_targets_the_map():
    """It built RNR_{sgh}_{rnr} — the concept code, a form the map never holds."""
    assert "'SGH_' + " in AFD_LAGERBEH_REKVIRENT_SQL
    assert "'_RNR_' + " in AFD_LAGERBEH_REKVIRENT_SQL
    assert "'RNR_' + " not in AFD_LAGERBEH_REKVIRENT_SQL
    assert "'RNR__' + " not in AFD_LAGERBEH_REKVIRENT_SQL


# ---------------------------------------------------------------------------
# Key semantics, via a Python mirror of the shared expression
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("sgh", "lgr", "expected"),
    [
        ("6000", "100", "SGH_6000_LGR_100"),
        ("4200", "0100", "SGH_4200_LGR_0100"),
        ("4200", "10000", "SGH_4200_LGR_10000"),
        # Trim-only, as map.Lager was minted: no de-spacing, no case-folding.
        ("4200", "  100  ", "SGH_4200_LGR_100"),
        ("6000", "1ce", "SGH_6000_LGR_1ce"),
        ("4200", None, "SGH_4200"),
        ("4200", "", "SGH_4200"),
        ("4200", "   ", "SGH_4200"),
        (None, "100", None),
        ("", "100", None),
        (None, None, None),
    ],
)
def test_lager_clean_code_semantics(sgh, lgr, expected):
    assert _clean_code(sgh, lgr, "LGR") == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("sgh", "rnr", "expected"),
    [
        # Raw: neither padded nor truncated.
        ("4200", "840", "SGH_4200_RNR_840"),
        ("4200", "0840", "SGH_4200_RNR_0840"),
        ("4200", "244376", "SGH_4200_RNR_244376"),
        ("9999", "OUHLP2335", "SGH_9999_RNR_OUHLP2335"),
        ("6000", "S13", "SGH_6000_RNR_S13"),
        # Internal spaces are kept: map.Rekvirent holds SGH_6000_RNR_SLB TELE
        # (dw_ek 26242). De-spacing looked up SGH_6000_RNR_SLBTELE, which the
        # map cannot hold, so these udlev rows resolved to -1.
        ("6000", "SLB TELE", "SGH_6000_RNR_SLB TELE"),
        # Case is kept too; the databases collate Danish_Norwegian_CI_AS, so
        # the join ignores case without the expression folding it.
        ("6000", "0238s", "SGH_6000_RNR_0238s"),
        # 00 is a real requisitioner at 4200; padding collapsed it onto blank.
        ("4200", "00", "SGH_4200_RNR_00"),
        # rnr not stated -> the hospital-level member (dw_ek 710 for 4200).
        ("4200", None, "SGH_4200"),
        ("4200", "", "SGH_4200"),
        ("4200", "   ", "SGH_4200"),
        # No hospital -> nothing to resolve.
        (None, "840", None),
        ("", "840", None),
        (None, None, None),
    ],
)
def test_rekvirent_clean_code_semantics(sgh, rnr, expected):
    assert _clean_code(sgh, rnr, "RNR") == expected


@pytest.mark.unit
def test_blank_and_zero_rnr_no_longer_share_a_key():
    """The pad-only bug: '' -> 0000 and '00' -> 0000 resolved to one member."""
    blank = _clean_code("4200", "", "RNR")
    zeros = _clean_code("4200", "00", "RNR")
    assert blank != zeros
    assert blank == "SGH_4200"
    assert zeros == "SGH_4200_RNR_00"


# ---------------------------------------------------------------------------
# Backfill migration + lineage documentation
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_checkpoint_reset_migration_clears_both_owners():
    sql = CHECKPOINT_MIGRATION.read_text(encoding="utf-8")
    assert "DELETE FROM [meta].[DFMState]" in sql
    assert "[table_name] = 'Vareudlevering'" in sql
    assert "[schema_name] = 'dim'" in sql
    assert "[schema_name] = 'fct'" in sql
    assert "[target_type] = 'dimension'" in sql
    assert "[target_type] = 'fact'" in sql
    # The same migrations folder runs against DSO, where DFMState does not exist.
    assert "IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL" in sql


@pytest.mark.unit
def test_lagerbeholdning_reset_migration_clears_all_four_owners():
    sql = LAGERBEH_MIGRATION.read_text(encoding="utf-8")
    assert "DELETE FROM [meta].[DFMState]" in sql
    for table in (
        "LagerbeholdningEgenskaber",
        "AfdelingLagerbeholdningEgenskaber",
        "Lagerbeholdning",
        "AfdelingLagerbeholdning",
    ):
        assert f"'{table}'" in sql
    assert "IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL" in sql


@pytest.mark.unit
def test_lineage_documents_the_key_rules():
    text = LINEAGE.read_text(encoding="utf-8")
    block = text.split("dw_ek_Lager:")[1].split("dw_ek_Leverandoervare:")[0].lower()
    # Each FK names the map it resolves against, and the hospital-level fallback.
    assert "map.lager paa noeglen sgh_{sgh}_lgr_{lgr}" in block
    assert "map.rekvirent paa noeglen sgh_{sgh}_rnr_{rnr}" in block
    assert block.count("sygehus-medlemmet sgh_{sgh}") == 2
    assert block.count("uden sgh er") == 2
