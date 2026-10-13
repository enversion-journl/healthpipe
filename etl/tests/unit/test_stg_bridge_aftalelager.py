# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for stg_bridge_AftaleLager.

Critical properties: (1) the sgh-split fragment matches the reference-view
rule verbatim (spec Section B) — 2 tegn sygehus ved praefiks '55', ellers 4;
(2) dedup is on newest sysdato/prrowid DESC per (lnr, aftale, sgh), and the
PARTITION BY uses the TRIMMED keys (not raw) so dedup boundaries match the
stored, trimmed key boundaries — two rows differing only in whitespace must
not both win rn=1 and collide on the applier's unique temp index; (3)
clean_code_Lager uses the CANONICAL map.Lager key format — the
'SGH_{sgh}_LGR_{lgr}' composite that DSA/CodeSystem/lager/lager.py's
build_lager_hierarchy() builds and stg_dim_Lager.py passes through
unchanged as clean_code_Lager — NOT pre_stg_Vareudlevering_ils's bare
'LGR_' + RIGHT('0000' + ..., 4) format (that branch is dead code / a
different, incompatible key space). lager.py's own normalization
(_safe_strip) is str(value).strip() only — no UPPER, no zero-padding — so
the composite here must not invent either.
"""

import pytest

pytestmark = pytest.mark.unit

from DSA.SQL.staging.stg_bridge_AftaleLager import _build_staging_sql


def test_stg_bridge_aftalelager_splits_sgh_like_the_reference_view():
    sql = _build_staging_sql()
    assert "LEFT(LTRIM(RTRIM(" in sql and "'55'" in sql, "sygehus-split: 2 tegn ved '55', ellers 4"
    assert "LEN(LTRIM(RTRIM(" in sql, "lager kun naar LEN > sygehus-delen"


def test_stg_bridge_aftalelager_dedups_on_newest_sysdato():
    sql = _build_staging_sql()
    assert "PARTITION BY" in sql and "sysdato DESC" in sql and "prrowid DESC" in sql


def test_stg_bridge_aftalelager_dedup_partitions_on_normalized_keys():
    """Merely-trimmed PARTITION BY lnr, aftale, sgh would let two rows that
    differ only in case or internal whitespace both win rn=1, then collide
    once the applier stores the fully NORMALIZED clean_code_Aftale (UPPER +
    REPLACE-space) + sgh_raw under CI collation. The partition expressions
    must mirror clean_code_Aftale's actual normalization byte-for-byte: lnr
    gets UPPER+trim only (no REPLACE — clean_code_Aftale's lnr-led does not
    collapse internal spaces either), aftale gets UPPER+trim+REPLACE-space,
    sgh gets UPPER+trim."""
    sql = _build_staging_sql()
    assert (
        "PARTITION BY LTRIM(RTRIM(lnr)), LTRIM(RTRIM(aftale)), LTRIM(RTRIM(sgh))" not in sql
    ), "merely-trimmed (not normalized) partition must be gone"
    assert "PARTITION BY lnr, aftale, sgh\n" not in sql, "raw (untrimmed) partition is gone"
    assert (
        "UPPER(LTRIM(RTRIM(CAST(lnr AS NVARCHAR(100)))))," in sql
    ), "lnr partition key must match clean_code_Aftale's lnr normalization (UPPER+trim, no REPLACE)"
    assert (
        "UPPER(LTRIM(RTRIM(REPLACE(CAST(aftale AS NVARCHAR(100)), ' ', ''))))," in sql
    ), "aftale partition key must match clean_code_Aftale's aftale normalization (UPPER+trim+REPLACE-space)"
    assert (
        "UPPER(LTRIM(RTRIM(sgh)))" in sql
    ), "sgh partition key must be UPPER+trim to match sgh_raw under CI"


def test_stg_bridge_aftalelager_clean_code_aftale_matches_fact_staging_expr():
    sql = _build_staging_sql()
    assert (
        "'AFTALE_' + UPPER(LTRIM(RTRIM(CAST(lnr AS NVARCHAR(100))))) + '_'" in sql
    ), "clean_code_Aftale skal matche stg_fct_AftaleVarePris_ils's udtryk"


def test_stg_bridge_aftalelager_clean_code_lager_uses_canonical_codesystem_composite():
    sql = _build_staging_sql()
    assert "'SGH_' + " in sql
    assert "'_LGR_' + " in sql
    assert (
        "'SGH_' + sygehus + '_LGR_' + lager" in sql
    ), "must build the SGH_{sgh}_LGR_{lgr} composite"
    # The old bare-LGR_ + zero-padded format (pre_stg_Vareudlevering_ils's
    # dead-code branch) must be gone — it resolves to the wrong key space.
    assert "'LGR_' + RIGHT('0000'" not in sql
    assert "RIGHT('0000'" not in sql, "codesystem's _safe_strip does not zero-pad"
    assert "UPPER(LTRIM(RTRIM(lager)))" not in sql, "codesystem's _safe_strip does not UPPER"


def test_stg_bridge_aftalelager_sygehus_fallback_for_hospital_level_rows():
    """Ruling 5 (2026-09-01): sygehus-only raekker (lager IS NULL) resolver mod
    codesystemets SGH_{sygehus}-koncept i stedet for NULL/-1 — dw_ek_Lager er
    blandet granularitet, og raekkens lager IS NULL markerer niveauet."""
    from DSA.SQL.staging.stg_bridge_AftaleLager import _build_staging_sql

    sql = _build_staging_sql()
    assert "ELSE 'SGH_' + sygehus" in sql, "sygehus-fallback mangler i clean_code_Lager"
    assert (
        "ELSE NULL"
        not in sql.split("clean_code_Lager")[0] + sql.split("clean_code_Lager")[1].split("INTO")[0]
    ), "clean_code_Lager maa ikke laengere kunne blive NULL"


def test_stg_bridge_aftalelager_reads_from_arc_lafthsgh():
    sql = _build_staging_sql()
    assert "FROM [arc].[ils_lafthsgh]" in sql
    assert "INTO [stg].[bridge_AftaleLager]" in sql


def test_stg_bridge_aftalelager_filters_blank_keys():
    sql = _build_staging_sql()
    assert "lnr IS NOT NULL" in sql
    assert "aftale IS NOT NULL" in sql
    assert "sgh IS NOT NULL" in sql
