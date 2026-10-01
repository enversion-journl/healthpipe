# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import re
from pathlib import Path

from etl.DSA.SQL.common.dato_dimension import (
    DATO_AFTER_KEY,
    DATO_BEFORE_KEY,
    DATO_BLANK_KEY,
    DATO_MAX_KEY,
    DATO_MIN_KEY,
    DATO_UNKNOWN_KEY,
    dato_key,
    dato_tid,
)

REPO = Path(__file__).resolve().parents[2]  # -> etl/
SEED_SQL = REPO / "infrastructure/ddl/tables/shared_dimensions/04_seeds.sql"
DIMS_SQL = REPO / "infrastructure/ddl/tables/shared_dimensions/02_dims.sql"
DATEKEY_FN_SQL = REPO / "infrastructure/ddl/common/functions/fn_datetime_to_datekey.sql"


def _seed_date_keys():
    """Parse dim.Dato's seed range from 04_seeds.sql as YYYYMMDD ints.

    The DateCTE anchors at CAST('1970-01-01' AS DATE) and is bounded by
    d < '2050-12-31'. Extract all 'YYYY-MM-DD' literals in that region and
    return (min_key, max_key).
    """
    sql = SEED_SQL.read_text(encoding="utf-8")
    # Restrict to the DateCTE region (anchor .. WHERE d < bound) so we don't
    # pick up unrelated date literals elsewhere in the seed file.
    cte = re.search(r"WITH DateCTE AS \((.*?)\bINSERT INTO", sql, re.DOTALL)
    region = cte.group(1) if cte else sql
    dates = re.findall(r"'(\d{4}-\d{2}-\d{2})'", region)
    assert dates, f"no date literals found in DateCTE region of {SEED_SQL}"
    keys = sorted(int(d.replace("-", "")) for d in dates)
    return keys[0], keys[-1]


def test_bounds_match_seed_range():
    min_key, max_key = _seed_date_keys()
    assert DATO_MIN_KEY == min_key, "helper bounds must mirror dim.Dato seed range in 04_seeds.sql"
    assert DATO_MAX_KEY == max_key, "helper bounds must mirror dim.Dato seed range in 04_seeds.sql"


def test_sentinel_constants():
    assert DATO_UNKNOWN_KEY == -1
    assert DATO_BEFORE_KEY == -2
    assert DATO_BLANK_KEY == 99999998
    assert DATO_AFTER_KEY == 99999999


def test_dato_key_four_way_sentinels():
    sql = dato_key("g.Bogfoeringsdato")
    # empty/whitespace string -> BLANK
    assert "IS NULL AND (g.Bogfoeringsdato IS NOT NULL AND" in sql
    assert f"THEN {DATO_BLANK_KEY}" in sql
    # NULL/unparseable (non-blank) -> UNKNOWN
    assert f"IS NULL THEN {DATO_UNKNOWN_KEY}" in sql
    # valid but before dim.Dato range -> BEFORE sentinel
    assert f"< {DATO_MIN_KEY} THEN {DATO_BEFORE_KEY}" in sql
    # valid but after dim.Dato range -> AFTER sentinel
    assert f"> {DATO_MAX_KEY} THEN {DATO_AFTER_KEY}" in sql
    # source expression is threaded in
    assert "TRY_CONVERT(DATETIME2, g.Bogfoeringsdato)" in sql
    # no trailing column alias
    assert sql.rstrip().endswith("END")


def test_dato_key_composes_with_aggregate():
    sql = dato_key("MAX(src.udlobdto)")
    assert "MAX(src.udlobdto)" in sql


def test_dato_tid_is_try_convert():
    assert dato_tid("g.Dato") == "TRY_CONVERT(DATETIME2, g.Dato)"


def test_datekey_fn_mirrors_canonical_bounds_and_sentinels():
    """dbo.fn_datetime_to_datekey er en slutbruger-funktion (ikke brugt af
    pipelinen), men dens clamp-range og sentinels SKAL spejle repoets
    kanoniske dato_key()-konstanter — ellers emitter funktionen keys der ikke
    resolver mod dim.Dato. Guard mod drift."""
    sql = DATEKEY_FN_SQL.read_text(encoding="utf-8")
    assert f"IS NULL THEN {DATO_UNKNOWN_KEY}" in sql
    assert f"< {DATO_MIN_KEY} THEN {DATO_BEFORE_KEY}" in sql
    assert f"> {DATO_MAX_KEY} THEN {DATO_AFTER_KEY}" in sql
    # SCHEMABINDING goer den inline-egnet (Froid) — det var perf-grunden til at
    # den blev genskabt saadan; pin det saa det ikke forsvinder ved en rewrite.
    assert "WITH SCHEMABINDING" in sql


def test_dim_dato_seeds_all_four_sentinels():
    """02_dims.sql must INSERT the -1, -2, 99999998 and 99999999 sentinel rows so that
    out-of-range fact keys always resolve to an existing dim.Dato row."""
    sql = DIMS_SQL.read_text(encoding="utf-8")
    # Restrict to the INSERT INTO [dim].[Dato] ... ; statement (multi-row VALUES).
    m = re.search(r"INSERT INTO \[dim\]\.\[Dato\].*?;", sql, re.DOTALL)
    assert m, "INSERT INTO [dim].[Dato] seed statement not found in 02_dims.sql"
    insert = m.group(0)
    for key in (DATO_UNKNOWN_KEY, DATO_BEFORE_KEY, DATO_BLANK_KEY, DATO_AFTER_KEY):
        # Each sentinel is a VALUES tuple opener: "( <key>,"
        assert re.search(rf"\(\s*{key}\s*,", insert), (
            f"dim.Dato seed in 02_dims.sql must INSERT a sentinel row with " f"dw_ek_Dato = {key}"
        )
