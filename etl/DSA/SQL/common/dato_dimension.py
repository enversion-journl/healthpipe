# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared SQL builders for dim.Dato entity keys.

Keys are clamped to dim.Dato's seed range so out-of-range source dates map to a
sentinel row instead of an orphaned key. dim.Dato carries four sentinel rows:

  -1        UNKNOWN  — source is NULL or unparseable (non-blank)
  -2        BEFORE   — valid date earlier than dim.Dato's range
  99999998  BLANK    — source is present but empty/whitespace (tom dato)
  99999999  AFTER    — valid date later than dim.Dato's range

Bounds MUST mirror dim.Dato's seed range in
infrastructure/ddl/tables/shared_dimensions/04_seeds.sql, and the sentinel rows
MUST exist in dim.Dato (seeded in 02_dims.sql, migrated for existing DBs) so
every emitted key resolves to a real row.
"""

DATO_MIN_KEY = 19700101
DATO_MAX_KEY = 20501231

DATO_UNKNOWN_KEY = -1
DATO_BEFORE_KEY = -2
DATO_BLANK_KEY = 99999998
DATO_AFTER_KEY = 99999999


def dato_key(dt_sql: str) -> str:
    """Clamped YYYYMMDD key expression (no alias) with four sentinels.

    Resolves to:
      - 99999998 (BLANK) when the source is non-NULL but empty/whitespace
      - -1 (UNKNOWN) when the source is NULL or unparseable
      - -2 (BEFORE) when the date is earlier than dim.Dato's range
      - 99999999 (AFTER) when later than dim.Dato's range
      - the plain YYYYMMDD key when in range

    `dt_sql` is any datetime-ish SQL expression, e.g. "g.Bogfoeringsdato" or
    "MAX(src.udlobdto)". Caller appends `AS dw_ek_Dato_<rolle>`.
    """
    ts = f"TRY_CONVERT(DATETIME2, {dt_sql})"
    key = f"CAST(CONVERT(CHAR(8), {ts}, 112) AS INT)"
    # Present-but-empty string (typed NULL stays UNKNOWN). Must be tested
    # BEFORE the ts branches: TRY_CONVERT(DATETIME2, '') "succeeds" as
    # 1900-01-01, so a blank string never reaches a ts-IS-NULL branch and
    # would otherwise land on BEFORE via the range clamp.
    blank = (
        f"({dt_sql} IS NOT NULL AND "
        f"NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(50), {dt_sql}))), N'') IS NULL)"
    )
    return (
        f"CASE WHEN {blank} THEN {DATO_BLANK_KEY} "
        f"WHEN {ts} IS NULL THEN {DATO_UNKNOWN_KEY} "
        f"WHEN {key} < {DATO_MIN_KEY} THEN {DATO_BEFORE_KEY} "
        f"WHEN {key} > {DATO_MAX_KEY} THEN {DATO_AFTER_KEY} "
        f"ELSE {key} END"
    )


def dato_tid(dt_sql: str) -> str:
    """Raw companion timestamp expression (NULL when unparseable)."""
    return f"TRY_CONVERT(DATETIME2, {dt_sql})"
