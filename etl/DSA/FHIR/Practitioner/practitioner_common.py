# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared helpers for Practitioner and PractitionerRole from SilkeborgData E26PERS.

Owns the three things both assets need: the CPR validity gate, the archive
snapshot queries, and the CPR-correction pairing rule.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, cast

from core.dfm_config import RunModeConfig
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from utils.sql_safety import safe_table_ref

ARCHIVE_TABLE = "arc.silkeborgdata_e26_pers"
SOURCE_TABLE_CODE = "silkeborgdata_e26_pers"

# The source uses far dates as missing markers. The list is not exhaustive —
# 2050-06-01 and a tail of 2081-2099 values in Jubilaeumsdato are not in the
# archive DDL's list either — so date handling also rejects anything after the
# row's own delivery period rather than relying on this set alone.
_MISSING_DATE_MARKERS = frozenset(
    {
        date(1, 1, 1),
        date(1899, 12, 31),
        date(1900, 1, 1),
        date(2050, 6, 1),
        date(2099, 12, 31),
        date(9999, 12, 31),
    }
)

# A payroll record can carry a birth date implying an impossible age. Only a
# lower bound is defensible: the archive holds real staff born as early as 1918,
# so an upper bound would discard correct data about pension-age employees.
_MIN_PLAUSIBLE_AGE = 13

# Discokode 999999 is the source's "uoplyst" marker, not an occupation.
_DISCO_UNKNOWN = "999999"

# Modes that re-post a named subset. They must stay narrow, so they keep the
# delta window as a row filter; delta and full_load own the whole population.
_TARGETED_MODES = frozenset({"ids", "period"})


def cpr_birth_date(cpr: str | None) -> date | None:
    """Birth date from a CPR number, or None when the number is unusable.

    The gate is the whole number, not just the date part: a value like
    01019034X6 has a parsable 1990-01-01 but a letter in position 9, and its
    last digit — which carries sex — is then meaningless too.

    Century comes from digit 7 together with the two-digit year (CPR office
    rule). Modulus 11 is deliberately not checked: it was abandoned in Denmark
    in 2007, so valid numbers issued since then fail it.
    """
    if not cpr or len(cpr) != 10 or not cpr.isdigit():
        return None

    dd, mm, yy = int(cpr[0:2]), int(cpr[2:4]), int(cpr[4:6])
    serial_first = int(cpr[6])

    if serial_first <= 3:
        century = 1900
    elif serial_first in (4, 9):
        century = 2000 if yy <= 36 else 1900
    else:
        century = 2000 if yy <= 57 else 1800

    try:
        return date(century + yy, mm, dd)
    except ValueError:
        return None


def cpr_gender(cpr: str | None) -> str | None:
    """FHIR AdministrativeGender from CPR, or None when the number is unusable.

    Behind the same gate as the birth date: parity of the last digit says
    nothing on an administratively constructed number.
    """
    if cpr_birth_date(cpr) is None:
        return None
    return "male" if int(cast(str, cpr)[-1]) % 2 == 1 else "female"


def plausible_birth_date(cpr: str | None, period: date | None) -> date | None:
    """Birth date, suppressed when it implies an age under _MIN_PLAUSIBLE_AGE."""
    born = cpr_birth_date(cpr)
    if born is None or period is None:
        return None
    if period.year - born.year < _MIN_PLAUSIBLE_AGE:
        return None
    return born


def usable_date(value: Any, period: date | None) -> date | None:
    """A source date, or None when it is a missing-marker or after the period."""
    if not isinstance(value, date):
        return None
    if value in _MISSING_DATE_MARKERS or value.year < 1900:
        return None
    if period is not None and value > period:
        return None
    return value


def split_name(navn: str | None) -> tuple[str | None, list[str], str | None]:
    """Split "Efternavn,Fornavn(e)" into (family, given, text).

    99,998 % of source names carry the comma. Without it no split is guessed —
    only text is returned, so nothing is invented. A family name containing a
    space cannot be told from several given names; that costs precision in
    given, never in family, and text always holds the original.
    """
    if navn is None:
        return None, [], None
    raw = navn.strip()
    if not raw:
        return None, [], None
    if "," not in raw:
        return None, [], raw
    family, _, rest = raw.partition(",")
    given = [part for part in rest.split() if part]
    return (family.strip() or None), given, raw


def disco_code(value: Any) -> str | None:
    """Discokode, or None for the source's unknown marker."""
    code = str(value).strip() if value is not None else ""
    if not code or code == _DISCO_UNKNOWN:
        return None
    return code


_EPISODE_KEY_RE = re.compile(r"^\d{8}$")


def role_identifier_value(inst: Any, tjenestenummer: Any, episode_key: Any) -> str | None:
    """Composite employment-episode key "{Inst}-{Tjenestenummer}-{YYYYMMDD}".

    Inst is required: 12.027 tjenestenumre occur under more than one
    institution, so the number alone is not unique.

    The third element is the episode's first delivery period, which is what
    makes a REUSED tjenestenummer resolve to one resource per holder instead of
    only the latest one. It is deliberately not derived from the CPR: a hash of
    a 10-digit number with date structure is not a pseudonym, and the CPR is the
    only part of the key designed to change — a corrected number would move the
    identifier and orphan the resource it used to resolve to.

    The key is built in SQL as CONVERT(CHAR(8), ..., 112) so the driver cannot
    decide the format; anything else reaching here is a regression in that
    query, not a value to be coerced, so it yields None rather than a key.
    """
    inst_s = str(inst).strip() if inst is not None else ""
    tj_s = str(tjenestenummer).strip() if tjenestenummer is not None else ""
    episode_s = str(episode_key).strip() if episode_key is not None else ""
    if not inst_s or not tj_s or not _EPISODE_KEY_RE.match(episode_s):
        return None
    return f"{inst_s}-{tj_s}-{episode_s}"


# ---------------------------------------------------------------------------
# Archive queries
# ---------------------------------------------------------------------------

# A CPR correction is the same person under a new number. Detected as: same
# employment, identical person fingerprint, and the new number starting within
# one delivery period of the old one ending. Measured on the full archive
# 2026-09-07 this matches 4 pairs (it was 3 on 2026-09-04) — which is why it is
# a rule and not a list: the fourth was handled without a code change.
#
# The pair is directional: the later number survives as the active person, and
# the earlier one becomes its own resource marked inactive. It is deliberately
# NOT carried as a second identifier under SYSTEM_CPR — extract_identifier_value
# is LIMIT 1 and the unique index sits on that expression, so two identifiers of
# the same system make the index key depend on array position, and the
# superseded number's own resource would be the collision partner. The old
# number travels as an extension instead, which no index reads.
_CPR_PAIR_CTE = """
        fingerprint AS (
            SELECT [CprNr], [Inst], [Tjenestenummer],
                   MIN([Udtraeksdato]) AS [first_period],
                   MAX([Udtraeksdato]) AS [last_period],
                   MIN(NULLIF([Anciennitetsdato], '9999-12-31')) AS [ancien],
                   MIN([Ansaettelsesdato]) AS [ansaet],
                   MAX([Navn]) AS [navn]
            FROM [arc].[silkeborgdata_e26_pers]
            WHERE [CprNr] IS NOT NULL AND [CprNr] <> ''
            GROUP BY [CprNr], [Inst], [Tjenestenummer]
        ),
        -- One row per (keep, alias) pair. GROUP BY rather than DISTINCT so the
        -- two ordering keys below are available: the pair is unique, but NEITHER
        -- side of it is. The fingerprint groups on (CprNr, Inst,
        -- Tjenestenummer), so a person with two employments can pair twice.
        cpr_pair AS (
            SELECT keep.[CprNr] AS [keep_cpr],
                   old.[CprNr] AS [alias_cpr],
                   MAX(old.[last_period]) AS [alias_last_period],
                   MIN(keep.[first_period]) AS [keep_first_period]
            FROM fingerprint keep
            JOIN fingerprint old
              ON old.[Inst] = keep.[Inst]
             AND old.[Tjenestenummer] = keep.[Tjenestenummer]
             AND old.[CprNr] <> keep.[CprNr]
             AND old.[navn] = keep.[navn]
             AND old.[ancien] = keep.[ancien]
             AND old.[ansaet] = keep.[ansaet]
             AND old.[last_period] < keep.[first_period]
             AND DATEDIFF(DAY, old.[last_period], keep.[first_period]) <= 40
            GROUP BY keep.[CprNr], old.[CprNr]
        ),
        -- The two CTEs below exist ONLY to make the LEFT JOINs one-to-one. A
        -- LEFT JOIN straight onto cpr_pair fans out — and with the unique index
        -- on extract_identifier_value(resource, 'urn:dk:cpr') that is not noise
        -- but a unique violation that fails the whole asset.
        --
        -- Semi-joins (EXISTS / NOT EXISTS) must keep reading the raw cpr_pair:
        -- they cannot fan out, and they have to see EVERY superseded number.
        -- Narrowing them to _rn = 1 would let a dropped alias reappear as an
        -- active person — exactly what alias_ranked exists to prevent.

        -- One alias per surviving number: the one held immediately before.
        -- A person with two superseded numbers therefore carries only the most
        -- recent in urn:source:silkeborgdata:afloest_cpr — the extension is a
        -- single valueString, and a second SYSTEM_CPR identifier is ruled out
        -- above. Nothing is lost: every superseded number still owns its own
        -- inactive resource via alias_ranked; only the cross-reference from the
        -- survivor is capped at one.
        cpr_pair_by_keep AS (
            SELECT [keep_cpr], [alias_cpr],
                   ROW_NUMBER() OVER (
                       PARTITION BY [keep_cpr]
                       ORDER BY [alias_last_period] DESC, [alias_cpr] DESC
                   ) AS [_rn]
            FROM cpr_pair
        ),
        -- One successor per superseded number: the one that took over first.
        cpr_pair_by_alias AS (
            SELECT [keep_cpr], [alias_cpr],
                   ROW_NUMBER() OVER (
                       PARTITION BY [alias_cpr]
                       ORDER BY [keep_first_period] ASC, [keep_cpr] DESC
                   ) AS [_rn]
            FROM cpr_pair
        )
"""


# The folded employment episode, and the third element of the role's key.
#
# Folded on _practitioner_cpr and NOT on raw CprNr: otherwise a corrected number
# yields two roles that both point at the same Practitioner. That is K2's
# fan-out, reintroduced through the key instead of through the join.
#
# Reads the whole archive, never `windowed`: a closed episode stops appearing in
# new deliveries, and its start date still has to be resolvable. Same reason
# alias_ranked reads the archive.
#
# CprNr is normalised to '' rather than NULL because the key is compared with
# '=' below, and NULL = NULL is not true — rows without a CPR would lose their
# episode and with it their role.
#
# CONVERT(..., 112) is ISO yyyymmdd. The format is pinned here rather than in
# Python so the driver cannot decide it: a date and a string would give two
# different keys for the same row.
_ROLE_EPISODE_CTE = """
        role_cpr AS (
            SELECT a.[Inst], a.[Tjenestenummer], a.[Udtraeksdato],
                   COALESCE(p.[keep_cpr], NULLIF(LTRIM(RTRIM(a.[CprNr])), ''), '')
                       AS [_practitioner_cpr]
            FROM [arc].[silkeborgdata_e26_pers] a
            LEFT JOIN cpr_pair_by_alias p
              ON p.[alias_cpr] = a.[CprNr] AND p.[_rn] = 1
        ),
        episode AS (
            SELECT [Inst], [Tjenestenummer], [_practitioner_cpr],
                   CONVERT(CHAR(8), MIN([Udtraeksdato]), 112) AS [_episode_key]
            FROM role_cpr
            GROUP BY [Inst], [Tjenestenummer], [_practitioner_cpr]
        )
"""

# The folded CPR. Written once and interpolated three times — select list,
# PARTITION BY and the episode join — because they must be identical character
# for character: a partition that differs from the select list ranks on one
# person and joins on another, and nothing downstream would notice.
_FOLDED_CPR_EXPR = "COALESCE(p.[keep_cpr], NULLIF(LTRIM(RTRIM(w.[CprNr])), ''), '')"


def _snapshot_source(config: RunModeConfig) -> tuple[str, str]:
    """Row source for the snapshot CTE, and the guard that makes a run a no-op.

    In delta the window is a *trigger*, not a row filter. The archive gains one
    delivery per month, so the window is either empty — 29 days out of 30 — or
    the entire new delivery. What it never holds is the people who STOPPED
    appearing, and those are exactly the ones whose `active` has to flip to
    false. Reading the archive instead is the move `alias_ranked` already makes
    below, for the same reason, on a narrower population: a person who left
    needs it just as much as a corrected number does.

    Rebuilding the lot is close to free here. The newest delivery is 37.963 of
    the 53.129 persons, so windowing saves 28 % of the rows once a month — and
    buys a silent correctness bug with the saving.

    The guard keeps the other 29 days honest: with no new delivery the window is
    empty, every branch returns nothing, the asset posts nothing and the
    checkpoint stays put. Without it this query would rebuild everything daily.
    """
    if config.mode in _TARGETED_MODES:
        return "windowed", ""
    guard = "\n              AND EXISTS (SELECT 1 FROM windowed)" if config.mode == "delta" else ""
    return safe_table_ref(ARCHIVE_TABLE), guard


def build_practitioner_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Newest archive row per person, with the superseded CPR attached.

    In delta the query rebuilds every person and no-ops when no delivery has
    landed — see _snapshot_source for why the window cannot be a row filter here.

    include_deleted is required, not optional: the archive has no
    dw_is_deleted column (it never detects deletes), and the shared builder
    otherwise adds a filter on it and the query fails on a missing column.
    """
    windowed = build_fhir_archive_query(ARCHIVE_TABLE, checkpoint, config, include_deleted=True)
    source, guard = _snapshot_source(config)
    return f"""
        WITH windowed AS (
            {windowed}
        ),
        {_CPR_PAIR_CTE},
        latest_period AS (
            SELECT MAX([Udtraeksdato]) AS [period] FROM [arc].[silkeborgdata_e26_pers]
        ),
        ranked AS (
            SELECT w.*,
                   -- The tie-break is not decoration. 2.066 persons hold several
                   -- concurrent employments, so a person has SEVERAL rows in the
                   -- newest period and Udtraeksdato alone leaves the winner to the
                   -- query plan — the name could differ between two runs over
                   -- unchanged data. Inst+Tjenestenummer makes the ordering total,
                   -- so the same row always wins. (The role query carries its
                   -- own tie-break on CprNr. Its partition used to BE the
                   -- archive's primary key, so a tie could not exist; with the
                   -- episode in the partition that argument no longer holds.)
                   ROW_NUMBER() OVER (
                       PARTITION BY w.[CprNr]
                       ORDER BY w.[Udtraeksdato] DESC, w.[Inst], w.[Tjenestenummer]
                   ) AS [_rn]
            FROM {source} w
            WHERE w.[CprNr] IS NOT NULL AND w.[CprNr] <> ''
              AND NOT EXISTS (SELECT 1 FROM cpr_pair s WHERE s.[alias_cpr] = w.[CprNr])
        ),
        -- The superseded number gets its own resource, marked inactive, so a
        -- correction never leaves an unreferenced person claiming active=true.
        --
        -- Read from the archive and NOT from `windowed`: a corrected number stops
        -- appearing in new deliveries, so in delta mode its rows are older than
        -- the checkpoint and the window does not contain them. Selecting from the
        -- window would work in full_load and silently do nothing in delta —
        -- exactly the case this row exists to cover.
        --
        -- The survivor branch now reads the archive for the same reason, via
        -- _snapshot_source: a person who left stops appearing too, and `active`
        -- has to flip for them. This branch just got there first, on the four
        -- rows where the failure was visible.
        --
        -- In delta it carries the same no-op guard as the survivor branch, so a
        -- day without a delivery posts nothing at all. On a day WITH one it is
        -- emitted unconditionally: 4 rows out of 1.6M, identical payload every
        -- time, idempotent upsert — the cost buys self-healing, because a run
        -- that missed the correction still repairs it. In `ids` mode the alias
        -- resources ride along even when not asked for; harmless, and the
        -- alternative is a conditional that can be wrong.
        alias_ranked AS (
            SELECT a.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY a.[CprNr]
                       ORDER BY a.[Udtraeksdato] DESC, a.[Inst], a.[Tjenestenummer]
                   ) AS [_rn]
            FROM [arc].[silkeborgdata_e26_pers] a
            WHERE EXISTS (SELECT 1 FROM cpr_pair s WHERE s.[alias_cpr] = a.[CprNr])
        )
        SELECT r.*,
               p.[alias_cpr] AS [_alias_cpr],
               CASE WHEN r.[Udtraeksdato] = (SELECT [period] FROM latest_period)
                    THEN 1 ELSE 0 END AS [_is_latest],
               0 AS [_is_superseded]
        FROM ranked r
        LEFT JOIN cpr_pair_by_keep p ON p.[keep_cpr] = r.[CprNr] AND p.[_rn] = 1
        WHERE r.[_rn] = 1{guard}
        UNION ALL
        -- _is_latest is hard-coded to 0 rather than derived from the period. The
        -- pairing rule requires the old number's last period to precede the new
        -- one's first, so a derived value would be 0 anyway — but only as long as
        -- that rule holds. The literal states the intent instead of inheriting it.
        --
        -- _is_superseded is a flag of its own and NOT derived from _is_latest.
        -- Both are 0 for a person who simply left, and there are some 15.000 of
        -- those: deriving the retired-identity marker from _is_latest would soft
        -- delete every former employee. It has to be a separate statement.
        SELECT a.*,
               CAST(NULL AS NVARCHAR(10)) AS [_alias_cpr],
               0 AS [_is_latest],
               1 AS [_is_superseded]
        FROM alias_ranked a
        WHERE a.[_rn] = 1{guard}
    """


def build_practitioner_role_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Newest archive row per employment EPISODE, with the surviving CPR resolved.

    An episode is one person's unbroken hold on one (Inst, Tjenestenummer). 36
    numbers have been held by more than one person over time; keying on the
    number alone kept only the latest holder's role and lost the earlier ones.
    The episode's first delivery period is therefore the third element of the
    key — see role_identifier_value for why it is not derived from the CPR.

    Re-employment of the SAME person on the same number folds into one episode
    keeping its original start date: the alternative is a gap threshold, which
    would make the key depend on a second heuristic on top of cpr_pair.

    The reference must point at the CPR that survived the pairing rule above,
    or the role would reference a Practitioner that was never created.

    Same delta contract as the person query: rebuild everything when a delivery
    has landed, no-op otherwise. See _snapshot_source.
    """
    windowed = build_fhir_archive_query(ARCHIVE_TABLE, checkpoint, config, include_deleted=True)
    source, guard = _snapshot_source(config)
    return f"""
        WITH windowed AS (
            {windowed}
        ),
        {_CPR_PAIR_CTE},
        {_ROLE_EPISODE_CTE},
        latest_period AS (
            SELECT MAX([Udtraeksdato]) AS [period] FROM [arc].[silkeborgdata_e26_pers]
        ),
        ranked AS (
            SELECT w.*,
                   {_FOLDED_CPR_EXPR} AS [_practitioner_cpr],
                   -- The tie-break on CprNr is not decoration. The partition is
                   -- no longer a subset of the archive's primary key, and that
                   -- two numbers for one person cannot share a period follows
                   -- from the cpr_pair rule — that is, from a heuristic. The
                   -- tie-break makes the ordering total without relying on it.
                   ROW_NUMBER() OVER (
                       PARTITION BY w.[Inst], w.[Tjenestenummer], {_FOLDED_CPR_EXPR}
                       ORDER BY w.[Udtraeksdato] DESC, w.[CprNr] DESC
                   ) AS [_rn]
            FROM {source} w
            LEFT JOIN cpr_pair_by_alias p ON p.[alias_cpr] = w.[CprNr] AND p.[_rn] = 1
        )
        -- LEFT JOIN, never INNER: a missing episode must not silently DELETE the
        -- role. It arrives as a NULL key, the transform counts it in
        -- skipped_no_episode, and the number lands in the log. A failure has to
        -- be visible.
        SELECT r.*,
               e.[_episode_key],
               CASE WHEN r.[Udtraeksdato] = (SELECT [period] FROM latest_period)
                    THEN 1 ELSE 0 END AS [_is_latest]
        FROM ranked r
        LEFT JOIN episode e
          ON e.[Inst] = r.[Inst]
         AND e.[Tjenestenummer] = r.[Tjenestenummer]
         AND e.[_practitioner_cpr] = r.[_practitioner_cpr]
        WHERE r.[_rn] = 1{guard}
    """
