# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Asset checks reporting on the SilkeborgData E26PERS archive.

A growing month archive hides three failure classes particularly well, and all
three leave the runs green:

  - a skipped delivery (period_continuity)
  - a truncated or half-written file (period_volume)
  - a DFM checkpoint that has drifted from the archive's own content, or has
    stopped moving altogether (checkpoint_alignment, checkpoint_not_stalled)

The last two are about the DFM contract itself: the delta selection reads the
checkpoint, and the checkpoint is derived from MAX(dw_leverance_dato). If those
two disagree, deliveries are either skipped permanently or the whole history is
reloaded every day — invisibly.

Only period_volume blocks. It is the one check that judges the delivery this run
just wrote, and the consequence of being wrong about that delivery is not a
crooked archive one can fix in peace: `active` is recomputed for the whole
population on every delivery (_is_latest is "this person's newest Udtraeksdato ==
MAX(Udtraeksdato) in the archive"), so a half-written file marks everyone it does
not contain as departed — in the reporting database, until the next correct
delivery a month later.

The extract now carries the same test as a pre-commit gate, which is the cheaper
place to stop it: a rejected delivery is rolled back out of ext and never reaches
the archive, so there is nothing to undo. This check is the second line. It sees
what the gate cannot — a period the gate skipped because the archive was empty at
the time, a tolerance override used wider than intended, or a backfill that
rewrote history — and it is the one that runs on the archive as it actually
stands. Both read VOLUME_TOLERANCE_PCT, so they cannot disagree.

The other three stay non-blocking WARN on purpose. They describe the archive's
history or the DFM state, not the bytes this run wrote: they can be red for days
without this delivery being wrong, so they would block on something the run
cannot fix. What they buy is a named, dated warning in the Dagster UI — acting on
it is a human's job.

Remember to keep them registered in definitions.py's postfix_asset_checks list —
an unregistered check never runs and never fails.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSI.csv_files.silkeborgdata_e26_pers import VOLUME_TOLERANCE_PCT

_ARC_TABLE = "[arc].[silkeborgdata_e26_pers]"
_ASSET_KEY = dg.AssetKey("arc_silkeborgdata_e26_pers")

# A monthly source that has not delivered in this long is either broken upstream
# or not reaching us. Two months plus a week of slack.
_STALE_AFTER_DAYS = 70

# The tolerance is owned by the extract, which gates the same quantity before it
# commits; see VOLUME_TOLERANCE_PCT there for why it is 3 % and not 10 %. Imported
# rather than restated: two copies of a threshold drift, and a check that is
# laxer than the gate it backs up would never fire.
#
# This check is the second line, not the first. The gate stops a truncated file
# before it reaches the archive; this one catches what the gate cannot see — a
# period that entered the archive some other way, a tolerance override that was
# used wider than intended, or a backfill that rewrote history.
_VOLUME_TOLERANCE_PCT = VOLUME_TOLERANCE_PCT

# Largest gap between two consecutive periods that is still normal drift.
#
# The obvious continuity test — count the distinct calendar months and compare
# them to the calendar span — is WRONG for this source, and the archive says so.
# The period is a payroll period whose day wanders: measured over every period in
# the archive it lands between the 19th and the 29th, and consecutive periods sit
# 27 to 35 days apart. The source therefore does not deliver on calendar months;
# it delivers on a rhythm that happens to average one per month.
#
# A calendar-month model reports a hole that does not exist as soon as a period
# lands late in a month and the next gap carries it past a short month. It has
# not happened yet — 40 periods, 40 distinct calendar months — but it came within
# one draw: the January 2026 period fell on the 28th, where a 32-day gap would
# have skipped February, and gaps of 32 days or more occur in 14 of the 39 gaps
# measured. The check would have warned about a delivery that arrived on time.
#
# Measuring the distance instead is both correct and roomy. Normal drift tops out
# at 35 days; a genuinely skipped delivery would show up at roughly twice the
# cadence, 55 days and up. 45 sits between the two with margin on both sides, so
# it cannot fire on drift and cannot miss a skipped month.
_MAX_PERIOD_GAP_DAYS = 45


def _fetch_one(resource: MSSQLResource, query: str) -> tuple | None:
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchone()
        finally:
            cursor.close()


def _fetch_all(resource: MSSQLResource, query: str) -> list[tuple]:
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()


@dg.asset_check(
    name="silkeborgdata_e26_pers_period_continuity",
    asset=_ASSET_KEY,
    blocking=False,
    compute_kind="mssql",
    description=(
        "No gap longer than _MAX_PERIOD_GAP_DAYS between consecutive Udtraeksdato "
        "values in arc.silkeborgdata_e26_pers. Catches a skipped delivery — the error a "
        "growing archive hides best, because every individual run looks normal."
    ),
)
def silkeborgdata_e26_pers_period_continuity(
    context: dg.AssetCheckExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    """Distance between periods, not calendar months — see _MAX_PERIOD_GAP_DAYS.

    LAG over the DISTINCT periods rather than over the rows: the archive holds one
    row per employment per period, so the raw table has tens of thousands of rows
    carrying the same Udtraeksdato.
    """
    row = _fetch_one(
        mssql_target_staging,
        f"""
        WITH [periods] AS (
            SELECT DISTINCT [Udtraeksdato] AS [period]
            FROM {_ARC_TABLE}
        ),
        [gaps] AS (
            SELECT [period],
                   DATEDIFF(DAY, LAG([period]) OVER (ORDER BY [period]), [period])
                       AS [gap_days]
            FROM [periods]
        )
        SELECT
            (SELECT COUNT(*) FROM [periods]) AS [period_count],
            (SELECT CONVERT(CHAR(10), MIN([period]), 23) FROM [periods]) AS [first_period],
            (SELECT CONVERT(CHAR(10), MAX([period]), 23) FROM [periods]) AS [last_period],
            MAX([gap_days]) AS [max_gap_days],
            SUM(CASE WHEN [gap_days] > {_MAX_PERIOD_GAP_DAYS} THEN 1 ELSE 0 END)
                AS [oversized_gaps],
            MIN(CASE WHEN [gap_days] > {_MAX_PERIOD_GAP_DAYS}
                     THEN CONVERT(CHAR(10), [period], 23) END) AS [first_gap_after]
        FROM [gaps]
        """,
    )

    if row is None or row[0] in (None, 0):
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            description="Archive is empty — nothing to check yet.",
            metadata={"period_count": 0},
        )

    period_count, first_period, last_period, max_gap, oversized, first_gap_after = row

    # One period means no gap to measure. Not a hole, and not something this check
    # can say anything about — the volume check owns "is there enough here".
    if period_count == 1:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            description=f"Only one period in the archive ({first_period}) — no gap to measure.",
            metadata={"period_count": 1, "first_period": str(first_period)},
        )

    oversized = int(oversized or 0)

    return dg.AssetCheckResult(
        passed=oversized == 0,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"{period_count} periods ({first_period} .. {last_period}), "
            f"largest gap {max_gap} day(s)"
            + (
                f" — {oversized} gap(s) over {_MAX_PERIOD_GAP_DAYS} days, "
                f"first one ending {first_gap_after}."
                if oversized
                else " — no delivery skipped."
            )
        ),
        metadata={
            "period_count": int(period_count),
            "max_gap_days": int(max_gap) if max_gap is not None else 0,
            "gap_limit_days": _MAX_PERIOD_GAP_DAYS,
            "oversized_gaps": oversized,
            "first_period": str(first_period),
            "last_period": str(last_period),
        },
    )


class PeriodVolumeConfig(dg.Config):
    """Override for a delivery whose size really did change.

    A blocking check cannot be waved through with a full_load — it reads the
    archive, not the run mode — so without this there is no way past a genuine
    reorganisation except editing the constant. Set tolerance_pct when launching
    the check to accept a confirmed delivery, then let the rest of the chain run.
    """

    tolerance_pct: float | None = None


@dg.asset_check(
    name="silkeborgdata_e26_pers_period_volume",
    asset=_ASSET_KEY,
    blocking=True,
    compute_kind="mssql",
    description=(
        f"BLOCKING. Row count in the latest period is within ±{_VOLUME_TOLERANCE_PCT:.0f}% of "
        "the previous period. Catches a truncated or half-delivered file, which otherwise "
        "looks like a perfectly normal load — field-count validation does not catch a "
        "file cut at a line break. Blocking because `active` is recomputed for the whole "
        "population on every delivery, so an accepted truncation marks everyone missing "
        "as departed all the way into the reporting database. Override a confirmed large "
        "change with the tolerance_pct config."
    ),
)
def silkeborgdata_e26_pers_period_volume(
    context: dg.AssetCheckExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: PeriodVolumeConfig,
) -> dg.AssetCheckResult:
    tolerance_pct = (
        config.tolerance_pct if config.tolerance_pct is not None else _VOLUME_TOLERANCE_PCT
    )
    rows = _fetch_all(
        mssql_target_staging,
        f"""
        SELECT TOP (2)
            CONVERT(CHAR(10), [Udtraeksdato], 23) AS periode,
            COUNT(*) AS antal
        FROM {_ARC_TABLE}
        GROUP BY [Udtraeksdato]
        ORDER BY [Udtraeksdato] DESC
        """,
    )

    if len(rows) < 2:
        # The first delivery ever. Nothing to compare against, so this passes —
        # severity is inert on a pass and stated only so the branch is not read
        # as a softer one than the check itself.
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.ERROR,
            description=(
                f"Only {len(rows)} period(s) in the archive — need two to compare volumes."
            ),
            metadata={"periods_available": len(rows)},
        )

    (latest_period, latest_count), (previous_period, previous_count) = rows
    deviation_pct = (int(latest_count) - int(previous_count)) / int(previous_count) * 100.0

    return dg.AssetCheckResult(
        passed=abs(deviation_pct) <= tolerance_pct,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            f"{latest_period}: {latest_count} rows vs {previous_period}: "
            f"{previous_count} rows ({deviation_pct:+.1f}%)"
        ),
        metadata={
            "latest_period": str(latest_period),
            "latest_rows": int(latest_count),
            "previous_period": str(previous_period),
            "previous_rows": int(previous_count),
            "deviation_pct": round(deviation_pct, 2),
            "tolerance_pct": tolerance_pct,
            "tolerance_overridden": config.tolerance_pct is not None,
        },
    )


@dg.asset_check(
    name="silkeborgdata_e26_pers_checkpoint_alignment",
    asset=_ASSET_KEY,
    blocking=False,
    compute_kind="mssql",
    description=(
        "The DFM checkpoint in meta.DFMState equals MAX(dw_leverance_dato) in the "
        "archive. Divergence means the delta selection has stopped working: ahead of "
        "the archive, deliveries are skipped permanently; behind it, the whole history "
        "reloads every day. Both are invisible — the runs stay green."
    ),
)
def silkeborgdata_e26_pers_checkpoint_alignment(
    context: dg.AssetCheckExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    row = _fetch_one(
        mssql_target_staging,
        f"""
        SELECT
            (SELECT CONVERT(CHAR(10), MAX([dw_leverance_dato]), 23)
             FROM {_ARC_TABLE})                            AS arkiv_max,
            (SELECT LEFT([checkpoint_value], 10)
             FROM [meta].[DFMState]
             WHERE [scope] = 'staging'
               AND [target_type] = 'archive'
               AND [schema_name] = 'arc'
               AND [table_name] = 'silkeborgdata_e26_pers') AS dfm_checkpoint
        """,
    )

    archive_max = row[0] if row else None
    checkpoint = row[1] if row else None

    if archive_max is None and checkpoint is None:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            description="Archive is empty and no checkpoint is set yet — first run pending.",
            metadata={"archive_max": "None", "dfm_checkpoint": "None"},
        )

    aligned = (archive_max or "").strip() == (checkpoint or "").strip()

    return dg.AssetCheckResult(
        passed=aligned,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"MAX(dw_leverance_dato)={archive_max!r}, DFM checkpoint={checkpoint!r}"
            + ("" if aligned else " — the two have diverged; reset with dfm_reset_job.")
        ),
        metadata={
            "archive_max": str(archive_max),
            "dfm_checkpoint": str(checkpoint),
            "aligned": aligned,
        },
    )


@dg.asset_check(
    name="silkeborgdata_e26_pers_checkpoint_not_stalled",
    asset=_ASSET_KEY,
    blocking=False,
    compute_kind="mssql",
    description=(
        f"The newest delivery in the archive is less than {_STALE_AFTER_DAYS} days old. "
        "Guards the state where deliveries are selected but never advance the "
        "checkpoint, and the state where the share has gone empty or SD has stopped "
        "delivering. Without this, 'green and loading nothing' is invisible."
    ),
)
def silkeborgdata_e26_pers_checkpoint_not_stalled(
    context: dg.AssetCheckExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    row = _fetch_one(
        mssql_target_staging,
        f"""
        SELECT
            CONVERT(CHAR(10), MAX([dw_leverance_dato]), 23) AS seneste_leverance,
            DATEDIFF(DAY, MAX([dw_leverance_dato]), CAST(SYSDATETIME() AS date)) AS dage_siden
        FROM {_ARC_TABLE}
        """,
    )

    if row is None or row[0] is None:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            description="Archive is empty — no delivery to age yet.",
            metadata={"latest_delivery": "None"},
        )

    latest_delivery, days_since = row
    days_since = int(days_since)

    return dg.AssetCheckResult(
        passed=days_since < _STALE_AFTER_DAYS,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"Newest delivery folder {latest_delivery} is {days_since} day(s) old "
            f"(threshold {_STALE_AFTER_DAYS})."
        ),
        metadata={
            "latest_delivery": str(latest_delivery),
            "days_since_delivery": days_since,
            "threshold_days": _STALE_AFTER_DAYS,
        },
    )


# The folded episode, restated in one place for the orphan check. It has to
# agree with _ROLE_EPISODE_CTE in practitioner_common: the whole point of the
# check is to compare Aidbox's roles against what the transform WOULD produce
# now, so a divergence here would report the difference between two of our own
# queries rather than a real orphan. It is not imported, because that module
# builds a delta-aware query around a checkpoint and this check needs the plain
# full-archive form.
_EPISODE_KEYS_SQL = f"""
    WITH fingerprint AS (
        SELECT [CprNr], [Inst], [Tjenestenummer],
               MIN([Udtraeksdato]) AS [first_period],
               MAX([Udtraeksdato]) AS [last_period],
               MIN(NULLIF([Anciennitetsdato], '9999-12-31')) AS [ancien],
               MIN([Ansaettelsesdato]) AS [ansaet],
               MAX([Navn]) AS [navn]
        FROM {_ARC_TABLE}
        WHERE [CprNr] IS NOT NULL AND [CprNr] <> ''
        GROUP BY [CprNr], [Inst], [Tjenestenummer]
    ),
    cpr_pair AS (
        SELECT keep.[CprNr] AS [keep_cpr],
               old.[CprNr] AS [alias_cpr],
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
    cpr_pair_by_alias AS (
        SELECT [keep_cpr], [alias_cpr],
               ROW_NUMBER() OVER (
                   PARTITION BY [alias_cpr]
                   ORDER BY [keep_first_period] ASC, [keep_cpr] DESC
               ) AS [_rn]
        FROM cpr_pair
    ),
    role_cpr AS (
        SELECT a.[Inst], a.[Tjenestenummer], a.[Udtraeksdato],
               COALESCE(p.[keep_cpr], NULLIF(LTRIM(RTRIM(a.[CprNr])), ''), '')
                   AS [_practitioner_cpr]
        FROM {_ARC_TABLE} a
        LEFT JOIN cpr_pair_by_alias p
          ON p.[alias_cpr] = a.[CprNr] AND p.[_rn] = 1
    ),
    expected AS (
        SELECT LTRIM(RTRIM([Inst])) + '-' + LTRIM(RTRIM([Tjenestenummer])) + '-'
                   + CONVERT(CHAR(8), MIN([Udtraeksdato]), 112) AS [noegle]
        FROM role_cpr
        WHERE LTRIM(RTRIM([Inst])) <> '' AND LTRIM(RTRIM([Tjenestenummer])) <> ''
        GROUP BY [Inst], [Tjenestenummer], [_practitioner_cpr]
    )
"""


@dg.asset_check(
    name="silkeborgdata_practitionerrole_no_orphans",
    asset=dg.AssetKey("sof_practitioner_role_dfm"),
    blocking=False,
    compute_kind="mssql",
    description=(
        "Every active PractitionerRole still corresponds to an employment episode the "
        "transform would produce today. The role's identity contains the episode's first "
        "delivery period, and that period MOVES when the cpr_pair rule fires late: two "
        "episodes fold into one afterwards, the role built under the later number's own "
        "start date stops being produced, and it is left in Aidbox with active=1. Nothing "
        "else catches it — dqa_fhir_server_cleanup handles resources deleted ON THE "
        "SERVER, not resources the transform stopped producing. Same failure class as "
        "period_volume: a person who looks employed without being one."
    ),
)
def silkeborgdata_practitionerrole_no_orphans(
    context: dg.AssetCheckExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    # Runs entirely in MSSQL: fhir.PractitionerRole is a projection of Aidbox via
    # the SQL-on-FHIR extract, so the comparison needs no second connection. It
    # is therefore only meaningful AFTER that extract has run — hence the asset
    # key above, not the archive's.
    row = _fetch_one(
        mssql_target_staging,
        f"""
        {_EPISODE_KEYS_SQL}
        SELECT COUNT(*) AS foraeldreloese,
               (SELECT COUNT(*) FROM [fhir].[PractitionerRole]
                WHERE [active] = 1 AND [dw_is_deleted] = 0) AS aktive
        FROM [fhir].[PractitionerRole] pr
        WHERE pr.[active] = 1
          AND pr.[dw_is_deleted] = 0
          AND NOT EXISTS (
                SELECT 1 FROM expected e
                WHERE e.[noegle] = pr.[identifier_tjenestenummer]
          )
        """,
    )
    if row is None:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            description="No rows returned — fhir.PractitionerRole is not populated yet.",
            metadata={"orphans": 0},
        )

    orphans, active_total = int(row[0]), int(row[1])
    # WARN and not blocking, for one delivery cycle: an orphan is a real defect,
    # but it has never been observed, and the first thing to learn is whether the
    # count is zero in practice. Blocking on an unmeasured quantity stops the
    # pipeline on the query's bugs rather than on the data's.
    return dg.AssetCheckResult(
        passed=orphans == 0,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"{orphans} active PractitionerRole(s) out of {active_total} no longer "
            "correspond to an employment episode the transform produces"
        ),
        metadata={"orphans": orphans, "active_roles": active_total},
    )


__all__ = [
    "silkeborgdata_e26_pers_checkpoint_alignment",
    "silkeborgdata_e26_pers_checkpoint_not_stalled",
    "silkeborgdata_e26_pers_period_continuity",
    "silkeborgdata_e26_pers_period_volume",
    "silkeborgdata_practitionerrole_no_orphans",
]
