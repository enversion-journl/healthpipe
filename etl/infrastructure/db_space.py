# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Collect SQL Server file + volume occupancy and evaluate headroom.

Used by the daily ``infra_db_space_*`` assets. No Dagster imports — keep the
thresholds and forecast testable without a run.

The cap a database actually runs into is not always the same object, and that
distinction decides which thresholds mean anything:

* A **per-database quota** (Azure ``MaxSizeInBytes``, or a fixed ``max_size``
  on every data file) is owned by this database alone, so "N% used" describes
  this database and a percentage threshold is meaningful.
* **Unlimited autogrowth on a volume** is shared with every other database on
  that volume. Percentage-of-volume then says more about the neighbours than
  about us -- a 6.5 TB volume can sit at 85% while this database still has a
  terabyte of runway -- so only the absolute free space is judged. Headroom is
  the space already reserved inside the data files *plus* the volume's free
  space, because growth consumes the reserved space first and needs no volume
  at all until the file autogrows.

The volume's own free space is judged separately from ``remaining_gb``, since
in-file headroom would otherwise mask a volume that neighbours have filled --
and a volume at zero is an outage regardless of what the data file still holds.
Both gates are absolute GB. ``volume_used_pct`` is recorded but never judged
unless ``DB_SPACE_WARN_VOLUME_PCT`` is set, and then only as a warning.

Known limitation: when several databases share a volume, each one forecasts
against the whole of that shared free space, so ``weeks_until_full`` is
optimistic by roughly the number of growing databases on the volume. The
volume-level occupancy is reported alongside it (``volume_used_pct``) so an
operator can see the shared picture.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

DEFAULT_WARN_FREE_GB = 150.0
DEFAULT_FAIL_FREE_GB = 80.0
DEFAULT_WARN_USED_PCT = 75.0
DEFAULT_FAIL_USED_PCT = 85.0
DEFAULT_WARN_WEEKS = 8.0
DEFAULT_FAIL_WEEKS = 3.0
# The volume floor is judged on its own, so it reuses the free-space cutoffs.
DEFAULT_WARN_VOLUME_FREE_GB = DEFAULT_WARN_FREE_GB
DEFAULT_FAIL_VOLUME_FREE_GB = DEFAULT_FAIL_FREE_GB
# Ignore growth smaller than this when forecasting (noise / shrink).
_MIN_WEEKLY_GROWTH_GB = 0.05
_PRIOR_SNAPSHOT_MIN_DAYS = 6
# A daily monitor whose newest row is older than this has stopped running.
_SNAPSHOT_MAX_AGE_HOURS = 36.0
TOP_OBJECT_LIMIT = 20

# What bounds this database's growth. Only CAP_DATABASE makes a percentage
# meaningful; see the module docstring.
CAP_DATABASE = "database_quota"
CAP_VOLUME = "volume"
CAP_UNKNOWN = "unknown"


def _env_float_or_none(name: str) -> float | None:
    """Unset means the gate is off, not that it defaults to something."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name}={raw!r} is not a number (expected a percentage)") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        # Loud beats silently monitoring against the wrong number.
        raise ValueError(f"{name}={raw!r} is not a number (expected GB/percent/weeks)") from exc


def headroom_thresholds() -> dict[str, float | None]:
    """Warn/fail cutoffs. Override with DB_SPACE_* env vars per environment."""
    return {
        "warn_free_gb": _env_float("DB_SPACE_WARN_FREE_GB", DEFAULT_WARN_FREE_GB),
        "fail_free_gb": _env_float("DB_SPACE_FAIL_FREE_GB", DEFAULT_FAIL_FREE_GB),
        "warn_used_pct": _env_float("DB_SPACE_WARN_USED_PCT", DEFAULT_WARN_USED_PCT),
        "fail_used_pct": _env_float("DB_SPACE_FAIL_USED_PCT", DEFAULT_FAIL_USED_PCT),
        "warn_weeks": _env_float("DB_SPACE_WARN_WEEKS", DEFAULT_WARN_WEEKS),
        "fail_weeks": _env_float("DB_SPACE_FAIL_WEEKS", DEFAULT_FAIL_WEEKS),
        "warn_volume_free_gb": _env_float(
            "DB_SPACE_WARN_VOLUME_FREE_GB", DEFAULT_WARN_VOLUME_FREE_GB
        ),
        "fail_volume_free_gb": _env_float(
            "DB_SPACE_FAIL_VOLUME_FREE_GB", DEFAULT_FAIL_VOLUME_FREE_GB
        ),
        # Off unless set. Percentage of a volume is a capacity-planning nudge,
        # not an outage predictor, so it can only ever warn.
        "warn_volume_pct": _env_float_or_none("DB_SPACE_WARN_VOLUME_PCT"),
    }


def _num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


@dataclass(frozen=True)
class TopObject:
    schema: str
    table: str
    index_name: str | None
    index_type: str | None
    used_mb: float


@dataclass(frozen=True)
class VolumeInfo:
    mount_point: str | None
    total_gb: float | None
    free_gb: float | None


@dataclass(frozen=True)
class Headroom:
    """Growth headroom, and whether a percentage of it means anything."""

    cap_kind: str
    remaining_gb: float | None
    # Only set when a per-database quota exists; None on a shared volume.
    used_pct: float | None
    # Occupancy of the whole volume — informational, never a threshold.
    volume_used_pct: float | None


@dataclass
class SpaceReport:
    database_name: str
    data_file_size_gb: float
    data_file_used_gb: float
    data_file_max_size_gb: float | None = None
    log_file_size_gb: float | None = None
    log_file_used_gb: float | None = None
    # The binding volume: fullest of those holding a data file.
    volume_mount_point: str | None = None
    volume_total_gb: float | None = None
    volume_free_gb: float | None = None
    # Free space summed over every volume holding a data file. Reported, not
    # judged: a filegroup may not be able to reach all of it.
    volume_free_total_gb: float | None = None
    volumes: list[VolumeInfo] = field(default_factory=list)
    azure_max_size_gb: float | None = None
    cap_kind: str = CAP_UNKNOWN
    used_pct: float | None = None
    volume_used_pct: float | None = None
    remaining_gb: float | None = None
    weekly_growth_gb: float | None = None
    weeks_until_full: float | None = None
    top_objects: list[TopObject] = field(default_factory=list)
    # Why a reading is missing or degraded — logged by the caller.
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class HeadroomVerdict:
    level: str  # "ok" | "warn" | "fail"
    reasons: tuple[str, ...]


_FILE_SQL = """
SELECT
    DB_NAME() AS database_name,
    CAST(SUM(CASE WHEN df.type_desc = 'ROWS' THEN df.size ELSE 0 END)
         * 8.0 / 1024 / 1024 AS decimal(12, 2)) AS data_file_size_gb,
    CAST(SUM(CASE WHEN df.type_desc = 'ROWS'
                  THEN FILEPROPERTY(df.name, 'SpaceUsed') ELSE 0 END)
         * 8.0 / 1024 / 1024 AS decimal(12, 2)) AS data_file_used_gb,
    CASE
        WHEN MAX(CASE WHEN df.type_desc = 'ROWS' AND df.max_size = -1
                      THEN 1 ELSE 0 END) = 1
        THEN NULL
        ELSE CAST(SUM(CASE WHEN df.type_desc = 'ROWS' THEN df.max_size ELSE 0 END)
                  * 8.0 / 1024 / 1024 AS decimal(12, 2))
    END AS data_file_max_size_gb,
    CAST(SUM(CASE WHEN df.type_desc = 'LOG' THEN df.size ELSE 0 END)
         * 8.0 / 1024 / 1024 AS decimal(12, 2)) AS log_file_size_gb,
    CAST(SUM(CASE WHEN df.type_desc = 'LOG'
                  THEN FILEPROPERTY(df.name, 'SpaceUsed') ELSE 0 END)
         * 8.0 / 1024 / 1024 AS decimal(12, 2)) AS log_file_used_gb,
    CAST(CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint)
         / 1024.0 / 1024 / 1024 AS decimal(12, 2)) AS azure_max_size_gb
FROM sys.database_files df
"""

# One row per distinct volume holding a data file, tightest first. Data files
# routinely span volumes (QA DSO sits on three), and the two facts that matter
# are different aggregates: the fullest volume is what stops the next
# autogrowth and is judged, while the sum across volumes is what the database
# can still grow into and is reported. Grouping by volume_id rather than the
# mount point because the mount point comes back NULL on Linux.
_VOLUME_SQL = """
SELECT
    MIN(vs.volume_mount_point) AS volume_mount_point,
    CAST(MIN(vs.total_bytes) / 1024.0 / 1024 / 1024 AS decimal(12, 2)) AS volume_total_gb,
    CAST(MIN(vs.available_bytes) / 1024.0 / 1024 / 1024 AS decimal(12, 2)) AS volume_free_gb
FROM sys.database_files df
CROSS APPLY sys.dm_os_volume_stats(DB_ID(), df.file_id) vs
WHERE df.type_desc = 'ROWS'
GROUP BY vs.volume_id
ORDER BY MIN(vs.available_bytes) ASC
"""

_TOP_OBJECTS_SQL = f"""
SELECT TOP ({TOP_OBJECT_LIMIT})
    SCHEMA_NAME(t.schema_id) AS [schema],
    t.name AS [table],
    i.name AS [index],
    i.type_desc,
    CAST(SUM(ps.used_page_count) * 8.0 / 1024 AS decimal(12, 1)) AS used_mb
FROM sys.dm_db_partition_stats ps
JOIN sys.indexes i
    ON ps.object_id = i.object_id AND ps.index_id = i.index_id
JOIN sys.tables t ON t.object_id = i.object_id
GROUP BY t.schema_id, t.name, i.name, i.type_desc
ORDER BY SUM(ps.used_page_count) DESC
"""

_PRIOR_USED_SQL = f"""
SELECT TOP 1
    data_file_used_gb,
    captured_at
FROM [meta].[DbSpaceSnapshot]
WHERE database_name = DB_NAME()
  AND captured_at <= DATEADD(day, -{_PRIOR_SNAPSHOT_MIN_DAYS}, SYSUTCDATETIME())
ORDER BY captured_at DESC
"""

_LATEST_SNAPSHOT_SQL = """
SELECT TOP 1
    captured_at, database_name, volume_mount_point, volume_total_gb, volume_free_gb,
    data_file_size_gb, data_file_used_gb, data_file_max_size_gb,
    log_file_size_gb, log_file_used_gb, azure_max_size_gb,
    cap_kind, used_pct, volume_used_pct, remaining_gb,
    weekly_growth_gb, weeks_until_full, volume_free_total_gb,
    DATEDIFF(minute, captured_at, SYSUTCDATETIME()) AS age_minutes
FROM [meta].[DbSpaceSnapshot]
WHERE database_name = DB_NAME()
ORDER BY captured_at DESC, snapshot_id DESC
"""


def compute_headroom(
    *,
    data_file_used_gb: float,
    data_file_size_gb: float,
    data_file_max_size_gb: float | None,
    volume_total_gb: float | None,
    volume_free_gb: float | None,
    azure_max_size_gb: float | None,
) -> Headroom:
    """Headroom from the tightest known cap, tagged with which cap that was."""
    # Space already reserved inside the data files: growth eats this before it
    # needs a single byte of the volume.
    in_file_free_gb = max(data_file_size_gb - data_file_used_gb, 0.0)

    volume_used_pct = None
    if volume_total_gb and volume_total_gb > 0 and volume_free_gb is not None:
        volume_used_pct = 100.0 * (volume_total_gb - volume_free_gb) / volume_total_gb

    volume_remaining_gb = None
    if volume_free_gb is not None:
        volume_remaining_gb = in_file_free_gb + volume_free_gb

    quota_gb = azure_max_size_gb if azure_max_size_gb and azure_max_size_gb > 0 else None
    if quota_gb is None and data_file_max_size_gb and data_file_max_size_gb > 0:
        quota_gb = data_file_max_size_gb

    if quota_gb is not None:
        quota_remaining_gb = quota_gb - data_file_used_gb
        used_pct = 100.0 * data_file_used_gb / quota_gb
        # A quota the volume cannot actually satisfy is not the binding cap.
        if volume_remaining_gb is not None and volume_remaining_gb < quota_remaining_gb:
            return Headroom(CAP_VOLUME, volume_remaining_gb, used_pct, volume_used_pct)
        return Headroom(CAP_DATABASE, quota_remaining_gb, used_pct, volume_used_pct)

    if volume_remaining_gb is not None:
        return Headroom(CAP_VOLUME, volume_remaining_gb, None, volume_used_pct)

    # Neither a quota nor volume stats. Free space inside the current file is
    # NOT a cap -- an autogrowing file sits near-full by design -- so report no
    # cap rather than a guaranteed false alarm.
    return Headroom(CAP_UNKNOWN, None, None, volume_used_pct)


def compute_forecast(
    *,
    remaining_gb: float | None,
    current_used_gb: float,
    prior_used_gb: float | None,
    prior_captured_at: datetime | None,
    now: datetime | None = None,
) -> tuple[float | None, float | None]:
    """Return (weekly_growth_gb, weeks_until_full) from a ≥6-day-old snapshot."""
    if prior_used_gb is None or prior_captured_at is None:
        return None, None
    now = now or datetime.now(UTC)
    if prior_captured_at.tzinfo is None:
        prior_captured_at = prior_captured_at.replace(tzinfo=UTC)
    elapsed_days = (now - prior_captured_at).total_seconds() / 86400.0
    if elapsed_days < _PRIOR_SNAPSHOT_MIN_DAYS:
        return None, None
    weekly_growth = (current_used_gb - prior_used_gb) * 7.0 / elapsed_days
    if weekly_growth < _MIN_WEEKLY_GROWTH_GB:
        return weekly_growth, None
    if remaining_gb is None:
        return weekly_growth, None
    return weekly_growth, remaining_gb / weekly_growth


def evaluate_headroom(
    report: SpaceReport,
    thresholds: dict[str, float | None] | None = None,
) -> HeadroomVerdict:
    """Classify occupancy. Fail outranks warn; reasons are stable for tests."""
    t = thresholds or headroom_thresholds()
    fail: list[str] = []
    warn: list[str] = []

    remaining = report.remaining_gb
    if remaining is None:
        # Not "healthy" -- we simply could not measure. Say so instead of
        # passing green or inventing a cap.
        warn.append(
            "no space cap could be determined (volume stats unavailable and no "
            "database quota set) — headroom is unknown"
        )
    elif remaining < t["fail_free_gb"]:
        fail.append(f"remaining {remaining:.2f} GB is below fail {t['fail_free_gb']:.0f} GB")
    elif remaining < t["warn_free_gb"]:
        warn.append(f"remaining {remaining:.2f} GB is below warn {t['warn_free_gb']:.0f} GB")

    # Only judged against a per-database quota. Percentage of a shared volume
    # tracks the neighbours, not this database.
    used_pct = report.used_pct
    if used_pct is not None:
        if used_pct >= t["fail_used_pct"]:
            fail.append(f"used {used_pct:.1f}% of quota is at/above fail {t['fail_used_pct']:.0f}%")
        elif used_pct >= t["warn_used_pct"]:
            warn.append(f"used {used_pct:.1f}% of quota is at/above warn {t['warn_used_pct']:.0f}%")

    # Judged on its own, never folded into remaining_gb. The space reserved
    # inside the data files is real, but a volume at zero stops the log growing,
    # tempdb spilling and backups running whatever room that file still has --
    # so in-file headroom must not be able to mask a full volume. Absolute GB
    # rather than a percentage: 85% of 6.4 TiB is ~1 TB free while 85% of 200 GB
    # is 30 GB, and one cutoff cannot be right for both.
    volume_free = report.volume_free_gb
    if volume_free is not None:
        fail_floor = t["fail_volume_free_gb"]
        warn_floor = t["warn_volume_free_gb"]
        if volume_free < fail_floor:
            fail.append(f"volume has {volume_free:.2f} GB free, below fail {fail_floor:.0f} GB")
        elif volume_free < warn_floor:
            warn.append(f"volume has {volume_free:.2f} GB free, below warn {warn_floor:.0f} GB")

    # Opt-in only (DB_SPACE_WARN_VOLUME_PCT). Useful when the volume can be
    # expanded, where "85% full" means "go ask for more disk" rather than
    # "an outage is coming"; meaningless on a large shared volume otherwise.
    volume_pct_warn = t.get("warn_volume_pct")
    volume_used_pct = report.volume_used_pct
    if volume_pct_warn is not None and volume_used_pct is not None:
        if volume_used_pct >= volume_pct_warn:
            warn.append(
                f"volume is {volume_used_pct:.1f}% full, at/above warn {volume_pct_warn:.0f}%"
            )

    weeks = report.weeks_until_full
    if weeks is not None:
        if weeks < t["fail_weeks"]:
            fail.append(f"{weeks:.1f} weeks until full is below fail {t['fail_weeks']:.0f} weeks")
        elif weeks < t["warn_weeks"]:
            warn.append(f"{weeks:.1f} weeks until full is below warn {t['warn_weeks']:.0f} weeks")

    if fail:
        return HeadroomVerdict("fail", tuple(fail))
    if warn:
        return HeadroomVerdict("warn", tuple(warn))
    return HeadroomVerdict("ok", ())


def collect_space_report(cursor) -> SpaceReport:
    """Read file sizes, volume (best-effort), top objects, and growth forecast."""
    notes: list[str] = []

    cursor.execute(_FILE_SQL)
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("sys.database_files returned no rows")

    database_name = row[0]
    data_file_size_gb = _num(row[1]) or 0.0
    data_file_used_gb = _num(row[2]) or 0.0
    data_file_max_size_gb = _num(row[3])
    log_file_size_gb = _num(row[4])
    log_file_used_gb = _num(row[5])
    azure_max_size_gb = _num(row[6])

    volumes: list[VolumeInfo] = []
    volume_mount_point = None
    volume_total_gb = None
    volume_free_gb = None
    volume_free_total_gb = None
    try:
        cursor.execute(_VOLUME_SQL)
        rows = cursor.fetchall() or []
        if not rows:
            notes.append("sys.dm_os_volume_stats returned no rows for this database's data files")
        else:
            volumes = [VolumeInfo(r[0], _num(r[1]), _num(r[2])) for r in rows]
            # Tightest first, per _VOLUME_SQL's ORDER BY.
            binding = volumes[0]
            volume_mount_point = binding.mount_point
            volume_total_gb = binding.total_gb
            volume_free_gb = binding.free_gb
            volume_free_total_gb = sum(v.free_gb or 0.0 for v in volumes)
    except Exception as exc:
        # Azure SQL / missing VIEW SERVER STATE. Never silent: losing the volume
        # reading changes which cap is judged, so it has to show up in the run.
        notes.append(f"volume stats unavailable ({type(exc).__name__}: {exc})")

    headroom = compute_headroom(
        data_file_used_gb=data_file_used_gb,
        data_file_size_gb=data_file_size_gb,
        data_file_max_size_gb=data_file_max_size_gb,
        volume_total_gb=volume_total_gb,
        volume_free_gb=volume_free_gb,
        azure_max_size_gb=azure_max_size_gb,
    )

    top_objects: list[TopObject] = []
    cursor.execute(_TOP_OBJECTS_SQL)
    for obj in cursor.fetchall() or []:
        top_objects.append(
            TopObject(
                schema=obj[0],
                table=obj[1],
                index_name=obj[2],
                index_type=obj[3],
                used_mb=_num(obj[4]) or 0.0,
            )
        )

    prior_used = None
    prior_at = None
    try:
        cursor.execute(_PRIOR_USED_SQL)
        prior = cursor.fetchone()
        if prior is not None:
            prior_used = _num(prior[0])
            prior_at = prior[1]
    except Exception as exc:
        # Table not created yet — first run still reports occupancy.
        notes.append(f"no snapshot history to forecast from ({type(exc).__name__}: {exc})")

    weekly_growth_gb, weeks_until_full = compute_forecast(
        remaining_gb=headroom.remaining_gb,
        current_used_gb=data_file_used_gb,
        prior_used_gb=prior_used,
        prior_captured_at=prior_at,
    )

    return SpaceReport(
        database_name=database_name,
        data_file_size_gb=data_file_size_gb,
        data_file_used_gb=data_file_used_gb,
        data_file_max_size_gb=data_file_max_size_gb,
        log_file_size_gb=log_file_size_gb,
        log_file_used_gb=log_file_used_gb,
        volume_mount_point=volume_mount_point,
        volume_total_gb=volume_total_gb,
        volume_free_gb=volume_free_gb,
        volume_free_total_gb=volume_free_total_gb,
        volumes=volumes,
        azure_max_size_gb=azure_max_size_gb,
        cap_kind=headroom.cap_kind,
        used_pct=headroom.used_pct,
        volume_used_pct=headroom.volume_used_pct,
        remaining_gb=headroom.remaining_gb,
        weekly_growth_gb=weekly_growth_gb,
        weeks_until_full=weeks_until_full,
        top_objects=top_objects,
        notes=tuple(notes),
    )


def latest_snapshot(cursor) -> SpaceReport | None:
    """Rebuild the newest persisted snapshot, or None if there is none.

    The check reads what the asset wrote instead of re-measuring, so the
    materialization and the check can never disagree, and the check costs one
    seek on IX_DbSpaceSnapshot_DatabaseCaptured instead of a partition-stats
    scan. Thresholds are still applied live, so an env-var change takes effect
    without waiting for the next capture.
    """
    cursor.execute(_LATEST_SNAPSHOT_SQL)
    row = cursor.fetchone()
    if row is None:
        return None

    notes: list[str] = []
    age_minutes = _num(row[18])
    if age_minutes is not None and age_minutes > _SNAPSHOT_MAX_AGE_HOURS * 60:
        notes.append(
            f"newest snapshot is {age_minutes / 60:.1f} h old "
            f"(expected daily) — the monitor may have stopped running"
        )

    return SpaceReport(
        database_name=row[1],
        data_file_size_gb=_num(row[5]) or 0.0,
        data_file_used_gb=_num(row[6]) or 0.0,
        data_file_max_size_gb=_num(row[7]),
        log_file_size_gb=_num(row[8]),
        log_file_used_gb=_num(row[9]),
        volume_mount_point=row[2],
        volume_total_gb=_num(row[3]),
        volume_free_gb=_num(row[4]),
        azure_max_size_gb=_num(row[10]),
        cap_kind=row[11] or CAP_UNKNOWN,
        used_pct=_num(row[12]),
        volume_used_pct=_num(row[13]),
        remaining_gb=_num(row[14]),
        weekly_growth_gb=_num(row[15]),
        weeks_until_full=_num(row[16]),
        volume_free_total_gb=_num(row[17]),
        notes=tuple(notes),
    )


def insert_snapshot(cursor, report: SpaceReport, verdict: HeadroomVerdict) -> int:
    """Persist the report and the verdict it produced. Caller commits."""
    cursor.execute(
        """
        INSERT INTO [meta].[DbSpaceSnapshot] (
            database_name, volume_mount_point, volume_total_gb, volume_free_gb,
            data_file_size_gb, data_file_used_gb, data_file_max_size_gb,
            log_file_size_gb, log_file_used_gb, azure_max_size_gb,
            cap_kind, used_pct, volume_used_pct, remaining_gb,
            weekly_growth_gb, weeks_until_full, headroom_level, volume_free_total_gb
        )
        OUTPUT INSERTED.snapshot_id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            report.database_name,
            report.volume_mount_point,
            report.volume_total_gb,
            report.volume_free_gb,
            report.data_file_size_gb,
            report.data_file_used_gb,
            report.data_file_max_size_gb,
            report.log_file_size_gb,
            report.log_file_used_gb,
            report.azure_max_size_gb,
            report.cap_kind,
            report.used_pct,
            report.volume_used_pct,
            report.remaining_gb,
            report.weekly_growth_gb,
            report.weeks_until_full,
            verdict.level,
            report.volume_free_total_gb,
        ),
    )
    snapshot_id = int(cursor.fetchone()[0])
    for rank_no, obj in enumerate(report.top_objects, start=1):
        cursor.execute(
            """
            INSERT INTO [meta].[DbSpaceTopObject] (
                snapshot_id, rank_no, object_schema, object_table,
                index_name, index_type, used_mb
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                rank_no,
                obj.schema,
                obj.table,
                obj.index_name,
                obj.index_type,
                obj.used_mb,
            ),
        )
    return snapshot_id


def report_metadata(report: SpaceReport, verdict: HeadroomVerdict) -> dict[str, Any]:
    """Dagster metadata carrying only what was actually measured.

    A key is omitted rather than emitted as null. ``used_pct`` is absent
    because no per-database quota exists -- ``cap_kind`` already says so -- not
    because a reading failed, and nothing null is ever judged anyway (every
    gate in ``evaluate_headroom`` guards on None). A panel of nulls buries the
    handful of numbers that matter.

    The forecast is the exception: "not ready yet" is worth stating, so it
    becomes an explicit ``forecast`` line instead of a silent omission.
    """
    metadata: dict[str, Any] = {
        "database_name": report.database_name,
        "cap_kind": report.cap_kind,
        "headroom_level": verdict.level,
        "data_file_size_gb": report.data_file_size_gb,
        "data_file_used_gb": report.data_file_used_gb,
    }

    def put(key: str, value: Any) -> None:
        if value is not None:
            metadata[key] = value

    put("remaining_gb", report.remaining_gb)
    # Set only under a per-database quota; on a shared volume it stays absent.
    put("used_pct", report.used_pct)
    put("data_file_max_size_gb", report.data_file_max_size_gb)
    put("azure_max_size_gb", report.azure_max_size_gb)
    put("log_file_size_gb", report.log_file_size_gb)
    put("log_file_used_gb", report.log_file_used_gb)
    # NULL on Linux, where the DMV cannot name a container bind mount.
    put("volume_mount_point", report.volume_mount_point)
    put("volume_total_gb", report.volume_total_gb)
    put("volume_free_gb", report.volume_free_gb)
    put("volume_used_pct", report.volume_used_pct)

    # Only when it says something the binding volume's own figure does not --
    # i.e. the data files really do span volumes. Checked on the values rather
    # than on len(volumes), so it still appears for the check, which reads the
    # persisted total but does not rebuild the per-volume breakdown.
    if (
        report.volume_free_total_gb is not None
        and report.volume_free_gb is not None
        and abs(report.volume_free_total_gb - report.volume_free_gb) > 0.005
    ):
        metadata["volume_free_total_gb"] = report.volume_free_total_gb
    if len(report.volumes) > 1:
        metadata["volume_count"] = len(report.volumes)
        metadata["volumes"] = [
            {
                "mount_point": v.mount_point or "(unnamed)",
                "total_gb": v.total_gb,
                "free_gb": v.free_gb,
            }
            for v in report.volumes
        ]

    if report.weekly_growth_gb is None:
        metadata["forecast"] = (
            f"not yet — needs {_PRIOR_SNAPSHOT_MIN_DAYS} days of snapshot history"
        )
    else:
        metadata["weekly_growth_gb"] = report.weekly_growth_gb
        if report.weeks_until_full is not None:
            metadata["weeks_until_full"] = report.weeks_until_full
        else:
            metadata["forecast"] = (
                f"growth under {_MIN_WEEKLY_GROWTH_GB} GB/week — no runway estimate"
            )

    # Empty lists render as a "[Show JSON]" link with nothing behind it.
    if verdict.reasons:
        metadata["headroom_reasons"] = list(verdict.reasons)
    if report.notes:
        metadata["notes"] = list(report.notes)
    if report.top_objects:
        metadata["top_objects"] = [
            {
                "schema": o.schema,
                "table": o.table,
                "index": o.index_name,
                "type": o.index_type,
                "used_mb": o.used_mb,
            }
            for o in report.top_objects
        ]
    return metadata
