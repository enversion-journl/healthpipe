# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the daily DSA/DSO disk-space monitor."""

from datetime import UTC, datetime, timedelta

import dagster as dg
import pytest
from infrastructure.db_space import (
    CAP_DATABASE,
    CAP_UNKNOWN,
    CAP_VOLUME,
    DEFAULT_FAIL_FREE_GB,
    DEFAULT_FAIL_USED_PCT,
    DEFAULT_WARN_FREE_GB,
    SpaceReport,
    compute_forecast,
    compute_headroom,
    evaluate_headroom,
    headroom_thresholds,
    report_metadata,
)
from jobs.db_space_jobs import db_space_daily_job
from schedules.db_space_schedules import db_space_daily_schedule


def _report(**overrides) -> SpaceReport:
    base = dict(
        database_name="DSO_Basismodel",
        data_file_size_gb=456.89,
        data_file_used_gb=456.89,
        volume_total_gb=499.98,
        volume_free_gb=200.0,
        cap_kind=CAP_VOLUME,
        remaining_gb=200.0,
    )
    base.update(overrides)
    return SpaceReport(**base)


@pytest.mark.unit
def test_shared_volume_headroom_counts_in_file_free_space():
    """The Dev reading that used to FAIL: a 6.5 TB volume at 85%, ~1 TB left."""
    headroom = compute_headroom(
        data_file_used_gb=986.97,
        data_file_size_gb=1255.70,
        data_file_max_size_gb=None,
        volume_total_gb=6553.6,
        volume_free_gb=952.19,
        azure_max_size_gb=None,
    )
    assert headroom.cap_kind == CAP_VOLUME
    # 268.73 GB reserved inside the data file + 952.19 GB on the volume.
    assert headroom.remaining_gb == pytest.approx(1220.92, abs=0.01)
    # Percentage of a shared volume is not this database's occupancy.
    assert headroom.used_pct is None
    assert headroom.volume_used_pct == pytest.approx(85.47, abs=0.01)

    report = _report(
        database_name="healthpipe_staging",
        data_file_size_gb=1255.70,
        data_file_used_gb=986.97,
        volume_total_gb=6553.6,
        volume_free_gb=952.19,
        cap_kind=headroom.cap_kind,
        used_pct=headroom.used_pct,
        volume_used_pct=headroom.volume_used_pct,
        remaining_gb=headroom.remaining_gb,
    )
    assert evaluate_headroom(report).level == "ok"


@pytest.mark.unit
def test_volume_occupancy_alone_still_fails_when_actually_full():
    headroom = compute_headroom(
        data_file_used_gb=456.89,
        data_file_size_gb=456.89,
        data_file_max_size_gb=None,
        volume_total_gb=499.98,
        volume_free_gb=0.01,
        azure_max_size_gb=None,
    )
    assert headroom.cap_kind == CAP_VOLUME
    assert headroom.remaining_gb == pytest.approx(0.01)
    verdict = evaluate_headroom(_report(remaining_gb=headroom.remaining_gb, used_pct=None))
    assert verdict.level == "fail"
    assert any("0.01 GB" in r for r in verdict.reasons)


@pytest.mark.unit
def test_azure_quota_used_when_volume_unknown():
    headroom = compute_headroom(
        data_file_used_gb=400.0,
        data_file_size_gb=400.0,
        data_file_max_size_gb=None,
        volume_total_gb=None,
        volume_free_gb=None,
        azure_max_size_gb=500.0,
    )
    assert headroom.cap_kind == CAP_DATABASE
    assert headroom.remaining_gb == pytest.approx(100.0)
    # A quota belongs to this database, so a percentage of it is meaningful.
    assert headroom.used_pct == pytest.approx(80.0)


@pytest.mark.unit
def test_volume_wins_when_it_cannot_satisfy_the_quota():
    headroom = compute_headroom(
        data_file_used_gb=987.0,
        data_file_size_gb=1000.0,
        data_file_max_size_gb=4096.0,
        volume_total_gb=6553.6,
        volume_free_gb=40.0,
        azure_max_size_gb=None,
    )
    assert headroom.cap_kind == CAP_VOLUME
    # 13 GB in-file + 40 GB volume, not the 3109 GB the quota nominally allows.
    assert headroom.remaining_gb == pytest.approx(53.0)
    assert headroom.used_pct == pytest.approx(100.0 * 987.0 / 4096.0)


@pytest.mark.unit
def test_no_cap_reports_unknown_rather_than_a_false_alarm():
    """An autogrowing file sits near-full by design; that is not a shortage."""
    headroom = compute_headroom(
        data_file_used_gb=452.0,
        data_file_size_gb=456.89,
        data_file_max_size_gb=None,
        volume_total_gb=None,
        volume_free_gb=None,
        azure_max_size_gb=None,
    )
    assert headroom.cap_kind == CAP_UNKNOWN
    assert headroom.remaining_gb is None
    assert headroom.used_pct is None

    verdict = evaluate_headroom(_report(cap_kind=CAP_UNKNOWN, remaining_gb=None, used_pct=None))
    assert verdict.level == "warn"
    assert any("no space cap" in r for r in verdict.reasons)


@pytest.mark.unit
def test_forecast_needs_six_days_of_history():
    now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    weekly, weeks = compute_forecast(
        remaining_gb=100.0,
        current_used_gb=500.0,
        prior_used_gb=486.0,
        prior_captured_at=now - timedelta(days=7),
        now=now,
    )
    assert weekly == pytest.approx(14.0)
    assert weeks == pytest.approx(100.0 / 14.0)

    none_weekly, none_weeks = compute_forecast(
        remaining_gb=100.0,
        current_used_gb=500.0,
        prior_used_gb=499.0,
        prior_captured_at=now - timedelta(days=2),
        now=now,
    )
    assert none_weekly is None
    assert none_weeks is None


@pytest.mark.unit
def test_evaluate_headroom_ok_warn_fail():
    assert evaluate_headroom(_report()).level == "ok"

    warn = evaluate_headroom(_report(remaining_gb=120.0, volume_free_gb=120.0))
    assert warn.level == "warn"
    assert any("120.00 GB" in r for r in warn.reasons)

    fail = evaluate_headroom(_report(remaining_gb=0.01, volume_free_gb=0.01))
    assert fail.level == "fail"
    assert any("0.01 GB" in r for r in fail.reasons)


@pytest.mark.unit
def test_quota_percentage_still_fails():
    verdict = evaluate_headroom(_report(cap_kind=CAP_DATABASE, remaining_gb=400.0, used_pct=90.0))
    assert verdict.level == "fail"
    assert any("90.0% of quota" in r for r in verdict.reasons)


@pytest.mark.unit
def test_weeks_until_full_can_fail_on_its_own():
    verdict = evaluate_headroom(_report(remaining_gb=200.0, weeks_until_full=2.0))
    assert verdict.level == "fail"
    assert any("2.0 weeks" in r for r in verdict.reasons)


@pytest.mark.unit
def test_thresholds_read_env(monkeypatch):
    monkeypatch.setenv("DB_SPACE_FAIL_FREE_GB", "10")
    t = headroom_thresholds()
    assert t["fail_free_gb"] == 10.0
    assert t["warn_free_gb"] == DEFAULT_WARN_FREE_GB
    assert t["fail_used_pct"] == DEFAULT_FAIL_USED_PCT
    assert DEFAULT_FAIL_FREE_GB == 80.0


@pytest.mark.unit
def test_malformed_threshold_names_the_variable(monkeypatch):
    monkeypatch.setenv("DB_SPACE_FAIL_FREE_GB", "80 GB")
    with pytest.raises(ValueError, match="DB_SPACE_FAIL_FREE_GB"):
        headroom_thresholds()


@pytest.mark.unit
def test_monitor_assets_have_no_etl_deps():
    from infrastructure.assets.db_space import infra_db_space_dso, infra_db_space_staging

    for asset in (infra_db_space_staging, infra_db_space_dso):
        deps = {k.to_user_string() for k in asset.dependency_keys}
        assert deps == set(), f"{asset.key} must not depend on ETL/infra: {deps}"


@pytest.mark.unit
def test_registered_in_definitions():
    from jobs import all_jobs
    from schedules import all_schedules

    from infrastructure import all_infrastructure_assets

    keys = {a.key.path[-1] for a in all_infrastructure_assets}
    assert "infra_db_space_staging" in keys
    assert "infra_db_space_dso" in keys
    assert "staging_infra_db_space" in keys

    assert db_space_daily_job in all_jobs
    assert db_space_daily_schedule in all_schedules
    assert db_space_daily_job.name == "db_space_daily_job"
    assert db_space_daily_job.selection == dg.AssetSelection.groups("infra_ops")


# ============================================================================
# The check itself: severity mapping and reading back the persisted snapshot
# ============================================================================


class _FakeCursor:
    """Replays one row per SQL prefix and records what was executed."""

    def __init__(self, rows_by_prefix: dict[str, list[tuple]]):
        self._rows_by_prefix = rows_by_prefix
        self._pending: list[tuple] = []
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._pending = []
        for prefix, rows in self._rows_by_prefix.items():
            if prefix in sql:
                self._pending = list(rows)
                break

    def fetchone(self):
        return self._pending.pop(0) if self._pending else None

    def fetchall(self):
        rows, self._pending = self._pending, []
        return rows

    def close(self):
        pass


class _FakeResource:
    def __init__(self, cursor):
        self._cursor = cursor

    def get_connection(self):
        cursor = self._cursor

        class _Conn:
            def cursor(self):
                return cursor

            def commit(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return _Conn()


class _FakeLog:
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    def info(self, msg):
        self.messages.append(("info", msg))

    def warning(self, msg):
        self.messages.append(("warning", msg))

    def error(self, msg):
        self.messages.append(("error", msg))


class _FakeContext:
    def __init__(self):
        self.log = _FakeLog()


def _snapshot_row(*, remaining_gb, used_pct=None, weeks=None, age_minutes=5, cap=CAP_VOLUME):
    """One [meta].[DbSpaceSnapshot] row in _LATEST_SNAPSHOT_SQL column order."""
    return (
        datetime(2026, 9, 1, 1, 0),  # captured_at
        "healthpipe_staging",
        None,  # volume_mount_point
        6553.6,
        952.19,
        1255.70,
        986.97,
        None,  # data_file_max_size_gb
        8.01,
        0.04,
        None,  # azure_max_size_gb
        cap,
        used_pct,
        85.47,
        remaining_gb,
        None,  # weekly_growth_gb
        weeks,
        None,  # volume_free_total_gb
        age_minutes,
    )


def _run_check(row, *, rows=None):
    from infrastructure.assets.db_space import _check_headroom

    cursor = _FakeCursor({"FROM [meta].[DbSpaceSnapshot]": rows if rows is not None else [row]})
    context = _FakeContext()
    return _check_headroom(context, _FakeResource(cursor)), context


@pytest.mark.unit
def test_check_passes_on_the_dev_reading():
    result, _ = _run_check(_snapshot_row(remaining_gb=1220.92))
    assert result.passed is True
    assert result.metadata["cap_kind"].value == CAP_VOLUME
    # The volume-level number is still reported, just never judged.
    assert result.metadata["volume_used_pct"].value == pytest.approx(85.47)


@pytest.mark.unit
def test_check_fails_with_error_severity_when_critical():
    result, context = _run_check(_snapshot_row(remaining_gb=12.0))
    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.ERROR
    assert ("error", result.description) in context.log.messages


@pytest.mark.unit
def test_warning_is_reported_as_a_failed_warn_not_a_green_pass():
    """Dagster reads `severity` only on a failed check.

    passed=True renders a plain green pass and discards the severity, which
    would delete the whole warn tier. WARN severity does not fail the run.
    """
    result, _ = _run_check(_snapshot_row(remaining_gb=120.0))
    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert "120.00 GB" in result.description


@pytest.mark.unit
def test_check_warns_when_the_snapshot_is_stale():
    result, _ = _run_check(_snapshot_row(remaining_gb=1220.92, age_minutes=4320))
    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert "may have stopped running" in result.description


@pytest.mark.unit
def test_check_warns_when_there_is_no_snapshot():
    result, _ = _run_check(None, rows=[])
    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert "materialize the asset first" in result.description


@pytest.mark.unit
def test_check_reads_the_snapshot_instead_of_re_measuring():
    """One indexed seek, no partition-stats scan, no second opinion to disagree."""
    from infrastructure.assets.db_space import _check_headroom

    cursor = _FakeCursor({"FROM [meta].[DbSpaceSnapshot]": [_snapshot_row(remaining_gb=1220.92)]})
    _check_headroom(_FakeContext(), _FakeResource(cursor))

    assert len(cursor.executed) == 1
    sql = cursor.executed[0][0]
    assert "dm_db_partition_stats" not in sql
    assert "dm_os_volume_stats" not in sql


@pytest.mark.unit
def test_insert_snapshot_columns_placeholders_and_params_line_up():
    from infrastructure.db_space import HeadroomVerdict, insert_snapshot

    cursor = _FakeCursor({"OUTPUT INSERTED.snapshot_id": [(42,)]})
    snapshot_id = insert_snapshot(
        cursor,
        _report(volume_used_pct=85.47, remaining_gb=1220.92),
        HeadroomVerdict("ok", ()),
    )
    assert snapshot_id == 42

    sql, params = cursor.executed[0]
    columns = sql.split("(", 1)[1].split(")", 1)[0]
    assert len(columns.split(",")) == sql.count("?") == len(params)
    assert "headroom_level" in columns


# ============================================================================
# The volume floor: in-file headroom must not mask a volume the neighbours fill
# ============================================================================


@pytest.mark.unit
def test_full_volume_fails_even_with_in_file_headroom():
    """staging has 268 GB reserved inside its data file; that is not enough.

    A volume at zero stops the log growing, tempdb spilling and backups
    running, so it must fail on its own rather than being averaged away.
    """
    in_file_free = 1255.70 - 986.97
    verdict = evaluate_headroom(
        _report(
            data_file_size_gb=1255.70,
            data_file_used_gb=986.97,
            volume_total_gb=6553.6,
            volume_free_gb=0.19,
            volume_used_pct=100.0,
            remaining_gb=in_file_free + 0.19,
        )
    )
    assert verdict.level == "fail"
    assert any("volume has 0.19 GB free" in r for r in verdict.reasons)
    # The headroom gate alone would have passed it: 268.92 GB looks comfortable.
    assert not any("remaining" in r for r in verdict.reasons)


@pytest.mark.unit
def test_volume_floor_warns_before_it_fails():
    verdict = evaluate_headroom(_report(remaining_gb=1220.92, volume_free_gb=120.0))
    assert verdict.level == "warn"
    assert any("volume has 120.00 GB free" in r for r in verdict.reasons)


@pytest.mark.unit
def test_dev_volume_free_space_is_comfortable():
    """952 GB free on the Dev volume stays green under the absolute floor."""
    verdict = evaluate_headroom(
        _report(remaining_gb=1220.92, volume_free_gb=952.19, volume_used_pct=85.47)
    )
    assert verdict.level == "ok"


@pytest.mark.unit
def test_volume_percentage_gate_is_off_by_default():
    t = headroom_thresholds()
    assert t["warn_volume_pct"] is None
    assert t["warn_volume_free_gb"] == DEFAULT_WARN_FREE_GB
    assert t["fail_volume_free_gb"] == DEFAULT_FAIL_FREE_GB

    # 85.47% of a 6.5 TB shared volume: no verdict without the opt-in.
    verdict = evaluate_headroom(
        _report(remaining_gb=1220.92, volume_free_gb=952.19, volume_used_pct=85.47)
    )
    assert verdict.level == "ok"


@pytest.mark.unit
def test_volume_percentage_gate_warns_when_opted_in(monkeypatch):
    monkeypatch.setenv("DB_SPACE_WARN_VOLUME_PCT", "80")
    verdict = evaluate_headroom(
        _report(remaining_gb=1220.92, volume_free_gb=952.19, volume_used_pct=85.47)
    )
    assert verdict.level == "warn"
    assert any("volume is 85.5% full" in r for r in verdict.reasons)


@pytest.mark.unit
def test_volume_percentage_gate_can_never_fail(monkeypatch):
    """Opting in must not turn a large shared volume into a blocked job."""
    monkeypatch.setenv("DB_SPACE_WARN_VOLUME_PCT", "50")
    verdict = evaluate_headroom(
        _report(remaining_gb=1220.92, volume_free_gb=952.19, volume_used_pct=99.9)
    )
    assert verdict.level == "warn"


@pytest.mark.unit
def test_volume_gates_skipped_on_a_quota_only_database():
    """Azure SQL has no volume reading; the floor must not fire on None."""
    verdict = evaluate_headroom(
        _report(
            cap_kind=CAP_DATABASE,
            volume_total_gb=None,
            volume_free_gb=None,
            volume_used_pct=None,
            used_pct=40.0,
            remaining_gb=300.0,
        )
    )
    assert verdict.level == "ok"


@pytest.mark.unit
def test_malformed_volume_pct_names_the_variable(monkeypatch):
    monkeypatch.setenv("DB_SPACE_WARN_VOLUME_PCT", "eighty")
    with pytest.raises(ValueError, match="DB_SPACE_WARN_VOLUME_PCT"):
        headroom_thresholds()


# ============================================================================
# Multi-volume data files (QA DSO spans three, one of them full)
# ============================================================================


def _volume_cursor(volume_rows):
    """A cursor whose volume query returns one row per volume, tightest first."""
    return _FakeCursor(
        {
            "DB_NAME() AS database_name": [
                ("healthpipe_dso", 499.87, 450.00, None, 8.01, 0.07, None)
            ],
            "dm_os_volume_stats": volume_rows,
            "dm_db_partition_stats": [],
            "FROM [meta].[DbSpaceSnapshot]": [],
        }
    )


@pytest.mark.unit
def test_qa_dso_three_volumes_judges_the_full_one_and_reports_the_rest():
    from infrastructure.db_space import collect_space_report

    # Real QA DSO reading: D:\ full, G:\ and E:\ roomy.
    report = collect_space_report(
        _volume_cursor(
            [
                ("D:\\", 499.98, 0.01),
                ("G:\\", 255.98, 186.43),
                ("E:\\", 174.98, 166.92),
            ]
        )
    )

    # The fullest volume binds: it is what stops the next autogrowth.
    assert report.volume_mount_point == "D:\\"
    assert report.volume_free_gb == pytest.approx(0.01)
    # ...but the operator is told the database can still grow into 353 GB.
    assert report.volume_free_total_gb == pytest.approx(353.36)
    assert [v.mount_point for v in report.volumes] == ["D:\\", "G:\\", "E:\\"]

    verdict = evaluate_headroom(report)
    assert verdict.level == "fail"
    assert any("volume has 0.01 GB free" in r for r in verdict.reasons)

    # Spanning volumes is a normal layout, not a degraded reading.
    assert report.notes == ()

    metadata = report_metadata(report, verdict)
    assert metadata["volume_count"] == 3
    assert metadata["volume_free_total_gb"] == pytest.approx(353.36)
    assert metadata["volumes"][0]["mount_point"] == "D:\\"


@pytest.mark.unit
def test_single_volume_totals_match_the_binding_volume():
    from infrastructure.db_space import collect_space_report

    # Dev: one shared 6.4 TiB filesystem, mount point NULL on Linux.
    report = collect_space_report(_volume_cursor([(None, 6553.6, 940.16)]))
    assert report.volume_mount_point is None
    assert report.volume_free_gb == pytest.approx(940.16)
    assert report.volume_free_total_gb == pytest.approx(940.16)
    assert len(report.volumes) == 1

    # One volume: the count and the per-volume breakdown would say nothing the
    # binding figure does not, so they are left out.
    metadata = report_metadata(report, evaluate_headroom(report))
    assert "volume_count" not in metadata
    assert "volumes" not in metadata
    assert "volume_free_total_gb" not in metadata
    assert metadata["volume_free_gb"] == pytest.approx(940.16)


@pytest.mark.unit
def test_volume_query_deduplicates_and_orders_by_free_space():
    from infrastructure.db_space import _VOLUME_SQL

    # Several data files on one volume must not be counted several times.
    assert "GROUP BY vs.volume_id" in _VOLUME_SQL
    assert "ORDER BY MIN(vs.available_bytes) ASC" in _VOLUME_SQL
    assert "TOP 1" not in _VOLUME_SQL


@pytest.mark.unit
def test_missing_volume_stats_leaves_totals_unset():
    from infrastructure.db_space import collect_space_report

    report = collect_space_report(_volume_cursor([]))
    assert report.volumes == []
    assert report.volume_free_gb is None
    assert report.volume_free_total_gb is None
    assert any("returned no rows" in n for n in report.notes)


# ============================================================================
# Metadata carries only what was measured — no null-filled panels
# ============================================================================


@pytest.mark.unit
def test_metadata_never_reports_a_null():
    """The Dev reading used to emit 7 nulls; a null is an omission now."""
    report = _report(
        database_name="healthpipe_staging",
        data_file_size_gb=1255.70,
        data_file_used_gb=926.98,
        log_file_size_gb=8.01,
        log_file_used_gb=0.05,
        volume_total_gb=6553.6,
        volume_free_gb=901.83,
        volume_used_pct=86.24,
        volume_free_total_gb=901.83,
        remaining_gb=1230.55,
    )
    metadata = report_metadata(report, evaluate_headroom(report))

    assert all(v is not None for v in metadata.values()), metadata
    for absent in (
        "used_pct",  # no per-database quota; cap_kind says so
        "data_file_max_size_gb",  # unlimited autogrowth
        "azure_max_size_gb",  # not Azure
        "volume_mount_point",  # NULL on Linux
        "volume_count",  # single volume
        "volume_free_total_gb",  # same as the binding volume
        "weekly_growth_gb",  # no history yet
        "weeks_until_full",
        "headroom_reasons",  # verdict is ok
        "notes",
        "top_objects",  # none collected in this fixture
    ):
        assert absent not in metadata, f"{absent} should be omitted, not null"

    # "no forecast yet" is worth saying out loud rather than showing as null.
    assert "needs 6 days" in metadata["forecast"]
    assert metadata["cap_kind"] == CAP_VOLUME
    assert metadata["headroom_level"] == "ok"


@pytest.mark.unit
def test_metadata_grows_only_when_there_is_something_to_say():
    quota = _report(
        cap_kind=CAP_DATABASE,
        used_pct=91.0,
        azure_max_size_gb=500.0,
        volume_total_gb=None,
        volume_free_gb=None,
        remaining_gb=45.0,
        weekly_growth_gb=14.0,
        weeks_until_full=3.2,
        notes=("volume stats unavailable (OperationalError: nope)",),
    )
    metadata = report_metadata(quota, evaluate_headroom(quota))
    assert all(v is not None for v in metadata.values())
    assert metadata["used_pct"] == 91.0
    assert metadata["azure_max_size_gb"] == 500.0
    assert metadata["weeks_until_full"] == 3.2
    assert metadata["headroom_reasons"]
    assert metadata["notes"]
    assert "volume_free_gb" not in metadata
    assert "forecast" not in metadata


@pytest.mark.unit
def test_check_still_reports_the_spread_on_a_multi_volume_database():
    """The check reads the persisted total but not the breakdown.

    It must still surface "353 GB free elsewhere", or a QA alert reads as if
    the database were out of room.
    """
    row = list(_snapshot_row(remaining_gb=49.88))
    row[4] = 0.01  # volume_free_gb  -> the full D:\
    row[17] = 353.36  # volume_free_total_gb across D:\, G:\, E:\
    result, _ = _run_check(tuple(row))

    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.ERROR
    assert result.metadata["volume_free_total_gb"].value == pytest.approx(353.36)
    assert all(v.value is not None for v in result.metadata.values())
