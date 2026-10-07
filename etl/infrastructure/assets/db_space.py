# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Daily DSA + DSO disk-space monitors.

Independent of the ETL graph: a full filegroup must not be discovered as a
failed CarePlan MERGE. Each asset brings ``meta.DbSpaceSnapshot`` up to date
itself -- the DDL file is self-healing precisely so this can run without
staging_migrations -- captures occupancy, and exposes a headroom check.
"""

import dagster as dg
from core.resources import MSSQLResource
from infrastructure.db_space import (
    collect_space_report,
    evaluate_headroom,
    insert_snapshot,
    latest_snapshot,
    report_metadata,
)
from infrastructure.utils.ddl_loader import execute_sql_file

_DDL_PATH = "tables/meta/05_db_space_snapshot.sql"

_CHECK_DESCRIPTION = (
    "Judges the persisted snapshot. WARN below 150 GB of headroom or of volume "
    "free space, or 8 weeks left; FAIL (blocks the job) below 80 GB of either, "
    "or 3 weeks left. The volume floor is judged separately from headroom so "
    "in-file free space cannot mask a volume the neighbours have filled. "
    "A percentage is only judged against a per-database quota (Azure max size "
    "or a fixed max_size); percentage of a shared volume is recorded but not "
    "judged unless DB_SPACE_WARN_VOLUME_PCT is set, and then only as a warning. "
    "Override with DB_SPACE_* env vars."
)


def _capture(context: dg.AssetExecutionContext, mssql: MSSQLResource) -> dg.MaterializeResult:
    execute_sql_file(mssql, _DDL_PATH, context)
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            report = collect_space_report(cursor)
            verdict = evaluate_headroom(report)
            snapshot_id = insert_snapshot(cursor, report, verdict)
            conn.commit()
        finally:
            cursor.close()

    metadata = report_metadata(report, verdict)
    metadata["snapshot_id"] = snapshot_id

    remaining = f"{report.remaining_gb:.1f} GB" if report.remaining_gb is not None else "unknown"
    weeks = f"{report.weeks_until_full:.1f} weeks" if report.weeks_until_full is not None else "n/a"
    context.log.info(
        f"{report.database_name}: used={report.data_file_used_gb:.1f} GB, "
        f"remaining={remaining} (cap={report.cap_kind}), "
        f"weeks_until_full={weeks}, level={verdict.level}"
    )
    if len(report.volumes) > 1:
        spread = ", ".join(
            f"{v.mount_point or 'unnamed'} {v.free_gb:.2f} GB free" for v in report.volumes
        )
        context.log.info(
            f"data files span {len(report.volumes)} volumes "
            f"({report.volume_free_total_gb:.2f} GB free in total); "
            f"judging the fullest: {spread}"
        )
    for note in report.notes:
        context.log.warning(note)
    for reason in verdict.reasons:
        if verdict.level == "fail":
            context.log.error(reason)
        else:
            context.log.warning(reason)

    return dg.MaterializeResult(metadata=metadata)


def _check_headroom(
    context: dg.AssetCheckExecutionContext, mssql: MSSQLResource
) -> dg.AssetCheckResult:
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            report = latest_snapshot(cursor)
        finally:
            cursor.close()

    if report is None:
        description = "No snapshot in meta.DbSpaceSnapshot — materialize the asset first"
        context.log.warning(description)
        # Nothing measured is not the same as enough space: fail at WARN so the
        # check shows amber instead of a green pass over missing data.
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=description,
        )

    verdict = evaluate_headroom(report)
    reasons = list(verdict.reasons) + list(report.notes)
    description = "; ".join(reasons) if reasons else "Headroom is within thresholds"
    metadata = report_metadata(report, verdict)

    if verdict.level == "fail":
        context.log.error(description)
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            description=description,
            metadata=metadata,
        )
    if verdict.level == "warn" or report.notes:
        context.log.warning(description)
        # passed=False is what makes a WARN visible: Dagster reads `severity`
        # only on a failed check, so passed=True would render a plain green
        # pass. WARN severity does not fail or block the run.
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=description,
            metadata=metadata,
        )
    return dg.AssetCheckResult(passed=True, description=description, metadata=metadata)


@dg.asset(
    name="infra_db_space_staging",
    group_name="infra_ops",
    compute_kind="mssql",
    tags={"layer": "staging", "type": "ops", "object": "disk_space"},
    description=(
        "Snapshot DSA file + volume occupancy into meta.DbSpaceSnapshot. "
        "No ETL deps — run daily even when the pipeline is down."
    ),
)
def infra_db_space_staging(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _capture(context, mssql_target_staging)


@dg.asset(
    name="infra_db_space_dso",
    group_name="infra_ops",
    compute_kind="mssql",
    tags={"layer": "dso", "type": "ops", "object": "disk_space"},
    description=(
        "Snapshot DSO file + volume occupancy into meta.DbSpaceSnapshot. "
        "No ETL deps — run daily even when the pipeline is down."
    ),
)
def infra_db_space_dso(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    return _capture(context, mssql_target_dso)


@dg.asset_check(
    name="db_space_headroom",
    asset="infra_db_space_staging",
    blocking=True,
    required_resource_keys={"mssql_target_staging"},
    compute_kind="mssql",
    description=_CHECK_DESCRIPTION,
)
def infra_db_space_staging_headroom(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    return _check_headroom(context, context.resources.mssql_target_staging)


@dg.asset_check(
    name="db_space_headroom",
    asset="infra_db_space_dso",
    blocking=True,
    required_resource_keys={"mssql_target_dso"},
    compute_kind="mssql",
    description=_CHECK_DESCRIPTION,
)
def infra_db_space_dso_headroom(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    return _check_headroom(context, context.resources.mssql_target_dso)


all_db_space_assets = [infra_db_space_staging, infra_db_space_dso]
all_db_space_checks = [infra_db_space_staging_headroom, infra_db_space_dso_headroom]
