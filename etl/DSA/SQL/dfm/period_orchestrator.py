# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Period Orchestrator — splits a date range into partitions and runs a target
job once per partition in mode='period'.

Designed for large historical reloads where running the entire range as a
single period would be too heavy, but regular delta mode doesn't apply
(because there's no existing checkpoint).

The orchestrator:
  1. Generates date partitions (day / week / month / quarter / year).
  2. Runs the target job once per partition, passing mode='period'.
  3. Child assets handle their own DFM checkpoint advancement via the
     ``advance_checkpoint`` flag in RunModeConfig.

Example usage (from Dagster Launchpad):
  asset_names:  ["ext_encounter_ifdb", "arc_encounter_ifdb"]
  date_from:    "2020-01-01"
  date_to:      "2025-12-31"
  granularity:  "month"
"""

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from typing import NamedTuple

import dagster as dg
from dagster._core.definitions.reconstruct import ReconstructableJob, ReconstructableRepository
from dagster._core.execution.api import execute_run

# ===========================================================================
# Date partitioning
# ===========================================================================


class DatePartition(NamedTuple):
    """A single (start, end) date partition as ISO date strings.

    Semantics: start is inclusive (>= start), end is inclusive (<= end).
    For DATETIME columns this translates to:
      col >= CAST(start AS DATETIME2) AND col < DATEADD(day, 1, CAST(end AS DATE)).
    """

    start: str
    end: str


def generate_date_partitions(
    date_from: str,
    date_to: str,
    granularity: str = "month",
) -> list[DatePartition]:
    """Split a date range into (start, end) partitions.

    Each partition uses inclusive-start / inclusive-end semantics:
      ``col >= partition.start AND col <= partition.end``
    (implemented as ``col >= start AND col < end + 1 day`` for DATETIME columns).

    Subsequent partitions chain: each partition's ``start`` equals
    the previous partition's ``end + 1 day`` to avoid overlap.

    Supported granularities: day, week, month, quarter, year.
    """
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)

    if start > end:
        raise ValueError(f"date_from ({date_from}) is after date_to ({date_to})")

    builders = {
        "day": _partitions_by_day,
        "week": _partitions_by_week,
        "month": _partitions_by_month,
        "quarter": _partitions_by_quarter,
        "year": _partitions_by_year,
    }
    builder = builders.get(granularity)
    if builder is None:
        raise ValueError(
            f"Unknown granularity {granularity!r}. " f"Choose from: {', '.join(sorted(builders))}"
        )
    return builder(start, end)


def _partitions_by_day(start: date, end: date) -> list[DatePartition]:
    result: list[DatePartition] = []
    current = start
    while current <= end:
        result.append(DatePartition(current.isoformat(), current.isoformat()))
        current += timedelta(days=1)
    return result


def _partitions_by_week(start: date, end: date) -> list[DatePartition]:
    result: list[DatePartition] = []
    current = start
    while current <= end:
        week_end = min(current + timedelta(days=6 - current.weekday()), end)
        result.append(DatePartition(current.isoformat(), week_end.isoformat()))
        current = week_end + timedelta(days=1)
    return result


def _partitions_by_month(start: date, end: date) -> list[DatePartition]:
    result: list[DatePartition] = []
    current = start
    while current <= end:
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = min(date(current.year, current.month, last_day), end)
        result.append(DatePartition(current.isoformat(), month_end.isoformat()))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return result


def _partitions_by_quarter(start: date, end: date) -> list[DatePartition]:
    result: list[DatePartition] = []
    current = start
    while current <= end:
        quarter_month = ((current.month - 1) // 3) * 3 + 3
        last_day = calendar.monthrange(current.year, quarter_month)[1]
        quarter_end = min(date(current.year, quarter_month, last_day), end)
        result.append(DatePartition(current.isoformat(), quarter_end.isoformat()))
        if quarter_month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, quarter_month + 1, 1)
    return result


def _partitions_by_year(start: date, end: date) -> list[DatePartition]:
    result: list[DatePartition] = []
    current = start
    while current <= end:
        year_end = min(date(current.year, 12, 31), end)
        result.append(DatePartition(current.isoformat(), year_end.isoformat()))
        current = date(current.year + 1, 1, 1)
    return result


# ===========================================================================
# Config
# ===========================================================================


class PartitionedPeriodConfig(dg.Config):
    """Config for the period orchestrator asset.

    Specify either ``job_name`` (an existing registered job) or
    ``asset_names`` (a list of asset key strings to materialize as an
    ad-hoc job).  If both are set, ``job_name`` takes precedence.
    Upstream dependencies (e.g. dfm_ifdb_flow_start) are resolved
    automatically — you only need to list the assets you care about.

    Checkpoint advancement is handled by the child assets themselves
    via the ``advance_checkpoint`` flag in RunModeConfig (default True).

    ``flow_start_override`` (IFDB only): when set, it is propagated to the
    ``FlowStartConfig`` of every child run so the ``dfm_ifdb_flow_start`` asset
    skips its live Oracle JOB_LOG query (which would otherwise pin a "now"
    timestamp unrelated to the historical period). With per-chunk checkpoint
    advancement the archive checkpoint is derived from each chunk's ``date_to``
    regardless, so this is hygiene — it avoids a redundant Oracle round-trip per
    chunk and prevents a historical reload from overwriting the shared
    ``dfm.ifdb_flow_start`` row that a concurrent scheduled run may rely on.

    Set via Dagster Launchpad when materializing the orchestrator.
    """

    job_name: str = ""
    asset_names: list[str] = []
    date_from: str
    date_to: str
    granularity: str = "month"
    delta_column: str = ""
    advance_checkpoint: bool = False
    flow_start_override: str = ""


# ===========================================================================
# Partition result tracking
# ===========================================================================


@dataclass
class PartitionResult:
    partition: DatePartition
    success: bool
    run_id: str | None = None
    error: str | None = None


# ===========================================================================
# Run config builder
# ===========================================================================


def build_period_run_config(
    job_def: dg.JobDefinition,
    partition: DatePartition,
    delta_column: str | None = None,
    advance_checkpoint: bool = False,
    flow_start_override: str | None = None,
) -> dict:
    """Build a run_config dict that configures every configurable op.

    Introspects the job definition and configures two kinds of op:
      - ops that accept a ``mode`` config field (RunModeConfig) → set to
        ``mode='period'`` with the partition's dates.
      - ops that accept a ``flow_start_override`` field (FlowStartConfig, i.e.
        ``dfm_ifdb_flow_start``) → set to the given override, but only when
        ``flow_start_override`` is provided.

    Ops with neither field (e.g. pure infra setup) are skipped.
    """
    period_config: dict = {
        "mode": "period",
        "date_from": partition.start,
        "date_to": partition.end,
        "advance_checkpoint": advance_checkpoint,
    }
    if delta_column:
        period_config["delta_column"] = delta_column

    flow_start_config: dict | None = (
        {"flow_start_override": flow_start_override} if flow_start_override else None
    )

    ops: dict = {}
    for node in job_def.all_node_defs:
        cs = node.config_schema
        if not (cs and cs.config_type and hasattr(cs.config_type, "fields")):
            continue
        fields = cs.config_type.fields
        if "mode" in fields:
            ops[node.name] = {"config": period_config}
        elif flow_start_config is not None and "flow_start_override" in fields:
            ops[node.name] = {"config": flow_start_config}

    return {"ops": ops}


# ===========================================================================
# Job resolution
# ===========================================================================


class ResolvedJob(NamedTuple):
    """Resolved job definition plus optional asset selection for subsetting."""

    job_def: dg.JobDefinition
    asset_selection: frozenset[dg.AssetKey] | None


def resolve_job_def(
    config: PartitionedPeriodConfig,
    repo_def: dg.RepositoryDefinition,
) -> ResolvedJob:
    """Resolve the target job definition from config.

    Priority:
      1. job_name — looks up an existing registered job (no subsetting).
      2. asset_names — uses the global implicit job with asset_selection
         to restrict execution to only the requested assets.
      3. Neither — raises.

    Returns a ResolvedJob with the job_def and optional asset_selection.
    When asset_selection is set, it must be passed to create_run_for_job
    so Dagster only executes those specific assets (not the full job).
    """
    if config.job_name:
        return ResolvedJob(repo_def.get_job(config.job_name), None)

    if config.asset_names:
        asset_keys = frozenset(dg.AssetKey(name) for name in config.asset_names)
        job_def = repo_def.get_implicit_job_def_for_assets(asset_keys)
        if job_def is None:
            raise dg.Failure(
                description=(
                    f"Could not create an implicit job for assets: "
                    f"{config.asset_names}. Verify the asset names exist."
                ),
            )
        return ResolvedJob(job_def, asset_keys)

    raise dg.Failure(
        description="Either job_name or asset_names must be provided.",
    )


# ===========================================================================
# Asset
# ===========================================================================


def _target_label(config: PartitionedPeriodConfig) -> str:
    if config.job_name:
        return f"job {config.job_name!r}"
    return f"assets {config.asset_names}"


@dg.asset(
    name="period_orchestrator",
    group_name="dfm_operations",
    compute_kind="python",
    description=(
        "Meta-asset: splits a date range into partitions and runs a target "
        "job (or list of assets) once per partition in mode='period'. "
        "Child assets advance their own DFM checkpoints. "
        "Use for historical reloads."
    ),
)
def period_orchestrator(
    context: dg.AssetExecutionContext,
    config: PartitionedPeriodConfig,
) -> dg.MaterializeResult:
    """Run a target job for each date partition, advancing checkpoint on success."""
    partitions = generate_date_partitions(config.date_from, config.date_to, config.granularity)

    target = _target_label(config)
    context.log.info(
        f"Period orchestrator: {len(partitions)} partitions for "
        f"{target} ({config.date_from} → {config.date_to}, "
        f"granularity={config.granularity}, "
        f"advance_checkpoint={config.advance_checkpoint})"
    )

    resolved = resolve_job_def(config, context.repository_def)
    instance = context.instance

    Results: list[PartitionResult] = _run_partitions_sequentially(
        resolved=resolved,
        instance=instance,
        config=config,
        partitions=partitions,
        context=context,
    )

    succeeded = [r for r in Results if r.success]
    failed = [r for r in Results if not r.success]
    skipped = len(partitions) - len(Results)

    last_completed = succeeded[-1].partition.end if succeeded else None

    metadata: dict = {
        "target": _target_label(config),
        "date_from": config.date_from,
        "date_to": config.date_to,
        "granularity": config.granularity,
        "advance_checkpoint": config.advance_checkpoint,
        "total_partitions": len(partitions),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "skipped": skipped,
        "last_completed_partition": last_completed or "none",
    }

    if failed:
        failed[0].error or "see child run logs"
        resume_from = succeeded[-1].partition.end if succeeded else config.date_from
        metadata["resume_from"] = resume_from
        raise dg.Failure(
            description=(
                f"{len(failed)} partition(s) failed, {skipped} skipped. "
                f"Last completed partition ended at {last_completed or 'none'}. "
                f"To resume, set date_from to '{resume_from}' "
                f"(the day after the last completed partition, or the original "
                f"date_from if no partitions succeeded)."
            ),
            metadata=metadata,
        )

    return dg.MaterializeResult(metadata=metadata)


# ===========================================================================
# Sequential execution
# ===========================================================================


def _get_child_run_errors(
    instance: dg.DagsterInstance,
    run_id: str,
) -> str | None:
    """Extract error messages from a failed child run's event log."""
    try:
        records = instance.get_records_for_run(run_id).records
        errors: list[str] = []
        for record in records:
            event = record.event_log_entry.dagster_event
            if event and event.is_step_failure:
                err_data = event.step_failure_data
                if err_data and err_data.error:
                    msg = err_data.error.message
                    errors.append(f"[{event.step_key}] {msg[:500]}")
        return " | ".join(errors) if errors else None
    except Exception:
        return None


def _get_reconstructable_job(
    context: dg.AssetExecutionContext,
    target_job_name: str,
) -> ReconstructableJob:
    """Build a ReconstructableJob for the target from the orchestrator's context.

    The orchestrator itself runs inside a reconstructable execution environment
    (Dagster dev / production).  We grab the ReconstructableRepository from the
    running process and use it to obtain a reconstructable reference to the
    *target* job — required by ``execute_run`` when the executor spawns
    subprocesses (e.g. ``multiprocess_executor``).
    """
    step_ctx = context.get_step_execution_context()
    current_job = step_ctx.plan_data.job

    if isinstance(current_job, ReconstructableJob):
        recon_repo: ReconstructableRepository = current_job.get_reconstructable_repository()
        return recon_repo.get_reconstructable_job(target_job_name)

    raise dg.Failure(
        description=(
            "Cannot obtain a reconstructable job reference.  The "
            "period_orchestrator must run inside a Dagster environment that "
            "supports process hand-off (dagster dev / production deployment)."
        ),
    )


def _run_partitions_sequentially(
    resolved: ResolvedJob,
    instance: dg.DagsterInstance,
    config: PartitionedPeriodConfig,
    partitions: list[DatePartition],
    context: dg.AssetExecutionContext,
) -> list[PartitionResult]:
    """Execute partitions one at a time, stopping on first failure.

    Child runs execute synchronously in-process via execute_run().
    For very large partition counts (100+), consider coarser granularity
    or splitting the date range across multiple orchestrator invocations.
    """
    job_def = resolved.job_def
    recon_job = _get_reconstructable_job(context, job_def.name)
    Results: list[PartitionResult] = []

    for i, partition in enumerate(partitions, 1):
        context.log.info(
            f"[{i}/{len(partitions)}] Running {_target_label(config)} "
            f"for {partition.start} → {partition.end}"
        )

        run_config = build_period_run_config(
            job_def,
            partition,
            delta_column=config.delta_column or None,
            advance_checkpoint=config.advance_checkpoint,
            flow_start_override=config.flow_start_override or None,
        )

        try:
            dagster_run = instance.create_run_for_job(
                job_def=job_def,
                run_config=run_config,
                asset_selection=resolved.asset_selection,
                tags={
                    "dagster/orchestrator": "period_orchestrator",
                    "dagster/partition_start": partition.start,
                    "dagster/partition_end": partition.end,
                    "dagster/parent_run_id": context.run_id,
                },
            )

            execution_result = execute_run(
                job=recon_job,
                dagster_run=dagster_run,
                instance=instance,
            )

            success = execution_result.success

            if success:
                context.log.info(
                    f"[{i}/{len(partitions)}] Partition {partition.start} → "
                    f"{partition.end} succeeded (run_id={dagster_run.run_id})"
                )
                Results.append(
                    PartitionResult(
                        partition=partition,
                        success=True,
                        run_id=dagster_run.run_id,
                    )
                )
            else:
                error_detail = _get_child_run_errors(instance, dagster_run.run_id)
                context.log.error(
                    f"[{i}/{len(partitions)}] Partition {partition.start} → "
                    f"{partition.end} FAILED (run_id={dagster_run.run_id}). "
                    f"Error: {error_detail or 'unknown'}. "
                    f"Stopping sequential execution."
                )
                Results.append(
                    PartitionResult(
                        partition=partition,
                        success=False,
                        run_id=dagster_run.run_id,
                        error=error_detail,
                    )
                )
                break

        except Exception as exc:
            Results.append(
                PartitionResult(
                    partition=partition,
                    success=False,
                    error=str(exc),
                )
            )
            context.log.error(
                f"[{i}/{len(partitions)}] Partition {partition.start} → "
                f"{partition.end} raised: {exc}. Stopping sequential execution."
            )
            break

    return Results
