# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Reconcile one backfill window against IFDB, key by key.

Without this a green window only means its steps did not fail. It says nothing
about whether every row of the month reached the archive, and a systematic loss
would surface only after the whole history had run.

For each archive subject in ``_ARC`` the source is the exact query the window's
extract ran: ``build_oracle_where`` for the window and ``build_oracle_extract_query``
over the extract config — the same function the extract itself calls. The
subject's own DQA source is built for the flow-start bound and does not always
carry the extract's delta column.

Only the MISSING direction is computed (``missing_only``). The source is bounded
to the window and the archive is not, so every archive key outside the month
would count as extra.

The remediation capture is off. A window's gaps belong to the backfill itself,
which takes them month by month; queued for replay they would be written into
the archive outside any window, and the daily chain would meet the whole batch
at once.

Archives without an ``_ARC`` subject cannot be reconciled here. They are listed
in ``ARCHIVES_WITHOUT_RECONCILIATION`` and reported in the metadata, so a green
window never looks more complete than it is.
"""

from dataclasses import replace
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from dqa.checks.archive_reconciliation import archive_reconciliation
from dqa.config.arc_subjects import _ARC, ARC_SUBJECTS
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import build_oracle_extract_query
from DSI.oracle.ifdb_extract import build_oracle_where

# Archive assets in ifdb_archive that have no _ARC subject, so no source query
# to reconcile against. A test keeps this list in step with the definitions.
ARCHIVES_WITHOUT_RECONCILIATION: tuple[str, ...] = (
    "arc_care_discharge_report_ifdb",
    "arc_care_discharge_report_receiver_ifdb",
    "arc_epikrise_ifdb",
    "arc_epikrise_modtager_ifdb",
    "arc_epikrise_tekst_ifdb",
    "arc_healthissue_ifdb",
    "arc_rehabilitation_plan_ifdb",
    "arc_rehabilitation_plan_receiver_ifdb",
)


def build_window_source_query(
    extract_config: DfmExtractConfig, date_from: str, date_to: str
) -> str | None:
    """The query the window's extract ran, or None for a full-load source."""
    where = build_oracle_where(
        RunModeConfig(mode="period", date_from=date_from, date_to=date_to), None, extract_config
    )
    if where is None:
        return None
    return build_oracle_extract_query(extract_config, where)


def _subjects_with_extract() -> list[tuple[Any, DfmExtractConfig]]:
    """(DQA subject, extract config) pairs; ARC_SUBJECTS is built 1:1 from _ARC."""
    pairs = []
    for subject, spec in zip(ARC_SUBJECTS, _ARC, strict=True):
        if subject.name != spec[0]:
            raise RuntimeError(f"ARC_SUBJECTS and _ARC out of step at {subject.name!r}")
        pairs.append((subject, spec[4]))
    return pairs


def _short(error: Exception) -> str:
    message = " ".join(str(error).split())[:200]
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


def reconcile_window(context: Any, config: RunModeConfig) -> dg.MaterializeResult:
    """The reconciliation itself, without the Dagster wrapper, so tests can call it."""
    if config.mode != "period" or not config.date_from or not config.date_to:
        context.log.info(
            f"Reconciliation runs for a window only (mode=period with dates); "
            f"mode={config.mode!r} — skipping."
        )
        return dg.MaterializeResult(metadata={"skipped": dg.MetadataValue.text("not a window")})

    missing: dict[str, int] = {}
    measured: dict[str, int] = {}
    skipped: list[str] = []
    failed: dict[str, str] = {}

    for subject, extract_config in _subjects_with_extract():
        query = build_window_source_query(extract_config, config.date_from, config.date_to)
        spec = next((s for s in subject.checks if s.check_type == "archive_reconciliation"), None)
        if query is None or spec is None:
            skipped.append(subject.name)
            continue

        params = dict(spec.params)
        params["source_table"] = f"({query}) window_source"
        params["missing_only"] = True
        # The window bounds the query itself; the flow-start bound and the
        # remediation capture do not belong here.
        for key in (
            "source_filter",
            "flow_start_bound_column",
            "flow_start_state",
            "capture_replay",
        ):
            params.pop(key, None)

        try:
            outcome = archive_reconciliation(
                context, subject, replace(spec, params=params), context.resources
            )
        except Exception as error:  # noqa: BLE001 — one archive must not hide the rest
            # An archive that cannot be reconciled leaves the window unproven.
            failed[subject.name] = _short(error)
            continue
        measured[subject.name] = int(outcome.extra["source_count"])
        if outcome.extra["missing_in_arc"]:
            missing[subject.name] = int(outcome.extra["missing_in_arc"])

    context.log.info(
        f"Window reconciliation {config.date_from} -> {config.date_to}: "
        f"{len(measured)} archives reconciled, {len(skipped)} skipped, "
        f"{len(ARCHIVES_WITHOUT_RECONCILIATION)} without a subject, {len(missing)} with gaps."
    )
    for name, count in sorted(missing.items(), key=lambda item: -item[1]):
        context.log.error(f"  {name}: {count:,} IFDB keys missing from the archive")
    for name, reason in sorted(failed.items()):
        context.log.error(f"  {name}: could not be reconciled — {reason}")

    if missing or failed:
        parts = []
        if missing:
            parts.append(
                f"{sum(missing.values()):,} source keys did not reach the archive "
                f"({len(missing)} archives)"
            )
        if failed:
            parts.append(f"{len(failed)} archives could not be reconciled")
        raise dg.Failure(
            description=(
                f"Window {config.date_from} -> {config.date_to} is not proven complete: "
                + "; ".join(parts)
                + ". The marker must not move."
            ),
            metadata={
                **{name: dg.MetadataValue.int(count) for name, count in missing.items()},
                **{f"{name} (error)": dg.MetadataValue.text(r) for name, r in failed.items()},
            },
        )

    return dg.MaterializeResult(
        metadata={
            "window": dg.MetadataValue.text(f"{config.date_from} -> {config.date_to}"),
            "archives_reconciled": dg.MetadataValue.int(len(measured)),
            "archives_skipped": dg.MetadataValue.json(skipped),
            "archives_without_reconciliation": dg.MetadataValue.json(
                list(ARCHIVES_WITHOUT_RECONCILIATION)
            ),
            "source_keys_in_window": dg.MetadataValue.int(sum(measured.values())),
        }
    )


@dg.asset(
    name="ifdb_backfill_window_reconciliation",
    # Its own group, selected by no job: the backfill picks it by name, and a
    # group job would run it before the archive merges it checks.
    group_name="ifdb_backfill_reconciliation",
    compute_kind="oracle",
    required_resource_keys={"ifdb_oracle", "mssql_target_staging"},
    description=(
        "Reconcile a backfill window against IFDB key by key, for every archive with an "
        "_ARC subject. Fails, and keeps the marker in place, when any of them lacks a "
        "source key for the month. Archives without a subject are listed in the metadata."
    ),
)
def ifdb_backfill_window_reconciliation(
    context: dg.AssetExecutionContext, config: RunModeConfig
) -> dg.MaterializeResult:
    return reconcile_window(context, config)
