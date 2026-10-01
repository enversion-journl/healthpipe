# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Factory: turn declarative ``DqaSubject`` configs into Dagster objects.

For each subject we emit one **anchor asset per cadence present** (a no-op asset
in the ``data_quality`` group, tagged with its cadence/track) and bind one
``@asset_check`` per configured check to it. The anchor's ``deps`` (if any) make
the checks run after the relevant ETL assets and show correct lineage.
"""

import dagster as dg
import dqa.checks  # noqa: F401  -- populates the registry
from dqa.check_config import DqaCheckConfig
from dqa.model import Cadence, CheckSpecification, DqaSubject
from dqa.registry import CHECK_TYPES
from dqa.result import to_asset_check_result

# NB: no ``from __future__ import annotations`` here -- it would stringify the
# ``config: DqaCheckConfig`` annotation on the check fn below and break Dagster's
# config-parameter detection.


def _group_name(subject: DqaSubject) -> str:
    """Separate asset group per track: ``dqa_fhir`` / ``dqa_dwh``."""
    return f"dqa_{subject.track.value}"


def _anchor_key(subject: DqaSubject, cadence: Cadence) -> str:
    return f"dqa_{subject.name}_{cadence.value}"


def _check_name(subject: DqaSubject, spec: CheckSpecification) -> str:
    parts = [subject.name, spec.check_type, spec.name_suffix]
    return "__".join(p for p in parts if p)


def _param_hint(spec: CheckSpecification, subject: DqaSubject) -> str:
    """A compact 'which columns' summary for the Dagster check description.

    So an operator can see what a check compares without reading the config.
    """
    p = spec.params
    t = spec.check_type
    table = subject.table
    # Row-level checks scope to active rows when the subject has a soft-delete col.
    active = (
        " (active rows only)"
        if subject.deleted_column and t in ("not_null", "referential_integrity")
        else ""
    )
    if t == "not_null":
        return f"{table} columns {p.get('columns')}{active}"
    if t == "sentinel_rate":
        cap = f", max_rate {p['max_rate']}" if p.get("max_rate") is not None else ", observability"
        return f"{table} {p.get('sentinel', -1)}-rate of {p.get('columns')}{cap}"
    if t == "uniqueness":
        # Mirrors the check's resolution: an explicit params value (incl. None ->
        # strict) wins over the subject's soft-delete column.
        dc = p["deleted_column"] if "deleted_column" in p else subject.deleted_column
        return f"{table} grain {p.get('grain')}{' (active rows only)' if dc else ''}"
    if t == "referential_integrity":
        return f"{table}.{p.get('column')} -> {p.get('parent_table')}.{p.get('parent_key')}{active}"
    if t == "source_reconciliation":
        return f"{p.get('source_table')}.{p.get('source_key')} vs {table}.{p.get('target_key')}"
    if t == "archive_reconciliation":
        return f"{p.get('source_table')} {p.get('key_columns')} vs {table}"
    if t == "patient_arc_integrity":
        return f"{table}.{p.get('target_key')} -> {p.get('source_table')}.{p.get('source_key')} (CPR-deduped)"
    if t == "fhir_server_reconciliation":
        return f"{table} vs FHIR server (Aidbox '{p.get('resource_table')}')"
    if t == "completeness":
        return f"{p.get('source_query') and 'source'} -> DSA -> DSO ({table})"
    return p.get("source_table") or table


def _build_anchor(subject: DqaSubject, cadence: Cadence) -> dg.AssetsDefinition:
    key = _anchor_key(subject, cadence)

    @dg.asset(
        name=key,
        group_name=_group_name(subject),
        deps=list(subject.depends_on),
        compute_kind="mssql",
        tags={"dqa_cadence": cadence.value, "dqa_track": subject.track.value},
        description=(
            f"DQA anchor for {subject.table} ({cadence.value} checks). "
            f"Materialize to run its data-quality checks."
        ),
    )
    def _anchor() -> None:
        return None

    return _anchor


def _build_check(subject: DqaSubject, cadence: Cadence, spec: CheckSpecification):
    anchor_key = _anchor_key(subject, cadence)
    name = _check_name(subject, spec)
    impl = spec.fn or CHECK_TYPES[spec.check_type]
    required = {subject.resource_key, *spec.requires}

    @dg.asset_check(
        name=name,
        asset=anchor_key,
        blocking=spec.severity.is_blocking,
        required_resource_keys=required,
        compute_kind="mssql",
        metadata={
            "dqa_cadence": cadence.value,
            "dqa_severity": spec.severity.value,
            "dqa_subject": subject.table,
        },
        description=f"{spec.check_type} on {subject.table}"
        + (f" [{spec.name_suffix}]" if spec.name_suffix else "")
        + f" -- {_param_hint(spec, subject)}",
    )
    # The inner fn takes `context` and the special `config` param (Dagster does NOT
    # treat `config` as an asset input). `subject`/`spec`/`impl` are captured from
    # this function's scope, which is fresh per _build_check call (no loop-capture
    # bug). `config.max_keys` overrides the remediation-capture guard for this run.
    def _check(context, config: DqaCheckConfig) -> dg.AssetCheckResult:
        # Lazy import: avoids pulling the remediation package in at factory import.
        from dqa.remediation.capture import reset_max_keys_override, set_max_keys_override

        token = set_max_keys_override(config.max_keys)
        try:
            outcome = impl(context, subject, spec, context.resources)
            return to_asset_check_result(context, subject, spec, outcome, context.resources)
        finally:
            reset_max_keys_override(token)

    return _check


def build_subject(
    subject: DqaSubject,
) -> tuple[list[dg.AssetsDefinition], list]:
    """Return (anchor assets, asset checks) for one subject."""
    assets: list[dg.AssetsDefinition] = []
    checks: list = []
    cadences = {s.cadence for s in subject.checks if s.enabled}
    for cadence in sorted(cadences, key=lambda c: c.value):
        assets.append(_build_anchor(subject, cadence))
        for spec in subject.checks:
            if spec.enabled and spec.cadence == cadence:
                checks.append(_build_check(subject, cadence, spec))
    return assets, checks


def build_all(subjects: list[DqaSubject]) -> tuple[list[dg.AssetsDefinition], list]:
    """Return (all anchor assets, all asset checks) across subjects."""
    assets: list[dg.AssetsDefinition] = []
    checks: list = []
    for subject in subjects:
        a, c = build_subject(subject)
        assets.extend(a)
        checks.extend(c)
    return assets, checks
