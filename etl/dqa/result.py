# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Normalize a ``CheckOutcome`` into a Dagster ``AssetCheckResult`` and (optionally)
persist it to ``dqa.Results`` for the cross-run / cross-environment overview.

Persistence is always on and best-effort: a missing/unwritable table just logs a
warning, never fails the check. The ``dqa.Results`` table is owned by the
``staging_infra_dqa`` infrastructure asset (DDL:
``infrastructure/ddl/tables/dqa/01_results.sql``); this module only INSERTs.
"""

from __future__ import annotations

import json
from typing import Any

import dagster as dg
from dqa.checks.helpers import dqa_environment, get_conn, sql_nliteral
from dqa.model import CheckOutcome, CheckSpecification, DqaSubject, Severity
from DSA.FHIR.utils.gdpr_masking import mask_cpr_numbers


def _json(value: Any) -> dg.MetadataValue:
    try:
        return dg.MetadataValue.json(value)
    except Exception:
        return dg.MetadataValue.text(str(value))


def _redact_example_value(value: Any) -> Any:
    """Mask CPR in an example value while preserving structure and internal keys.

    Only CPR-shaped 10-digit runs are masked; internal keys (PATIENT_ID/OBJECT_ID
    and other non-CPR identifiers) are kept so remediation can still locate rows.
    """
    if isinstance(value, str):
        return mask_cpr_numbers(value)
    if isinstance(value, list | tuple):
        return [_redact_example_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _redact_example_value(v) for k, v in value.items()}
    return value


def _redact_examples(examples: Any) -> list[Any]:
    """Redact CPR from a list of DQA example rows before surfacing/persisting them."""
    return [_redact_example_value(e) for e in examples]


def to_asset_check_result(
    context: dg.AssetCheckExecutionContext,
    subject: DqaSubject,
    spec: CheckSpecification,
    outcome: CheckOutcome,
    resources: Any,
) -> dg.AssetCheckResult:
    # INFO checks are observability-only: surface metrics but never fail.
    passed = outcome.passed or spec.severity is Severity.INFO

    metadata: dict[str, dg.MetadataValue] = {
        "subject": dg.MetadataValue.text(subject.table),
        "track": dg.MetadataValue.text(subject.track.value),
        "check_type": dg.MetadataValue.text(spec.check_type),
        "severity": dg.MetadataValue.text(spec.severity.value),
        "cadence": dg.MetadataValue.text(spec.cadence.value),
        "checked": dg.MetadataValue.int(outcome.checked),
        "violations": dg.MetadataValue.int(outcome.violations),
        "examples": _json(_redact_examples(outcome.examples[:20])),
    }
    if spec.name_suffix:
        metadata["name_suffix"] = dg.MetadataValue.text(spec.name_suffix)
    for key, value in outcome.extra.items():
        metadata[key] = _json(value)

    # One concise line per check: outcome + that we're now writing to dqa.Results
    # (the persist opens its own MSSQL connection, so it accounts for the small
    # gap between the check's last query and the yielded result).
    suffix = f" [{spec.name_suffix}]" if spec.name_suffix else ""
    checked = f", {outcome.checked} checked" if outcome.checked >= 0 else ""
    context.log.info(
        f"DQA {subject.name}/{spec.check_type}{suffix}: "
        f"{'PASS' if passed else 'FAIL'} ({outcome.violations} violations{checked}) "
        f"-> persisting to dqa.Results"
    )

    try:
        _persist(context, subject, spec, outcome, passed, resources)
    except Exception as exc:  # never let persistence fail the check
        context.log.warning(f"DQA result persistence failed (non-fatal): {exc}")

    return dg.AssetCheckResult(
        passed=passed,
        severity=spec.severity.dagster_severity,
        metadata=metadata,
    )


def _persist(
    context: dg.AssetCheckExecutionContext,
    subject: DqaSubject,
    spec: CheckSpecification,
    outcome: CheckOutcome,
    passed: bool,
    resources: Any,
) -> None:
    environment = dqa_environment()
    run_id = getattr(context, "run_id", None)
    examples_json = json.dumps(_redact_examples(outcome.examples[:20]), default=str)
    detail_json = json.dumps(outcome.extra, default=str)

    # DSO subjects run on the DSO server but persist to the unified dqa.Results on
    # DSA -> results_resource_key overrides where the row is written.
    results_key = subject.results_resource_key or subject.resource_key
    with get_conn(resources, results_key) as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO [dqa].[Results]
                (run_id, environment, track, subject, check_type, name_suffix,
                 severity, cadence, passed, checked_count, violation_count,
                 examples_json, detail_json)
            VALUES ({sql_nliteral(run_id) if run_id else "NULL"},
                    {sql_nliteral(environment)},
                    {sql_nliteral(subject.track.value)},
                    {sql_nliteral(subject.table)},
                    {sql_nliteral(spec.check_type)},
                    {sql_nliteral(spec.name_suffix) if spec.name_suffix else "NULL"},
                    {sql_nliteral(spec.severity.value)},
                    {sql_nliteral(spec.cadence.value)},
                    {1 if passed else 0},
                    {int(outcome.checked)},
                    {int(outcome.violations)},
                    {sql_nliteral(examples_json)},
                    {sql_nliteral(detail_json)})
            """
        )
        conn.commit()
