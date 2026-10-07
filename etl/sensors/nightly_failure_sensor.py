# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Nightly failure reporter: failed runs become JSON reports on a dated branch.

On any run FAILURE in this code location, each failed step is fingerprinted
(step key + root-cause exception class + normalized code frames) and upserted
as ``.nightly/reports/<fingerprint>.json`` on the ``nightly/<report-date>-<env>``
branch, created from ``NIGHTLY_REPORT_BASE_BRANCH`` (default ``development``)
when missing. Pushes go through the ADO Git Pushes REST API - no local clone.
The 07:00 triage agent checks out that branch and uses the reports as
context for fixes.

The report date rolls at 07:00 (the agent's schedule): a failure at 23:00 and
one at 05:00 the next morning land on the SAME branch - the one the agent
reads at its next run - so the whole overnight window stays together.

Dagster wraps op exceptions, so fingerprint and report are keyed on the
innermost cause; the report carries the full outer-to-root chain per
occurrence. Repeat failures append occurrences to the existing report file
instead of creating new ones.

Only automation-lineage runs are reported (``dagster/schedule_name`` or
``dagster/triggered_by`` tag, both inherited by re-executions) - ad-hoc
developer runs would otherwise spam reports. Feature-branch deployments never
report (``resolve_deployment_env`` returns None, and the sensor's default
status is STOPPED there anyway); ``NIGHTLY_REPORT_ENV`` overrides for testing.
Deployments without a PAT configured skip with a visible reason, which keeps
qa/prod inert until their secrets are seeded. Since the GitLab migration there is
no ADO backend to seed at all: the transport needs porting to the GitLab API
before this sensor can report again.
"""

import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
from schedules.schedule_default_status import get_sensor_default_status
from utils.azure_devops.reports_branch import (
    ReportsBranchClient,
    failure_fingerprint,
    truncate_trace,
)

_TZ = ZoneInfo("Europe/Copenhagen")
_MAX_REPORTS_PER_RUN = 20
_MAX_OCCURRENCES = 10
_AGENT_DEADLINE_HOUR = 7
_REPORTS_DIR = "/.nightly/reports"
_AUTOMATION_TAGS = ("dagster/schedule_name", "dagster/triggered_by")

_ENV_OVERRIDE = "NIGHTLY_REPORT_ENV"
_KNOWN_ENVS = ("dev", "qa", "prod")


def resolve_deployment_env() -> str | None:
    """Map the deployment context to dev/qa/prod; None for feature deployments."""
    override = os.getenv(_ENV_OVERRIDE, "").strip().lower()
    if override in _KNOWN_ENVS:
        return override
    location = os.getenv("DAGSTER_CODE_LOCATION_NAME", "").strip().lower()
    postfix = os.getenv("DAGSTER_BRANCH_POSTFIX", "").strip().lower()
    if location == "development":
        return "dev"
    if location in ("production", "prod") or postfix.startswith("main-"):
        return "prod"
    if postfix.startswith("release-"):
        return "qa"
    return None


def is_automation_run(run_tags: dict[str, str] | None) -> bool:
    """Whether the run was started by a schedule or sensor (directly or via re-execution)."""
    tags = run_tags or {}
    return any(tag in tags for tag in _AUTOMATION_TAGS)


def report_date(now: datetime | None = None) -> str:
    """Branch date for a failure: rolls at 07:00 so the overnight window stays together."""
    now = now or datetime.now(tz=_TZ)
    return (now + timedelta(hours=24 - _AGENT_DEADLINE_HOUR)).strftime("%Y-%m-%d")


def _innermost_error(error):
    """Follow the cause chain to the root error (Dagster wraps op exceptions)."""
    while getattr(error, "cause", None) is not None:
        error = error.cause
    return error


def _error_chain_text(error) -> str:
    """Full outer-to-root trace text, causes joined like Python's traceback output."""
    parts = []
    while error is not None:
        message = (getattr(error, "message", "") or "").strip()
        stack = "".join(getattr(error, "stack", None) or [])
        parts.append(f"{message}\n{stack}" if stack else message)
        error = getattr(error, "cause", None)
        if error is not None:
            parts.append("\nThe above exception was caused by:\n")
    return "".join(parts)


def failure_from_error(step_key: str, error) -> dict:
    """Failure dict keyed on the root cause, carrying the full chain for reporting."""
    root = _innermost_error(error)
    return {
        "step_key": step_key,
        "error_class": getattr(root, "cls_name", None) or "UnknownError",
        "message": (getattr(root, "message", "") or "").strip(),
        "stack": list(getattr(root, "stack", None) or []),
        "chain": _error_chain_text(error),
    }


def collect_step_failures(context: dg.RunStatusSensorContext) -> list[dict]:
    """Failed steps of the run, each keyed on its root-cause error."""
    run_id = context.dagster_run.run_id
    entries = context.instance.all_logs(run_id, of_type=dg.DagsterEventType.STEP_FAILURE)
    if not entries:
        entries = context.instance.all_logs(run_id, of_type=dg.DagsterEventType.RUN_FAILURE)
    failures = []
    for entry in entries:
        event = entry.dagster_event
        if event is None:
            continue
        error = getattr(event.event_specific_data, "error", None)
        failures.append(failure_from_error(event.step_key or context.dagster_run.job_name, error))
    return failures


def _run_url(run_id: str) -> str | None:
    base = os.getenv("DAGSTER_WEBSERVER_URL", "").rstrip("/")
    return f"{base}/runs/{run_id}" if base else None


def build_report_content(
    existing: str | None, fingerprint: str, env: str, failure: dict, occurrence: dict
) -> str:
    """New or updated JSON report for a fingerprint; occurrences capped, newest kept."""
    if existing:
        report = json.loads(existing)
    else:
        report = {
            "fingerprint": fingerprint,
            "env": env,
            "step_key": failure["step_key"],
            "error_class": failure["error_class"],
            "message": failure["message"],
            "first_seen": occurrence["timestamp"],
            "occurrences": [],
        }
    report["occurrences"] = (report.get("occurrences", []) + [occurrence])[-_MAX_OCCURRENCES:]
    return json.dumps(report, indent=2, ensure_ascii=False)


def build_nightly_failure_sensor() -> dg.SensorDefinition:
    """Build the run-failure → nightly reports-branch sensor."""

    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.FAILURE,
        name="nightly_failure_report_sensor",
        default_status=get_sensor_default_status(),
        description=(
            "On run FAILURE (schedule/sensor-triggered runs only): commit one "
            "JSON report per failed step to the nightly/<date>-<env> branch "
            "for the 07:00 triage agent."
        ),
    )
    def nightly_failure_report_sensor(
        context: dg.RunStatusSensorContext,
    ) -> dg.SkipReason:
        run = context.dagster_run
        env = resolve_deployment_env()
        if env is None:
            return dg.SkipReason("Feature deployment — nightly failure reporting disabled.")
        if not is_automation_run(run.tags):
            return dg.SkipReason(
                f"Run '{run.run_id}' is ad-hoc (no schedule/sensor lineage tag); not reported."
            )
        client = ReportsBranchClient()
        if not client.configured:
            return dg.SkipReason(
                "Nightly triage is dormant: it reports via the Azure DevOps API, which "
                "was decommissioned in the GitLab migration. Seeding ADO_ORG_URL / "
                "ADO_PROJECT / ADO_PAT will not help — the transport needs porting to "
                f"GitLab. Failure of run '{run.run_id}' not reported."
            )

        failures = collect_step_failures(context)
        if not failures:
            return dg.SkipReason(f"Run '{run.run_id}' failed but no failure events were found.")

        now = datetime.now(tz=_TZ)
        branch = f"nightly/{report_date(now)}-{env}"
        base_branch = os.getenv("NIGHTLY_REPORT_BASE_BRANCH", "development")
        occurrence_base = {
            "run_id": run.run_id,
            "run_url": _run_url(run.run_id),
            "job": run.job_name,
            "partition": (run.tags or {}).get("dagster/partition") or None,
            "timestamp": now.isoformat(),
        }

        reported = 0
        errors = 0
        for failure in failures[:_MAX_REPORTS_PER_RUN]:
            fingerprint = failure_fingerprint(
                failure["step_key"], failure["error_class"], failure["stack"]
            )
            trace = failure["chain"] or f"{failure['error_class']}: {failure['message']}"
            occurrence = {**occurrence_base, "chain": truncate_trace(trace)}

            def _update(existing, _fp=fingerprint, _failure=failure, _occ=occurrence):
                return build_report_content(existing, _fp, env, _failure, _occ)

            try:
                client.upsert_json_file(
                    branch=branch,
                    base_branch=base_branch,
                    path=f"{_REPORTS_DIR}/{fingerprint}.json",
                    update_fn=_update,
                    comment=(
                        f"nightly: {failure['step_key']} "
                        f"({failure['error_class']}) [{fingerprint}]"
                    ),
                )
                reported += 1
                context.log.info(
                    f"Reported '{failure['step_key']}' (fingerprint {fingerprint}) "
                    f"to branch {branch}."
                )
            except Exception:
                errors += 1
                context.log.exception(
                    f"Failed to report step '{failure['step_key']}' "
                    f"(fingerprint {fingerprint}) to branch {branch}."
                )

        skipped = len(failures) - min(len(failures), _MAX_REPORTS_PER_RUN)
        summary = (
            f"Reported {reported}/{len(failures)} failure(s) of run "
            f"'{run.run_id}' to branch {branch}."
        )
        if skipped:
            summary += f" {skipped} further failure(s) capped at {_MAX_REPORTS_PER_RUN}."
        if errors:
            summary += f" {errors} report error(s) — see sensor logs."
        return dg.SkipReason(summary)

    return nightly_failure_report_sensor
