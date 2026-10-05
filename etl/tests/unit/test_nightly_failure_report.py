# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Nightly failure reporter: fingerprinting, env routing, report building, ADO push contracts."""

import json

import pytest
import responses
from sensors.nightly_failure_sensor import (
    build_report_content,
    failure_from_error,
    is_automation_run,
    report_date,
    resolve_deployment_env,
)
from utils.azure_devops.reports_branch import (
    ReportsBranchClient,
    failure_fingerprint,
    normalize_frames,
    truncate_trace,
)

_STACK_V1 = [
    '  File "/opt/dagster/app/etl/DSA/FHIR/patient.py", line 120, in transform\n',
    '  File "/opt/dagster/app/etl/core/resources.py", line 55, in get_connection\n',
]
_STACK_V2 = [
    '  File "/opt/dagster/app/etl/DSA/FHIR/patient.py", line 987, in transform\n',
    '  File "/opt/dagster/app/etl/core/resources.py", line 61, in get_connection\n',
]


@pytest.mark.unit
class TestFingerprint:
    def test_stable_across_line_number_changes(self):
        fp1 = failure_fingerprint("stg_patient", "OperationalError", _STACK_V1)
        fp2 = failure_fingerprint("stg_patient", "OperationalError", _STACK_V2)
        assert fp1 == fp2

    def test_differs_on_exception_class(self):
        fp1 = failure_fingerprint("stg_patient", "OperationalError", _STACK_V1)
        fp2 = failure_fingerprint("stg_patient", "IntegrityError", _STACK_V1)
        assert fp1 != fp2

    def test_differs_on_step_key(self):
        fp1 = failure_fingerprint("stg_patient", "OperationalError", _STACK_V1)
        fp2 = failure_fingerprint("stg_encounter", "OperationalError", _STACK_V1)
        assert fp1 != fp2

    def test_stable_across_container_path_prefix(self):
        relocated = [f.replace("/opt/dagster/app/", "/workspace/checkout/") for f in _STACK_V1]
        fp1 = failure_fingerprint("stg_patient", "OperationalError", _STACK_V1)
        fp2 = failure_fingerprint("stg_patient", "OperationalError", relocated)
        assert fp1 == fp2


@pytest.mark.unit
class TestNormalizeFrames:
    def test_repo_relative_and_line_numbers_stripped(self):
        frames = normalize_frames(_STACK_V1)
        assert frames == [
            "etl/DSA/FHIR/patient.py:transform",
            "etl/core/resources.py:get_connection",
        ]

    def test_keeps_innermost_frames_when_over_limit(self):
        stack = [f'  File "/app/etl/m{i}.py", line {i}, in f{i}\n' for i in range(10)]
        frames = normalize_frames(stack, max_frames=3)
        assert frames == [
            "etl/m7.py:f7",
            "etl/m8.py:f8",
            "etl/m9.py:f9",
        ]

    def test_non_frame_lines_ignored(self):
        stack = ["Traceback (most recent call last):\n", *_STACK_V1, "OperationalError: boom\n"]
        assert normalize_frames(stack) == normalize_frames(_STACK_V1)


@pytest.mark.unit
class TestTruncateTrace:
    def test_keeps_tail_within_limit(self):
        trace = "HEAD-" + "x" * 100 + "-TAIL"
        result = truncate_trace(trace, limit=20)
        assert result.endswith("-TAIL")
        assert "HEAD" not in result
        assert result.startswith("… (truncated)")

    def test_short_trace_unchanged(self):
        assert truncate_trace("short", limit=20) == "short"


@pytest.mark.unit
class TestResolveDeploymentEnv:
    def test_development_location_is_dev(self, monkeypatch):
        monkeypatch.delenv("NIGHTLY_REPORT_ENV", raising=False)
        monkeypatch.setenv("DAGSTER_CODE_LOCATION_NAME", "development")
        monkeypatch.delenv("DAGSTER_BRANCH_POSTFIX", raising=False)
        assert resolve_deployment_env() == "dev"

    def test_release_postfix_is_qa(self, monkeypatch):
        monkeypatch.delenv("NIGHTLY_REPORT_ENV", raising=False)
        monkeypatch.delenv("DAGSTER_CODE_LOCATION_NAME", raising=False)
        monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "release-1-2-3")
        assert resolve_deployment_env() == "qa"

    def test_main_postfix_is_prod(self, monkeypatch):
        monkeypatch.delenv("NIGHTLY_REPORT_ENV", raising=False)
        monkeypatch.delenv("DAGSTER_CODE_LOCATION_NAME", raising=False)
        monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "main-1-2-3")
        assert resolve_deployment_env() == "prod"

    def test_feature_postfix_is_none(self, monkeypatch):
        monkeypatch.delenv("NIGHTLY_REPORT_ENV", raising=False)
        monkeypatch.delenv("DAGSTER_CODE_LOCATION_NAME", raising=False)
        monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "feature-jira-123-fix")
        assert resolve_deployment_env() is None

    def test_override_wins(self, monkeypatch):
        monkeypatch.setenv("NIGHTLY_REPORT_ENV", "qa")
        monkeypatch.setenv("DAGSTER_CODE_LOCATION_NAME", "development")
        assert resolve_deployment_env() == "qa"


@pytest.mark.unit
class TestIsAutomationRun:
    def test_schedule_triggered_reported(self):
        assert is_automation_run({"dagster/schedule_name": "ifdb_dfm_base_schedule"})

    def test_sensor_triggered_reported(self):
        assert is_automation_run({"dagster/triggered_by": "dfm_base_job_success_sensor"})

    def test_adhoc_run_not_reported(self):
        assert not is_automation_run({"user": "daniel"})
        assert not is_automation_run(None)


@pytest.mark.unit
class TestReportDate:
    def _dt(self, hour, minute=0):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        return datetime(2026, 7, 10, hour, minute, tzinfo=ZoneInfo("Europe/Copenhagen"))

    def test_late_evening_files_for_next_morning(self):
        assert report_date(self._dt(23)) == "2026-07-11"

    def test_early_morning_files_for_same_morning(self):
        assert report_date(self._dt(5)) == "2026-07-10"
        assert report_date(self._dt(6, 59)) == "2026-07-10"

    def test_after_agent_deadline_rolls_to_next_morning(self):
        assert report_date(self._dt(7, 30)) == "2026-07-11"


class _Err:
    def __init__(self, cls_name, message, stack, cause=None):
        self.cls_name = cls_name
        self.message = message
        self.stack = stack
        self.cause = cause


_WRAPPER_MSG = (
    "dagster._core.errors.DagsterExecutionStepExecutionError: " "Error occurred while executing op"
)
_WRAPPER_FRAME = '  File "/usr/local/lib/python3.12/site-packages/dagster/plan.py", line 24, in x\n'


def _wrapped_chain(root_cls="ProgrammingError", root_msg="Invalid column name 'PropertyCode'"):
    root = _Err(
        root_cls,
        f"mssql_python.exceptions.{root_cls}: {root_msg}",
        ['  File "/opt/dagster/app/etl/DSO/load/sync_utils.py", line 252, in _stage\n'],
    )
    return _Err("DagsterExecutionStepExecutionError", _WRAPPER_MSG, [_WRAPPER_FRAME], cause=root)


@pytest.mark.unit
class TestCauseChain:
    def test_root_cause_drives_class_and_stack(self):
        failure = failure_from_error("dso_sync_ean", _wrapped_chain())
        assert failure["error_class"] == "ProgrammingError"
        assert "PropertyCode" in failure["message"]
        assert "etl/DSO/load/sync_utils.py" in failure["stack"][0]

    def test_different_root_causes_get_different_fingerprints(self):
        f1 = failure_from_error("dso_sync_ean", _wrapped_chain("ProgrammingError"))
        f2 = failure_from_error("dso_sync_ean", _wrapped_chain("IntegrityError"))
        fp1 = failure_fingerprint(f1["step_key"], f1["error_class"], f1["stack"])
        fp2 = failure_fingerprint(f2["step_key"], f2["error_class"], f2["stack"])
        assert fp1 != fp2

    def test_chain_text_includes_outer_and_root(self):
        failure = failure_from_error("dso_sync_ean", _wrapped_chain())
        assert "Error occurred while executing op" in failure["chain"]
        assert "PropertyCode" in failure["chain"]
        assert "caused by" in failure["chain"]

    def test_error_without_cause_used_directly(self):
        failure = failure_from_error("step", _Err("ValueError", "ValueError: boom", []))
        assert failure["error_class"] == "ValueError"
        assert failure["chain"] == "ValueError: boom"

    def test_missing_error_yields_unknown(self):
        failure = failure_from_error("step", None)
        assert failure["error_class"] == "UnknownError"
        assert failure["chain"] == ""


def _occurrence(run_id="run-1", ts="2026-07-10T23:00:00+02:00"):
    return {
        "run_id": run_id,
        "run_url": f"https://dagster/runs/{run_id}",
        "job": "codesystem_sfi_sync_job",
        "partition": None,
        "timestamp": ts,
        "chain": "ProgrammingError: Invalid column name 'PropertyCode'",
    }


def _failure():
    return failure_from_error("dso_sync_ean", _wrapped_chain())


@pytest.mark.unit
class TestBuildReportContent:
    def test_new_report_skeleton(self):
        content = build_report_content(None, "abc123def456", "dev", _failure(), _occurrence())
        report = json.loads(content)
        assert report["fingerprint"] == "abc123def456"
        assert report["env"] == "dev"
        assert report["error_class"] == "ProgrammingError"
        assert report["first_seen"] == "2026-07-10T23:00:00+02:00"
        assert len(report["occurrences"]) == 1
        assert report["occurrences"][0]["run_id"] == "run-1"

    def test_existing_report_appends_occurrence(self):
        first = build_report_content(None, "abc123def456", "dev", _failure(), _occurrence("run-1"))
        second = build_report_content(
            first, "abc123def456", "dev", _failure(), _occurrence("run-2")
        )
        report = json.loads(second)
        assert [o["run_id"] for o in report["occurrences"]] == ["run-1", "run-2"]
        assert report["first_seen"] == "2026-07-10T23:00:00+02:00"

    def test_occurrences_capped_keeps_newest(self):
        content = build_report_content(
            None, "abc123def456", "dev", _failure(), _occurrence("run-0")
        )
        for i in range(1, 15):
            content = build_report_content(
                content, "abc123def456", "dev", _failure(), _occurrence(f"run-{i}")
            )
        report = json.loads(content)
        assert len(report["occurrences"]) == 10
        assert report["occurrences"][-1]["run_id"] == "run-14"
        assert report["occurrences"][0]["run_id"] == "run-5"


_BASE = "https://dev.azure.com/enversion/healthpipe/_apis/git/repositories/healthpipe"


@pytest.mark.unit
class TestReportsBranchClient:
    def _client(self):
        return ReportsBranchClient(
            org_url="https://dev.azure.com/enversion",
            project="healthpipe",
            pat="secret",
        )

    def test_not_configured_without_pat(self, monkeypatch):
        # The constructor falls back to ADO_PAT / ADO_WORKITEM_PAT when the
        # passed pat is falsy, so this assertion only holds with the ambient
        # environment cleared. Both are protected CI variables: absent on
        # feature branches, present on development — which made this test pass
        # locally and on MRs while failing every pipeline on development.
        monkeypatch.delenv("ADO_PAT", raising=False)
        monkeypatch.delenv("ADO_WORKITEM_PAT", raising=False)

        client = ReportsBranchClient(org_url="https://x", project="P", pat="")
        assert not client.configured

    def test_repository_defaults_to_project(self):
        assert self._client().repository == "healthpipe"

    @responses.activate
    def test_get_branch_head_matches_exact_ref(self):
        responses.add(
            responses.GET,
            f"{_BASE}/refs",
            json={
                "value": [
                    {"name": "refs/heads/nightly/2026-07-11-dev-old", "objectId": "bad"},
                    {"name": "refs/heads/nightly/2026-07-11-dev", "objectId": "aaa111"},
                ]
            },
        )
        assert self._client().get_branch_head("nightly/2026-07-11-dev") == "aaa111"

    @responses.activate
    def test_get_branch_head_none_when_missing(self):
        responses.add(responses.GET, f"{_BASE}/refs", json={"value": []})
        assert self._client().get_branch_head("nightly/2026-07-11-dev") is None

    @responses.activate
    def test_create_branch_from_base_head(self):
        responses.add(
            responses.GET,
            f"{_BASE}/refs",
            json={"value": [{"name": "refs/heads/development", "objectId": "base99"}]},
        )
        responses.add(responses.POST, f"{_BASE}/refs", json={"value": [{"success": True}]})
        head = self._client().create_branch("nightly/2026-07-11-dev", "development")
        assert head == "base99"
        body = json.loads(responses.calls[1].request.body)
        assert body[0]["name"] == "refs/heads/nightly/2026-07-11-dev"
        assert body[0]["oldObjectId"] == "0" * 40
        assert body[0]["newObjectId"] == "base99"

    @responses.activate
    def test_get_file_none_on_404(self):
        responses.add(responses.GET, f"{_BASE}/items", status=404, json={})
        assert self._client().get_file("nightly/2026-07-11-dev", "/.nightly/reports/x.json") is None

    @responses.activate
    def test_upsert_new_file_pushes_add(self):
        responses.add(
            responses.GET,
            f"{_BASE}/refs",
            json={"value": [{"name": "refs/heads/nightly/2026-07-11-dev", "objectId": "head1"}]},
        )
        responses.add(responses.GET, f"{_BASE}/items", status=404, json={})
        responses.add(
            responses.POST, f"{_BASE}/pushes", json={"refUpdates": [{"newObjectId": "head2"}]}
        )
        self._client().upsert_json_file(
            "nightly/2026-07-11-dev",
            "development",
            "/.nightly/reports/abc.json",
            lambda existing: '{"new": true}' if existing is None else "WRONG",
            "nightly: test",
        )
        push = json.loads(responses.calls[-1].request.body)
        assert push["refUpdates"][0]["oldObjectId"] == "head1"
        change = push["commits"][0]["changes"][0]
        assert change["changeType"] == "add"
        assert change["item"]["path"] == "/.nightly/reports/abc.json"
        assert change["newContent"]["content"] == '{"new": true}'

    @responses.activate
    def test_upsert_existing_file_pushes_edit_with_existing_content(self):
        responses.add(
            responses.GET,
            f"{_BASE}/refs",
            json={"value": [{"name": "refs/heads/nightly/2026-07-11-dev", "objectId": "head1"}]},
        )
        responses.add(responses.GET, f"{_BASE}/items", json={"content": '{"old": 1}'})
        responses.add(
            responses.POST, f"{_BASE}/pushes", json={"refUpdates": [{"newObjectId": "head2"}]}
        )
        seen = {}
        self._client().upsert_json_file(
            "nightly/2026-07-11-dev",
            "development",
            "/.nightly/reports/abc.json",
            lambda existing: seen.setdefault("existing", existing) or '{"merged": true}',
            "nightly: test",
        )
        assert seen["existing"] == '{"old": 1}'
        push = json.loads(responses.calls[-1].request.body)
        assert push["commits"][0]["changes"][0]["changeType"] == "edit"

    @responses.activate
    def test_upsert_retries_on_stale_head(self):
        for object_id in ("head1", "head2"):
            responses.add(
                responses.GET,
                f"{_BASE}/refs",
                json={
                    "value": [{"name": "refs/heads/nightly/2026-07-11-dev", "objectId": object_id}]
                },
            )
        responses.add(responses.GET, f"{_BASE}/items", status=404, json={})
        responses.add(responses.POST, f"{_BASE}/pushes", status=409, json={})
        responses.add(responses.GET, f"{_BASE}/items", status=404, json={})
        responses.add(
            responses.POST, f"{_BASE}/pushes", json={"refUpdates": [{"newObjectId": "head3"}]}
        )
        self._client().upsert_json_file(
            "nightly/2026-07-11-dev",
            "development",
            "/.nightly/reports/abc.json",
            lambda existing: "{}",
            "nightly: test",
        )
        pushes = [c for c in responses.calls if c.request.url.startswith(f"{_BASE}/pushes")]
        assert len(pushes) == 2
        assert json.loads(pushes[1].request.body)["refUpdates"][0]["oldObjectId"] == "head2"
