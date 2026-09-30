# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Azure DevOps reports-branch client and failure fingerprinting for nightly triage.

Failure reports are committed as JSON files to a dated ``nightly/<date>-<env>``
branch via the ADO Git Pushes REST API - no local clone or merge conflicts;
concurrent sensor ticks are handled with a compare-and-swap retry on the
branch head.
"""

import hashlib
import os
import re
from collections.abc import Callable

import requests

_API_VERSION = "7.1"
_TIMEOUT_SECONDS = 30
_ZERO_OID = "0" * 40
_PUSH_RETRIES = 3
_RETRYABLE_PUSH_STATUS = (400, 409, 412)

MAX_TRACE_CHARS = 3000

_FRAME_RE = re.compile(r'File "(?P<path>[^"]+)", line \d+, in (?P<func>\S+)')


def normalize_frames(stack: list[str], max_frames: int = 5) -> list[str]:
    """Repo-relative ``path:function`` for the innermost frames, line numbers stripped."""
    frames = []
    for line in stack:
        match = _FRAME_RE.search(line)
        if not match:
            continue
        path = match.group("path").replace("\\", "/")
        # Anchor on the directory boundary: bare "etl/" would also match inside
        # unrelated path segments that happen to end in "etl".
        anchor = path.rfind("/etl/")
        if anchor != -1:
            path = path[anchor + 1 :]
        frames.append(f"{path}:{match.group('func')}")
    return frames[-max_frames:]


def failure_fingerprint(step_key: str, error_class: str, stack: list[str]) -> str:
    """Stable 12-char id for a failure signature (step, exception class, code path)."""
    raw = "|".join([step_key, error_class, *normalize_frames(stack)])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def truncate_trace(trace: str, limit: int = MAX_TRACE_CHARS) -> str:
    """Keep the tail of the trace (innermost frames) within ``limit`` chars."""
    if len(trace) <= limit:
        return trace
    return "… (truncated)\n" + trace[-limit:]


class ReportsBranchClient:
    """Minimal ADO Git REST client: read/create branch refs and push JSON files."""

    def __init__(
        self,
        org_url: str | None = None,
        project: str | None = None,
        repository: str | None = None,
        pat: str | None = None,
    ):
        self.org_url = (org_url or os.getenv("ADO_ORG_URL", "")).rstrip("/")
        self.project = project or os.getenv("ADO_PROJECT", "")
        self.repository = repository or os.getenv("ADO_REPOSITORY", "") or self.project
        self._pat = pat or os.getenv("ADO_PAT", "") or os.getenv("ADO_WORKITEM_PAT", "")

    @property
    def configured(self) -> bool:
        return bool(self.org_url and self.project and self._pat)

    def _url(self, path: str) -> str:
        return f"{self.org_url}/{self.project}/_apis/git/repositories/{self.repository}/{path}"

    def _auth(self) -> tuple[str, str]:
        return ("", self._pat)

    def get_branch_head(self, branch: str) -> str | None:
        """Commit id at the tip of the branch, or None if the branch doesn't exist."""
        response = requests.get(
            self._url("refs"),
            params={"filter": f"heads/{branch}", "api-version": _API_VERSION},
            auth=self._auth(),
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        for ref in response.json().get("value", []):
            if ref.get("name") == f"refs/heads/{branch}":
                return ref["objectId"]
        return None

    def create_branch(self, branch: str, from_branch: str) -> str:
        """Create the branch at the tip of ``from_branch`` and return that commit id."""
        base = self.get_branch_head(from_branch)
        if base is None:
            raise ValueError(f"Base branch '{from_branch}' not found")
        response = requests.post(
            self._url("refs"),
            params={"api-version": _API_VERSION},
            json=[{"name": f"refs/heads/{branch}", "oldObjectId": _ZERO_OID, "newObjectId": base}],
            auth=self._auth(),
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        results = response.json().get("value", [])
        if results and not results[0].get("success", False):
            # Lost a creation race - another tick made the branch; use its head.
            existing = self.get_branch_head(branch)
            if existing is not None:
                return existing
            raise RuntimeError(f"Failed to create branch '{branch}'")
        return base

    def get_file(self, branch: str, path: str) -> str | None:
        """File content on the branch, or None if the file doesn't exist."""
        response = requests.get(
            self._url("items"),
            params={
                "path": path,
                "versionDescriptor.version": branch,
                "versionDescriptor.versionType": "branch",
                "includeContent": "true",
                "api-version": _API_VERSION,
            },
            headers={"Accept": "application/json"},
            auth=self._auth(),
            timeout=_TIMEOUT_SECONDS,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json().get("content")

    def push_file(
        self, branch: str, head: str, path: str, content: str, comment: str, new_file: bool
    ) -> str:
        """Push one commit adding/editing the file; returns the new branch head."""
        payload = {
            "refUpdates": [{"name": f"refs/heads/{branch}", "oldObjectId": head}],
            "commits": [
                {
                    "comment": comment,
                    "changes": [
                        {
                            "changeType": "add" if new_file else "edit",
                            "item": {"path": path},
                            "newContent": {"content": content, "contentType": "rawtext"},
                        }
                    ],
                }
            ],
        }
        response = requests.post(
            self._url("pushes"),
            params={"api-version": _API_VERSION},
            json=payload,
            auth=self._auth(),
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()["refUpdates"][0]["newObjectId"]

    def upsert_json_file(
        self,
        branch: str,
        base_branch: str,
        path: str,
        update_fn: Callable[[str | None], str],
        comment: str,
    ) -> None:
        """Read-modify-push with CAS retry when a concurrent push moves the head."""
        last_error: Exception | None = None
        for _ in range(_PUSH_RETRIES):
            head = self.get_branch_head(branch)
            if head is None:
                head = self.create_branch(branch, base_branch)
            existing = self.get_file(branch, path)
            content = update_fn(existing)
            try:
                self.push_file(branch, head, path, content, comment, new_file=existing is None)
                return
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status not in _RETRYABLE_PUSH_STATUS:
                    raise
                last_error = exc
        raise last_error if last_error else RuntimeError("upsert_json_file exhausted retries")
