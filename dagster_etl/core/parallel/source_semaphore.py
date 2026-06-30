# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Host-local cross-process counting semaphore for source DB connections.

Mirrors the fcntl advisory-lock pattern in core/cache/coordinator.py: a pool of
``capacity`` zero-byte lock files; acquiring = taking a non-blocking exclusive
flock on any free slot. Works across UNRELATED processes on the same node
(separate Dagster step subprocesses + their spawned workers) and is crash-safe —
fcntl locks release automatically on process exit.
"""

from __future__ import annotations

import errno
import fcntl
import os
import time
from contextlib import contextmanager

_DEFAULT_BASE_DIR = os.environ.get("EXT_SOURCE_SEM_DIR", "/tmp/ext_source_sem")


class SourceConnectionSemaphore:
    def __init__(self, source_key: str, capacity: int, base_dir: str = _DEFAULT_BASE_DIR):
        self.source_key = source_key
        self.capacity = max(1, int(capacity))
        self._dir = os.path.join(base_dir, source_key)
        os.makedirs(self._dir, exist_ok=True)
        self._slot_paths = [os.path.join(self._dir, f"slot_{i}.lock") for i in range(self.capacity)]

    @contextmanager
    def slot(self, timeout: float = 600.0, poll: float = 0.25):
        """Acquire one slot (blocking up to ``timeout``); release on exit."""
        deadline = time.monotonic() + timeout
        fd = None
        try:
            while True:
                for path in self._slot_paths:
                    candidate = open(path, "a")  # noqa: SIM115 — closed in finally
                    try:
                        fcntl.flock(candidate.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fd = candidate
                        break
                    except OSError as exc:
                        candidate.close()
                        if exc.errno not in (errno.EACCES, errno.EAGAIN):
                            raise
                if fd is not None:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Could not acquire a {self.source_key} source slot "
                        f"(capacity={self.capacity}) within {timeout}s"
                    )
                time.sleep(poll)
            yield
        finally:
            if fd is not None:
                try:
                    fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
                finally:
                    fd.close()
