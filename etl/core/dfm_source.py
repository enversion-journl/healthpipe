# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Source values for DfmManager.start_dfm_run: a resource, a URL or a CIFS share."""

import os
from urllib.parse import urlparse

import dagster as dg

MOUNTINFO_PATH = "/proc/self/mountinfo"
_CIFS_FSTYPES = {"cifs", "smb3"}


def http_source(url: str) -> dict:
    """Source kwargs for a download: the URL host and path."""
    parsed = urlparse(url)
    return {"source_user": None, "source_host": parsed.hostname, "source_table": parsed.path}


def resource_source(resource, table: str | None) -> dict:
    """Source kwargs for a database resource with `user` and `host` attributes."""
    return {"source_user": resource.user, "source_host": resource.host, "source_table": table}


def share_connection(path: str | None, mountinfo_path: str | None = None) -> dict:
    """Connection kwargs for a file delivery; host and user come from the CIFS mount that holds it."""
    host, user = _cifs_server_and_user(path, mountinfo_path or MOUNTINFO_PATH)
    return {"source_user": user, "source_host": host}


def share_source(path: str | None, table: str, mountinfo_path: str | None = None) -> dict:
    """Source kwargs for a file delivery: the share connection plus the dataset name."""
    return {**share_connection(path, mountinfo_path), "source_table": table}


def _cifs_server_and_user(path: str | None, mountinfo_path: str) -> tuple[str | None, str | None]:
    if not path:
        return None, None
    try:
        real = os.path.realpath(path)
        best: tuple[str, str, str, str] | None = None
        with open(mountinfo_path, encoding="utf-8") as handle:
            for line in handle:
                left, sep, right = line.partition(" - ")
                if not sep:
                    continue
                fields = left.split()
                after = right.split()
                if len(fields) < 6 or len(after) < 3:
                    continue
                mount_point = fields[4].replace("\\040", " ")
                is_prefix = real == mount_point or real.startswith(mount_point.rstrip("/") + "/")
                if is_prefix and (best is None or len(mount_point) >= len(best[0])):
                    options = ",".join([fields[5], after[2]])
                    best = (mount_point, after[0], after[1], options)
        if best is None or best[1] not in _CIFS_FSTYPES:
            return None, None
        server = best[2].lstrip("/").split("/", 1)[0] or None
        user = None
        for option in best[3].split(","):
            key, _, value = option.partition("=")
            if key in ("username", "user") and value:
                user = value
                break
        return server, user
    except Exception as exc:
        dg.get_dagster_logger().warning("Could not read mount information for %s: %r", path, exc)
        return None, None
