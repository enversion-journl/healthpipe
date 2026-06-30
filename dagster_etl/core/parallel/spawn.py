# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Spawn-worker import-path helpers."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterable
from pathlib import Path


def resolve_spawn_import_paths_for_file(
    file_path: str,
    *,
    package_root_name: str = "dagster_etl",
    include_project_root: bool = True,
) -> tuple[str, ...]:
    """Resolve absolute import roots that spawn workers need for a module file."""
    resolved = Path(file_path).resolve()
    package_root = next(
        (parent for parent in resolved.parents if parent.name == package_root_name),
        None,
    )
    if package_root is None:
        raise ValueError(f"Could not find package root {package_root_name!r} above {file_path!r}")

    paths = [str(package_root)]
    if include_project_root:
        paths.append(str(package_root.parent))
    return tuple(paths)


def ensure_spawn_import_paths(paths: Iterable[str]) -> None:
    """Put absolute paths on both ``sys.path`` and ``PYTHONPATH`` without duplicates."""
    normalized = [str(Path(path).resolve()) for path in paths if path]
    for path in reversed(normalized):
        if path not in sys.path:
            sys.path.insert(0, path)

    existing = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    merged = list(existing)
    for path in reversed(normalized):
        if path not in merged:
            merged.insert(0, path)
    os.environ["PYTHONPATH"] = os.pathsep.join(merged)


def configure_spawn_import_paths_for_file(
    file_path: str,
    *,
    package_root_name: str = "dagster_etl",
    include_project_root: bool = True,
) -> Callable[[], None]:
    """Configure spawn import paths now and return a hook to re-apply before pools."""
    paths = resolve_spawn_import_paths_for_file(
        file_path,
        package_root_name=package_root_name,
        include_project_root=include_project_root,
    )

    def _ensure() -> None:
        ensure_spawn_import_paths(paths)

    _ensure()
    return _ensure
