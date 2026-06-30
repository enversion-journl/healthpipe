# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""State-machine style coverage for identifier-cache lifecycle behavior."""

from __future__ import annotations

import ast
import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from core.cache import CacheCoordinator
from core.resources import FHIRDatabaseResource, SqliteIdentifierCache

_SYSTEM = "urn:test:cache-state-machine:patient"
_VALUE = "P1"


@dataclass
class _Harness:
    resource: FHIRDatabaseResource
    lookup: SqliteIdentifierCache
    db_path: str
    marker_path: Path
    cache_key: tuple[str, frozenset[str]]


def _build_resource() -> FHIRDatabaseResource:
    return FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )


def _build_harness(tmp_path: Path) -> _Harness:
    db_path = SqliteIdentifierCache.cache_path(
        "Patient",
        frozenset([_SYSTEM]),
        cache_dir=str(tmp_path),
    )
    SqliteIdentifierCache.build_from_iter(
        iter([(_SYSTEM, _VALUE, "patient-uuid-1")]),
        db_path,
    )
    lookup = SqliteIdentifierCache(db_path)
    cache_key = ("Patient", frozenset([_SYSTEM]))

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[cache_key] = lookup
    resource._identifier_cache = lookup
    resource._active_cache_key = cache_key

    return _Harness(
        resource=resource,
        lookup=lookup,
        db_path=db_path,
        marker_path=tmp_path / "patient.invalidated",
        cache_key=cache_key,
    )


def _rebind_loaded_cache(harness: _Harness) -> None:
    harness.lookup = SqliteIdentifierCache(harness.db_path)
    harness.resource._identifier_caches[harness.cache_key] = harness.lookup
    harness.resource._identifier_cache = harness.lookup
    harness.resource._active_cache_key = harness.cache_key


def _assert_lookup_reopen_still_works(harness: _Harness) -> None:
    # Force lazy reopen path every time we assert.
    harness.lookup.close()
    assert harness.lookup.get((_SYSTEM, _VALUE)) == "patient-uuid-1"


@pytest.mark.parametrize(
    "sequence",
    [
        ("suspend", "clear_selected", "suspend"),
        ("clear_all", "rebind", "suspend", "clear_all"),
        ("suspend", "rebind", "clear_selected", "rebind", "clear_all"),
    ],
)
def test_identifier_cache_state_machine_runtime_sequences(
    tmp_path: Path,
    sequence: tuple[str, ...],
) -> None:
    """Runtime clear operations must stay non-destructive across sequences."""
    harness = _build_harness(tmp_path)
    clear_ran = False

    for op in sequence:
        if op == "suspend":
            harness.resource.suspend_identifier_caches()
        elif op == "clear_selected":
            harness.resource.clear_identifier_caches_for_resources({"Patient"})
            clear_ran = True
            assert harness.resource._identifier_cache is None
            assert harness.resource._active_cache_key is None
        elif op == "clear_all":
            harness.resource.clear_identifier_caches()
            clear_ran = True
            assert harness.resource._identifier_cache is None
            assert harness.resource._active_cache_key is None
        elif op == "rebind":
            _rebind_loaded_cache(harness)
            assert harness.resource._active_cache_key == harness.cache_key
        else:  # pragma: no cover - exhaustive by construction
            raise AssertionError(f"Unknown operation: {op}")

        assert os.path.exists(harness.db_path)
        _assert_lookup_reopen_still_works(harness)
        if clear_ran:
            assert harness.marker_path.exists()
            assert os.path.getmtime(harness.marker_path) >= os.path.getmtime(harness.db_path)


def test_runtime_modules_forbid_destructive_delete_outside_allowlist() -> None:
    """Guardrail: runtime code must not reintroduce ad-hoc file deletes."""
    project_root = Path(__file__).resolve().parents[2]
    targets = {
        project_root
        / "core/fhir/identifier_lookup.py": {
            "purge_identifier_cache_files",
        },
        project_root
        / "core/cache/coordinator.py": {
            "delete",
        },
        project_root
        / "core/cache/sqlite_cache.py": {
            "delete",
            "delete_namespace",
        },
    }

    func_stack: list[str] = []
    violations: list[tuple[str, str, int]] = []

    def _is_cache_delete_call(node: ast.Call) -> bool:
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "delete":
            return False
        owner = node.func.value
        if isinstance(owner, ast.Name):
            return owner.id in {"self", "SharedIdentifierCache"}
        return (
            isinstance(owner, ast.Attribute)
            and isinstance(owner.value, ast.Name)
            and owner.value.id == "self"
            and owner.attr == "cache_coordinator"
        )

    class _Visitor(ast.NodeVisitor):
        def __init__(self, *, target: Path, allowed_functions: set[str]) -> None:
            self.target = target
            self.allowed_functions = allowed_functions

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            func_stack.append(node.name)
            self.generic_visit(node)
            func_stack.pop()

        def visit_Call(self, node: ast.Call) -> None:
            if _is_cache_delete_call(node):
                current_func = func_stack[-1] if func_stack else "<module>"
                if current_func not in self.allowed_functions:
                    violations.append(
                        (
                            str(self.target.relative_to(project_root)),
                            current_func,
                            node.lineno,
                        )
                    )
            self.generic_visit(node)

    for target, allowed_functions in targets.items():
        tree = ast.parse(target.read_text(encoding="utf-8"), filename=str(target))
        _Visitor(target=target, allowed_functions=allowed_functions).visit(tree)

    assert not violations, (
        "Destruktiv cache delete er kun tilladt i eksplicitte purge/delete helpers. "
        f"Fundet: {violations}"
    )
