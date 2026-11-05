# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The base ``tables/`` DDL must never run before the migrations on a database.

``tables/**.sql`` describes the schema at migration head and creates each table
under an ``IF NOT EXISTS (INFORMATION_SCHEMA.TABLES ...)`` guard keyed on the
*current* table name. On a database that still holds pre-rename objects — every
database provisioned before the 2026-07-08 naming overhaul — that guard reads
"missing" while the old-named table is still there holding the constraint
names, so the CREATE TABLE fires and collides:

    There is already an object named 'PK_dfm_state' in the database.

The rename migrations are what bring such a database to head, so any asset that
executes DDL out of ``tables/`` has to sit downstream of its ``*_migrations``
asset. Most of them already declare that dependency; this test keeps the rest
from drifting back out of order.
"""

import ast
import inspect

import dagster as dg
import pytest
from infrastructure.assets import dso_infrastructure, staging_infrastructure

pytestmark = pytest.mark.unit

MODULES = [
    (dso_infrastructure, "dso_migrations"),
    (staging_infrastructure, "staging_migrations"),
]


def _dependency_graph(module) -> dict[str, set[str]]:
    """Map every asset name in the module to its declared dependency names."""
    graph: dict[str, set[str]] = {}
    for attr in vars(module).values():
        if isinstance(attr, dg.AssetsDefinition):
            for key in attr.keys:
                graph[key.to_user_string()] = {
                    dep.to_user_string() for dep in attr.asset_deps.get(key, set())
                }
    return graph


def _upstream_of(graph: dict[str, set[str]], name: str) -> set[str]:
    """All assets reachable upstream of ``name`` (transitive closure)."""
    seen: set[str] = set()
    stack = list(graph.get(name, set()))
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(graph.get(current, set()))
    return seen


def _assets_executing_table_ddl(module) -> set[str]:
    """Names of asset functions whose body reads DDL out of ``tables/`` or ``sources/``."""
    source = inspect.getsource(module)
    tree = ast.parse(source)
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            folder in (ast.get_source_segment(source, node) or "")
            for folder in ('"tables/', '"sources/')
        )
    }


@pytest.mark.parametrize("module,migrations_asset", MODULES, ids=["dso", "staging"])
def test_table_ddl_assets_depend_on_migrations(module, migrations_asset):
    graph = _dependency_graph(module)
    candidates = _assets_executing_table_ddl(module)
    assert candidates, "no table-DDL assets found — the source scan is broken"

    offenders = sorted(
        name
        for name in candidates
        if name in graph and migrations_asset not in _upstream_of(graph, name)
    )

    assert offenders == [], (
        f"{offenders} execute tables/ DDL but do not depend on "
        f"{migrations_asset!r}, so they can race ahead of the rename "
        f"migrations and collide with pre-rename constraint names"
    )
