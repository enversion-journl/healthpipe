# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class LineageTable:
    name: str  # from dso_lineage.TableName, e.g. "dim.Ean"
    columns: tuple[str, ...]
    source_file: str
    # Per-column annotation mapping (column name -> its YAML dict), used by the
    # relationship-annotation check to inspect Type / FK-Target / FK-None. Defaults
    # to empty so hand-built LineageTables (tests) stay valid.
    column_specs: dict[str, dict] = field(default_factory=dict)
    # Declared source systems (system -> declared tables from SourceSystems),
    # used by the Source-reference check. Tables are declared as "schema.tabel"
    # or as a bare schema-less name (registry-style sources like SOR_registry).
    source_systems: dict[str, tuple[str, ...]] = field(default_factory=dict)


def _parse_source_systems(node: dict, rel: str) -> tuple[dict[str, tuple[str, ...]], str | None]:
    """Parse the SourceSystems list into ``{system: (declared tables...)}``.

    Returns ``(source_systems, error)``; *error* is a malformed-message when the
    node does not have the ``[{system: [tables]}, ...]`` shape.
    """
    source_systems: dict[str, tuple[str, ...]] = {}
    declared = node.get("SourceSystems") or []
    if not isinstance(declared, list):
        return {}, f"{rel}: 'SourceSystems' must be a list of {{system: [tables]}}"
    for entry in declared:
        if not isinstance(entry, dict):
            return {}, f"{rel}: invalid SourceSystems entry {entry!r}"
        for system, system_tables in entry.items():
            if not isinstance(system_tables, list):
                return {}, f"{rel}: SourceSystems tables for '{system}' must be a list"
            source_systems[system] = source_systems.get(system, ()) + tuple(
                str(t) for t in system_tables
            )
    return source_systems, None


def read_lineage(
    lineage_dir: Path, schemas: tuple[str, ...]
) -> tuple[dict[str, LineageTable], list[str]]:
    """Read lineage YAML for *schemas* under *lineage_dir*.

    Returns ``(tables_by_name, malformed_messages)``. Files that cannot be
    interpreted as a lineage doc are reported in *malformed_messages* rather
    than raising, so the verifier can present them as findings.
    """
    tables: dict[str, LineageTable] = {}
    malformed: list[str] = []
    for schema in schemas:
        for path in sorted((lineage_dir / schema).glob("*.yml")):
            rel = str(path.relative_to(lineage_dir))
            try:
                doc = yaml.safe_load(path.read_text(encoding="utf-8"))
            except yaml.YAMLError as exc:
                malformed.append(f"{rel}: YAML parse error: {exc}")
                continue
            node = (doc or {}).get("dso_lineage")
            if not isinstance(node, dict):
                malformed.append(f"{rel}: missing 'dso_lineage' mapping")
                continue
            name = node.get("TableName")
            columns = node.get("Columns")
            if not name:
                malformed.append(f"{rel}: missing 'TableName'")
                continue
            if not isinstance(columns, dict):
                malformed.append(f"{rel}: missing or invalid 'Columns' mapping")
                continue
            if name in tables:
                malformed.append(
                    f"{rel}: duplicate TableName {name} " f"(also in {tables[name].source_file})"
                )
                continue
            source_systems, error = _parse_source_systems(node, rel)
            if error is not None:
                malformed.append(error)
                continue
            tables[name] = LineageTable(
                name=name,
                columns=tuple(columns.keys()),
                source_file=rel,
                column_specs={
                    col: (meta if isinstance(meta, dict) else {}) for col, meta in columns.items()
                },
                source_systems=source_systems,
            )
    return tables, malformed
