# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Reference resolution from the data-lineage YAMLs (single source of truth).

Both tracks resolve their FK edges from the lineage, with no separate registry:

- **FHIR** (``data_lineage/fhir/*.yml``): explicit ``FK-Target`` / ``FK-Key`` /
  ``FK-Required`` / ``FK-Ignore`` on reference columns.
- **DWH** (``data_lineage/fct/*.yml`` + ``dim/*.yml``): columns marked ``Type: FK``,
  resolved via an explicit ``FK-Target`` or the ``dw_ek_<Dim>[_<role>]`` naming
  convention. The parent key is always the dim/map PK ``dw_ek_<Dim>``, and ``-1``
  (the unknown-member surrogate) is ignored.

``referential_specs(table)`` turns the edges for a table into
``referential_integrity`` CheckSpecifications, so a subject config never hand-lists FKs.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from pathlib import Path

import yaml
from dqa.model import Cadence, CheckSpecification, Severity

_LINEAGE_DIR = Path(__file__).resolve().parents[2] / "data_lineage"
_FHIR_DIR = _LINEAGE_DIR / "fhir"
_DWH_DIRS = (_LINEAGE_DIR / "fct", _LINEAGE_DIR / "dim", _LINEAGE_DIR / "bridge")

# dw_ek_<Dim> or dw_ek_<Dim>_<role>; the dim name must not contain underscores.
_DW_EK_RE = re.compile(r"^dw_ek_([A-Za-z][A-Za-z0-9]*)(?:_(.+))?$")
_DWH_SENTINELS = ("-1",)  # unknown-member surrogate keys


@dataclass(frozen=True)
class FkEdge:
    child_table: str  # "fhir.Encounter" / "fct.Fakturalinje"
    child_column: str  # "service_provider_id" / "dw_ek_Kreditor"
    parent_table: str  # "fhir.Organization" / "dim.Kreditor"
    parent_key: str  # "organization_id" / "dw_ek_Kreditor"
    required: bool
    ignore_values: tuple[str, ...]
    track: str  # "fhir" | "dwh"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _iter_docs(directory: Path):
    for path in sorted(directory.glob("*.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        dl = doc.get("dso_lineage") or {}
        if dl.get("TableName"):
            yield dl


@functools.lru_cache(maxsize=1)
def load_fhir_edges() -> tuple[FkEdge, ...]:
    """Parse all explicit FK-Target edges declared in the FHIR lineage YAMLs."""
    edges: list[FkEdge] = []
    for dl in _iter_docs(_FHIR_DIR):
        table = dl["TableName"]
        for col, meta in (dl.get("Columns") or {}).items():
            if not isinstance(meta, dict) or "FK-Target" not in meta:
                continue
            edges.append(
                FkEdge(
                    child_table=table,
                    child_column=col,
                    parent_table=meta["FK-Target"],
                    parent_key=meta.get("FK-Key") or _fhir_default_key(meta["FK-Target"]),
                    required=bool(meta.get("FK-Required", False)),
                    ignore_values=tuple(meta.get("FK-Ignore", [""])),
                    track="fhir",
                )
            )
    return tuple(edges)


@functools.lru_cache(maxsize=1)
def load_dwh_edges() -> tuple[FkEdge, ...]:
    """Parse FK edges from the DWH lineage (fct + dim ``Type: FK`` columns)."""
    edges: list[FkEdge] = []
    for directory in _DWH_DIRS:
        for dl in _iter_docs(directory):
            table = dl["TableName"]
            for col, meta in (dl.get("Columns") or {}).items():
                if not isinstance(meta, dict) or meta.get("Type") != "FK":
                    continue
                resolved = _dwh_target(col, meta.get("FK-Target"))
                if resolved is None:
                    continue
                parent_table, parent_key = resolved
                edges.append(
                    FkEdge(
                        child_table=table,
                        child_column=col,
                        parent_table=parent_table,
                        parent_key=parent_key,
                        required=False,  # dw_ek_ are NOT NULL but use -1 for "unknown"
                        ignore_values=_DWH_SENTINELS,
                        track="dwh",
                    )
                )
    return tuple(edges)


@functools.lru_cache(maxsize=1)
def _all_edges() -> tuple[FkEdge, ...]:
    return load_fhir_edges() + load_dwh_edges()


def edges_for(table: str) -> list[FkEdge]:
    return [e for e in _all_edges() if e.child_table == table]


def dw_ek_columns(table: str) -> list[str]:
    """All ``dw_ek_`` columns (PK + FKs) declared for a DWH table -- used by not_null."""
    for directory in _DWH_DIRS:
        for dl in _iter_docs(directory):
            if dl["TableName"] == table:
                return [c for c in (dl.get("Columns") or {}) if c.startswith("dw_ek_")]
    return []


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def _fhir_default_key(parent_table: str) -> str:
    """Conventional FHIR PK column when FK-Key is omitted (fhir.Organization -> organization_id)."""
    return parent_table.split(".")[-1].lower() + "_id"


def _dwh_target(col: str, fk_target: str | None) -> tuple[str, str] | None:
    """Resolve a DWH FK column to ``(parent_table, parent_key)``.

    Explicit ``FK-Target`` wins; otherwise ``dw_ek_<Dim>[_<role>]`` -> ``dim.<Dim>``.
    The parent key is always the target's PK ``dw_ek_<targetName>``.
    """
    if fk_target:
        parent_table = fk_target
    else:
        match = _DW_EK_RE.match(col)
        if not match:
            return None
        parent_table = f"dim.{match.group(1)}"
    parent_key = f"dw_ek_{parent_table.split('.')[-1]}"
    return parent_table, parent_key


# ---------------------------------------------------------------------------
# CheckSpecification builders
# ---------------------------------------------------------------------------


def referential_specs(
    table: str,
    *,
    cadence: Cadence = Cadence.WEEKLY,
    required_severity: Severity = Severity.BLOCKING,
    optional_severity: Severity = Severity.ERROR,
) -> list[CheckSpecification]:
    """Build referential_integrity CheckSpecifications for every FK edge declared on ``table``.

    Required edges (FK-Required: true) default to BLOCKING; optional edges to ERROR.
    Works for both tracks (FHIR FK-Target edges and DWH dw_ek_ edges).
    """
    specs: list[CheckSpecification] = []
    for e in edges_for(table):
        specs.append(
            CheckSpecification(
                check_type="referential_integrity",
                severity=required_severity if e.required else optional_severity,
                cadence=cadence,
                params={
                    "column": e.child_column,
                    "parent_table": e.parent_table,
                    "parent_key": e.parent_key,
                    "required": e.required,
                    "ignore_values": list(e.ignore_values),
                },
                name_suffix=e.child_column,
            )
        )
    return specs
