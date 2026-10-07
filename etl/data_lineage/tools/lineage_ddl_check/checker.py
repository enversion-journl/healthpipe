# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import re
from dataclasses import dataclass, field

from data_lineage.tools.lineage_ddl_check.ddl_parser import DdlTable
from data_lineage.tools.lineage_ddl_check.lineage_reader import LineageTable


@dataclass(frozen=True)
class ColumnFinding:
    table: str
    column: str


@dataclass
class Report:
    tables_missing_lineage: list[str] = field(default_factory=list)
    lineage_missing_ddl: list[str] = field(default_factory=list)
    columns_ddl_not_lineage: list[ColumnFinding] = field(default_factory=list)
    columns_lineage_not_ddl: list[ColumnFinding] = field(default_factory=list)
    columns_missing_fk_annotation: list[ColumnFinding] = field(default_factory=list)
    invalid_source_refs: list[str] = field(default_factory=list)
    malformed_lineage: list[str] = field(default_factory=list)

    @property
    def has_findings(self) -> bool:
        return bool(
            self.tables_missing_lineage
            or self.lineage_missing_ddl
            or self.columns_ddl_not_lineage
            or self.columns_lineage_not_ddl
            or self.columns_missing_fk_annotation
            or self.invalid_source_refs
            or self.malformed_lineage
        )

    def render(self) -> str:
        lines = ["Lineage vs DDL verification report", "=" * 40]

        def section(title: str, items: list[str]) -> None:
            lines.append("")
            lines.append(f"{title}: {len(items)}")
            for item in items:
                lines.append(f"  - {item}")

        section("DDL tables missing a lineage file", self.tables_missing_lineage)
        section("Lineage files with no matching DDL table", self.lineage_missing_ddl)
        section(
            "Columns in DDL but not in lineage",
            [f"{f.table}.{f.column}" for f in self.columns_ddl_not_lineage],
        )
        section(
            "Columns in lineage but not in DDL",
            [f"{f.table}.{f.column}" for f in self.columns_lineage_not_ddl],
        )
        section(
            "Relationship columns missing an FK annotation "
            "(add FK-Target, or FK-None: true if it is not a reference)",
            [f"{f.table}.{f.column}" for f in self.columns_missing_fk_annotation],
        )
        section("Invalid Source references", self.invalid_source_refs)
        section("Malformed lineage files", self.malformed_lineage)
        lines.append("")
        lines.append("RESULT: " + ("FAIL" if self.has_findings else "OK"))
        return "\n".join(lines)


_DW_EK_PREFIXES = ("dw_ek_",)

# DWH-computed layers that a Source reference must never point at — Source
# documents provenance in the *source system*; these are transformation steps
# (data_lineage_guide.md § "Source skal pege på kildesystemer").
_DWH_LAYER_PREFIXES = frozenset(
    {"arc", "stg", "pre_stg", "map", "dim", "fct", "codesystem", "conceptmap", "ref", "sec"}
)

# Hierarchy-column range shorthand, e.g. "nummer1..nummer20" — documented
# exception for OmkostningsstedHierarki-style level columns (see the guide).
_COLUMN_RANGE_SHORTHAND = re.compile(r"^\w*\d+\.\.\w*\d+$", re.UNICODE)

# A single source column name. Hyphens occur in real MySupply columns
# (e.g. "KundeReferenceE-Mail", "ZERO-RATED_AfgiftsKategori"); '#' occurs in
# Progress/ILS array-expanded columns (e.g. "lvnr##1" in dbo.laftdstam).
_COLUMN_NAME = re.compile(r"^[\w#-]+$", re.UNICODE)


def _validate_source_ref(ref: str, source_systems: dict[str, tuple[str, ...]]) -> str | None:
    """Validate one Source reference; return a reason when invalid, else None.

    A valid reference is ``system.schema.tabel.kolonne`` where ``system`` and
    ``schema.tabel`` are declared in the file's SourceSystems (schema-less
    registry tables like ``SOR_registry`` give 3-part refs and are matched the
    same way). Comparison is case-sensitive so casing drift surfaces here.
    """
    system, _, rest = ref.partition(".")
    if system in _DWH_LAYER_PREFIXES:
        return f"points at DWH-computed layer '{system}' — reference the source system instead"
    if system not in source_systems:
        return f"system '{system}' is not declared in SourceSystems"
    if not rest:
        return "missing table and column (expected system.schema.tabel.kolonne)"
    matches = [t for t in source_systems[system] if rest == t or rest.startswith(t + ".")]
    if not matches:
        return f"table is not declared under '{system}' in SourceSystems (case-sensitive)"
    table = max(matches, key=len)
    column = rest[len(table) + 1 :]
    if not column:
        return "missing column (expected system.schema.tabel.kolonne)"
    if not (_COLUMN_NAME.match(column) or _COLUMN_RANGE_SHORTHAND.match(column)):
        return f"invalid column part '{column}'"
    return None


def _is_relationship_column(schema: str, table_short: str, col: str) -> bool:
    """Is ``col`` a relationship column that must declare its FK intent?

    - FHIR: any ``*_id`` column (a reference to another resource, or the row's own
      id, or a source/business id — the annotation disambiguates which).
    - DWH (dim/fct/bridge/dm): any ``dw_ek_<Dim>`` column EXCEPT the table's own
      PK (``dw_ek_<TableName>``), which is not a foreign key. Post-overhaul the
      prefix is lowercase only (navngivningsstandard.md); the pre-overhaul
      uppercase ``DW_EK_`` spelling is intentionally NOT recognized so any
      residue surfaces instead of passing silently.
    """
    if schema == "fhir":
        return col.lower().endswith("_id")
    if schema in ("dim", "fct", "bridge", "dm"):
        prefix = next((p for p in _DW_EK_PREFIXES if col.startswith(p)), None)
        if prefix is None:
            return False
        return col != f"{prefix}{table_short}"
    return False


def _has_fk_intent(meta: dict) -> bool:
    """True if the column declares its relationship intent explicitly.

    Valid markers: ``Type: PK`` (the row's own id), ``FK-Target`` (a resolved
    reference — ``FK-Key``/``FK-Required``/``FK-Ignore`` refine it), ``Type: FK``
    (DWH ``DW_EK_`` resolved by naming convention), or ``FK-None: true`` (an
    explicit opt-out for a source/business id that is not a reference).
    """
    return meta.get("Type") in ("PK", "FK") or "FK-Target" in meta or meta.get("FK-None") is True


def check(
    ddl: dict[str, DdlTable],
    lineage: dict[str, LineageTable],
    *,
    malformed: list[str] | None = None,
    system_columns: frozenset[str] = frozenset(),
    ignored_tables: frozenset[str] = frozenset(),
) -> Report:
    """Bidirectionally compare parsed DDL against parsed lineage.

    Names are compared case-sensitively. ``system_columns`` are DDL columns
    allowed to be absent from lineage; ``ignored_tables`` are DDL tables allowed
    to have no lineage file.
    """
    report = Report(malformed_lineage=list(malformed or []))

    for name in sorted(ddl):
        if name not in lineage and name not in ignored_tables:
            report.tables_missing_lineage.append(name)

    for name in sorted(lineage):
        if name not in ddl:
            report.lineage_missing_ddl.append(name)

    for name in sorted(set(ddl) & set(lineage)):
        ddl_cols = set(ddl[name].columns)
        lin_cols = set(lineage[name].columns)
        for col in sorted(ddl_cols - lin_cols - set(system_columns)):
            report.columns_ddl_not_lineage.append(ColumnFinding(name, col))
        for col in sorted(lin_cols - ddl_cols):
            report.columns_lineage_not_ddl.append(ColumnFinding(name, col))

    # Relationship-annotation check: every FK-shaped column (FHIR *_id, DWH
    # DW_EK_<Dim>) must declare its intent (FK-Target / PK / FK-None), so a new
    # reference can't be added without either wiring the FK (which the DQA
    # referential_integrity check then picks up) or explicitly opting out.
    for name in sorted(lineage):
        if name in ignored_tables:
            continue
        schema, _, table_short = name.partition(".")
        for col, meta in lineage[name].column_specs.items():
            if col in system_columns:
                continue
            if _is_relationship_column(schema, table_short, col) and not _has_fk_intent(meta):
                report.columns_missing_fk_annotation.append(ColumnFinding(name, col))

    # Source-reference check: every Source ref must resolve, case-sensitively,
    # to a system+table declared in the file's SourceSystems and carry a column,
    # and must never point at a DWH-computed layer. Legacy 'coalesce:' keys are
    # invisible to dqa/star_schema tooling and are rejected outright.
    for name in sorted(lineage):
        if name in ignored_tables:
            continue
        table = lineage[name]
        for col, meta in table.column_specs.items():
            if "coalesce" in meta:
                report.invalid_source_refs.append(
                    f"{name}.{col}: legacy 'coalesce:' key — use 'Source:' (fallback order in Expr)"
                )
            refs = meta.get("Source")
            if refs is None:
                continue
            if not isinstance(refs, list):
                report.invalid_source_refs.append(f"{name}.{col}: 'Source' must be a list")
                continue
            for ref in refs:
                reason = _validate_source_ref(str(ref), table.source_systems)
                if reason is not None:
                    report.invalid_source_refs.append(f"{name}.{col}: '{ref}' — {reason}")

    return report
