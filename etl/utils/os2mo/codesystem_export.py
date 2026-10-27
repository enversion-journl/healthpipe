# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The shared export model: one ordered CSV row set per codesystem we hand OS2mo.

Every codesystem delivered to Magenta goes through :func:`build_export_rows`,
and every one comes out in the same format — the phase-1 SOR file, generalised
and named in OS2mo's own vocabulary:

``user_key;parent_user_key;validity_from;validity_to;name;org_unit_type;active;deleted;created;updated``
followed by ``map_code_K;map_system_K;map_relation_K`` for each pre-mapped code.

A column that corresponds to a field on OS2mo's ``OrganisationUnitCreateInput``
carries that field's name, so Magenta's import maps it one to one:

* ``user_key``, ``name`` and ``org_unit_type`` are the input's own fields (the
  type as the class's user_key, which is how Magenta key the SOR classes).
* ``parent_user_key`` stands for ``parent``: the input takes the parent's UUID,
  which only OS2mo knows, so the file names the parent by its user_key.
* ``validity_from`` / ``validity_to`` are ``validity { from to }`` flattened,
  as ISO dates; an empty ``validity_to`` is open-ended, like null in OS2mo. A
  source end date in year 9000 or later is a "never ends" sentinel and is
  rendered empty too.

``active`` and ``deleted`` are the code's own status flags, ``true``/``false``,
or empty where the codesystem has no such flag. A codesystem whose dates do not
say whether a code is still in use (Okosted's are always 1900-01-01 →
9999-12-31) is delivered whole, and these columns carry the status instead.

``created`` and ``updated`` are the warehouse's own timestamps (ISO 8601, local
time, to the second) — when the code first entered the codesystem and when its
row in this file last changed. They let a consumer act on the rows that
changed rather than on the whole file. ``updated`` moves with any change to the
code's concept row, any of its properties or its parent. It does not move with
mapping changes: the ConceptMap merge stamps every row nightly.

``parent_user_key`` is the only hierarchy information in the file. There are no
depth or ancestor-name columns: an ancestor's rename or move would change them
without changing the row itself, and a consumer could build the tree from
names instead of from the parent key.

What differs between codesystems is described by a :class:`CodesystemExportSpec`
— which properties carry the dates and the type, whether the set is filtered on
an "active" flag, where the tree starts and which ConceptMaps
supply pre-mapped codes. Nothing in this module performs I/O.

**Order** is breadth-first from the root(s), so a parent always precedes its
children; siblings are sorted by code so two runs over the same data render
byte-identical files.
"""

import re
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from utils.sor_codes import parse_sor_date

__all__ = [
    "BASE_COLUMNS",
    "CodesystemExportSpec",
    "MappingSource",
    "build_export_rows",
    "export_columns",
    "format_export_date",
    "format_export_end_date",
    "format_flag",
    "map_slots_needed",
    "parse_export_date",
    "parse_export_timestamp",
    "unreadable_values",
]

BASE_COLUMNS = [
    "user_key",
    "parent_user_key",
    "validity_from",
    "validity_to",
    "name",
    "org_unit_type",
    "active",
    "deleted",
    "created",
    "updated",
]

EXPORT_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Table and property names end up inside SQL, so a spec may only carry plain
# identifiers. They come from code, never from config — this is a tripwire.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class MappingSource:
    """A ConceptMap whose targets travel with the source codes as MapCode columns.

    ``table`` is the name under ``conceptmap.`` (e.g. ``OkostedToSOR``);
    ``system`` is what the MapSystem column says (e.g. ``SOR``).
    """

    table: str
    system: str

    def __post_init__(self) -> None:
        if not _IDENTIFIER_RE.match(self.table):
            raise ValueError(f"Not a plain table name: {self.table!r}")


@dataclass(frozen=True)
class CodesystemExportSpec:
    """Everything that differs between two codesystem deliveries.

    Attributes:
        name: The codesystem's name in the warehouse — the ``<name>`` in
            ``codesystem.<name>Concept`` — and in DFM and the file name.
        asset_prefix: Lowercase slug for asset names (``os2mo_<slug>_export``).
        source_deps: Assets that must have rebuilt the codesystem (and its
            ConceptMaps) before the set is built.
        from_property / to_property: Properties holding the validity dates.
        type_property: Property rendered as org_unit_type, or None for empty.
        active_property: When set, only codes whose property equals
            ``active_value`` are delivered, and only those reachable from the
            root through active codes. None delivers the whole codesystem.
        status_active_property / status_deleted_property: Properties rendered
            as the ``active`` / ``deleted`` flags, or None for empty.
        root_code: The single root the tree is walked from. None means every
            code without a parent in the set is a root.
        mappings: ConceptMaps rendered as map_code/map_system/map_relation columns.
        min_map_slots: The file always carries at least this many mapping
            groups, so its header is stable on a night with fewer mappings.
    """

    name: str
    asset_prefix: str
    source_deps: tuple[str, ...]
    from_property: str
    to_property: str
    type_property: str | None = None
    active_property: str | None = None
    active_value: str = "true"
    status_active_property: str | None = None
    status_deleted_property: str | None = None
    root_code: str | None = None
    mappings: tuple[MappingSource, ...] = field(default_factory=tuple)
    min_map_slots: int = 0

    def __post_init__(self) -> None:
        for value in (self.name, self.from_property, self.to_property):
            if not _IDENTIFIER_RE.match(value):
                raise ValueError(f"Not a plain identifier: {value!r}")
        for optional in (
            self.type_property,
            self.active_property,
            self.status_active_property,
            self.status_deleted_property,
        ):
            if optional is not None and not _IDENTIFIER_RE.match(optional):
                raise ValueError(f"Not a plain identifier: {optional!r}")
        if self.min_map_slots and not self.mappings:
            raise ValueError("min_map_slots without any mappings would add empty columns")

    @property
    def filename_pattern(self) -> str:
        """The delivered files' name, for descriptions and docs."""
        return f"rsyd_{self.name.lower()}_<YYYYMMDD_HHMMSS>.csv"

    def filename(self, built_at: datetime) -> str:
        """The delivered file's name: one distinct name per built set.

        Stamped with the BUILD time, not the upload time, so a retried upload
        of the same set sends the same name — and Magenta can tell which
        deliveries they have already processed.
        """
        return f"rsyd_{self.name.lower()}_{built_at:%Y%m%d_%H%M%S}.csv"


def format_export_date(value: Any) -> str:
    """Render a codesystem date as ISO ``YYYY-MM-DD``, or "" when unparsable.

    OS2mo's validity is date-granular, so the time of day is dropped.
    """
    parsed = parse_sor_date(value)
    if parsed is None:
        return ""
    return parsed.strftime("%Y-%m-%d")


# A source end date this far out means "never ends", not a real closing date.
OPEN_ENDED_FROM_YEAR = 9000


def format_export_end_date(value: Any) -> str:
    """Like :func:`format_export_date`, but a never-ends sentinel renders empty."""
    parsed = parse_sor_date(value)
    if parsed is None or parsed.year >= OPEN_ENDED_FROM_YEAR:
        return ""
    return parsed.strftime("%Y-%m-%d")


_TRUE_VALUES = frozenset({"1", "true", "yes", "ja"})
_FALSE_VALUES = frozenset({"0", "false", "no", "nej"})


def format_flag(value: Any) -> str:
    """A status flag as ``true``/``false``, or "" when absent or unreadable.

    Accepts what the codesystems store: ``true``/``false`` text and ``1``/``0``
    from integer source columns.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value if value is not None else "").strip().lower()
    if text in _TRUE_VALUES:
        return "true"
    if text in _FALSE_VALUES:
        return "false"
    return ""


def parse_export_date(value: Any) -> date | None:
    """Inverse of :func:`format_export_date`, for the export table."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def parse_export_timestamp(value: Any) -> datetime | None:
    """A warehouse timestamp as a datetime (to the second), or None.

    Accepts what the database driver returns (datetime) and the ISO text the
    file carries, so the same function serves the builder and the table.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0, tzinfo=None)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).replace(microsecond=0, tzinfo=None)
    except ValueError:
        return None


def _format_timestamp(value: datetime | None) -> str:
    return value.strftime(EXPORT_TIMESTAMP_FORMAT) if value else ""


def unreadable_values(source_rows: list[dict[str, Any]]) -> dict[str, int]:
    """How many source values the file can only render as empty.

    A date or a status flag that is present but unreadable becomes "" in the
    file — and an empty validity_to reads as "open-ended". Counting them makes
    a change in the source's format visible instead of silent.
    """
    counts = {"valid_from": 0, "valid_to": 0, "status_active": 0, "status_deleted": 0}
    for row in source_rows:
        for key in ("valid_from", "valid_to"):
            if str(row.get(key) or "").strip() and parse_sor_date(row.get(key)) is None:
                counts[key] += 1
        for key in ("status_active", "status_deleted"):
            if str(row.get(key) or "").strip() and not format_flag(row.get(key)):
                counts[key] += 1
    return {key: count for key, count in counts.items() if count}


def export_columns(map_slots: int) -> list[str]:
    """The file's header for this codesystem with ``map_slots`` mapping groups."""
    columns = list(BASE_COLUMNS)
    for slot in range(1, map_slots + 1):
        columns += [f"map_code_{slot}", f"map_system_{slot}", f"map_relation_{slot}"]
    return columns


def map_slots_needed(spec: CodesystemExportSpec, export_rows: list[dict[str, Any]]) -> int:
    """How many mapping groups this set needs — never fewer than the spec's minimum.

    A code can map to more than one target (a ConceptMap's key is the pair),
    so the width follows the data rather than truncating a mapping away.
    """
    widest = max((len(row.get("_mappings") or []) for row in export_rows), default=0)
    return max(spec.min_map_slots, widest)


def _is_active(spec: CodesystemExportSpec, row: dict[str, Any]) -> bool:
    """True when the row passes the spec's active filter (or there is none).

    Compared as text, case-insensitively: the property arrives as the string
    stored in ``codesystem.<name>Properties``, and a real bool is tolerated.
    """
    if spec.active_property is None:
        return True
    value = row.get("active")
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == spec.active_value.lower()


def build_export_rows(
    spec: CodesystemExportSpec,
    source_rows: list[dict[str, Any]],
    mappings_by_code: dict[str, list[tuple[str, str, str]]] | None = None,
) -> list[dict[str, Any]]:
    """Build the ordered export set for one codesystem.

    Args:
        spec: The codesystem's delivery description.
        source_rows: One dict per code with ``code``, ``display``,
            ``parent_code``, ``valid_from``, ``valid_to``, ``type``, ``active``,
            ``status_active``, ``status_deleted``, ``created`` and ``updated``
            (all but the first three may be absent).
        mappings_by_code: Pre-mapped targets per source code, as
            ``(code, system, relationship)`` tuples.

    Returns:
        One dict per exported code, keyed by the base columns, plus a
        ``_mappings`` list the CSV renderer spreads into map_code columns.

    Raises:
        ValueError: if the spec names a root that is not in the selected set —
            the codesystem is then in a state no delivery should be built on.
    """
    mappings_by_code = mappings_by_code or {}
    selected = {str(row["code"]): row for row in source_rows if _is_active(spec, row)}

    if spec.root_code is not None:
        if spec.root_code not in selected:
            raise ValueError(
                f"{spec.name} root {spec.root_code} is not present in the selected set "
                f"({len(selected)} codes). codesystem.{spec.name}* is incomplete or its "
                "active flag was not computed — refuse to build a delivery from it."
            )
        roots = [spec.root_code]
    else:
        roots = sorted(
            code
            for code, row in selected.items()
            if not str(row.get("parent_code") or "") or str(row["parent_code"]) not in selected
        )

    children: dict[str, list[str]] = {}
    for code, row in selected.items():
        parent = str(row.get("parent_code") or "")
        if parent and parent in selected and code not in roots:
            children.setdefault(parent, []).append(code)
    for siblings in children.values():
        siblings.sort()

    export: list[dict[str, Any]] = []
    root_set = set(roots)
    seen: set[str] = set(roots)
    queue: deque[str] = deque(roots)

    while queue:
        code = queue.popleft()
        export.append(
            _build_export_row(
                code, selected[code], code in root_set, mappings_by_code.get(code, [])
            )
        )
        for child in children.get(code, []):
            if child in seen:
                # Defensive: a hierarchy is a tree, but a cycle or a repeated
                # edge must not turn into an unbounded walk.
                continue
            seen.add(child)
            queue.append(child)

    return export


def _build_export_row(
    code: str,
    row: dict[str, Any],
    is_root: bool,
    mappings: list[tuple[str, str, str]],
) -> dict[str, Any]:
    """Render one code into the export layout.

    A root's parent_user_key is empty even when the codesystem gives it a
    parent outside the delivered set: the file must not point at a code it does
    not contain.
    """
    parent = str(row.get("parent_code") or "")
    return {
        "user_key": code,
        "parent_user_key": "" if is_root else parent,
        "validity_from": format_export_date(row.get("valid_from")),
        "validity_to": format_export_end_date(row.get("valid_to")),
        "name": str(row.get("display") or ""),
        "org_unit_type": str(row.get("type") or ""),
        "active": format_flag(row.get("status_active")),
        "deleted": format_flag(row.get("status_deleted")),
        "created": _format_timestamp(parse_export_timestamp(row.get("created"))),
        "updated": _format_timestamp(parse_export_timestamp(row.get("updated"))),
        "_mappings": sorted(mappings),
    }
