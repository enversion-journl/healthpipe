# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The shared export model: one ordered CSV row set per codesystem we hand OS2mo.

Every codesystem delivered to Magenta goes through :func:`build_export_rows`,
and every one comes out in the same format — the phase-1 SOR file, generalised
and renamed to Example Region's own column naming standard, as agreed with
Magenta:

``kode;kode_parent;gyldig_fra;gyldig_til;navn;type;niveau;aktiv;dw_is_deleted;dw_timestamp_insert;dw_timestamp_update``

* ``kode``, ``navn`` and ``type`` are the code's own fields: its identifier,
  display name and classification.
* ``kode_parent`` names the code's parent in the delivered tree by its own
  ``kode`` — empty for a root, even when the codesystem gives it a parent
  outside the delivered set. It is the only ancestry information in the file:
  there are no grandparent or ancestor-name columns, so an ancestor's rename
  or move changes nothing but its own row.
* ``gyldig_fra`` / ``gyldig_til`` are the code's validity, as ISO dates, and
  never empty: where the source names no start, ``gyldig_fra`` is
  ``1900-01-01``; where it names no end, ``gyldig_til`` is ``9999-12-31``
  (open-ended). A source end date in year 9000 or later is a "never ends"
  sentinel and is rendered ``9999-12-31`` too.
* ``niveau`` is the code's depth in the delivered tree: a root is ``"1"``, a
  child is its parent's niveau plus one. It is computed from the walk, not
  read from the source, so it always matches the tree the file actually
  delivers.

``aktiv`` and ``dw_is_deleted`` are the code's own status flags, ``true``/
``false``, or empty where the codesystem has no such flag. A codesystem whose
dates do not say whether a code is still in use (Okosted's are always
1900-01-01 → 9999-12-31) is delivered whole, and these columns carry the
status instead.

``dw_timestamp_insert`` and ``dw_timestamp_update`` are the code's own
source/warehouse timestamps (ISO 8601, local time, to the second) — not an
audit trail of this file, but when the code first entered the codesystem and
when its own row last changed. They let a consumer act on the rows that
changed rather than on the whole file. ``dw_timestamp_update`` does not move
when only an ancestor changes: the file carries no ancestor information for it
to move with.

What differs between codesystems is described by a :class:`CodesystemExportSpec`
— which properties carry the dates and the type, whether the set is limited to
the codes with a given property value, and where the tree starts. Nothing in
this module performs I/O. The file carries no mappings: a code's ConceptMap
targets are not part of this delivery — they are set up by hand in OS2mo
instead.

**Order** is breadth-first from the root(s), so a parent always precedes its
children; siblings are sorted by code so two runs over the same data render
byte-identical files.
"""

import re
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from utils.sor_codes import parse_sor_date

__all__ = [
    "BASE_COLUMNS",
    "CodeSource",
    "CodesystemExportSpec",
    "build_export_rows",
    "export_columns",
    "format_export_date",
    "format_export_end_date",
    "format_flag",
    "parse_export_date",
    "parse_export_timestamp",
    "unreadable_values",
]

BASE_COLUMNS = [
    "kode",
    "kode_parent",
    "gyldig_fra",
    "gyldig_til",
    "navn",
    "type",
    "niveau",
    "aktiv",
    "dw_is_deleted",
    "dw_timestamp_insert",
    "dw_timestamp_update",
]

EXPORT_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Table and property names end up inside SQL, so a spec may only carry plain
# identifiers. They come from code, never from config — this is a tripwire.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class CodeSource:
    """A code set read from a table other than ``codesystem.<name>Concept``.

    The delivered codes are the distinct non-empty values of ``column`` in
    ``[schema].[table]``; a code is first seen at the earliest
    ``dw_timestamp_insert`` of its rows there.
    """

    schema: str
    table: str
    column: str

    def __post_init__(self) -> None:
        for value in (self.schema, self.table, self.column):
            if not _IDENTIFIER_RE.match(value):
                raise ValueError(f"Not a plain identifier: {value!r}")


@dataclass(frozen=True)
class CodesystemExportSpec:
    """Everything that differs between two codesystem deliveries.

    Attributes:
        name: The codesystem's name in the warehouse — the ``<name>`` in
            ``codesystem.<name>Concept`` — and in DFM and the file name.
        asset_prefix: Lowercase slug for asset names (``os2mo_<slug>_export``).
        source_deps: Assets that must have rebuilt the codesystem before the
            set is built.
        from_property / to_property: Properties holding the validity dates,
            or None when the codesystem carries none (EAN, Lager) — the file's
            ``gyldig_fra`` / ``gyldig_til`` are then ``1900-01-01`` /
            ``9999-12-31``.
        type_property: Property rendered as type, or None for empty.
        scope_property: When set, only codes whose property equals
            ``scope_value`` are delivered, and only those reachable from the
            root through such codes. None delivers the whole codesystem.
        status_active_property / status_deleted_property: Properties rendered
            as the ``aktiv`` / ``dw_is_deleted`` flags, or None for empty.
        root_code: The single root the tree is walked from. None means every
            code without a parent in the set is a root.
        code_source: When set, the codes come from this table instead of
            ``codesystem.<name>Concept`` (EAN: the invoice receivers in
            MySupply), and every code in it is delivered. Such a spec is flat
            and reads only ``display_property`` from the codesystem.
        display_property: Property rendered as navn — empty when the code has
            none — instead of the concept's display. Only with ``code_source``.
    """

    name: str
    asset_prefix: str
    source_deps: tuple[str, ...]
    from_property: str | None
    to_property: str | None
    type_property: str | None = None
    scope_property: str | None = None
    scope_value: str = "true"
    status_active_property: str | None = None
    status_deleted_property: str | None = None
    root_code: str | None = None
    code_source: CodeSource | None = None
    display_property: str | None = None

    def __post_init__(self) -> None:
        if not _IDENTIFIER_RE.match(self.name):
            raise ValueError(f"Not a plain identifier: {self.name!r}")
        for optional in (
            self.from_property,
            self.to_property,
            self.type_property,
            self.scope_property,
            self.status_active_property,
            self.status_deleted_property,
            self.display_property,
        ):
            if optional is not None and not _IDENTIFIER_RE.match(optional):
                raise ValueError(f"Not a plain identifier: {optional!r}")
        if self.display_property is not None and self.code_source is None:
            raise ValueError("display_property needs a code_source")
        if self.code_source is not None:
            # The external-code query reads nothing but the name, so any other
            # property here would be silently ignored.
            unread = {
                "from_property": self.from_property,
                "to_property": self.to_property,
                "type_property": self.type_property,
                "scope_property": self.scope_property,
                "status_active_property": self.status_active_property,
                "status_deleted_property": self.status_deleted_property,
                "root_code": self.root_code,
            }
            set_fields = sorted(key for key, value in unread.items() if value is not None)
            if set_fields:
                raise ValueError(f"A code_source spec does not read {', '.join(set_fields)}")

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

# The validity a code is delivered with where the source names no date (none,
# unreadable, or a never-ends sentinel): valid since always, until further
# notice — so gyldig_fra and gyldig_til are never empty in the file.
DEFAULT_VALID_FROM = "1900-01-01"
DEFAULT_VALID_TO = "9999-12-31"


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
    """How many present source values the file cannot carry as read.

    A status flag that is present but unreadable becomes "" in the file, and
    such a date becomes the default — an unreadable gyldig_til then reads as
    "open-ended". Counting them makes a change in the source's format visible
    instead of silent.
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


def export_columns() -> list[str]:
    """The file's header for every codesystem: the base columns, in order."""
    return list(BASE_COLUMNS)


def _in_scope(spec: CodesystemExportSpec, row: dict[str, Any]) -> bool:
    """True when the row is in the spec's scope (or the spec has none).

    Compared as text, case-insensitively: the property arrives as the string
    stored in ``codesystem.<name>Properties``, and a real bool is tolerated.
    """
    if spec.scope_property is None:
        return True
    value = row.get("scope")
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == spec.scope_value.lower()


def build_export_rows(
    spec: CodesystemExportSpec,
    source_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build the ordered export set for one codesystem.

    Args:
        spec: The codesystem's delivery description.
        source_rows: One dict per code with ``code``, ``display``,
            ``parent_code``, ``valid_from``, ``valid_to``, ``type``, ``scope``,
            ``status_active``, ``status_deleted``, ``created`` and ``updated``
            (all but the first three may be absent).

    Returns:
        One dict per exported code, keyed exactly by :data:`BASE_COLUMNS`.

    Raises:
        ValueError: if the spec names a root that is not in the selected set —
            the codesystem is then in a state no delivery should be built on.
    """
    selected = {str(row["code"]): row for row in source_rows if _in_scope(spec, row)}

    if spec.root_code is not None:
        if spec.root_code not in selected:
            raise ValueError(
                f"{spec.name} root {spec.root_code} is not present in the selected set "
                f"({len(selected)} codes). codesystem.{spec.name}* is incomplete or its "
                "scope flag was not computed — refuse to build a delivery from it."
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
    queue: deque[tuple[str, int]] = deque((code, 1) for code in roots)

    while queue:
        code, depth = queue.popleft()
        export.append(_build_export_row(code, selected[code], code in root_set, depth))
        for child in children.get(code, []):
            if child in seen:
                # Defensive: a hierarchy is a tree, but a cycle or a repeated
                # edge must not turn into an unbounded walk.
                continue
            seen.add(child)
            queue.append((child, depth + 1))

    return export


def _build_export_row(
    code: str,
    row: dict[str, Any],
    is_root: bool,
    depth: int,
) -> dict[str, Any]:
    """Render one code into the export layout.

    A root's kode_parent is empty even when the codesystem gives it a parent
    outside the delivered set: the file must not point at a code it does not
    contain.
    """
    parent = str(row.get("parent_code") or "")
    return {
        "kode": code,
        "kode_parent": "" if is_root else parent,
        "gyldig_fra": format_export_date(row.get("valid_from")) or DEFAULT_VALID_FROM,
        "gyldig_til": format_export_end_date(row.get("valid_to")) or DEFAULT_VALID_TO,
        "navn": str(row.get("display") or ""),
        "type": str(row.get("type") or ""),
        "niveau": str(depth),
        "aktiv": format_flag(row.get("status_active")),
        "dw_is_deleted": format_flag(row.get("status_deleted")),
        "dw_timestamp_insert": _format_timestamp(parse_export_timestamp(row.get("created"))),
        "dw_timestamp_update": _format_timestamp(parse_export_timestamp(row.get("updated"))),
    }
