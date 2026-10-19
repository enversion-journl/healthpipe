# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Harvest LPR3 Metadata + ExportLog without unpacking or reading patient rows.

SDS' metadata header and ordered column records are returned verbatim per table.

Run from etl: python -m scripts.lpr3_schema DELIVERY.tar.bz2 --output schema.json
The shared forward-only reader scans the whole archive: the September 2026
delivery puts Metadata and ExportLog AFTER the data files. Skipped data members
still require decompression, but their CSV rows are not parsed or saved to disk.
"""

import csv
import io
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO

from DSI.csv_files._sds_tar import iter_members, open_member_csv

MEMBER_RE = re.compile(
    r"(?P<metadata>Metadata_)?(?P<table>(?:Dim|[Ff]ct)[A-Za-z0-9_]+?)"
    r"(?P<regional>_1083)?_(?P<cutoff>[0-9]{8})\.csv\Z"
)
EXPORT_RE = re.compile(r"SDP_ExportLog_(?P<cutoff>[0-9]{8})\.csv\Z")
MAX_CONTROL_BYTES = 4 * 1024 * 1024
EXPECTED_TABLES = 47
EXPORT_COLUMNS = ("FilNavn", "AntalRaekker", "FilStoerrelseBytes")


class SchemaHarvestError(ValueError):
    """A control-file failure; messages never include source field values."""


@dataclass(frozen=True)
class DeliveryControls:
    schema: dict[str, dict]
    present_files: frozenset[str]
    announced_files: frozenset[str]
    cutoff: str


def _control_reader(handle: IO[bytes], encoding: str):
    data = handle.read(MAX_CONTROL_BYTES + 1)
    if len(data) > MAX_CONTROL_BYTES:
        raise SchemaHarvestError("LPR3 control file exceeds the size limit")
    # A safe error replaces decoder/csv exceptions, which may contain input.
    try:
        return list(open_member_csv(io.BytesIO(data), encoding))
    except (UnicodeError, csv.Error):
        raise SchemaHarvestError("LPR3 control file has invalid encoding or CSV") from None


def parse_cutoff(raw: str) -> str:
    try:
        return datetime.strptime(raw, "%Y%m%d").date().isoformat()
    except ValueError:
        raise SchemaHarvestError("LPR3 control filename has an invalid cutoff date") from None


def inspect_members(
    members: Iterable[tuple[str, IO[bytes]]],
    *,
    encoding: str = "cp1252",
    expected_tables: int | None = EXPECTED_TABLES,
    allow_missing: bool = False,
) -> DeliveryControls:
    """Read control files in any member order; allow_missing relaxes the filename cross-checks."""
    schema: dict[str, dict] = {}
    export: dict[str, tuple[int, int]] | None = None
    cutoffs: set[str] = set()
    data_members: set[str] = set()
    for name, handle in members:
        log_match = EXPORT_RE.fullmatch(name)
        match = MEMBER_RE.fullmatch(name)
        if log_match:
            if export is not None:
                raise SchemaHarvestError("LPR3 delivery has multiple export logs")
            cutoffs.add(parse_cutoff(log_match["cutoff"]))
            records = _control_reader(handle, encoding)
            if (
                not records
                or len(set(records[0])) != len(records[0])
                or not set(EXPORT_COLUMNS).issubset(records[0])
            ):
                raise SchemaHarvestError("LPR3 export-log header does not match the contract")
            positions = [records[0].index(column) for column in EXPORT_COLUMNS]
            export = {}
            seen_names: set[str] = set()
            for row in records[1:]:
                if len(row) != len(records[0]):
                    raise SchemaHarvestError("LPR3 export-log row has an invalid column count")
                row = [row[index] for index in positions]
                entry = MEMBER_RE.fullmatch(row[0])
                if entry is None:
                    raise SchemaHarvestError("LPR3 export log contains an unrecognised filename")
                if row[0] in seen_names:
                    raise SchemaHarvestError("LPR3 export log contains a duplicate filename")
                seen_names.add(row[0])
                cutoffs.add(parse_cutoff(entry["cutoff"]))
                try:
                    count, size = int(row[1]), int(row[2])
                except ValueError:
                    raise SchemaHarvestError("LPR3 export-log totals must be integers") from None
                if count < 0 or size < 0:
                    raise SchemaHarvestError("LPR3 export-log totals must not be negative")
                if not entry["metadata"]:
                    export[row[0]] = (count, size)
        elif match and match["metadata"]:
            table = match["table"]
            if table.casefold() in {t.casefold() for t in schema}:
                raise SchemaHarvestError("LPR3 delivery has duplicate table metadata")
            cutoffs.add(parse_cutoff(match["cutoff"]))
            records = _control_reader(handle, encoding)
            if len(records) < 2 or not records[0]:
                raise SchemaHarvestError("LPR3 metadata must contain a header and columns")
            header, columns = records[0], records[1:]
            if len(set(header)) != len(header) or any(not h for h in header):
                raise SchemaHarvestError("LPR3 metadata has duplicate or empty header fields")
            if any(len(row) != len(header) for row in columns):
                raise SchemaHarvestError("LPR3 metadata row has an invalid column count")
            schema[table] = {
                "regional": bool(match["regional"]),
                "metadata_header": header,
                "columns": columns,
                "datafile": name.removeprefix("Metadata_"),
            }
        elif match:
            if name in data_members:
                raise SchemaHarvestError("LPR3 delivery has duplicate data members")
            data_members.add(name)
            cutoffs.add(parse_cutoff(match["cutoff"]))
        else:
            raise SchemaHarvestError("LPR3 delivery contains an unexpected member")

    if export is None:
        raise SchemaHarvestError("LPR3 export log is missing")
    if len(cutoffs) != 1:
        raise SchemaHarvestError("LPR3 control files have inconsistent cutoff dates")
    if expected_tables is not None and (
        len(schema) != expected_tables or len(export) != expected_tables
    ):
        raise SchemaHarvestError(
            f"LPR3 expected {expected_tables} tables; "
            f"metadata={len(schema)}, export-log data entries={len(export)}"
        )
    metadata_files = {item["datafile"] for item in schema.values()}
    if not allow_missing and metadata_files != set(export):
        raise SchemaHarvestError("LPR3 metadata and export-log data filenames differ")
    if not allow_missing and data_members != set(export):
        raise SchemaHarvestError("LPR3 data members and export-log data filenames differ")
    if not data_members or not data_members <= metadata_files & set(export):
        raise SchemaHarvestError("LPR3 data members must have both Metadata and ExportLog")
    for item in schema.values():
        logged_rows, logged_bytes = export.get(item["datafile"], (None, None))
        item["rows_exportlog"] = str(logged_rows)
        item["bytes_exportlog"] = str(logged_bytes)
    return DeliveryControls(
        dict(sorted(schema.items())),
        frozenset(data_members),
        frozenset(metadata_files | set(export) | data_members),
        next(iter(cutoffs)),
    )


def harvest_members(members, *, encoding="cp1252", expected_tables=EXPECTED_TABLES):
    """Strict 47-table export of the metadata header and ordered column records per table."""
    return inspect_members(members, encoding=encoding, expected_tables=expected_tables).schema


def delivery_controls(path: str, *, encoding="cp1252") -> DeliveryControls:
    """Production preflight: absent data tables are reported, never silently reused."""
    members = iter_members(path)
    try:
        return inspect_members(members, encoding=encoding, expected_tables=None, allow_missing=True)
    finally:
        members.close()


def harvest_schema(path: Path, *, encoding: str = "cp1252") -> dict[str, dict]:
    """Stream all members and close the compressed file on success or failure."""
    members = iter_members(str(path))
    try:
        return harvest_members(members, encoding=encoding)
    finally:
        members.close()
