# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared streaming machinery for SDS' tar.bz2 deliveries.

SDS ships every source on the NGC share the same way: one bz2-compressed tar
per delivery, holding semicolon-separated, fully quoted cp1252 CSVs, SDS' own
column schemas (``Metadata_*.csv``) and an export log (``SDP_ExportLog_*.csv``)
with the row count per file. Members are streamed straight into the ext tables
— multi-gigabyte deliveries are never unpacked to disk.

Two sources already implement that machinery (``drg.py``, ``udredningsret.py``)
and a third is coming (LPR3 Grunddata, 47 members per tar), so the genuinely
identical parts live here: the chunked ext writer, the export-log parser, the
ext DDL shape and the small comparisons that feed the per-source guards. It
also holds ``upsert_register``, the MERGE into the meta.* delivery registers,
which CPR and HAIBA use too. The tar walk and the non-seekable stream adapter
live in ``core/file_delivery.py``; the folder checks live in
``FileShareResource.root()``.

WHAT DELIBERATELY DOES NOT LIVE HERE
- Messages. Every error and log line stays with its source, because they are
  pinned by that source's tests and because the two existing sources speak
  different languages (DRG Danish, udredningsret English). A helper here either
  takes no message at all and RETURNS data for the caller to raise on, or takes
  the sentence as a parameter.
- Row handling. ``_stream_data_member`` and ``load_archive_into_ext`` stay per
  source: the per-row guards, the row tuples, the value normalisation and the
  order of the post-loop checks differ by design, and a lowest-common-
  denominator version of them would be harder to read than two honest copies.
- Delivery detection. ``DSI/connectors/delivery_detection.py`` remains the
  single home for delivery_unit/delivery_identity/is_new_delivery; this module
  knows nothing about checkpoints and names no source.

GDPR: nothing here ever puts a row value, a CPR or any field content into an
exception, a log line or a return value. Column names, file names and counts
only.

BEHAVIOUR NOTE (the one deliberate change of the extraction): DRG's
load path now runs through :func:`run_writers`, so a designed failure mid-load
DISCARDS the buffered rows instead of flushing them on the way out. Flushing
them issued one more INSERT, and when that INSERT failed the driver's error
replaced the real cause (mojibake, key-width overflow, export-log mismatch) as
the raised exception. udredningsret already worked this way (review F19).
"""

import csv
import re
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import datetime
from typing import Any, Protocol, TypeVar

from core.file_delivery import DELIMITER
from utils.sql_safety import safe_bracket_quote

T = TypeVar("T")

INSERT_CHUNK_SIZE = 1000

# Encoding sanity: U+FFFD appears when the sender itself has replaced
# undecodable characters; "ï¿½" is U+FFFD's UTF-8 bytes misread as cp1252 (i.e.
# the file was really UTF-8). Both are unambiguous corruption markers with no
# false positives in Danish health data.
MOJIBAKE_MARKERS: tuple[str, str] = ("�", "ï¿½")

# The delivery and audit columns every ext landing table ends with.
_TRAILING_LINES = (
    "[leverance_dato] DATE NOT NULL",
    "[dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()",
    "[dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()",
)

# Members whose row counts are not part of the export log's control totals.
_METADATA_PREFIX = "Metadata_"


class _Log(Protocol):
    def info(self, message: str) -> None: ...


class LogContext(Protocol):
    """Anything carrying a ``.log.info`` — Dagster's execution context, or a
    test double. Deliberately narrow: shared code never logs a warning or an
    error, because not every caller's context double has those methods."""

    @property
    def log(self) -> _Log: ...


# ---------------------------------------------------------------------------
# filenames
# ---------------------------------------------------------------------------


def parse_archive_timestamp(raw: str) -> datetime | None:
    """The packaging timestamp SDS puts in every archive name, or None."""
    try:
        return datetime.strptime(raw, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def dataset_prefix(name: str, member_re: re.Pattern[str]) -> str | None:
    """The dataset a data member belongs to, from its file name.

    ``member_re`` is the source's own pattern and must carry a ``prefix``
    group. A member that does not match is not a data file (Metadata_*, a
    readme, ...) and the caller decides what that means.
    """
    match = member_re.match(name)
    return match["prefix"] if match else None


# ---------------------------------------------------------------------------
# writing into ext
# ---------------------------------------------------------------------------


class ExtWriter:
    """Chunked executemany writer for one ext table.

    ``chunk_size`` is passed in rather than read from this module's constant so
    each source keeps its OWN ``INSERT_CHUNK_SIZE`` module attribute — which is
    what its tests patch to force a flush per row. ``row_word`` is the noun in
    the progress line, so each source keeps its own wording.
    """

    def __init__(
        self,
        conn,
        ext_table: str,
        columns: Sequence[str],
        context: LogContext,
        *,
        chunk_size: int = INSERT_CHUNK_SIZE,
        row_word: str = "rows",
    ):
        self.ext_table = ext_table
        self.context = context
        self.chunk_size = chunk_size
        self.row_word = row_word
        self.cursor = conn.cursor()
        if hasattr(self.cursor, "fast_executemany"):
            self.cursor.fast_executemany = True
        placeholders = ", ".join("?" for _ in columns)
        column_sql = ", ".join(f"[{c}]" for c in columns)
        self.insert_sql = f"INSERT INTO [ext].[{ext_table}] ({column_sql}) VALUES ({placeholders})"
        self.buffer: list[tuple] = []
        self.total = 0

    def add(self, row: tuple) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= self.chunk_size:
            self.flush()

    def flush(self) -> None:
        if self.buffer:
            self.cursor.executemany(self.insert_sql, self.buffer)
            self.total += len(self.buffer)
            if self.total % 500_000 < self.chunk_size:
                self.context.log.info(f"ext.{self.ext_table}: {self.total:,} {self.row_word}")
            self.buffer.clear()

    def discard(self) -> None:
        """Drop buffered rows without sending them — the error path (review F19).

        Flushing on the way out of a failed load would issue one more INSERT,
        and if THAT fails (e.g. truncation) its SQL error replaces the real
        cause as the raised exception. A later close()/flush() is a no-op once
        the buffer is empty, and the connection is closed without commit anyway.
        """
        self.buffer.clear()

    def close(self) -> None:
        self.flush()
        self.cursor.close()


def run_writers(writers: Mapping[str, ExtWriter], load: Callable[[], T]) -> T:
    """Run ``load`` against ``writers``, flushing on success and discarding on
    failure, and close every writer either way.

    The discard is the point: it keeps the original exception — the designed,
    GDPR-safe one — as the one that surfaces, instead of a driver error from a
    doomed final INSERT.

    Closing is best-effort for the same reason. A load that failed leaves the
    connection in an error state, which is exactly when ``cursor.close()`` can
    raise — and an exception from a ``finally`` REPLACES the one in flight,
    which would reintroduce the masking this function exists to remove. So every
    writer is closed even if an earlier one failed to, and a close error is
    swallowed rather than allowed to overwrite the real cause.
    """
    try:
        result = load()
        for writer in writers.values():
            writer.flush()
        return result
    except BaseException:
        for writer in writers.values():
            writer.discard()
        raise
    finally:
        for writer in writers.values():
            try:
                writer.close()
            except Exception:  # noqa: BLE001 — see docstring: never mask the real cause
                pass


def execute_statements(conn, sql: str, *, commit: bool = True) -> None:
    """Run a ``;\\n``-separated DDL script on one cursor."""
    cursor = conn.cursor()
    try:
        for statement in sql.split(";\n"):
            if statement.strip():
                cursor.execute(statement)
        if commit:
            conn.commit()
    finally:
        cursor.close()


def ext_create_sql(
    ext_table: str,
    leading_lines: Sequence[str],
    columns: Sequence[tuple[str, int, bool]],
    *,
    delivery_date_first: bool = False,
) -> str:
    """DROP + CREATE for one ext landing table.

    ``leading_lines`` are the source's own delivery-stamp columns, already
    rendered, and ``columns`` is ``(name, nvarchar_width, not_null)`` per source
    column. The delivery date and the two audit columns are appended here
    because every ext table in this layer carries them. LPR3 puts the cutoff
    alongside its other leading stamps via ``delivery_date_first``.
    """
    lines = list(leading_lines)
    if delivery_date_first:
        lines.append(_TRAILING_LINES[0])
    for name, width, not_null in columns:
        lines.append(f"[{name}] NVARCHAR({width}) {'NOT NULL' if not_null else 'NULL'}")
    lines.extend(_TRAILING_LINES[1:] if delivery_date_first else _TRAILING_LINES)
    return (
        f"DROP TABLE IF EXISTS [ext].[{ext_table}];\n"
        f"CREATE TABLE [ext].[{ext_table}] (\n    " + ",\n    ".join(lines) + "\n);"
    )


# ---------------------------------------------------------------------------
# delivery registers
# ---------------------------------------------------------------------------


def upsert_register(
    cursor,
    table: str,
    keys: Sequence[str],
    values: Mapping[str, Any],
    *,
    casts: Mapping[str, str] | None = None,
    insert_only: Collection[str] = (),
    matched_condition: str | None = None,
) -> None:
    """MERGE one row into meta.<table> on ``keys``; update the other columns and dw_timestamp_update.

    A ``casts`` entry holding ``?`` is the SQL expression for the value; any other entry is a type
    for ``CAST(? AS <type>)``. ``insert_only`` columns are written on insert and never updated.
    """
    casts = casts or {}
    quoted = {column: safe_bracket_quote(column) for column in values}
    parameters = {c: casts[c] if "?" in casts[c] else f"CAST(? AS {casts[c]})" for c in casts}
    source = ", ".join(f"{parameters.get(c, '?')} AS {q}" for c, q in quoted.items())
    on = " AND ".join(f"tgt.{quoted[k]} = src.{quoted[k]}" for k in keys)
    updates = "".join(
        f"tgt.{q} = src.{q}, " for c, q in quoted.items() if c not in keys and c not in insert_only
    )
    condition = f" AND {matched_condition}" if matched_condition else ""
    columns = ", ".join(quoted.values())
    src_columns = ", ".join(f"src.{q}" for q in quoted.values())
    cursor.execute(
        f"MERGE [meta].{safe_bracket_quote(table)} AS tgt USING (SELECT {source}) AS src ON {on} "
        f"WHEN MATCHED{condition} THEN UPDATE SET {updates}"
        "tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS DATETIME2(0)) "
        f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({src_columns});",
        tuple(values.values()),
    )


# ---------------------------------------------------------------------------
# control totals and quality comparisons
# ---------------------------------------------------------------------------


def parse_export_log(text: str, delimiter: str = DELIMITER) -> dict[str, int]:
    """SDS' SDP_ExportLog as ``{file name: expected row count}``."""
    rows: dict[str, int] = {}
    for record in csv.DictReader(text.splitlines(), delimiter=delimiter):
        rows[record["FilNavn"]] = int(record["AntalRaekker"])
    return rows


def export_log_deviations(
    export_log: Mapping[str, int],
    actual_counts: Mapping[str, int],
) -> dict[str, dict[str, int | None]]:
    """Files whose loaded row count differs from the export log's.

    ``Metadata_*`` members carry no data rows and are not part of the control
    totals. A file the log announces but the load never saw is reported with
    ``indlaest: None``. The caller raises its own message on a non-empty result.

    This compares the WHOLE log, so it assumes the run loaded every data file the
    delivery announces — true for both current sources. A source that loads a
    subset per run must narrow ``export_log`` to the files that run handled
    before calling, or every unhandled file is reported as missing.
    """
    deviations: dict[str, dict[str, int | None]] = {}
    for filename, expected in export_log.items():
        if filename.startswith(_METADATA_PREFIX):
            continue
        actual = actual_counts.get(filename)
        if actual != expected:
            deviations[filename] = {"eksportlog": expected, "indlaest": actual}
    return deviations


def schema_drift(
    columns: Sequence[str],
    expected: Sequence[str],
    optional: Collection[str] = (),
) -> tuple[list[str], list[str]]:
    """``(unknown, missing)`` between a member's header and its spec.

    ``unknown`` keeps the header's order, ``missing`` the spec's, because both
    end up quoted in the caller's message. ``optional`` is the set that is
    allowed to be absent for this particular member.
    """
    unknown = [c for c in columns if c not in expected]
    missing = [c for c in expected if c not in columns and c not in optional]
    return unknown, missing


def has_mojibake(value: str) -> bool:
    """Does this value carry an unambiguous encoding-corruption marker?"""
    return bool(value) and any(marker in value for marker in MOJIBAKE_MARKERS)
