# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared SilkeborgData delivery logic — filesystem discovery, DFM helpers, parsing.

SilkeborgData drops one dated folder per delivery on a read-only SMB share that
is already mounted into the code-server; we stand on the receiving side of their
SFTP transfer and only ever read the filesystem:

    <SILKEBORGDATA_BASE_DIR>/<YYYYMMDD>/E26PERS.TXT

All 15 E26 files land in the same dated folder and are released together, so
everything here is deliberately file-agnostic: a new SD file supplies its own
FILE_NAME, column list and types, and reuses this module unchanged.

Two facts drive the design and are easy to get wrong:

1. The mount is scoped per branch — a branch whose slug is not in
   SMB_SHARE_BRANCHES on the acr-watcher has no /mnt/ftproot at all — so the
   path is resolved per run and an unset variable is a hard error, never a
   fallback. An unmounted share and "no new deliveries" look identical from the
   outside, and that is the failure mode that stays green for months.

2. The encoding is per source, never a shared constant. SilkeborgData is
   DOS-Nordic cp865; HAIBA on the SAME mount is cp1252. The two disagree about
   where æ ø å live in 0x80-0x9F, so reading one with the other's encoding
   yields wrong Danish characters rather than an error — hence errors="strict"
   and the encoding override.

The delivery folder's date is the DFM checkpoint (see the archive module), so
selection happens on folder names alone: no file is opened and the database is
not touched to decide what to load.
"""

import csv
import os
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import dagster as dg

# Injected into the code-server by the Nomad job that mounts the SMB share.
# dev/QA: "/mnt/ftproot/LØN/arkiv/E26". Never defaulted — see module docstring.
BASE_DIR_ENV = "SILKEBORGDATA_BASE_DIR"
ENCODING_ENV = "SILKEBORGDATA_FILE_ENCODING"

# DOS-Nordic, NOT cp1252. Per-source by design; see module docstring.
DEFAULT_ENCODING = "cp865"
DELIMITER = ";"

# Delivery folders are named YYYYMMDD, e.g. "20260730".
DELIVERY_SUBDIR_FMT = "%Y%m%d"

# The folder appears before its contents are finished: the 20260730 delivery was
# written over ~30 minutes (02:21-02:52). A file read mid-window archives half a
# delivery and moves the checkpoint past it, so the finished file is never
# fetched. An unsettled delivery is deferred to the next run together with
# everything newer than it — see partition_settled_deliveries() for why the
# "newer than it" half is not optional.
MIN_FILE_AGE_MINUTES = 60

INSERT_CHUNK_SIZE = 1000
_PROGRESS_EVERY = 100_000

# Stamped onto every ext row from the folder it came from. Not in the file.
LEVERANCE_COLUMNS: tuple[str, ...] = ("dw_leverance_dato", "dw_leverance_mappe")

_SD_DATE_FMT = "%Y-%m-%d"

# SAS missing-value tokens and the zero-date forms mainframe extracts emit.
_MISSING_DATE_TOKENS = frozenset({"", ".", "0000-00-00", "00000000"})
_MISSING_DECIMAL_TOKENS = frozenset({"", "."})


@dataclass(frozen=True)
class Delivery:
    """One dated delivery folder that holds the requested file."""

    delivery_date: date
    folder: str
    path: str


def silkeborgdata_dir() -> str:
    """The mounted delivery folder, or a hard failure.

    Resolved per run rather than at import time: the mount appears with the
    allocation, not with the code, so a module constant would freeze a value
    from before the mount existed. An unset variable must NOT fall back to a
    plausible-looking local path.

    An existing but EMPTY directory is also an error: a Docker bind mount whose
    source is missing silently creates one, and it would otherwise look exactly
    like "no new deliveries".

    Note the limit of that guard. A share pointed one level too high — at
    .../arkiv rather than .../arkiv/E26 — is NOT empty, so it passes here and
    then yields no matching delivery folders, which is a legitimate green no-op
    by design (a directory with entries but no YYYYMMDD folder holding the file
    genuinely means "nothing new"). Nothing stops a run in that state. The
    checkpoint_not_stalled asset check reports it downstream, but it is
    non-blocking and only warns, so the run still goes green and someone has to
    read the warning for it to have any effect.
    """
    root = os.getenv(BASE_DIR_ENV, "").strip()
    if not root:
        raise ValueError(
            f"{BASE_DIR_ENV} is unset or empty. The SilkeborgData share is mounted "
            f"per branch — this branch's slug must be added to SMB_SHARE_BRANCHES "
            f"on the acr-watcher before the code-server gets /mnt/ftproot. Locally, "
            f"point it at a manual copy or run against dev/QA."
        )
    if not os.path.isdir(root):
        raise ValueError(
            f"{BASE_DIR_ENV}={root!r} is not a directory. The SMB share is probably "
            f"not mounted in this allocation."
        )
    with os.scandir(root) as entries:
        if next(entries, None) is None:
            raise ValueError(
                f"{BASE_DIR_ENV}={root!r} is an empty directory. That is an unmounted "
                f"or wrongly pointed share, not an absence of deliveries — a bind "
                f"mount with a missing source creates exactly this. Refusing to "
                f"report 'no new deliveries'."
            )
    return root


def silkeborgdata_encoding() -> str:
    """Per-source encoding, from the environment.

    SilkeborgData is DOS-Nordic cp865; HAIBA on the same mount is cp1252. One
    shared constant would corrupt one of them.
    """
    return os.getenv(ENCODING_ENV, "").strip() or DEFAULT_ENCODING


def parse_delivery_date(dir_name: str) -> date | None:
    """Folder name -> delivery date, or None when it is not a delivery folder.

    Other folders can sit under BASE_DIR; they are skipped, not an error.
    """
    try:
        return datetime.strptime(dir_name.strip(), DELIVERY_SUBDIR_FMT).date()
    except ValueError:
        return None


def parse_checkpoint_date(value: str | None) -> date | None:
    """DFM checkpoint -> date.

    The checkpoint is an NVARCHAR(50). Normally it holds "2026-07-01", but a
    deliberate period-mode run with advance_checkpoint=True writes
    "<date_to> 23:59:59", so only the first 10 characters are parsed. Comparison
    is always between date objects — never between raw strings and never between
    folder names.

    An unreadable value raises: silently treating it as "load everything" would
    reload the whole history on every run, green.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], _SD_DATE_FMT).date()
    except ValueError as exc:
        raise ValueError(
            f"Unreadable DFM checkpoint {value!r}: expected ISO YYYY-MM-DD "
            f"(optionally followed by a time). Refusing to fall back to a full load."
        ) from exc


def discover_deliveries(base_dir: str, file_name: str) -> list[Delivery]:
    """Every delivery folder under base_dir that holds file_name.

    One os.scandir over base_dir. The file name is matched on the WHOLE name,
    case-insensitively: E26PERSF.TXT sits next to E26PERS.TXT in the same folder,
    and because neither has a header the wrong file would not necessarily fail on
    field count — it would just fill the archive with the wrong extract. A CIFS
    mount on Linux is case-sensitive by default, so the comparison is folded to
    survive SD switching to lowercase.

    Returned ascending by delivery date; callers that dedupe reverse it.
    """
    wanted = file_name.upper()
    deliveries: list[Delivery] = []
    with os.scandir(base_dir) as entries:
        for entry in entries:
            if not entry.is_dir():
                continue
            delivery_date = parse_delivery_date(entry.name)
            if delivery_date is None:
                continue
            with os.scandir(entry.path) as files:
                match = next(
                    (f for f in files if f.is_file() and f.name.upper() == wanted),
                    None,
                )
            if match is None:
                continue
            deliveries.append(
                Delivery(delivery_date=delivery_date, folder=entry.name, path=match.path)
            )
    deliveries.sort(key=lambda d: d.delivery_date)
    return deliveries


def select_deliveries(
    deliveries: Iterable[Delivery],
    run_config,
    checkpoint: str | None,
    context: dg.AssetExecutionContext | None = None,
) -> list[Delivery]:
    """Apply the run mode to the discovered deliveries.

    Pure apart from the optional logger, so the mode matrix is unit-testable
    without a share or a database. This is where the expensive bug lives: pick
    the deliveries wrongly and you either load nothing for months or reload the
    whole history every day — both of which look like a green run.

    Returned in DESCENDING date order, which is what makes "first occurrence
    wins" in the extract's dedup keep the row from the NEWEST folder.
    """
    mode = getattr(run_config, "mode", "delta")
    items = sorted(deliveries, key=lambda d: d.delivery_date, reverse=True)

    if mode == "ids":
        # Not meaningful for a flat-file source. RunModeConfig's validator makes
        # this nearly unreachable from the launchpad, but treat it as delta
        # rather than letting a future config change turn it into a silent
        # "load everything".
        if context is not None:
            context.log.warning(
                "mode='ids' is not meaningful for a flat-file source — treating it as "
                "'delta'. Use mode='period' with date_from/date_to to reload specific "
                "deliveries."
            )
        mode = "delta"

    if mode == "full_load":
        return items

    if mode == "period":
        date_from = _require_iso_date(run_config.date_from, "date_from")
        # date_to is optional in RunModeConfig: an open upper bound loads every
        # delivery from date_from onwards.
        date_to = _optional_iso_date(run_config.date_to, "date_to")
        return [
            d
            for d in items
            if d.delivery_date >= date_from and (date_to is None or d.delivery_date <= date_to)
        ]

    # delta: strictly newer than the checkpoint, so the checkpoint's own delivery
    # is not fetched again and the day after it is not skipped. No checkpoint
    # (first ever run) means take everything.
    cutoff = parse_checkpoint_date(checkpoint)
    if cutoff is None:
        return items
    return [d for d in items if d.delivery_date > cutoff]


def _require_iso_date(value: str | None, field: str) -> date:
    if not value:
        raise ValueError(f"mode='period' requires {field} (format YYYY-MM-DD).")
    return _parse_iso_date(value, field)


def _optional_iso_date(value: str | None, field: str) -> date | None:
    if not value or not value.strip():
        return None
    return _parse_iso_date(value, field)


def _parse_iso_date(value: str, field: str) -> date:
    try:
        return datetime.strptime(value.strip()[:10], _SD_DATE_FMT).date()
    except ValueError as exc:
        raise ValueError(f"{field}={value!r} is not an ISO date (YYYY-MM-DD).") from exc


def is_delivery_settled(path: str, min_age_minutes: int = MIN_FILE_AGE_MINUTES) -> bool:
    """True when the file has not been written to for min_age_minutes.

    The only guard against reading a delivery that SD is still writing. Note
    that field-count validation is NOT a guard here: a file cut exactly at a
    line break parses cleanly.
    """
    age_seconds = time.time() - os.stat(path).st_mtime
    return age_seconds >= min_age_minutes * 60


def partition_settled_deliveries(
    deliveries: Iterable[Delivery],
    min_age_minutes: int = MIN_FILE_AGE_MINUTES,
) -> tuple[list[Delivery], list[Delivery], list[Delivery]]:
    """Split a selection at the oldest delivery that is still being written.

    Returns ``(loadable, unsettled, deferred)``, each newest-first.

    A delivery folder appears before its contents are finished, so an unsettled
    folder must not be read. It must ALSO block every folder NEWER than itself,
    and that is the part which is easy to get wrong: the archive checkpoint
    becomes MAX(dw_leverance_dato) over whatever was loaded, so loading a newer
    folder moves the checkpoint PAST the unsettled one, and the delta selection —
    which takes only folders strictly newer than the checkpoint — would never
    offer it again. Dropping the unsettled folder on its own therefore loses it
    permanently rather than deferring it.

    Everything from the oldest unsettled folder onwards is left for the next run,
    by which time it has settled. The checkpoint stands still across that range,
    so nothing is lost — the run is simply a day late for those folders.

    One os.stat per delivery.
    """
    items = sorted(deliveries, key=lambda d: d.delivery_date, reverse=True)
    checked = [(d, is_delivery_settled(d.path, min_age_minutes)) for d in items]

    unsettled = [d for d, settled in checked if not settled]
    if not unsettled:
        return items, [], []

    # Folder names are YYYYMMDD, so delivery dates are unique: nothing settled
    # can share the cutoff date.
    cutoff = min(d.delivery_date for d in unsettled)
    loadable = [d for d, _ in checked if d.delivery_date < cutoff]
    deferred = [d for d, settled in checked if settled and d.delivery_date > cutoff]
    return loadable, unsettled, deferred


def parse_sd_date(
    value: str,
    *,
    column: str,
    line_no: int,
    tolerate: bool,
    stats: dict[str, int] | None = None,
) -> date | None:
    """Parse an SD date field (yyyy-mm-dd); SAS missing tokens give None.

    Never guesses across several formats and never uses dateutil: a guess can
    swap day and month without failing, which is unrecoverable once archived.
    """
    text = value.strip()
    if text in _MISSING_DATE_TOKENS:
        return None
    try:
        return datetime.strptime(text, _SD_DATE_FMT).date()
    except ValueError:
        if not tolerate:
            raise ValueError(f"line {line_no}, column {column}: not a yyyy-mm-dd date") from None
        if stats is not None:
            stats["unparsable_dates"] = stats.get("unparsable_dates", 0) + 1
        return None


def parse_sd_decimal(
    value: str,
    *,
    column: str,
    line_no: int,
    tolerate: bool,
    stats: dict[str, int] | None = None,
) -> Decimal | None:
    """Parse a COMMAX field ("1.234,56", "-0,5000") to Decimal; empty gives None.

    COMMAX is Danish notation: comma decimal separator, period thousands
    separator. Mixing the two up would scale every amount by 1000 silently,
    which is why the order below is fixed: strip thousands separators first,
    then swap the decimal comma.

    Trailing minus ("123,45-") occurs in some mainframe extracts; it is
    normalised to a leading minus and counted, so the form is visible in
    metadata rather than silently accepted.
    """
    text = value.strip()
    if text in _MISSING_DECIMAL_TOKENS:
        return None

    if text.endswith("-"):
        text = "-" + text[:-1].strip()
        if stats is not None:
            stats["trailing_minus_values"] = stats.get("trailing_minus_values", 0) + 1

    normalised = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(normalised)
    except (InvalidOperation, ValueError):
        if not tolerate:
            raise ValueError(f"line {line_no}, column {column}: not a COMMAX decimal") from None
        if stats is not None:
            stats["unparsable_decimals"] = stats.get("unparsable_decimals", 0) + 1
        return None


def open_delivery(path: str, encoding: str):
    """Open a delivery file for streaming.

    errors="strict" so a wrong encoding fails visibly instead of writing
    replacement characters into the archive.
    """
    return open(path, encoding=encoding, errors="strict", newline="")


def read_delivery_rows(path: str, encoding: str) -> Iterator[tuple[int, list[str]]]:
    """Stream (line_no, fields) from a delivery file.

    QUOTE_NONE is deliberate: a stray double quote in a name or address field
    would otherwise swallow the following lines. The consequence — a semicolon
    inside a text field shifting the fields — is caught by the field-count
    validation in the file-specific module.

    One single pass. Every pass crosses the network, and E26TILL.TXT is 35 MB,
    so "count the lines first, parse afterwards" is not an option.
    """
    with open_delivery(path, encoding) as handle:
        reader = csv.reader(handle, delimiter=DELIMITER, quoting=csv.QUOTE_NONE)
        for fields in reader:
            yield reader.line_num, fields


def write_to_ext_table(
    conn,
    table: str,
    column_types: dict[str, str],
    rows: Iterator[dict],
    context: dg.AssetExecutionContext,
    pre_commit: Callable[[int], None] | None = None,
) -> int:
    """DROP/CREATE ext.<table> and insert rows in streaming chunks.

    ext is typed exactly like arc (no constraints, no dw_timestamp_*), so the
    archive MERGE becomes a plain column copy with no implicit SQL conversions.
    Values are already converted to date/Decimal in Python.

    ``pre_commit`` is the last gate before the data becomes visible. It is called
    with the inserted row count after the final flush and BEFORE the commit; if
    it raises, the whole unit of work is rolled back and the exception
    propagates. SQL Server's DDL is transactional, so the rollback takes the
    DROP/CREATE with it and ext is left exactly as the previous run wrote it —
    nothing half-written for the archive MERGE to pick up.

    The callback cannot be replaced by a check on the caller's side: once
    write_to_ext_table has returned, the rows are committed, and a caller that
    then refuses them has to undo an archive load rather than prevent one. That
    is the difference between this gate and an asset check downstream.
    """
    qualified = f"[ext].[{table}]"
    columns = list(column_types)
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {qualified}")
        col_defs = ",\n    ".join(f"[{c}] {column_types[c]}" for c in columns)
        cursor.execute(f"CREATE TABLE {qualified} (\n    {col_defs}\n)")

        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(f"[{c}]" for c in columns)
        insert_sql = f"INSERT INTO {qualified} ({col_names}) VALUES ({placeholders})"

        total = 0
        buffer: list[tuple] = []

        def _flush() -> None:
            nonlocal total
            if buffer:
                cursor.executemany(insert_sql, buffer)
                total += len(buffer)
                buffer.clear()

        for row in rows:
            buffer.append(tuple(row.get(col) for col in columns))
            if len(buffer) >= INSERT_CHUNK_SIZE:
                _flush()
                if total % _PROGRESS_EVERY == 0:
                    context.log.info(f"  ... {total} rows inserted into {qualified}")
        _flush()

        if pre_commit is not None:
            try:
                pre_commit(total)
            except Exception:
                # Explicit, not left to the connection close: a rollback here is
                # part of the contract the docstring promises, and it also
                # releases the locks the DROP/CREATE holds before the caller
                # starts logging and raising.
                conn.rollback()
                context.log.error(
                    f"Pre-commit gate rejected the load — rolled back {total} row(s); "
                    f"{qualified} is unchanged"
                )
                raise

        conn.commit()
        context.log.info(f"Inserted {total} rows into {qualified}")
        return total
    finally:
        cursor.close()


__all__ = [
    "BASE_DIR_ENV",
    "DEFAULT_ENCODING",
    "DELIMITER",
    "DELIVERY_SUBDIR_FMT",
    "ENCODING_ENV",
    "INSERT_CHUNK_SIZE",
    "LEVERANCE_COLUMNS",
    "MIN_FILE_AGE_MINUTES",
    "Delivery",
    "discover_deliveries",
    "is_delivery_settled",
    "open_delivery",
    "parse_checkpoint_date",
    "parse_delivery_date",
    "partition_settled_deliveries",
    "parse_sd_date",
    "parse_sd_decimal",
    "read_delivery_rows",
    "select_deliveries",
    "silkeborgdata_dir",
    "silkeborgdata_encoding",
    "write_to_ext_table",
]
