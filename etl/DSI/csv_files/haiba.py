# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
HAIBA extraction assets — weekly CSV deliveries from Sundhedsdatastyrelsen.

SDS drops one CSV per HAI type per week into two folders (HAIBAagg/ for the
aggregated KPI figures, HAIBAcpr/ for the CPR-level cases). Files accumulate:
~57 deliveries and 80 GB are present at any time, and old files are pruned
eventually.

The folder is a read-only SMB share, mounted into the code-server by its Nomad
job and located through HAIBA_BASE_DIR. The share is scoped per branch — a
branch that has not been opted in on the acr-watcher has no /mnt/ftproot at
all — so the path is resolved per run and an unset variable is a hard error,
never a fallback.

Every delivery is a full snapshot, not a delta. SDS' readme promises five
years of history, but the observed window is anchored to a calendar year and
rolls once a year rather than continuously: deliveries from February 2025 start
at 2020-01-01, while everything from April 2026 onwards starts at 2021-01-01.
The archive therefore sees one very large batch of disappearing rows each
January — roughly a fifth of the table — and only small corrections in
between.

Only the newest usable file per (HAI type, kind) is read, streamed straight
into ext.haiba_* and left for the archive layer to MERGE.

Two invariants protect the archive, which runs with delete detection and would
otherwise soft-delete anything missing from ext (see haiba_archive.py):

1. ext must hold EVERY known HAI type for its table, each at its own newest
   delivery — not only the types present on the newest delivery date. SDS
   skips individual types some weeks (HAIBAagg has no 2026-03-28 files at all;
   UTI was absent from 2026-01-10 to 2026-02-21).
2. A header-only file must never win as "newest". Three cpr files dated
   2026-03-28 are 197 bytes, and HAIBA_SSI_HIP_cases_RegS_2025_08_12.csv is 25
   bytes. Accepting one empties ext for that type and wipes its history from
   the archive. Files are therefore walked newest-first and the first one
   carrying an actual data row wins.

Encoding is Windows-1252 per SDS' own readme (0_laesmig_2021-11-16.txt), not
ISO-8859-1 — the difference lives in 0x80-0x9F, where cp1252 has typographic
characters and latin-1 has control characters, so misreading it corrupts text
silently instead of raising.
"""

import csv
import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime

import dagster as dg
from core.resources import MSSQLResource
from pydantic import Field

# Injected into the code-server by the Nomad job that mounts the SMB share.
# The share is scoped per branch, so a branch that has not been opted in has no
# /mnt/ftproot at all.
BASE_DIR_ENV = "HAIBA_BASE_DIR"
ENCODING_ENV = "HAIBA_FILE_ENCODING"

DEFAULT_ENCODING = "cp1252"
DELIMITER = ";"
INSERT_CHUNK_SIZE = 1000


def haiba_dir() -> str:
    """The mounted delivery folder, or a hard failure.

    Resolved per run rather than at import time: the mount appears with the
    allocation, not with the code. An unset variable must NOT fall back to a
    plausible-looking local path — an unmounted share and "no new deliveries"
    look identical from the outside, and that is the failure mode that stays
    green for months.
    """
    root = os.getenv(BASE_DIR_ENV, "").strip()
    if not root:
        raise ValueError(
            f"{BASE_DIR_ENV} is unset or empty. The HAIBA share is mounted per "
            f"branch — this branch's slug must be added to SMB_SHARE_BRANCHES on "
            f"the acr-watcher before the code-server gets /mnt/ftproot."
        )
    if not os.path.isdir(root):
        raise ValueError(
            f"{BASE_DIR_ENV}={root!r} is not a directory. The SMB share is "
            f"probably not mounted in this allocation."
        )
    return root


def haiba_encoding() -> str:
    """Per-source encoding, from the environment.

    HAIBA is cp1252 per SDS' own readme; the SilkeborgData share on the same
    mount is DOS-Nordic cp865. One shared constant would corrupt one of them.
    """
    return os.getenv(ENCODING_ENV, "").strip() or DEFAULT_ENCODING


# Only real deliveries match. The folders also hold 0_laesmig_2021-11-16.txt
# (SDS' readme — note the date in the name), nyTVHtest.txt and
# "tom CPR testfil.txt", all of which must be ignored rather than fail.
FILE_RE = re.compile(
    r"^HAIBA_(?P<hai_type>.+?)_(?P<kind>agg|cases)_RegS_"
    r"(?P<delivery>\d{4}[-_]\d{2}[-_]\d{2})\.csv$"
)

# Delivery-date column added to every ext table, derived from the filename.
DELIVERY_COLUMN = "leverance_dato"

_AGG_DIMENSIONS = (
    "HAI",
    "HAIBA_dato",
    "Rolle_ansvar",
    "ejerforhold",
    "RegionKode",
    "region",
    "Sundhedsinstitution",
    "Matrikel",
    "OverliggendeEnhed",
    "AfdelingAfsnit",
    "SORIdentifier",
)

# Verified unique across all eight agg deliveries of 2026-08-15 — zero
# duplicates, up to 2,138,224 rows in CDI_COHA. SORIdentifier is empty on
# 0.4% of the infection rows but up to 85% of Acute_HIP, because it is empty
# exactly when the row is a national aggregate (RegionKode 8888/9999), and the
# orthopedic files are dominated by those. RegionKode alone separates 8888 from
# 9999, so the key holds without widening it to the unit-name columns.
_AGG_KEY = ("HAI", "HAIBA_dato", "Rolle_ansvar", "RegionKode", "SORIdentifier")

_CASES_DIMENSIONS = (
    "HAI_type",
    "cprnr",
    "HAI_date",
    "HAI_Opholdsadresse",
    "HAI_Kontaktansvar",
    "extid",
    "larlabnr",
    "HAI_RegionKode",
    "HAI_Region",
    "HAI_ejerforhold",
    "HAI_Matrikel",
    "HAI_Sundhedsinstitution",
    "HAI_OverliggendeEnhed",
    "HAI_AfdelingAfsnit",
)

_SSI_DIMENSIONS = (
    "banr",
    "larlabnr",
    "extid",
    "Index_diagnosis",
    "Index_operation",
    "cprnr",
    "operation_date",
    "Index_acute",
    "Index_side",
    "HAI_producent",
    "HAI_kontaktansvarlig",
    "HAI_date",
    "reo_diagnosis",
    "reoperation",
    "reo_date",
    "reo_acute",
    "reo_side",
    "reo_producent",
    "reo_kontaktansvarlig",
    "HAI_kamme_positive_biopsies",
    "HAI_kamme_total_biopsies",
    "SSI_acute_365",
    "SSI_acute_90",
    "HAI_type",
    "HAI_RegionKode",
    "HAI_Region",
    "HAI_ejerforhold",
    "HAI_Matrikel",
    "HAI_Sundhedsinstitution",
    "HAI_OverliggendeEnhed",
    "HAI_AfdelingAfsnit",
)


@dataclass(frozen=True)
class HaibaSchema:
    """One ext table and the columns it accepts.

    ``findings_count`` is the MAXIMUM across HAI types, not what any single
    file carries: BACT ships findings1-7, UTI currently ships 4 (it shipped 6
    in February 2025), and the CDI types ship none. Columns are therefore
    matched by header NAME and the absent ones land as NULL.
    """

    name: str
    kind: str
    columns: tuple[str, ...]
    findings_count: int
    sas_date_columns: tuple[str, ...] = ()
    # Columns the archive MERGEs on. They keep '' rather than NULL: SQL Server
    # cannot put a NULL column in a primary key, and a NULL in the ON clause
    # never matches itself, so the MERGE would re-insert the row every run
    # until the PK blew up. SORIdentifier is empty on 0.5% of agg rows (the
    # RegionKode 8888/9999 aggregates), so this is not hypothetical.
    key_columns: tuple[str, ...] = ()

    @property
    def ext_table(self) -> str:
        return f"haiba_{self.name}"

    @property
    def all_columns(self) -> tuple[str, ...]:
        findings = tuple(f"findings{i}" for i in range(1, self.findings_count + 1))
        return self.columns + findings + (DELIVERY_COLUMN,)


AGG_INFEKTION = HaibaSchema(
    name="agg_infektion",
    kind="agg",
    columns=_AGG_DIMENSIONS + ("antal", "risikodoegn"),
    findings_count=0,
    key_columns=_AGG_KEY,
)

AGG_ORTOPAEDI = HaibaSchema(
    name="agg_ortopaedi",
    kind="agg",
    columns=_AGG_DIMENSIONS + ("antal90", "antal365", "operationer"),
    findings_count=0,
    key_columns=_AGG_KEY,
)

CASES_INFEKTION = HaibaSchema(
    name="cases_infektion",
    kind="cases",
    columns=_CASES_DIMENSIONS,
    findings_count=7,
    # Verified unique across all four case files. extid and larlabnr are
    # deliberately left out: they hold space-separated lists of lab ids with no
    # upper bound, which do not belong in a clustered key.
    key_columns=("HAI_type", "cprnr", "HAI_date"),
)

CASES_SSI = HaibaSchema(
    name="cases_ssi",
    kind="cases",
    columns=_SSI_DIMENSIONS,
    findings_count=9,
    # HAI_date is ISO here, but these two are still SAS-formatted.
    sas_date_columns=("operation_date", "reo_date"),
    # banr is left out for the same reason as extid/larlabnr on the
    # infection cases: it holds a space-separated list with no upper bound
    # (78 characters in the 2026-08-08 delivery, against the 8 the early
    # profiling suggested). (HAI_type, cprnr, operation_date) is unique on
    # its own in both SSI files.
    key_columns=("HAI_type", "cprnr", "operation_date"),
)

SCHEMAS = (AGG_INFEKTION, AGG_ORTOPAEDI, CASES_INFEKTION, CASES_SSI)

_SAS_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_SAS_RE = re.compile(r"^(\d{2})([A-Z]{3})(\d{4}):(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$")


class HaibaExtractConfig(dg.Config):
    """Run-time config for the HAIBA extract assets."""

    delivery_on_or_before: str | None = Field(
        default=None,
        description=(
            "Pin the load to the newest usable delivery on or before this date "
            '("YYYY-MM-DD"), instead of the newest one overall. Used to backfill '
            "years SDS has since dropped from its rolling window — e.g. "
            '"2025-12-27", the last delivery that still carried 2020. Leave unset '
            "for normal operation; a pin left in place would make the archive "
            "treat an old delivery as current."
        ),
    )


@dataclass(frozen=True)
class HaibaFile:
    """A discovered delivery file."""

    path: str
    hai_type: str
    kind: str
    delivery_date: date

    @property
    def filename(self) -> str:
        return os.path.basename(self.path)


def parse_filename(name: str) -> tuple[str, str, date] | None:
    """(hai_type, kind, delivery_date) for a delivery file, else None.

    Both date spellings occur: HAIBAagg uses 2026-08-15, HAIBAcpr uses
    2026_08_08.
    """
    match = FILE_RE.match(name)
    if not match:
        return None
    try:
        delivery = date.fromisoformat(match.group("delivery").replace("_", "-"))
    except ValueError:
        return None
    return match.group("hai_type"), match.group("kind"), delivery


def parse_sas_datetime(value: str) -> str:
    """10FEB2023:13:00:00.000 -> 2023-02-10 13:00:00.

    Values that are not SAS-formatted (ISO, empty) pass through untouched, so
    the column keeps working if SDS switches format.
    """
    if not value:
        return value
    match = _SAS_RE.match(value.strip().upper())
    if not match:
        return value
    day, mon, year, hour, minute, second = match.groups()
    month = _SAS_MONTHS.get(mon)
    if month is None:
        return value
    try:
        stamp = datetime(int(year), month, int(day), int(hour), int(minute), int(second))
    except ValueError:
        return value
    return stamp.strftime("%Y-%m-%d %H:%M:%S")


def detect_schema(kind: str, header: list[str]) -> HaibaSchema | None:
    """Pick the target schema from the header rather than the HAI type.

    SDS adds and retires HAI types (CDI_HOHA, SSI_*), so keying off a hardcoded
    type list would reject a new type instead of routing it. The measure
    columns are what actually separate the schemas.
    """
    columns = set(header)
    if kind == "agg":
        if "antal90" in columns or "operationer" in columns:
            return AGG_ORTOPAEDI
        if "antal" in columns:
            return AGG_INFEKTION
        return None
    if "banr" in columns or "operation_date" in columns:
        return CASES_SSI
    if "HAI_type" in columns:
        return CASES_INFEKTION
    return None


def _read_header(path: str) -> list[str]:
    with open(path, encoding=haiba_encoding(), newline="") as handle:
        reader = csv.reader(handle, delimiter=DELIMITER)
        for row in reader:
            return [c.strip() for c in row]
    return []


def has_data_row(path: str) -> bool:
    """True when the file carries at least one non-empty data row.

    This is invariant 2. Header-only files (197 bytes) are a real, recurring
    delivery defect, and accepting one as the newest file empties ext for that
    HAI type — which the archive then reads as "SDS deleted everything".
    """
    with open(path, encoding=haiba_encoding(), newline="") as handle:
        reader = csv.reader(handle, delimiter=DELIMITER)
        next(reader, None)  # header
        for row in reader:
            if any(cell.strip() for cell in row):
                return True
    return False


def discover_files(root: str) -> list[HaibaFile]:
    """Every delivery file under root, both subfolders."""
    found: list[HaibaFile] = []
    for dirpath, _, names in os.walk(root):
        for name in names:
            parsed = parse_filename(name)
            if parsed is None:
                continue
            hai_type, kind, delivery = parsed
            found.append(HaibaFile(os.path.join(dirpath, name), hai_type, kind, delivery))
    return found


def select_newest(
    files: list[HaibaFile],
    kind: str,
    context: dg.AssetExecutionContext | None = None,
    on_or_before: date | None = None,
) -> tuple[dict[str, HaibaFile], list[str]]:
    """Newest usable file per HAI type, walking each type newest-first.

    ``on_or_before`` pins the search to deliveries up to a date, so an
    operator can reload a window SDS has since dropped. Each type still
    picks its own newest usable file within that bound.

    Returns the selection plus notes about every type where an older file had
    to be used, so the fallback is visible in asset metadata instead of silent.
    """
    by_type: dict[str, list[HaibaFile]] = {}
    for file in files:
        if file.kind == kind and (on_or_before is None or file.delivery_date <= on_or_before):
            by_type.setdefault(file.hai_type, []).append(file)

    selected: dict[str, HaibaFile] = {}
    notes: list[str] = []
    for hai_type, candidates in sorted(by_type.items()):
        candidates.sort(key=lambda f: f.delivery_date, reverse=True)
        skipped: list[str] = []
        for candidate in candidates:
            if has_data_row(candidate.path):
                selected[hai_type] = candidate
                break
            skipped.append(candidate.filename)
        else:
            note = f"{hai_type}: no usable file among {len(candidates)} deliveries"
            notes.append(note)
            if context:
                context.log.error(note)
            continue
        if skipped:
            note = (
                f"{hai_type}: fell back to {selected[hai_type].filename} — "
                f"skipped empty {', '.join(skipped)}"
            )
            notes.append(note)
            if context:
                context.log.warning(note)
    return selected, notes


def iter_rows(file: HaibaFile, schema: HaibaSchema) -> Iterator[dict[str, str | None]]:
    """Stream one file as ext rows.

    Columns are matched by header name — never by position — because the
    findings count differs between HAI types and changes over time.
    """
    delivery = file.delivery_date.isoformat()
    known = set(schema.all_columns)
    with open(file.path, encoding=haiba_encoding(), newline="") as handle:
        reader = csv.reader(handle, delimiter=DELIMITER)
        header = [c.strip() for c in next(reader, [])]
        unknown = [c for c in header if c and c not in known]
        if unknown:
            # SDS changes schemas without notice. Dropping columns silently
            # would lose data no one notices until a KPI is wrong.
            raise ValueError(
                f"{file.filename}: unknown column(s) {unknown} not in "
                f"ext.{schema.ext_table} — SDS may have changed the schema"
            )
        for raw in reader:
            if not any(cell.strip() for cell in raw):
                continue
            if len(raw) != len(header):
                # A short row would silently drop its trailing columns and a
                # long one would drop data on the floor — both land as a wrong
                # KPI rather than an error.
                raise ValueError(
                    f"{file.filename} line {reader.line_num}: {len(raw)} fields, "
                    f"expected {len(header)}"
                )
            row: dict[str, str | None] = {}
            for column, value in zip(header, raw, strict=True):
                value = value.strip()
                if column in schema.sas_date_columns:
                    value = parse_sas_datetime(value)
                row[column] = value if column in schema.key_columns else (value or None)
            row[DELIVERY_COLUMN] = delivery
            yield row


def _write_to_ext_table(
    conn, schema: HaibaSchema, rows: Iterator[dict[str, str | None]], context
) -> int:
    """Drop/create ext.haiba_* and insert rows in streaming chunks."""
    table = f"[ext].[{schema.ext_table}]"
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        col_defs = []
        for col in schema.all_columns:
            if col == DELIVERY_COLUMN:
                col_defs.append(f"[{col}] DATE NOT NULL")
            elif col in schema.key_columns:
                col_defs.append(f"[{col}] NVARCHAR(500) NOT NULL")
            else:
                col_defs.append(f"[{col}] NVARCHAR(500) NULL")
        col_defs.append("[dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
        col_defs.append("[dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
        cursor.execute(f"CREATE TABLE {table} (\n    {','.join(col_defs)}\n)")

        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        columns = schema.all_columns
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(f"[{c}]" for c in columns)
        insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

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
                if total % 100_000 == 0:
                    context.log.info(f"  ... {total} rows inserted into {table}")
        _flush()

        conn.commit()
        context.log.info(f"Inserted {total} rows into {table}")
        return total
    finally:
        cursor.close()


def _extract_schema(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    schema: HaibaSchema,
    config: HaibaExtractConfig,
) -> dg.MaterializeResult:
    root = haiba_dir()

    cutoff = None
    if config.delivery_on_or_before:
        cutoff = date.fromisoformat(config.delivery_on_or_before)
        context.log.warning(
            f"PINNED to deliveries on or before {cutoff} — this is a backfill load, "
            f"not the current state. Running the archive in its normal mode against "
            f"a pinned extract would overwrite current values and soft-delete "
            f"everything newer."
        )

    selected, notes = select_newest(discover_files(root), schema.kind, context, cutoff)

    files: dict[str, HaibaFile] = {}
    for hai_type, file in selected.items():
        header = _read_header(file.path)
        if detect_schema(schema.kind, header) is schema:
            files[hai_type] = file

    if not files:
        raise ValueError(f"No {schema.kind} files matching ext.{schema.ext_table} under {root}")

    def _all_rows() -> Iterator[dict[str, str | None]]:
        for hai_type in sorted(files):
            file = files[hai_type]
            context.log.info(f"Reading {file.filename} ({hai_type})")
            yield from iter_rows(file, schema)

    with mssql.get_connection() as conn:
        row_count = _write_to_ext_table(conn, schema, _all_rows(), context)

    deliveries = {t: f.delivery_date.isoformat() for t, f in sorted(files.items())}
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "hai_types": dg.MetadataValue.int(len(files)),
            "deliveries": dg.MetadataValue.json(deliveries),
            "source_files": dg.MetadataValue.json(sorted(f.filename for f in files.values())),
            "fallbacks": dg.MetadataValue.json(notes),
            "target_table": dg.MetadataValue.text(f"ext.{schema.ext_table}"),
            "source_folder": dg.MetadataValue.text(root),
            "pinned_to": dg.MetadataValue.text(config.delivery_on_or_before or "(newest)"),
        }
    )


def _build_asset(schema: HaibaSchema):
    @dg.asset(
        name=f"ext_{schema.ext_table}",
        group_name="haiba_extract",
        compute_kind="csv",
        tags={"domain": "haiba", "layer": "extract", "source": "haiba"},
        description=(
            f"Load the newest usable HAIBA {schema.kind} delivery per HAI type "
            f"1:1 into ext.{schema.ext_table}. Header-only files are skipped in "
            f"favour of the previous delivery."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: HaibaExtractConfig,
    ) -> dg.MaterializeResult:
        return _extract_schema(context, mssql_target_staging, schema, config)

    return _asset


ext_haiba_agg_infektion = _build_asset(AGG_INFEKTION)
ext_haiba_agg_ortopaedi = _build_asset(AGG_ORTOPAEDI)
ext_haiba_cases_infektion = _build_asset(CASES_INFEKTION)
ext_haiba_cases_ssi = _build_asset(CASES_SSI)

haiba_assets = [
    ext_haiba_agg_infektion,
    ext_haiba_agg_ortopaedi,
    ext_haiba_cases_infektion,
    ext_haiba_cases_ssi,
]

__all__ = [
    "AGG_INFEKTION",
    "AGG_ORTOPAEDI",
    "CASES_INFEKTION",
    "CASES_SSI",
    "DELIVERY_COLUMN",
    "SCHEMAS",
    "haiba_dir",
    "haiba_encoding",
    "HaibaExtractConfig",
    "HaibaFile",
    "HaibaSchema",
    "detect_schema",
    "discover_files",
    "ext_haiba_agg_infektion",
    "ext_haiba_agg_ortopaedi",
    "ext_haiba_cases_infektion",
    "ext_haiba_cases_ssi",
    "haiba_assets",
    "has_data_row",
    "iter_rows",
    "parse_filename",
    "parse_sas_datetime",
    "select_newest",
]
