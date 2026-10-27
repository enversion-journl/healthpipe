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

Delivery detection makes the daily schedule cheap: the newest usable file per
HAI type is compared against the delivery checkpoint in meta.DFMState
(unit ``haiba_leverance_<type>_<kind>``, identity ``<delivery date>|<mtime>``
via DSI/connectors/delivery_detection.py — mtime is what makes a same-named
re-drop a new delivery). The decision is ALL-OR-NOTHING per ext table: only
when NO type has a new delivery is the load skipped (no CSV rows are streamed,
ext is left untouched — the usability and header probes still open each file);
one new type reloads every type. Reloading only the new types would write a
partial ext, and the archive merges with detect_deletes on an unscoped NOT
MATCHED BY SOURCE — the missing types would be soft-deleted. The tables decide
independently: a new cases delivery does not force the agg tables to reload.
One consequence is accepted: a type SDS retires (no files left on the share)
is only soft-deleted out of the archive when the NEXT new delivery of another
type in the same table triggers a reload — with weekly deliveries that is
days, and it errs on keeping data.

Every run that skips or loads rewrites a manifest table
(``ext.<table>_leverance_manifest``): emptied on a skip, emptied again before
ext is rewritten (so a load that fails mid-write leaves it empty), and filled
only after the guards pass. A run that fails BEFORE the load starts leaves
the previous run's manifest — consistent with the untouched ext beside it.
The archive reads the manifest as its merge contract and advances the
checkpoints from it AFTER the MERGE has committed, so a failed archive run
leaves the checkpoint unmoved and the next run re-reads the delivery.

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

The invariants are ENFORCED here, at the extract, by failing the asset — not
by scoping the archive's delete detection per HAI type. Rationale: the extract
is the only layer that can tell the two absence cases apart. A type whose
candidate files are ALL unusable (header-only) is a delivery defect and fails
the run (``allow_missing_hai_types`` is the per-run operator override while
SDS fixes it). A type with NO file left on the share is a genuine retirement:
it produces no candidates, no note, and is deliberately allowed to flow so the
archive soft-deletes it — that is a real deletion, not a defect. Scoping the
archive MERGE per type instead would let a half-written ext pass silently and
would not know about the share at all.

One more guard protects the archive MERGE itself:

3. Column width: every value is checked against the arc column widths
   (``arc_widths``, parity-tested against 00_archive.sql). An over-wide value
   would otherwise abort the whole archive MERGE with error 8152, which names
   neither column nor row. A non-key over-wide value skips its row and is
   counted per (column, HAI type) — never logged by value; CPR may be among
   them. A rejection on a MERGE-key column instead fails the run: a whole
   case/aggregate is lost, not an over-long free-text field, and the archive
   would soft-delete it.

Encoding is Windows-1252 per SDS' own readme (0_laesmig_2021-11-16.txt), not
ISO-8859-1 — the difference lives in 0x80-0x9F, where cp1252 has typographic
characters and latin-1 has control characters, so misreading it corrupts text
silently instead of raising.
"""

import csv
import os
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import cast

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import share_source
from core.resources import MSSQLResource
from DSI.connectors.delivery_detection import (
    DELIVERY_SCHEMA,
    DELIVERY_TARGET_TYPE,
    delivery_identity,
    delivery_unit,
    is_new_delivery,
)
from DSI.csv_files._sds_tar import execute_statements
from pydantic import Field
from utils.sql_safety import safe_bracket_quote

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

# NVARCHAR widths from tables/haiba/00_archive.sql — parity-tested against the
# DDL file, so a width change lands in both places or fails the suite.
_AGG_WIDTHS = {
    "HAI": 50,
    "HAIBA_dato": 30,
    "Rolle_ansvar": 50,
    "RegionKode": 10,
    "SORIdentifier": 30,
    "ejerforhold": 50,
    "region": 255,
    "Sundhedsinstitution": 255,
    "Matrikel": 255,
    "OverliggendeEnhed": 255,
    "AfdelingAfsnit": 255,
}

_CASES_INFEKTION_WIDTHS = {
    "HAI_type": 50,
    "cprnr": 10,
    "HAI_date": 30,
    "HAI_Opholdsadresse": 30,
    "HAI_Kontaktansvar": 30,
    "extid": 500,
    "larlabnr": 500,
    "HAI_RegionKode": 10,
    "HAI_Region": 255,
    "HAI_ejerforhold": 50,
    "HAI_Matrikel": 255,
    "HAI_Sundhedsinstitution": 255,
    "HAI_OverliggendeEnhed": 255,
    "HAI_AfdelingAfsnit": 255,
}

_CASES_SSI_WIDTHS = {
    "HAI_type": 50,
    "cprnr": 10,
    "operation_date": 30,
    "banr": 500,
    "larlabnr": 500,
    "extid": 500,
    "Index_diagnosis": 50,
    "Index_operation": 100,
    "Index_acute": 10,
    "Index_side": 20,
    "HAI_producent": 30,
    "HAI_kontaktansvarlig": 30,
    "HAI_date": 30,
    "reo_diagnosis": 50,
    "reoperation": 255,
    "reo_date": 30,
    "reo_acute": 10,
    "reo_side": 20,
    "reo_producent": 30,
    "reo_kontaktansvarlig": 30,
    "HAI_kamme_positive_biopsies": 10,
    "HAI_kamme_total_biopsies": 10,
    "SSI_acute_365": 10,
    "SSI_acute_90": 10,
    "HAI_RegionKode": 10,
    "HAI_Region": 255,
    "HAI_ejerforhold": 50,
    "HAI_Matrikel": 255,
    "HAI_Sundhedsinstitution": 255,
    "HAI_OverliggendeEnhed": 255,
    "HAI_AfdelingAfsnit": 255,
}

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
    # NVARCHAR widths of the arc table, column for column (findings columns are
    # uniform 500 and resolved by arc_width). ext is created as NVARCHAR(500)
    # across the board, so a value that fits ext but not arc would abort the
    # whole archive MERGE with error 8152 — the extract rejects such rows up
    # front instead (counted per column and HAI type, never logged by value).
    # A parity test asserts this map matches tables/haiba/00_archive.sql.
    arc_widths: dict[str, int] = field(default_factory=dict)

    @property
    def type_column(self) -> str:
        """The column carrying the HAI type — 'HAI' on agg, 'HAI_type' on cases."""
        return "HAI" if self.kind == "agg" else "HAI_type"

    def arc_width(self, column: str) -> int | None:
        if column.startswith("findings"):
            return 500
        return self.arc_widths.get(column)

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
    arc_widths={**_AGG_WIDTHS, "antal": 50, "risikodoegn": 50},
)

AGG_ORTOPAEDI = HaibaSchema(
    name="agg_ortopaedi",
    kind="agg",
    columns=_AGG_DIMENSIONS + ("antal90", "antal365", "operationer"),
    findings_count=0,
    key_columns=_AGG_KEY,
    arc_widths={**_AGG_WIDTHS, "antal90": 50, "antal365": 50, "operationer": 50},
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
    arc_widths=_CASES_INFEKTION_WIDTHS,
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
    arc_widths=_CASES_SSI_WIDTHS,
)

SCHEMAS = (AGG_INFEKTION, AGG_ORTOPAEDI, CASES_INFEKTION, CASES_SSI)

MANIFEST_SUFFIX = "leverance_manifest"


def manifest_table(schema: HaibaSchema) -> str:
    """ext table holding this schema's delivery manifest."""
    return f"{schema.ext_table}_{MANIFEST_SUFFIX}"


def haiba_delivery_unit(hai_type: str, kind: str) -> str:
    """HAIBA's delivery unit: (HAI type, kind), in the FILENAME's vocabulary.

    The filename type is what selection sees before any file is opened — the
    HAI column disagrees on the agg CDI types (file 'CDI_COHA', column 'COHA').
    """
    return delivery_unit("haiba", hai_type, kind)


def file_mtime(path: str) -> datetime:
    """The file's mtime at whole-second precision.

    Truncated, not rounded: the value must survive the round-trip through the
    manifest's DATETIME2(0) column unchanged, or the identity the archive
    stamps would differ from the one the extract compared against.
    """
    return datetime.fromtimestamp(int(os.stat(path).st_mtime))


def detection_enabled(on_or_before: date | None, force_reload: bool) -> bool:
    """Should this run skip deliveries it has already ingested?

    Not for the two deliberate re-reads: a pin reproduces an earlier state and
    force_reload is the ad-hoc recovery run — detection would skip exactly the
    delivery the operator is trying to re-read.
    """
    return on_or_before is None and not force_reload


def pending_types(identities: dict[str, str], checkpoints: dict[str, str | None]) -> list[str]:
    """HAI types whose newest usable file is a NEW delivery.

    The caller must treat the answer as all-or-nothing for its ext table:
    non-empty reloads EVERY type in ``identities``, empty skips the whole
    load. See the module docstring for why a per-type reload is forbidden.
    """
    return sorted(
        hai_type
        for hai_type, identity in identities.items()
        if is_new_delivery(checkpoints.get(hai_type), identity)
    )


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
    allow_missing_hai_types: list[str] = Field(
        default_factory=list,
        description=(
            "HAI types allowed to have ONLY unusable (header-only) files in THIS "
            "run without failing the asset. A deliberate operator override while "
            "SDS fixes a broken delivery — the type's archive history will be "
            "soft-deleted, exactly what the guard normally prevents. A type SDS "
            "has genuinely retired needs no entry here: it has no files at all "
            "and passes the guard by itself."
        ),
    )
    force_reload: bool = Field(
        default=False,
        description=(
            "Re-read and re-stage the newest deliveries even when the delivery "
            "checkpoint says they are already in the archive. For ad-hoc "
            "recovery runs — e.g. after repairing arc by hand. The archive "
            "re-merge is a data no-op. delivery_on_or_before disables the "
            "detection by itself; this switch is not needed with a pin."
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
) -> tuple[dict[str, HaibaFile], list[str], list[str]]:
    """Newest usable file per HAI type, walking each type newest-first.

    ``on_or_before`` pins the search to deliveries up to a date, so an
    operator can reload a window SDS has since dropped. Each type still
    picks its own newest usable file within that bound.

    Returns three things: the selection; human-readable notes about every type
    where an older file had to be used (visible in asset metadata instead of
    silent); and the structured list of HAI types with NO usable file at all.
    Invariant 1's block reads that third value directly rather than re-parsing
    a log string — the guard is destructive, so it must not hinge on wording.
    """
    by_type: dict[str, list[HaibaFile]] = {}
    for file in files:
        if file.kind == kind and (on_or_before is None or file.delivery_date <= on_or_before):
            by_type.setdefault(file.hai_type, []).append(file)

    selected: dict[str, HaibaFile] = {}
    notes: list[str] = []
    unusable: list[str] = []
    for hai_type, candidates in sorted(by_type.items()):
        candidates.sort(key=lambda f: f.delivery_date, reverse=True)
        skipped: list[str] = []
        for candidate in candidates:
            if has_data_row(candidate.path):
                selected[hai_type] = candidate
                break
            skipped.append(candidate.filename)
        else:
            unusable.append(hai_type)
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
    return selected, notes, unusable


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


def unusable_types_for_schema(
    files: list[HaibaFile], unusable: list[str], schema: HaibaSchema
) -> list[str]:
    """HAI types whose candidates are ALL unusable and belong to this schema.

    ``unusable`` is select_newest's third return value — the types with no
    usable file — so no log string is parsed. select_newest runs per kind, and
    both agg schemas share kind='agg', so a type may belong to the sibling
    table; the newest candidate's header decides ownership, and a header that
    cannot be schema-detected counts as belonging so the failure surfaces on at
    least one asset instead of on neither.

    ``files`` must be the SAME cutoff-filtered set select_newest saw. Passing
    the unfiltered set would let a file NEWER than the pin decide ownership
    between the two agg schemas, disagreeing with the selection it is meant to
    explain.
    """
    mine: list[str] = []
    for hai_type in unusable:
        candidates = [f for f in files if f.kind == schema.kind and f.hai_type == hai_type]
        if not candidates:
            continue
        newest = max(candidates, key=lambda f: f.delivery_date)
        try:
            detected = detect_schema(schema.kind, _read_header(newest.path))
        except (OSError, ValueError, UnicodeDecodeError, csv.Error):
            # A corrupt header (bad encoding, malformed CSV) must fail on the
            # delivery, not crash the ownership check with an unrelated error.
            detected = None
        if detected is schema or detected is None:
            mine.append(hai_type)
    return mine


def _rows_within_arc_widths(
    schema: HaibaSchema,
    hai_type: str,
    rows: Iterator[dict[str, str | None]],
    rejections: dict[tuple[str, str], int],
) -> Iterator[dict[str, str | None]]:
    """Drop rows carrying a value wider than its arc column, counting per column.

    ext is NVARCHAR(500) across the board while arc declares real widths — one
    over-wide value (an 11-digit "CPR", a runaway SOR string) would abort the
    whole archive MERGE with error 8152, which names neither column nor row.
    GDPR: only (column, HAI type) counts leave this function; never values.
    """
    for row in rows:
        wide = [
            column
            for column, value in row.items()
            if isinstance(value, str)
            and (width := schema.arc_width(column)) is not None
            and len(value) > width
        ]
        if wide:
            for column in wide:
                rejections[(column, hai_type)] = rejections.get((column, hai_type), 0) + 1
            continue
        yield row


def _arc_active_counts(conn, schema: HaibaSchema) -> dict[str, int]:
    """Active archive rows per HAI type; {} when arc does not exist yet (first run)."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT OBJECT_ID(N'[arc].[{schema.ext_table}]', N'U')")
        if cursor.fetchone()[0] is None:
            return {}
        cursor.execute(
            f"SELECT [{schema.type_column}], COUNT(*) FROM [arc].[{schema.ext_table}] "
            f"WHERE [dw_is_deleted] = 0 GROUP BY [{schema.type_column}]"
        )
        return {str(hai_type): int(n) for hai_type, n in cursor.fetchall()}
    finally:
        cursor.close()


def _read_checkpoints(
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
    kind: str,
    hai_types: list[str],
) -> dict[str, str | None]:
    """The delivery already archived per HAI type, from meta.DFMState.

    A type with no row has never been loaded, and NULL reads as "everything
    is new".
    """
    dfm = DfmManager(mssql, context)
    checkpoints: dict[str, str | None] = {}
    with dfm.connection():
        for hai_type in sorted(hai_types):
            checkpoints[hai_type] = dfm.get_checkpoint(
                DELIVERY_SCHEMA,
                haiba_delivery_unit(hai_type, kind),
                target_type=DELIVERY_TARGET_TYPE,
            )
    return checkpoints


def _manifest_create_sql(schema: HaibaSchema) -> str:
    table = safe_bracket_quote(manifest_table(schema))
    return (
        f"DROP TABLE IF EXISTS [ext].{table};\n"
        f"CREATE TABLE [ext].{table} (\n"
        f"    [hai_type] NVARCHAR(100) NOT NULL,\n"
        f"    [kind] NVARCHAR(10) NOT NULL,\n"
        f"    [filnavn] NVARCHAR(200) NOT NULL,\n"
        f"    [{DELIVERY_COLUMN}] DATE NOT NULL,\n"
        f"    [fil_mtime] DATETIME2(0) NOT NULL,\n"
        f"    [dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()\n"
        f");"
    )


def _write_manifest_rows(
    conn, schema: HaibaSchema, files: dict[str, HaibaFile], mtimes: dict[str, datetime]
) -> None:
    """Record what this run loaded — one row per HAI type."""
    table = safe_bracket_quote(manifest_table(schema))
    cursor = conn.cursor()
    try:
        for hai_type in sorted(files):
            file = files[hai_type]
            cursor.execute(
                f"INSERT INTO [ext].{table} "
                f"([hai_type], [kind], [filnavn], [{DELIVERY_COLUMN}], [fil_mtime]) "
                f"VALUES (?, ?, ?, ?, ?)",
                hai_type,
                file.kind,
                file.filename,
                file.delivery_date.isoformat(),
                mtimes[hai_type],
            )
        conn.commit()
    finally:
        cursor.close()


def _drop_ext_table(conn, schema: HaibaSchema) -> None:
    """Remove a half-written ext snapshot so nothing downstream can merge it."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS [ext].[{schema.ext_table}]")
        conn.commit()
    finally:
        cursor.close()


def _enforce_ext_is_loadable(
    schema: HaibaSchema,
    rows_per_type: dict[str, int],
    arc_active: dict[str, int],
    rejections: dict[tuple[str, str], int],
    empty_after_width: list[str],
    context: dg.AssetExecutionContext,
) -> None:
    """The post-write guards, gathered so the caller can drop ext on any failure.

    Each of these means the archive would soft-delete rows a delivery defect
    removed from ext — the exact failure ``detect_deletes`` makes expensive.
    """
    # A width rejection on a MERGE-key column drops a whole case/aggregate, not
    # an over-long free-text field — the row is gone from ext and the archive
    # soft-deletes its history. Fail. A non-key over-wide value stays a counted
    # warning (the value is truncated context, not an identity).
    key_hits = sorted(
        f"[{col}]/{t}={n}" for (col, t), n in rejections.items() if col in schema.key_columns
    )
    if key_hits:
        raise ValueError(
            f"Width rejection hit a merge-key column in ext.{schema.ext_table}: "
            f"{', '.join(key_hits)}. A whole row is dropped, so the archive would "
            f"soft-delete it — fix the source value, do not load."
        )
    if empty_after_width:
        raise ValueError(
            f"Selected file(s) contributed 0 rows to ext.{schema.ext_table} after "
            f"width filtering: {', '.join(empty_after_width)}. Every data row was "
            f"rejected, which reads as a retirement and would soft-delete the type."
        )
    # Vocabulary drift is visible, not fatal: a delivered type absent from a
    # non-empty archive is a genuinely new type or a filename/column mismatch.
    if arc_active:
        for delivered in sorted(rows_per_type):
            if delivered not in arc_active:
                context.log.warning(
                    f"{schema.ext_table}: delivered type {delivered!r} is not among "
                    f"the archive's active types {sorted(arc_active)} — a new type or "
                    f"a vocabulary mismatch."
                )


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

    all_files = discover_files(root)
    selected, notes, unusable = select_newest(all_files, schema.kind, context, cutoff)
    # Ownership must be decided on the SAME window select_newest saw, so a file
    # newer than the pin cannot claim a type between the two agg schemas.
    cutoff_files = [f for f in all_files if cutoff is None or f.delivery_date <= cutoff]

    # Invariant 1: a type that HAS deliveries but no usable file must fail the
    # run — proceeding would soft-delete its entire archive history. A type
    # with no files at all is a genuine retirement and passes by design.
    blocked = [
        hai_type
        for hai_type in unusable_types_for_schema(cutoff_files, unusable, schema)
        if hai_type not in config.allow_missing_hai_types
    ]
    if blocked:
        raise ValueError(
            f"HAI type(s) {blocked} have deliveries on the share but no usable "
            f"file (all header-only). Loading without them would soft-delete "
            f"their archive history. Fix the delivery, pin an older one with "
            f"delivery_on_or_before, or override deliberately with "
            f"allow_missing_hai_types."
        )

    files: dict[str, HaibaFile] = {}
    for hai_type, file in selected.items():
        header = _read_header(file.path)
        if detect_schema(schema.kind, header) is schema:
            files[hai_type] = file

    if not files:
        raise ValueError(f"No {schema.kind} files matching ext.{schema.ext_table} under {root}")

    mtimes = {hai_type: file_mtime(file.path) for hai_type, file in files.items()}
    # The unit must be constructible BEFORE anything loads: after the MERGE
    # the same ValueError would leave the archive asset red with the rows
    # already committed and the checkpoint loop stopped part way.
    for hai_type in files:
        try:
            haiba_delivery_unit(hai_type, schema.kind)
        except ValueError as exc:
            raise ValueError(
                f"{files[hai_type].filename}: HAI type {hai_type!r} cannot form "
                f"a delivery-checkpoint unit — SDS has changed the naming "
                f"scheme; extend the detection before loading."
            ) from exc
    identities = {
        hai_type: delivery_identity(files[hai_type].delivery_date, mtimes[hai_type])
        for hai_type in files
    }
    new_types: list[str] = []
    if detection_enabled(cutoff, config.force_reload):
        checkpoints = _read_checkpoints(mssql, context, schema.kind, list(files))
        new_types = pending_types(identities, checkpoints)
        if not new_types:
            # The EMPTIED manifest is what tells the archive this run loaded nothing.
            with mssql.get_connection() as conn:
                execute_statements(conn, _manifest_create_sql(schema))
            context.log.info(
                f"ext.{schema.ext_table}: no new deliveries — no CSV rows are "
                f"streamed, ext is not touched"
            )
            return dg.MaterializeResult(
                metadata={
                    "besluttet": dg.MetadataValue.text("intet nyt"),
                    "row_count": dg.MetadataValue.int(0),
                    "allerede_indlaest": dg.MetadataValue.json(
                        {
                            hai_type: f"{files[hai_type].filename} = {identities[hai_type]}"
                            for hai_type in sorted(files)
                        }
                    ),
                    "fallbacks": dg.MetadataValue.json(notes or ["ingen"]),
                    "target_table": dg.MetadataValue.text(f"ext.{schema.ext_table}"),
                    "source_folder": dg.MetadataValue.text(root),
                }
            )
        context.log.info(
            f"new delivery for {', '.join(new_types)} — all {len(files)} HAI "
            f"type(s) are re-read so ext stays a complete snapshot"
        )
    else:
        context.log.warning(
            "delivery detection is off for this run — the newest deliveries "
            "are re-read regardless of the checkpoint"
        )

    rows_per_type: dict[str, int] = {}
    rejections: dict[tuple[str, str], int] = {}
    empty_after_width: list[str] = []

    def _all_rows() -> Iterator[dict[str, str | None]]:
        for hai_type in sorted(files):
            file = files[hai_type]
            context.log.info(f"Reading {file.filename} ({hai_type})")
            survived = 0
            for row in _rows_within_arc_widths(
                schema, hai_type, iter_rows(file, schema), rejections
            ):
                # Count under the row's OWN type value, not the filename's: the
                # two diverge on the agg CDI types (file 'CDI_COHA', column
                # 'COHA'), and arc is grouped by the column. type_column is a
                # merge key, never None.
                key = row[schema.type_column] or hai_type
                rows_per_type[key] = rows_per_type.get(key, 0) + 1
                survived += 1
                yield row
            if survived == 0:
                # has_data_row guaranteed >=1 raw data row, so 0 survivors means
                # every row was width-rejected — the type would vanish from ext
                # and read as a retirement, soft-deleting its archive history.
                empty_after_width.append(f"{hai_type} ({file.filename})")

    with mssql.get_connection() as conn:
        # Emptied before ext is rewritten; refilled only after the guards pass.
        execute_statements(conn, _manifest_create_sql(schema))
        row_count = _write_to_ext_table(conn, schema, _all_rows(), context)
        arc_active = _arc_active_counts(conn, schema)

        for (column, hai_type), n in sorted(rejections.items()):
            context.log.warning(
                f"{schema.ext_table}/{hai_type}: rejected {n} row(s) whose [{column}] "
                f"exceeds the arc width {schema.arc_width(column)} — counted only, "
                f"values are never logged."
            )

        try:
            _enforce_ext_is_loadable(
                schema, rows_per_type, arc_active, rejections, empty_after_width, context
            )
            _write_manifest_rows(conn, schema, files, mtimes)
        except Exception:
            # ext is already written and committed. A failed guard — or a
            # failed manifest write — otherwise leaves a snapshot behind that
            # a manual re-run of only the archive asset (routine during
            # debugging) would treat wrongly: a truncated ext would be MERGEd
            # and soft-delete the shortfall, and a full ext beside an empty
            # manifest would be declared a no-op. Drop ext so there is
            # nothing to merge; the checkpoint is unmoved, so the next run
            # re-reads the delivery.
            _drop_ext_table(conn, schema)
            raise

    deliveries = {t: f.delivery_date.isoformat() for t, f in sorted(files.items())}
    return dg.MaterializeResult(
        metadata={
            "besluttet": dg.MetadataValue.text("indlaest"),
            "nye_leverancer": dg.MetadataValue.json(new_types or ["detektion slaaet fra"]),
            "row_count": dg.MetadataValue.int(row_count),
            "hai_types": dg.MetadataValue.int(len(files)),
            "deliveries": dg.MetadataValue.json(deliveries),
            "rows_per_type": dg.MetadataValue.json(rows_per_type),
            "rejected_for_width": dg.MetadataValue.json(
                {f"{column}/{hai_type}": n for (column, hai_type), n in sorted(rejections.items())}
            ),
            "arc_active_rows": dg.MetadataValue.json(arc_active),
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
            f"1:1 into ext.{schema.ext_table}, or skip the load entirely when "
            f"no type has a delivery newer than its checkpoint. Header-only "
            f"files are skipped in favour of the previous delivery."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: HaibaExtractConfig,
    ) -> dg.MaterializeResult:
        # Log-only DFM run: ext owns no checkpoint, so the run borrows the
        # downstream arc owner's (set by execute_archive_merge). The mode is
        # deliberately locked to full_load — every SDS delivery is a complete
        # snapshot, and the archive merges with detect_deletes +
        # upstream_is_full_load, so a partial ext would soft-delete whole HAI
        # types. The delivery watermark lives in the per-unit delivery
        # checkpoints (read here, advanced by the archive) and the register
        # in meta.HAIBALeverance.
        dfm = DfmManager(mssql_target_staging, context)
        run_ctx = dfm.start_dfm_run(
            "ext",
            schema.ext_table,
            checkpoint_from=("arc", schema.ext_table),
            dagster_run_id=context.run_id,
            mode="full_load",
            **share_source(os.getenv(BASE_DIR_ENV), f"HAIBA_*_{schema.kind}_RegS_*.csv"),
        )
        try:
            result = _extract_schema(context, mssql_target_staging, schema, config)
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise
        dfm.complete_dfm_run(
            run_ctx.log_id,
            rows_affected=cast(Mapping[str, dg.IntMetadataValue], result.metadata)[
                "row_count"
            ].value,
        )
        return result

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
    "haiba_delivery_unit",
    "manifest_table",
    "unusable_types_for_schema",
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
