# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DRG-grupperet LPR extraction assets — tar.bz2 deliveries from SDS.

SDS drops full snapshots per (leverancetype, takstsystem) on the NGC share:
`Region_1083_<DRG|DRGPSYK>_<T24|T25|T26>[_<M|U>]_<timestamp>.tar.bz2`. Files
accumulate; the NEWEST file per (leverancetype, takstsystem) is authoritative
regardless of cadence (decision on [internal ticket] — M and U are packaging
cadences of the same LPR update track, but M covers THREE activity years and
U only two, which the archive handles with year-scoped delete detection, see
drg_archive.py).

The share is a read-only SMB mount located through DRG_BASE_DIR — resolved
per run, and an unset variable is a hard error, never a fallback (an
unmounted share and "no new deliveries" must not look alike).

Each tar contains one folder (T2x/) with semicolon-separated, fully quoted
cp1252 CSVs plus SDS' own column schemas (Metadata_*.csv) and an export log
(SDP_ExportLog_*.csv) with the row count per file. Members are streamed
straight from the tar into the ext tables — the ~16 GB somatik deliveries are
never unpacked to disk. Row counts are validated against the export log after
each archive: a mismatch is a delivery defect and fails the run.

ext is a 1:1 mirror of the source INCLUDING the per-delivery integer IDs —
they are needed for the intra-delivery UUID resolution views in
drg_archive.py, and ext is the debugging surface for the latest delivery.
The archive layer deliberately excludes them (review decision on [internal ticket]).

Key columns are validated against their declared max width so the narrow
archive keys can never abort a MERGE with a truncation error (the HAIBA
lesson from [internal ticket]). GDPR: the files carry raw CPR — row contents are never
logged or reported, only file names, counts and delivery dates.
"""

import csv
import io
import os
import re
import tarfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import TypedDict

import dagster as dg
from core.resources import MSSQLResource
from DSI.csv_files.drg_datasets import (
    MIN_VALUE_WIDTH,
    PSYK_DATASETS,
    SOMATIK_DATASETS,
    DrgDataset,
)
from pydantic import Field

# Injected by the Nomad job that mounts the SMB share (per-branch opt-in via
# SMB_SHARE_BRANCHES on the acr-watcher). Points at .../Fra-SDS.
BASE_DIR_ENV = "DRG_BASE_DIR"
ENCODING_ENV = "DRG_FILE_ENCODING"

DEFAULT_ENCODING = "cp1252"
DELIMITER = ";"
INSERT_CHUNK_SIZE = 1000
TAKSTSYSTEM_COLUMN = "takstsystem"
DELIVERY_COLUMN = "leverance_dato"

# Forventet kadence: somatik faar U-leverancer ugentligt (mandag)
# og M ca. d. 14.-18.; psyk kun M ca. d. 12. Graenserne giver slack for
# helligdage. Kun den FRISKESTE leverance pr. leverancetype vurderes —
# pensionerede takstsystemer (fx somatik T24) faar aldrig nye filer.
MAX_DELIVERY_AGE_DAYS = {"DRG": 10, "DRGPSYK": 40}

# Encoding-sanity: U+FFFD opstaar naar afsenderen selv har
# erstattet udecodbare tegn; "ï¿½" er U+FFFD's UTF-8-bytes fejllaest som
# cp1252 (dvs. filen var i virkeligheden UTF-8). Begge er entydige
# korruptionsmarkoerer uden falske positiver i danske sundhedsdata.
_MOJIBAKE_MARKERS = ("�", "ï¿½")

# Region_1083_DRG_T26_U_20260824190546.tar.bz2 (kadence optional on the
# oldest files: Region_1083_DRG_T24_20250812235425.tar.bz2).
ARCHIVE_RE = re.compile(
    r"^Region_1083_(?P<leverancetype>DRG|DRGPSYK)_(?P<takstsystem>T\d{2})"
    r"(?:_(?P<kadence>[MU]))?_(?P<timestamp>\d{14})\.tar\.bz2$"
)

# Data member: T26_DRGKONTAKT_1083_20260823.csv / T26_Psyk_kontakt_1083_20260810.csv
DATA_MEMBER_RE = re.compile(r"^T\d{2}_(?P<prefix>.+)_1083_\d{8}\.csv$")


def drg_dir() -> str:
    """The mounted delivery folder, or a hard failure.

    Resolved per run rather than at import time: the mount appears with the
    allocation, not with the code, and an unset variable must never fall back
    to a plausible-looking local path.
    """
    root = os.getenv(BASE_DIR_ENV, "").strip()
    if not root:
        raise ValueError(
            f"{BASE_DIR_ENV} is unset or empty. The DRG share is mounted per "
            f"branch — this branch's slug must be added to SMB_SHARE_BRANCHES "
            f"on the acr-watcher before the code-server gets /mnt/ftproot."
        )
    if not os.path.isdir(root):
        raise ValueError(
            f"{BASE_DIR_ENV}={root!r} is not a directory. The SMB share is "
            f"probably not mounted in this allocation."
        )
    return root


def drg_encoding() -> str:
    """Per-source encoding from the environment; SDS delivers cp1252."""
    return os.getenv(ENCODING_ENV, "").strip() or DEFAULT_ENCODING


@dataclass(frozen=True)
class DrgArchiveFile:
    """One tar.bz2 delivery on the share."""

    path: str
    leverancetype: str
    takstsystem: str
    kadence: str | None
    timestamp: datetime

    @property
    def filename(self) -> str:
        return os.path.basename(self.path)

    @property
    def delivery_date(self) -> date:
        return self.timestamp.date()


class DrgExtractConfig(dg.Config):
    takstsystemer: list[str] | None = Field(
        default=None,
        description=(
            "Limit the run to these takstsystemer (e.g. ['T26']). Default: every "
            "takstsystem present in the delivery folder. NOTE: the archive's "
            "delete detection is scoped to the (takstsystem, aktivitetsaar) "
            "slices actually present in ext, so a filtered run only refreshes "
            "the filtered slices."
        ),
    )
    delivery_on_or_before: str | None = Field(
        default=None,
        description=(
            "Pin the run to deliveries packed on/before this ISO date "
            "(YYYY-MM-DD) for reproducing an earlier state. Default: newest."
        ),
    )
    max_delivery_age_days: int | None = Field(
        default=None,
        description=(
            "Override the staleness limit (days) for the newest usable "
            "delivery. Defaults per leverancetype: DRG=10 (U is weekly), "
            "DRGPSYK=40 (M is monthly). The check is skipped when "
            "delivery_on_or_before is set — a pinned run is deliberately old."
        ),
    )


def parse_archive_name(name: str) -> tuple[str, str, str | None, datetime] | None:
    match = ARCHIVE_RE.match(name)
    if not match:
        return None
    try:
        timestamp = datetime.strptime(match["timestamp"], "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return match["leverancetype"], match["takstsystem"], match["kadence"], timestamp


def discover_archives(root: str, leverancetype: str) -> list[DrgArchiveFile]:
    """Every parseable delivery for the leverancetype, Arkiv/ excluded."""
    found: list[DrgArchiveFile] = []
    for name in sorted(os.listdir(root)):
        parsed = parse_archive_name(name)
        if parsed is None:
            continue
        ltype, takstsystem, kadence, timestamp = parsed
        if ltype != leverancetype:
            continue
        found.append(
            DrgArchiveFile(
                path=os.path.join(root, name),
                leverancetype=ltype,
                takstsystem=takstsystem,
                kadence=kadence,
                timestamp=timestamp,
            )
        )
    return found


def is_usable(
    archive: DrgArchiveFile, datasets: tuple[DrgDataset, ...], encoding: str
) -> str | None:
    """Cheap pre-flight: stream the tar start and check the export log.

    Returns None when usable, otherwise a reason. Metadata files and the
    export log precede the data files, so only the first few MB are
    decompressed. A delivery is unusable when the export log is missing or
    reports zero rows for a mandatory dataset (the header-only lesson from
    HAIBA: an empty delivery must never win as "newest").
    """
    expected_prefixes = {d.source_prefix for d in datasets}
    try:
        with tarfile.open(archive.path, mode="r|bz2") as tar:
            for member in tar:
                base = os.path.basename(member.name)
                if not base.startswith("SDP_ExportLog"):
                    continue
                handle = tar.extractfile(member)
                if handle is None:
                    return "SDP_ExportLog kunne ikke laeses"
                rows = _parse_export_log(handle.read().decode(encoding))
                for prefix in expected_prefixes:
                    counts = [n for f, n in rows.items() if _dataset_prefix(f) == prefix]
                    if not counts:
                        return f"datasaettet {prefix} mangler i eksportloggen"
                    if max(counts) == 0 and prefix != "LEVERANCEINFO":
                        return f"datasaettet {prefix} har 0 raekker"
                return None
    except (tarfile.TarError, OSError, EOFError) as exc:
        return f"arkivet kunne ikke aabnes: {exc}"
    return "SDP_ExportLog mangler i arkivet"


def select_newest(
    archives: list[DrgArchiveFile],
    datasets: tuple[DrgDataset, ...],
    encoding: str,
    context: dg.AssetExecutionContext | None = None,
    on_or_before: date | None = None,
    takstsystemer: list[str] | None = None,
) -> tuple[dict[str, DrgArchiveFile], list[str]]:
    """Newest USABLE delivery per takstsystem, regardless of cadence.

    Walks candidates newest-first and takes the first that passes the
    pre-flight. Fallbacks are reported as notes so they are visible in the
    Dagster UI instead of silent — accepting an unusable file as "newest"
    would soft-delete the takstsystem's year slices in the archive.
    """
    notes: list[str] = []
    selected: dict[str, DrgArchiveFile] = {}
    by_takst: dict[str, list[DrgArchiveFile]] = {}
    for archive in archives:
        if takstsystemer is not None and archive.takstsystem not in takstsystemer:
            continue
        if on_or_before is not None and archive.delivery_date > on_or_before:
            continue
        by_takst.setdefault(archive.takstsystem, []).append(archive)

    for takstsystem in sorted(by_takst):
        candidates = sorted(by_takst[takstsystem], key=lambda a: a.timestamp, reverse=True)
        for candidate in candidates:
            reason = is_usable(candidate, datasets, encoding)
            if reason is None:
                selected[takstsystem] = candidate
                break
            note = f"{takstsystem}: springer {candidate.filename} over ({reason})"
            notes.append(note)
            if context is not None:
                context.log.warning(note)
        else:
            note = f"{takstsystem}: INGEN brugbar leverance fundet"
            notes.append(note)
            if context is not None:
                context.log.error(note)
    return selected, notes


def check_delivery_age(
    selected: dict[str, DrgArchiveFile],
    discovered_takstsystemer: set[str],
    leverancetype: str,
    today: date,
    max_age_days: int | None = None,
) -> dict[str, int]:
    """Foraeldet leverance-alarm: det AKTIVE takstsystem skal have
    en frisk, brugbar leverance.

    Det aktive takstaar er altid det hoejeste takstsystem paa sharet (T26 >
    T25 > ...): pensionerede takstsystemer faar aldrig nye filer og er
    forventet gamle, og en sen genleverance paa et gammelt takstsystem maa
    ikke skjule, at det produktive spor er doedt. Er det hoejeste takstsystem
    slet ikke i `selected` (alle dets filer afvist af pre-flighten), er det
    samme alarm. Fejler haardt frem for at advare — en foraeldet/manglende
    nyeste fil betyder at SMB-mountet er defekt, at SDS er holdt op med at
    levere, eller at pre-flighten afviser alt.
    Returnerer alderen i dage pr. takstsystem til asset-metadata.
    """
    limit = max_age_days if max_age_days is not None else MAX_DELIVERY_AGE_DAYS[leverancetype]
    ages = {t: (today - archive.delivery_date).days for t, archive in selected.items()}
    if not discovered_takstsystemer:
        return ages
    active = max(discovered_takstsystemer)
    if active not in selected:
        raise ValueError(
            f"{leverancetype}: det aktive takstsystem {active} har INGEN "
            f"brugbar leverance (alle filer afvist af pre-flighten) — "
            f"undersoeg leverancerne paa sharet."
        )
    if ages[active] > limit:
        raise ValueError(
            f"{leverancetype}: det aktive takstsystem {active}s nyeste "
            f"brugbare leverance er {ages[active]} dage gammel (graense: "
            f"{limit}). Er SMB-sharet mountet, og leverer SDS stadig? "
            f"Overstyr med max_delivery_age_days hvis alderen er forklaret."
        )
    return ages


def _dataset_prefix(csv_filename: str) -> str | None:
    match = DATA_MEMBER_RE.match(csv_filename)
    return match["prefix"] if match else None


def _parse_export_log(text: str) -> dict[str, int]:
    rows: dict[str, int] = {}
    for record in csv.DictReader(text.splitlines(), delimiter=DELIMITER):
        rows[record["FilNavn"]] = int(record["AntalRaekker"])
    return rows


def _parse_metadata(text: str) -> list[str]:
    return [
        record["column_name"] for record in csv.DictReader(text.splitlines(), delimiter=DELIMITER)
    ]


def validate_member_schema(
    dataset: DrgDataset, columns: list[str], filename: str, takstsystem: str | None = None
) -> None:
    """SDS' own Metadata_* schema against the union spec — fail loudly on drift.

    Optional columns may only be missing for the takstsystemer where they are
    KNOWN to be missing (T24/T25). A column that disappears from a newer
    takstsystem's schema is a regression: the merge would overwrite the
    archived values with NULL across the whole slice.
    """
    unknown = [c for c in columns if c not in dataset.source_columns]
    if unknown:
        raise ValueError(
            f"{filename}: ukendte kolonner {unknown} — SDS har aendret skemaet. "
            f"Opdater drg_datasets.py (og arc-DDL) foer indlaesning."
        )
    allowed_missing = (
        set(dataset.optional_columns)
        if takstsystem is None or takstsystem in dataset.optional_takstsystemer
        else set()
    )
    missing = [c for c in dataset.source_columns if c not in columns and c not in allowed_missing]
    if missing:
        raise ValueError(
            f"{filename}: obligatoriske kolonner mangler: {missing} — SDS har "
            f"aendret skemaet. En kolonne der forsvinder fra et nyere "
            f"takstsystem ville NULLe arkivets vaerdier ved merge."
        )


def _ext_create_sql(dataset: DrgDataset) -> str:
    key_source_columns = {k for k in dataset.merge_keys if k in dataset.source_columns}
    lines = [f"[{TAKSTSYSTEM_COLUMN}] NVARCHAR(3) NOT NULL"]
    for name in dataset.source_columns:
        if name in key_source_columns:
            width = dataset.key_widths.get(name, dataset.source_widths.get(name, 100))
            lines.append(f"[{name}] NVARCHAR({width}) NOT NULL")
        else:
            width = max(MIN_VALUE_WIDTH, dataset.source_widths.get(name, 100))
            lines.append(f"[{name}] NVARCHAR({width}) NULL")
    lines.append(f"[{DELIVERY_COLUMN}] DATE NOT NULL")
    lines.append("[dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
    lines.append("[dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
    return (
        f"DROP TABLE IF EXISTS [ext].[{dataset.ext_table}];\n"
        f"CREATE TABLE [ext].[{dataset.ext_table}] (\n    " + ",\n    ".join(lines) + "\n);"
    )


class _ExtWriter:
    """Chunked writer for one ext table; created lazily on first data row."""

    def __init__(self, conn, dataset: DrgDataset, context: dg.AssetExecutionContext):
        self.dataset = dataset
        self.context = context
        self.cursor = conn.cursor()
        if hasattr(self.cursor, "fast_executemany"):
            self.cursor.fast_executemany = True
        columns = [TAKSTSYSTEM_COLUMN, *dataset.source_columns, DELIVERY_COLUMN]
        placeholders = ", ".join("?" for _ in columns)
        column_sql = ", ".join(f"[{c}]" for c in columns)
        self.insert_sql = (
            f"INSERT INTO [ext].[{dataset.ext_table}] ({column_sql}) VALUES ({placeholders})"
        )
        self.buffer: list[tuple] = []
        self.total = 0

    def add(self, row: tuple) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= INSERT_CHUNK_SIZE:
            self.flush()

    def flush(self) -> None:
        if self.buffer:
            self.cursor.executemany(self.insert_sql, self.buffer)
            self.total += len(self.buffer)
            if self.total % 500_000 < INSERT_CHUNK_SIZE:
                self.context.log.info(f"ext.{self.dataset.ext_table}: {self.total:,} raekker")
            self.buffer.clear()

    def close(self) -> None:
        self.flush()
        self.cursor.close()


class _NonSeekableReader(io.RawIOBase):
    """RawIOBase-adapter: tarfile's stream-mode file objects lack seekable(),
    which io.TextIOWrapper requires."""

    def __init__(self, fileobj):
        self._fileobj = fileobj

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        data = self._fileobj.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def _stream_data_member(
    handle,
    dataset: DrgDataset,
    archive: DrgArchiveFile,
    writer: _ExtWriter,
    encoding: str,
    filename: str,
    aar_counter: Counter,
) -> int:
    """Stream one CSV member from the tar into its ext table. Returns row count."""
    text = io.TextIOWrapper(
        io.BufferedReader(_NonSeekableReader(handle)), encoding=encoding, newline=""
    )
    reader = csv.reader(text, delimiter=DELIMITER)
    header = next(reader, None)
    if header is None:
        raise ValueError(f"{filename}: filen er tom (ingen header)")
    validate_member_schema(dataset, header, filename, takstsystem=archive.takstsystem)

    key_source_columns = {k for k in dataset.merge_keys if k in dataset.source_columns}
    positions: list[tuple[int, int | None]] = []  # (index i source_columns, index i header)
    header_index = {name: i for i, name in enumerate(header)}
    for i, name in enumerate(dataset.source_columns):
        positions.append((i, header_index.get(name)))
    aar_index = (
        header_index.get(dataset.aktivitetsaar_column) if dataset.aktivitetsaar_column else None
    )
    key_width_overflow: Counter = Counter()
    mojibake: Counter = Counter()

    delivery = archive.delivery_date.isoformat()
    takstsystem = archive.takstsystem
    n = 0
    for raw in reader:
        if not raw:
            continue
        if len(raw) != len(header):
            raise ValueError(
                f"{filename}: raekke {reader.line_num} har {len(raw)} felter, "
                f"headeren har {len(header)}"
            )
        n += 1
        row_defective = False
        values: list[str | None] = [None] * len(dataset.source_columns)
        for spec_index, src_index in positions:
            if src_index is None:
                continue  # optional kolonne der ikke findes i dette takstsystems skema
            value = raw[src_index].strip()
            name = dataset.source_columns[spec_index]
            if value and (_MOJIBAKE_MARKERS[0] in value or _MOJIBAKE_MARKERS[1] in value):
                mojibake[name] += 1
            if name in key_source_columns:
                max_width = dataset.key_widths.get(name, dataset.source_widths.get(name, 100))
                if len(value) > max_width:
                    key_width_overflow[name] += 1
                    row_defective = True
                    continue  # taelles og fejler samlet nedenfor — vaerdien logges ALDRIG
                values[spec_index] = (
                    value  # noeglekolonner beholder '' (NULL matcher aldrig i MERGE ON)
                )
            else:
                values[spec_index] = value or None
        if aar_index is not None:
            aar_counter[raw[aar_index].strip()] += 1
        if row_defective:
            # Raekken maa aldrig naa databasen: NULL i en NOT NULL-noegle ville
            # give en raa driverfejl ved naeste buffer-flush FOER den designede
            # (GDPR-sikre) fejl nedenfor kunne kastes.
            continue
        writer.add((takstsystem, *values, delivery))
    if key_width_overflow:
        # GDPR: kun kolonnenavne og antal — aldrig vaerdierne.
        raise ValueError(
            f"{filename}: noeglekolonner overskrider deres deklarerede bredde: "
            f"{dict(key_width_overflow)}. Arkivets noegler er bevidst smalle "
            f" — undersoeg leverancen."
        )
    if mojibake:
        # GDPR: kun kolonnenavne og antal — aldrig vaerdierne.
        raise ValueError(
            f"{filename}: mulig encoding-korruption (U+FFFD/mojibake) i "
            f"kolonner: {dict(mojibake)}. Filen matcher naeppe {encoding!r} — "
            f"undersoeg leverancen foer indlaesning."
        )
    return n


class LoadArchiveResult(TypedDict):
    fil: str
    leverance_dato: str
    kadence: str
    raekker: dict[str, int]
    aktivitetsaar: dict[str, int]


def load_archive_into_ext(
    conn,
    archive: DrgArchiveFile,
    datasets: tuple[DrgDataset, ...],
    writers: dict[str, _ExtWriter],
    encoding: str,
    context: dg.AssetExecutionContext,
) -> LoadArchiveResult:
    """Stream every data member of one delivery into the ext tables.

    Member order in SDS' archives is alphabetical (Metadata_* < SDP_ExportLog
    < data files), but the loader is deliberately order-robust: actual row
    counts are collected during the stream and validated against the export
    log AFTER the whole archive is read.
    """
    by_prefix = {d.source_prefix: d for d in datasets}
    export_log: dict[str, int] | None = None
    actual_counts: dict[str, int] = {}
    aar_counter: Counter = Counter()
    context.log.info(f"laeser {archive.filename}")

    with tarfile.open(archive.path, mode="r|bz2") as tar:
        for member in tar:
            if not member.isfile():
                continue
            base = os.path.basename(member.name)
            handle = tar.extractfile(member)
            if handle is None:
                continue
            if base.startswith("Metadata_"):
                continue  # skemaet valideres mod headeren i selve datafilen
            if base.startswith("SDP_ExportLog"):
                export_log = _parse_export_log(handle.read().decode(encoding))
                continue
            prefix = _dataset_prefix(base)
            if prefix is None:
                context.log.warning(f"{archive.filename}: ignorerer ukendt medlem {base}")
                continue
            dataset = by_prefix.get(prefix)
            if dataset is None:
                raise ValueError(
                    f"{archive.filename}: ukendt datasaet {base} — SDS har "
                    f"tilfoejet et datasaet. Opdater drg_datasets.py."
                )
            actual_counts[base] = _stream_data_member(
                handle, dataset, archive, writers[dataset.ext_table], encoding, base, aar_counter
            )

    if export_log is None:
        raise ValueError(
            f"{archive.filename}: SDP_ExportLog mangler — kontroltotaller kan ikke haandhaeves"
        )
    deviations = {}
    for filename, expected in export_log.items():
        if filename.startswith("Metadata_"):
            continue
        actual = actual_counts.get(filename)
        if actual != expected:
            deviations[filename] = {"eksportlog": expected, "indlaest": actual}
    if deviations:
        raise ValueError(
            f"{archive.filename}: raekkeantal afviger fra SDP_ExportLog: {deviations} — "
            f"leverancen er defekt eller indlaesningen ufuldstaendig."
        )
    return {
        "fil": archive.filename,
        "leverance_dato": archive.delivery_date.isoformat(),
        "kadence": archive.kadence or "-",
        "raekker": dict(sorted(actual_counts.items())),
        "aktivitetsaar": dict(sorted(aar_counter.items())),
    }


def _run_extract(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    datasets: tuple[DrgDataset, ...],
    leverancetype: str,
    config: DrgExtractConfig,
) -> dg.MaterializeResult:
    root = drg_dir()
    encoding = drg_encoding()
    on_or_before = None
    if config.delivery_on_or_before:
        on_or_before = date.fromisoformat(config.delivery_on_or_before)
        context.log.warning(f"leverance-pin aktiv: paa/foer {on_or_before}")

    archives = discover_archives(root, leverancetype)
    selected, notes = select_newest(
        archives,
        datasets,
        encoding,
        context=context,
        on_or_before=on_or_before,
        takstsystemer=config.takstsystemer,
    )
    if not selected:
        raise ValueError(
            f"Ingen brugbare {leverancetype}-leverancer fundet i {root} "
            f"(takstsystem-filter: {config.takstsystemer})"
        )

    leverance_alder: dict[str, int] = {}
    if on_or_before is None:
        discovered = {
            a.takstsystem
            for a in archives
            if config.takstsystemer is None or a.takstsystem in config.takstsystemer
        }
        leverance_alder = check_delivery_age(
            selected, discovered, leverancetype, date.today(), config.max_delivery_age_days
        )
    else:
        context.log.warning("leverance-pin aktiv — foraeldelses-checket springes over")

    per_takst: list[dict] = []
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for dataset in datasets:
                for statement in _ext_create_sql(dataset).split(";\n"):
                    if statement.strip():
                        cursor.execute(statement)
            conn.commit()
        finally:
            cursor.close()

        writers = {d.ext_table: _ExtWriter(conn, d, context) for d in datasets}
        try:
            for takstsystem in sorted(selected):
                per_takst.append(
                    load_archive_into_ext(
                        conn, selected[takstsystem], datasets, writers, encoding, context
                    )
                )
            for writer in writers.values():
                writer.flush()
            conn.commit()
        finally:
            for writer in writers.values():
                writer.close()

    totals = {d.ext_table: 0 for d in datasets}
    by_prefix = {d.source_prefix: d.ext_table for d in datasets}
    for entry in per_takst:
        for filename, count in entry["raekker"].items():
            totals[by_prefix[_dataset_prefix(filename)]] += count

    return dg.MaterializeResult(
        metadata={
            "leverancetype": leverancetype,
            "takstsystemer": ", ".join(sorted(selected)),
            "leverancer": per_takst,
            "raekker_pr_ext_tabel": dict(sorted(totals.items())),
            "leverance_alder_dage": leverance_alder or "pin aktiv — ikke vurderet",
            "fallbacks": notes or ["ingen"],
        }
    )


def _build_asset(leverancetype: str, datasets: tuple[DrgDataset, ...], description: str):
    @dg.asset(
        name=f"ext_{leverancetype.lower()}",
        group_name="drg_extract",
        compute_kind="csv",
        tags={"domain": "drg", "layer": "extract", "source": "drg"},
        description=description,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: DrgExtractConfig,
    ) -> dg.MaterializeResult:
        return _run_extract(context, mssql_target_staging, datasets, leverancetype, config)

    return _asset


ext_drg = _build_asset(
    "DRG",
    SOMATIK_DATASETS,
    "Load the newest usable somatik delivery per takstsystem (DRG T24-T26) "
    "1:1 into ext.drg_* — streamed straight from the tar.bz2, validated "
    "against SDS' own export log.",
)

ext_drgpsyk = _build_asset(
    "DRGPSYK",
    PSYK_DATASETS,
    "Load the newest usable psykiatri delivery per takstsystem (DRGPSYK "
    "T24-T26) 1:1 into ext.drgpsyk_* — streamed straight from the tar.bz2, "
    "validated against SDS' own export log.",
)

drg_extract_assets = [ext_drg, ext_drgpsyk]

__all__ = ["drg_extract_assets", "ext_drg", "ext_drgpsyk"]
