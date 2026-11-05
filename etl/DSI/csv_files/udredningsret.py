# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Udredningsret extraction assets — SDS' datamart for monitoring of the
udredningsret (LPR_Udredningsret), tar.bz2 deliveries on the NGC share.

SDS drops a FULL snapshot from 2018-07 every Monday (`U`) and once a month
(`M`) as `Region_1083_LPR_Udredningsret_<U|M>_<timestamp>.tar.bz2`; U and M
are the same track packaged at different times, so the NEWEST usable file is
authoritative regardless of cadence (decision B6/B7). The
share keeps only ~3 months, so a delayed run is a real loss — hence the
staleness alarm.

The share is a read-only CIFS mount, the ``udredningsret_share`` resource bound to
UDREDNINGSRET_BASE_DIR — an unset variable is a hard error, never a fallback (an
unmounted share and "no new deliveries" must not look alike).

DELIVERY DETECTION (delivery-detection pattern, level 1): the extract reads NOTHING when
the newest file on the share is one it has already ingested. The unit is the
single track (`udredningsret_leverance_1083`), the identity is the filename's
packaging timestamp, and the checkpoint lives in meta.DFMState under
("dfm", <unit>), using DfmManager.get_checkpoint/set_checkpoint. Only the archive-side gate asset
(udredningsret_archive.py) advances it, once the delivery is reflected in arc;
a run without a new file writes an EMPTY manifest and leaves ext untouched as
the debugging surface for the last delivery. A pinned run
(`delivery_on_or_before`) bypasses the checkpoint and never advances it.

Each tar holds the folder 1083/ with one data file
(UdredningsretMont_1083_<dato>.csv, 28 columns, ~8.2 M rows, 2.5 GB),
UdredningsretMont_LEVERANCEINFO_1083_<dato>.csv (DATAOMRAADE,
LPROPDATERINGSDATO), SDS' own column schemas (Metadata_*.csv) and an export
log (SDP_ExportLog_<dato>.csv) with the row count per file. Members are
streamed straight from the tar into the ext tables — nothing is unpacked to
disk. Row counts are validated against the export log after the whole archive
is read: a mismatch is a delivery defect and fails the run.

ext is a 1:1 mirror of the source including the source's unstable ids (F_ID,
PK, OBJEKTID_FORLOBSMARKOR) plus the delivery's cadence, timestamp and date.
The business keys are computed in udredningsret_archive.py.

GDPR: the files carry raw CPR — row contents are never logged or reported,
only file names, counts and delivery dates.
"""

import os
import re
import tarfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import TypedDict

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import share_connection
from core.file_delivery import DELIMITER, iter_members, open_member_csv
from core.resources import FileShareResource, MSSQLResource
from DSI.csv_files._sds_tar import (
    INSERT_CHUNK_SIZE,
    ExtWriter,
    dataset_prefix,
    execute_statements,
    export_log_deviations,
    ext_create_sql,
    has_mojibake,
    parse_archive_timestamp,
    parse_export_log,
    run_writers,
    schema_drift,
)
from DSI.csv_files.udredningsret_datasets import (
    DATASET_PREFIX,
    DATASETS,
    LEVERANCEINFO,
    LEVERANCEINFO_PREFIX,
    REGION,
    UDREDNINGSRET_MONT,
    VALUE_WIDTH,
    UdredningsretDataset,
)
from pydantic import Field

KADENCE_COLUMN = "kadence"
TIMESTAMP_COLUMN = "leverance_tidsstempel"
DELIVERY_COLUMN = "leverance_dato"

# One delivery track for the whole source (U and M are the same snapshot), so
# the DFM unit carries only the region. Checkpoint value: the filename's
# packaging timestamp as ISO, e.g. "2026-09-07T19:35:44".
DELIVERY_UNIT = f"udredningsret_leverance_{REGION}"

# Per-run contract between extract and archive: exactly one row when a delivery
# was loaded, no rows when the run was a declared no-op. Lives in ext (volatile,
# recreated every run) like the other landing tables.
MANIFEST_TABLE = "udredningsret_leverance_manifest"

# The source is delivered every Monday evening; in July 2026 the monthly
# delivery arrived instead of the weekly one. 10 days leaves slack for holidays.
MAX_DELIVERY_AGE_DAYS = 10

# Share of rows whose CPR is not 10 digits above which the delivery is refused:
# a high share means a shifted column or a changed format, not replacement CPRs
# (measured 2026-09-07: 0 of 8,171,604).
MAX_CPR_FORMAT_DEVIATION = 0.01

ARCHIVE_RE = re.compile(
    r"^Region_1083_LPR_Udredningsret_(?P<kadence>[UM])_(?P<timestamp>\d{14})\.tar\.bz2$"
)
# UdredningsretMont_1083_20260906.csv / UdredningsretMont_LEVERANCEINFO_1083_20260906.csv
DATA_MEMBER_RE = re.compile(
    r"^(?P<prefix>UdredningsretMont(?:_LEVERANCEINFO)?)_1083_(?P<dato>\d{8})\.csv$"
)
_CPR_RE = re.compile(r"^\d{10}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class UdredningsretArchiveFile:
    """One tar.bz2 delivery on the share."""

    path: str
    kadence: str
    timestamp: datetime

    @property
    def filename(self) -> str:
        return os.path.basename(self.path)

    @property
    def delivery_date(self) -> date:
        return self.timestamp.date()

    @property
    def identity(self) -> str:
        """The DFM delivery identity: the packaging timestamp, fixed-width ISO."""
        return self.timestamp.isoformat(timespec="seconds")


class UdredningsretExtractConfig(dg.Config):
    delivery_on_or_before: str | None = Field(
        default=None,
        description=(
            "Pin the run to deliveries packaged on/before this ISO date (YYYY-MM-DD) to "
            "recreate an earlier state. Bypasses the delivery checkpoint and never "
            "advances it. Default: newest delivery not yet ingested."
        ),
    )


def parse_archive_name(name: str) -> tuple[str, datetime] | None:
    match = ARCHIVE_RE.match(name)
    if not match:
        return None
    timestamp = parse_archive_timestamp(match["timestamp"])
    if timestamp is None:
        return None
    return match["kadence"], timestamp


def discover_archives(root: str) -> list[UdredningsretArchiveFile]:
    """Every parsable delivery in the root folder — Arkiv/ (the 2021-22 formats) is excluded."""
    found: list[UdredningsretArchiveFile] = []
    for name in sorted(os.listdir(root)):
        parsed = parse_archive_name(name)
        if parsed is None:
            continue
        kadence, timestamp = parsed
        found.append(
            UdredningsretArchiveFile(
                path=os.path.join(root, name), kadence=kadence, timestamp=timestamp
            )
        )
    return found


def _dataset_prefix(csv_filename: str) -> str | None:
    return dataset_prefix(csv_filename, DATA_MEMBER_RE)


_parse_export_log = parse_export_log


def is_usable(archive: UdredningsretArchiveFile, encoding: str) -> str | None:
    """Cheap pre-flight: stream the start of the tar and check the export log.

    Metadata and export log precede the data file in SDS' archives, so only the
    first few MB are decompressed. Without an export log, or with 0 rows in the
    data file, the delivery is unusable (HAIBA lesson: an empty delivery must
    never win as "newest" — it would soft-delete the whole archive).
    """
    try:
        with tarfile.open(archive.path, mode="r|bz2") as tar:
            for member in tar:
                base = os.path.basename(member.name)
                if not base.startswith("SDP_ExportLog"):
                    continue
                handle = tar.extractfile(member)
                if handle is None:
                    return "SDP_ExportLog could not be read"
                rows = _parse_export_log(handle.read().decode(encoding))
                counts = [n for f, n in rows.items() if _dataset_prefix(f) == DATASET_PREFIX]
                if not counts:
                    return f"dataset {DATASET_PREFIX} is missing from the export log"
                if max(counts) == 0:
                    return f"dataset {DATASET_PREFIX} has 0 rows"
                return None
    except (tarfile.TarError, OSError, EOFError) as exc:
        return f"the archive could not be opened: {exc}"
    return "SDP_ExportLog is missing from the archive"


def select_candidates(
    archives: list[UdredningsretArchiveFile],
    checkpoint: str | None,
    on_or_before: date | None,
) -> list[UdredningsretArchiveFile]:
    """Level-1 delivery detection on filenames alone — no tar is opened.

    Pinned: every archive packaged on/before the pin date, checkpoint ignored.
    Otherwise: only archives whose identity is strictly newer than the
    checkpoint (all of them when no checkpoint exists — first load).
    """
    if on_or_before is not None:
        return [a for a in archives if a.delivery_date <= on_or_before]
    return [a for a in archives if checkpoint is None or a.identity > checkpoint]


def select_newest(
    candidates: list[UdredningsretArchiveFile],
    encoding: str,
    context: dg.AssetExecutionContext | None = None,
) -> tuple[UdredningsretArchiveFile | None, list[str]]:
    """Newest USABLE candidate regardless of cadence; pre-flight rejects are reported as notes.

    Pin and checkpoint filtering happen in select_candidates — this only
    decides among the files that are allowed to be loaded.
    """
    notes: list[str] = []
    for candidate in sorted(candidates, key=lambda a: a.timestamp, reverse=True):
        reason = is_usable(candidate, encoding)
        if reason is None:
            return candidate, notes
        note = f"skipping {candidate.filename} ({reason})"
        notes.append(note)
        if context is not None:
            context.log.warning(note)
    return None, notes


def check_delivery_age(
    newest: UdredningsretArchiveFile, today: date, max_age_days: int | None = None
) -> int:
    """Staleness alarm: fails hard when the newest delivery on the share is
    older than the limit — the mount is broken, SDS has stopped delivering, or
    nothing has landed. A share check, not a load check: it looks at file names
    only and is independent of checkpoint and pin. Returns the age in days for
    asset metadata."""
    limit = max_age_days if max_age_days is not None else MAX_DELIVERY_AGE_DAYS
    age = (today - newest.delivery_date).days
    if age > limit:
        raise ValueError(
            f"The newest udredningsret delivery on the share ({newest.filename}) is {age} "
            f"days old (limit: {limit}). The share keeps only ~3 months of deliveries — "
            f"is the CIFS share mounted, and is SDS still delivering?"
        )
    return age


def validate_member_schema(
    dataset: UdredningsretDataset, columns: list[str], filename: str
) -> None:
    """The header against the dataset's columns — fail loudly on schema drift."""
    unknown, missing = schema_drift(columns, dataset.source_columns)
    if unknown:
        raise ValueError(
            f"{filename}: unknown columns {unknown} — SDS has changed the schema. "
            f"Update udredningsret_datasets.py (and the arc DDL) before loading."
        )
    if missing:
        raise ValueError(
            f"{filename}: mandatory columns are missing: {missing} — SDS has changed the "
            f"schema. A missing column would NULL the archive's values on merge."
        )


def _ext_create_sql(dataset: UdredningsretDataset) -> str:
    """Every source column is NVARCHAR(VALUE_WIDTH) NULL — ext and arc share
    one width so they can never diverge (HAIBA lesson)."""
    return ext_create_sql(
        dataset.ext_table,
        [
            f"[{KADENCE_COLUMN}] NVARCHAR(1) NOT NULL",
            f"[{TIMESTAMP_COLUMN}] DATETIME2(0) NOT NULL",
        ],
        [(name, VALUE_WIDTH, False) for name in dataset.source_columns],
    )


def manifest_create_sql() -> str:
    """ext.udredningsret_leverance_manifest — recreated EMPTY at the start of every run."""
    return (
        f"DROP TABLE IF EXISTS [ext].[{MANIFEST_TABLE}];\n"
        f"CREATE TABLE [ext].[{MANIFEST_TABLE}] (\n"
        f"    [filnavn] NVARCHAR(200) NOT NULL,\n"
        f"    [{KADENCE_COLUMN}] NVARCHAR(1) NOT NULL,\n"
        f"    [{TIMESTAMP_COLUMN}] DATETIME2(0) NOT NULL,\n"
        f"    [{DELIVERY_COLUMN}] DATE NOT NULL,\n"
        f"    [lpropdateringsdato] NVARCHAR(10) NOT NULL,\n"
        f"    [checkpoint_enhed] NVARCHAR(128) NOT NULL,\n"
        f"    [checkpoint_identitet] NVARCHAR(50) NOT NULL,\n"
        f"    [ryk_checkpoint] BIT NOT NULL,\n"
        f"    [dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()\n"
        f");"
    )


_execute_statements = execute_statements


class _ExtWriter(ExtWriter):
    """Chunked writer for one ext table.

    INSERT_CHUNK_SIZE is read from THIS module at construction time, so a test
    that patches udredningsret.INSERT_CHUNK_SIZE still takes effect.
    """

    def __init__(self, conn, dataset: UdredningsretDataset, context: dg.AssetExecutionContext):
        super().__init__(
            conn,
            dataset.ext_table,
            [KADENCE_COLUMN, TIMESTAMP_COLUMN, *dataset.source_columns, DELIVERY_COLUMN],
            context,
            chunk_size=INSERT_CHUNK_SIZE,
            row_word="rows",
        )
        self.dataset = dataset


class MemberStats(TypedDict):
    raekker: int
    cpr_ikke_10_cifre: int
    blanke_datoer: dict[str, int]
    lpropdateringsdato: str | None


def _stream_data_member(
    handle,
    dataset: UdredningsretDataset,
    archive: UdredningsretArchiveFile,
    writer: _ExtWriter,
    encoding: str,
    filename: str,
) -> MemberStats:
    """Stream one CSV from the tar into its ext table."""
    reader = open_member_csv(handle, encoding, DELIMITER)
    header = next(reader, None)
    if header is None:
        raise ValueError(f"{filename}: the file is empty (no header)")
    header = [h.lstrip("﻿") for h in header]
    validate_member_schema(dataset, header, filename)

    header_index = {name: i for i, name in enumerate(header)}
    positions = [header_index[name] for name in dataset.source_columns]
    cpr_index = header_index.get("CPR")
    lpr_index = header_index.get("LPROPDATERINGSDATO")
    date_indexes = {c: header_index[c] for c in dataset.iso_date_columns}
    mojibake: Counter = Counter()
    non_iso_dates: Counter = Counter()
    blank_dates: Counter = Counter()

    delivery = archive.delivery_date.isoformat()
    stamp = archive.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    n = 0
    cpr_deviations = 0
    lpr_dato: str | None = None
    for raw in reader:
        if not raw:
            continue
        if len(raw) != len(header):
            raise ValueError(
                f"{filename}: row {reader.line_num} has {len(raw)} fields, "
                f"the header has {len(header)}"
            )
        n += 1
        values: list[str | None] = []
        for spec_index, src_index in enumerate(positions):
            value = raw[src_index].strip()
            if has_mojibake(value):
                mojibake[dataset.source_columns[spec_index]] += 1
            values.append(value or None)
        if cpr_index is not None and not _CPR_RE.match(raw[cpr_index].strip()):
            cpr_deviations += 1  # counted — the value is NEVER logged
        for column, index in date_indexes.items():
            value = raw[index].strip()
            if not value:
                blank_dates[column] += 1
            elif not _ISO_DATE_RE.match(value):
                non_iso_dates[column] += 1  # counted — the value is never logged
        if lpr_index is not None:
            lpr_dato = raw[lpr_index].strip() or lpr_dato
        writer.add((archive.kadence, stamp, *values, delivery))
    if mojibake:
        # GDPR: column names and counts only — never the values.
        raise ValueError(
            f"{filename}: possible encoding corruption (U+FFFD/mojibake) in columns: "
            f"{dict(mojibake)}. The file hardly matches {encoding!r} — inspect the delivery."
        )
    if non_iso_dates:
        raise ValueError(
            f"{filename}: {dict(non_iso_dates)} row(s) with a date that is not ISO "
            f"YYYY-MM-DD. The business keys and the delivery-direction guard order these "
            f"columns as strings, which is only chronological for ISO — refusing the delivery."
        )
    if cpr_index is not None and n and cpr_deviations / n > MAX_CPR_FORMAT_DEVIATION:
        raise ValueError(
            f"{filename}: {cpr_deviations:,} of {n:,} rows have a CPR that is not 10 "
            f"digits — the columns are probably shifted, or the format has changed."
        )
    return {
        "raekker": n,
        "cpr_ikke_10_cifre": cpr_deviations,
        "blanke_datoer": dict(blank_dates),
        "lpropdateringsdato": lpr_dato,
    }


class LoadArchiveResult(TypedDict):
    fil: str
    kadence: str
    leverance_tidsstempel: str
    leverance_dato: str
    lpropdateringsdato: str
    raekker: dict[str, int]
    cpr_ikke_10_cifre: int
    blanke_datoer: dict[str, int]


def load_archive_into_ext(
    archive: UdredningsretArchiveFile,
    writers: dict[str, _ExtWriter],
    encoding: str,
    context: dg.AssetExecutionContext,
) -> LoadArchiveResult:
    """Stream every data member of one delivery into ext.

    SDS' member order is Metadata_*, SDP_ExportLog, data file, LEVERANCEINFO
    (last!) — the loader is deliberately order-robust: row counts are collected
    while streaming and validated against the export log AFTER the whole
    archive has been read.
    """
    by_prefix = {d.source_prefix: d for d in DATASETS}
    export_log: dict[str, int] | None = None
    actual_counts: dict[str, int] = {}
    cpr_deviations = 0
    blank_dates: Counter = Counter()
    lpr_dato: str | None = None
    context.log.info(f"reading {archive.filename}")

    for base, handle in iter_members(archive.path):
        if base.startswith("Metadata_"):
            continue  # the schema is validated against the data file's own header
        if base.startswith("SDP_ExportLog"):
            export_log = _parse_export_log(handle.read().decode(encoding))
            continue
        prefix = _dataset_prefix(base)
        if prefix is None:
            context.log.warning(f"{archive.filename}: ignoring unknown member {base}")
            continue
        dataset = by_prefix.get(prefix)
        if dataset is None:
            raise ValueError(
                f"{archive.filename}: unknown dataset {base} — SDS has added a "
                f"dataset. Update udredningsret_datasets.py."
            )
        stats = _stream_data_member(
            handle, dataset, archive, writers[dataset.ext_table], encoding, base
        )
        actual_counts[base] = stats["raekker"]
        cpr_deviations += stats["cpr_ikke_10_cifre"]
        blank_dates.update(stats["blanke_datoer"])
        if stats["lpropdateringsdato"]:
            lpr_dato = stats["lpropdateringsdato"]

    if export_log is None:
        raise ValueError(
            f"{archive.filename}: SDP_ExportLog is missing — control totals cannot be enforced"
        )
    deviations = export_log_deviations(export_log, actual_counts)
    if deviations:
        raise ValueError(
            f"{archive.filename}: row counts deviate from SDP_ExportLog: {deviations} — "
            f"the delivery is defective or the load incomplete."
        )
    if not any(_dataset_prefix(f) == DATASET_PREFIX for f in actual_counts):
        raise ValueError(f"{archive.filename}: data file {DATASET_PREFIX}_1083_*.csv is missing")
    if lpr_dato is None:
        raise ValueError(
            f"{archive.filename}: {LEVERANCEINFO_PREFIX} is missing or has no "
            f"LPROPDATERINGSDATO — the archive's delivery-direction guard cannot run"
        )
    # Belt and braces: the per-row check above already refused a non-ISO value,
    # but the guard's string comparison depends on this one value in particular.
    if not _ISO_DATE_RE.match(lpr_dato):
        raise ValueError(
            f"{archive.filename}: LPROPDATERINGSDATO is not ISO YYYY-MM-DD — the "
            f"delivery-direction guard compares it as a string and would be meaningless."
        )
    return {
        "fil": archive.filename,
        "kadence": archive.kadence,
        "leverance_tidsstempel": archive.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "leverance_dato": archive.delivery_date.isoformat(),
        "lpropdateringsdato": lpr_dato,
        "raekker": dict(sorted(actual_counts.items())),
        "cpr_ikke_10_cifre": cpr_deviations,
        "blanke_datoer": dict(blank_dates),
    }


def load_delivery(
    conn,
    archive: UdredningsretArchiveFile,
    encoding: str,
    context: dg.AssetExecutionContext,
) -> LoadArchiveResult:
    """Recreate the ext tables, stream the delivery in and commit — or discard.

    On the error path the writers' buffers are DISCARDED before close(), so the
    original exception is the one that surfaces (review F19); the connection is
    closed by the caller without commit, which rolls the partial load back.
    """
    for dataset in DATASETS:
        _execute_statements(conn, _ext_create_sql(dataset))

    writers = {d.ext_table: _ExtWriter(conn, d, context) for d in DATASETS}
    return run_writers(writers, lambda: load_archive_into_ext(archive, writers, encoding, context))


def write_manifest(
    conn, archive: UdredningsretArchiveFile, lpropdateringsdato: str, advance_checkpoint: bool
) -> None:
    """The one manifest row for a loaded delivery — committed together with the data."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"INSERT INTO [ext].[{MANIFEST_TABLE}] ([filnavn], [{KADENCE_COLUMN}], "
            f"[{TIMESTAMP_COLUMN}], [{DELIVERY_COLUMN}], [lpropdateringsdato], "
            f"[checkpoint_enhed], [checkpoint_identitet], [ryk_checkpoint]) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            archive.filename,
            archive.kadence,
            archive.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            archive.delivery_date.isoformat(),
            lpropdateringsdato,
            DELIVERY_UNIT,
            archive.identity,
            1 if advance_checkpoint else 0,
        )
    finally:
        cursor.close()


def _run_extract(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    share: FileShareResource,
    config: UdredningsretExtractConfig,
) -> dg.MaterializeResult:
    root = share.root()
    encoding = share.encoding
    on_or_before = None
    if config.delivery_on_or_before:
        on_or_before = date.fromisoformat(config.delivery_on_or_before)
        context.log.warning(
            f"delivery pin active: on/before {on_or_before} — the delivery checkpoint is "
            f"bypassed and will not be advanced"
        )

    archives = discover_archives(root)
    checkpoint = DfmManager(mssql, context).get_checkpoint("dfm", DELIVERY_UNIT)
    candidates = select_candidates(archives, checkpoint, on_or_before)

    # The manifest is recreated EMPTY first: an empty manifest is the declared
    # no-op the archive short-circuits on, so it must never be a stale one.
    with mssql.get_connection() as conn:
        _execute_statements(conn, manifest_create_sql())

    common: dict[str, str | int] = {
        "checkpoint_enhed": DELIVERY_UNIT,
        "checkpoint_foer": checkpoint or "ingen (foerste indlaesning)",
        "arkiver_paa_sharet": len(archives),
        "kandidater": len(candidates),
    }

    if not candidates:
        if on_or_before is not None or checkpoint is None or not archives:
            raise ValueError(
                f"No udredningsret delivery to load in {root} ({len(archives)} archives "
                f"seen, pin: {config.delivery_on_or_before}, checkpoint: {checkpoint})"
            )
        newest = max(archives, key=lambda a: a.timestamp)
        context.log.info(
            f"no delivery newer than checkpoint {checkpoint} (newest on share: "
            f"{newest.filename}) — nothing read, ext left untouched, manifest empty"
        )
        return dg.MaterializeResult(
            metadata={
                "besluttet": "ingen ny leverance — no-op",
                "nyeste_paa_sharet": newest.identity,
                **common,
            }
        )

    selected, notes = select_newest(candidates, encoding, context=context)
    if selected is None:
        # A new file HAS landed but none of the new ones pass pre-flight. Falling
        # back to an already-ingested file would be a silent no-op hiding a
        # defective delivery, so this is an error, not a no-op.
        raise ValueError(
            f"All {len(candidates)} udredningsret candidate(s) in {root} are unusable: "
            f"{notes}. Inspect the newest delivery on the share."
        )

    prefix_by_table = {
        UDREDNINGSRET_MONT.ext_table: DATASET_PREFIX,
        LEVERANCEINFO.ext_table: LEVERANCEINFO_PREFIX,
    }
    with (
        DfmManager(mssql, context).target_runs(
            "ext",
            prefix_by_table,
            dagster_run_id=context.run_id,
            mode="full_load",
            **share_connection(root),
        ) as rows,
        mssql.get_connection() as conn,
    ):
        result = load_delivery(conn, selected, encoding, context)
        write_manifest(conn, selected, result["lpropdateringsdato"], on_or_before is None)
        conn.commit()
        rows.update(
            {
                table: sum(n for f, n in result["raekker"].items() if _dataset_prefix(f) == prefix)
                for table, prefix in prefix_by_table.items()
            }
        )

    # Candidates older than the selected one were never opened: the newest
    # usable file is a full snapshot and supersedes them (reported, not loaded).
    skipped = [a.filename for a in candidates if a.timestamp < selected.timestamp]
    return dg.MaterializeResult(
        metadata={
            "besluttet": (
                "indlaest" if on_or_before is None else "indlaest (pin — checkpoint rykkes ikke)"
            ),
            "leverance": dict(result),
            "raekker_pr_ext_tabel": rows,
            "sprunget_over_mellemliggende": skipped or ["ingen"],
            "fallbacks": notes or ["ingen"],
            **common,
        }
    )


@dg.asset(
    name="ext_udredningsret",
    group_name="udredningsret_extract",
    compute_kind="csv",
    tags={"domain": "udredningsret", "layer": "extract", "source": "udredningsret"},
    description=(
        "Load the newest udredningsret delivery (SDS LPR_Udredningsret, weekly full "
        "snapshot) that is newer than the DFM delivery checkpoint, 1:1 into "
        "ext.udredningsret_mont and ext.udredningsret_leveranceinfo — streamed straight "
        "from tar.bz2, validated against SDS' own export log. A run without a new file "
        "reads nothing and writes an empty manifest."
    ),
)
def ext_udredningsret(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    udredningsret_share: FileShareResource,
    config: UdredningsretExtractConfig,
) -> dg.MaterializeResult:
    return _run_extract(context, mssql_target_staging, udredningsret_share, config)


@dg.asset(
    name="udredningsret_leverance_alder",
    group_name="udredningsret_extract",
    compute_kind="csv",
    tags={"domain": "udredningsret", "layer": "extract", "source": "udredningsret"},
    description=(
        f"Staleness alarm for the udredningsret share: fails when the newest "
        f"Region_1083_LPR_Udredningsret_* file is more than {MAX_DELIVERY_AGE_DAYS} days "
        f"old. Reads file names only — no tar, no database — and is deliberately NOT a "
        f"dependency of the load, so a stale share never blocks ingesting what has arrived."
    ),
)
def udredningsret_leverance_alder(
    context: dg.AssetExecutionContext, udredningsret_share: FileShareResource
) -> dg.MaterializeResult:
    root = udredningsret_share.root()
    archives = discover_archives(root)
    if not archives:
        raise ValueError(f"No udredningsret deliveries found in {root} — is the share mounted?")
    newest = max(archives, key=lambda a: a.timestamp)
    age = check_delivery_age(newest, date.today())
    return dg.MaterializeResult(
        metadata={
            "nyeste_fil": newest.filename,
            "alder_dage": age,
            "graense_dage": MAX_DELIVERY_AGE_DAYS,
            "arkiver_paa_sharet": len(archives),
        }
    )


udredningsret_extract_assets = [ext_udredningsret, udredningsret_leverance_alder]

__all__ = [
    "DELIVERY_UNIT",
    "MANIFEST_TABLE",
    "UdredningsretArchiveFile",
    "UdredningsretExtractConfig",
    "check_delivery_age",
    "discover_archives",
    "ext_udredningsret",
    "is_usable",
    "load_archive_into_ext",
    "load_delivery",
    "manifest_create_sql",
    "parse_archive_name",
    "select_candidates",
    "select_newest",
    "udredningsret_extract_assets",
    "udredningsret_leverance_alder",
    "validate_member_schema",
    "write_manifest",
]
