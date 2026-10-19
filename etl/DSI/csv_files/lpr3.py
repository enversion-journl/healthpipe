# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""LPR3 full-snapshot extraction: weekly (U) and monthly (M) deliveries share one track.

SDS puts Metadata/ExportLog AFTER its data files. A control-only preflight pass
therefore validates the authoritative schema and cutoff before any ext writes.
The four loading assets then stream their disjoint table groups. Publication of
the manifest waits for ALL groups; only the archive gate may advance DFM.

Missing tables are absent from the current manifest, and their old ext tables
are removed after preflight. Every manifest belongs to one Dagster run, so an
archive invocation cannot accidentally reuse a previous run's landing tables.
"""

import csv
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from datetime import time as clock_time
from pathlib import Path

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import share_connection
from core.resources import MSSQLResource
from DSI.connectors.delivery_detection import (
    DELIVERY_SCHEMA,
    DELIVERY_TARGET_TYPE,
    delivery_identity,
    delivery_unit,
    is_new_delivery,
)
from DSI.csv_files._lpr3_control import MEMBER_RE, delivery_controls, parse_cutoff
from DSI.csv_files._lpr3_run import claim_run, owned_landing
from DSI.csv_files._sds_tar import (
    ExtWriter,
    execute_statements,
    export_log_deviations,
    ext_create_sql,
    has_mojibake,
    iter_members,
    open_member_csv,
    parse_archive_timestamp,
    run_writers,
    source_dir,
    source_encoding,
)
from DSI.csv_files.lpr3_datasets import (
    DELIVERY_COLUMNS,
    METADATA_HEADER,
    SOURCES_BY_PREFIX,
    LPR3SourceDataset,
    SourceColumn,
    extract_groups,
)

BASE_DIR_ENV = "LPR3_BASE_DIR"
ENCODING_ENV = "LPR3_FILE_ENCODING"
DELIVERY_UNIT = delivery_unit("lpr3", "1083")
MANIFEST_TABLE = "lpr3_leverance_manifest"
# Well below the 15–20k native-driver wedge range documented in bulk_copy.py.
INSERT_CHUNK_SIZE = 5000
GROUPS = extract_groups(4)
MAX_DELIVERY_AGE_DAYS = 12
ARCHIVE_RE = re.compile(
    r"Region1083_LPRGrunddata_(?P<kadence>[UM])_(?P<timestamp>\d{14})\.tar\.bz2\Z"
)
_DATETIME_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,7})?\Z")
_TIME_RE = re.compile(r"\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?\Z")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_UUID_RE = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\Z")
_HEX_RE = re.compile(r"[0-9a-fA-F]+\Z")
_INT_RE = re.compile(r"-?\d+\Z")


@dataclass(frozen=True)
class LPR3ArchiveFile:
    path: str
    kadence: str
    timestamp: datetime

    @property
    def identity(self):
        return delivery_identity(self.timestamp)

    @property
    def filename(self):
        return Path(self.path).name


class LPR3ExtractConfig(dg.Config):
    base_dir: str | None = None
    delivery_on_or_before: str | None = None


def lpr3_dir(override=None):
    return source_dir(
        BASE_DIR_ENV,
        override=override,
        hint_unset="Register LPR3_BASE_DIR and the read-only SDS mount before running.",
        hint_not_dir="The SDS share is probably not mounted in this allocation.",
    )


def discover_archives(root: str) -> list[LPR3ArchiveFile]:
    archives = []
    for path in Path(root).iterdir():
        match = ARCHIVE_RE.fullmatch(path.name)
        stamp = parse_archive_timestamp(match["timestamp"]) if match else None
        if match and stamp is not None and path.is_file():
            archives.append(LPR3ArchiveFile(str(path), match["kadence"], stamp))
    return sorted(archives, key=lambda archive: archive.timestamp)


def select_delivery(archives, checkpoint, pin: date | None = None):
    eligible = [
        a
        for a in archives
        if (a.timestamp.date() <= pin if pin else is_new_delivery(checkpoint, a.identity))
    ]
    if eligible:
        return max(eligible, key=lambda archive: archive.timestamp)
    if pin is not None or not archives or checkpoint is None:
        raise ValueError("No LPR3 delivery satisfies the requested selection")
    return None


def archive_fingerprint(path):
    stat = os.stat(path)
    return [stat.st_size, stat.st_mtime_ns]


def first_cutoff(path):
    members = iter_members(path)
    try:
        for name, _ in members:
            match = MEMBER_RE.fullmatch(name)
            if match:
                return parse_cutoff(match["cutoff"])
        raise ValueError("LPR3 archive contains no recognised table member")
    finally:
        members.close()


def check_cutoff(cutoff, previous):
    if previous is not None and date.fromisoformat(cutoff) < date.fromisoformat(str(previous)[:10]):
        raise ValueError("LPR3 cutoff is older than the archive; refusing before ext writes")


def read_last_cutoff(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(
            "IF OBJECT_ID(N'meta.LPR3Leverance', N'U') IS NOT NULL SELECT MAX([leverance_dato]) FROM [meta].[LPR3Leverance] WHERE [status]=N'archived'; ELSE SELECT CAST(NULL AS DATE)"
        )
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def preflight(archive, previous_cutoff, encoding="cp1252"):
    fingerprint = archive_fingerprint(archive.path)
    check_cutoff(first_cutoff(archive.path), previous_cutoff)
    controls = delivery_controls(archive.path, encoding=encoding)
    check_cutoff(controls.cutoff, previous_cutoff)
    for filename in controls.announced_files:
        match = MEMBER_RE.fullmatch(filename)
        if not match or match["table"] not in SOURCES_BY_PREFIX:
            raise ValueError("LPR3 delivery announces an unknown source table")
        dataset = SOURCES_BY_PREFIX[match["table"]]
        if filename != dataset.data_filename(controls.cutoff):
            raise ValueError("LPR3 regional filename or cutoff differs from the source contract")
    for prefix, item in controls.schema.items():
        dataset = SOURCES_BY_PREFIX[prefix]
        if (
            tuple(item["metadata_header"]) != METADATA_HEADER
            or tuple(map(tuple, item["columns"])) != dataset.metadata_records
        ):
            raise ValueError(
                f"{prefix}: SDS Metadata differs from lpr3_datasets; refusing before ext writes"
            )
    available = {
        prefix: {
            "filename": item["datafile"],
            "rows": int(item["rows_exportlog"]),
            "bytes": int(item["bytes_exportlog"]),
        }
        for prefix, item in controls.schema.items()
        if item["datafile"] in controls.present_files
    }
    if not available or not any(item["rows"] for item in available.values()):
        raise ValueError("LPR3 delivery has no nonempty source data tables")
    if archive_fingerprint(archive.path) != fingerprint:
        raise ValueError("LPR3 delivery changed during preflight")
    return {
        "path": archive.path,
        "filename": archive.filename,
        "kadence": archive.kadence,
        "identity": archive.identity,
        "cutoff": controls.cutoff,
        "fingerprint": fingerprint,
        "encoding": encoding,
        "available": available,
        "missing": sorted(set(SOURCES_BY_PREFIX) - set(available)),
    }


def manifest_create_sql():
    return f"""DROP TABLE IF EXISTS [ext].[{MANIFEST_TABLE}];
CREATE TABLE [ext].[{MANIFEST_TABLE}] (
    [run_id] NVARCHAR(64) NOT NULL,
    [kildetabel] NVARCHAR(80) NOT NULL,
    [ext_tabel] NVARCHAR(100) NOT NULL,
    [filnavn] NVARCHAR(160) NOT NULL,
    [kadence] NCHAR(1) NOT NULL,
    [leverance_tidsstempel] DATETIME2(0) NOT NULL,
    [leverance_dato] DATE NOT NULL,
    [raekker_ext] BIGINT NOT NULL,
    [raekker_kilde] BIGINT NOT NULL,
    [bytes_kilde] BIGINT NOT NULL,
    [audit_insert_min] BIGINT NULL,
    [audit_insert_distinct] BIGINT NOT NULL,
    [checkpoint_enhed] NVARCHAR(128) NOT NULL,
    [checkpoint_identitet] NVARCHAR(50) NOT NULL,
    [ryk_checkpoint] BIT NOT NULL,
    CONSTRAINT [PK_ext_lpr3_leverance_manifest] PRIMARY KEY ([kildetabel])
);"""


def create_ext_sql(dataset):
    sql = ext_create_sql(
        dataset.ext_table,
        ["[kadence] NCHAR(1) NOT NULL", "[leverance_tidsstempel] DATETIME2(0) NOT NULL"],
        [(column.name, column.wire_width, False) for column in dataset.columns],
        delivery_date_first=True,
    )
    # Configure the empty heap before native bulk copy with TABLOCK. PAGE
    # compression is lossless and applies as the bulk batches allocate pages.
    return sql + f"\nALTER TABLE [ext].[{dataset.ext_table}] REBUILD WITH (DATA_COMPRESSION=PAGE);"


def valid_format(column: SourceColumn, value: str) -> bool:
    if not value:
        # Empty text and SQL NULL have the same CSV representation. Numeric,
        # date and binary empties, however, must obey Metadata nullability.
        return column.nullable or column.base_type in ("nvarchar", "nchar", "varchar", "char")
    kind = column.base_type
    if len(value) > column.wire_width:
        return False
    if kind in ("nvarchar", "nchar", "varchar", "char"):
        return True
    if kind == "uniqueidentifier":
        return bool(_UUID_RE.fullmatch(value))
    if kind == "varbinary":
        # Metadata declares a maximum byte length, not a fixed-width UUID.
        # Real ResultatIndberetning snapshots also contain a six-byte ID.
        return bool(_HEX_RE.fullmatch(value)) and len(value) % 2 == 0
    if kind == "bit":
        return value in ("0", "1")
    if kind in ("bigint", "int", "smallint", "tinyint"):
        if not _INT_RE.fullmatch(value):
            return False
        bounds = {
            "bigint": (-(2**63), 2**63 - 1),
            "int": (-(2**31), 2**31 - 1),
            "smallint": (-32768, 32767),
            "tinyint": (0, 255),
        }
        low, high = bounds[kind]
        return low <= int(value) <= high
    if kind == "decimal":
        whole = column.precision - column.scale
        fraction = rf"(?:,\d{{1,{column.scale}}})?" if column.scale else ""
        return bool(re.fullmatch(rf"-?\d{{1,{whole}}}{fraction}", value))
    try:
        if kind == "datetime2":
            return bool(_DATETIME_RE.fullmatch(value)) and bool(datetime.fromisoformat(value))
        if kind == "date":
            return bool(_DATE_RE.fullmatch(value)) and bool(date.fromisoformat(value))
        if kind == "time":
            return bool(_TIME_RE.fullmatch(value)) and bool(clock_time.fromisoformat(value))
    except ValueError:
        return False
    raise ValueError("Unsupported LPR3 source type")


class _ExtWriter(ExtWriter):
    def __init__(self, conn, dataset, context):
        self.column_mappings = [*DELIVERY_COLUMNS, *dataset.source_columns]
        super().__init__(
            conn,
            dataset.ext_table,
            self.column_mappings,
            context,
            chunk_size=INSERT_CHUNK_SIZE,
            row_word="rows",
        )

    def flush(self):
        if not self.buffer:
            return
        try:
            # Raw driver errors can contain patient values; SDS fields stay text.
            data = [
                (row[0], datetime.fromisoformat(row[1]), date.fromisoformat(row[2]), *row[3:])
                for row in self.buffer
            ]
            result = self.cursor.bulkcopy(
                f"ext.{self.ext_table}",
                data,
                batch_size=0,
                timeout=120,
                table_lock=True,
                column_mappings=self.column_mappings,
            )
            if result.get("rows_copied") != len(data):
                raise ValueError("Incomplete native bulk copy")
        except Exception:  # noqa: BLE001 — SQL truncation errors can expose patient values.
            raise ValueError(
                f"ext.{self.ext_table}: SQL insert failed; source values suppressed"
            ) from None
        self.total += len(self.buffer)
        if self.total % 500_000 < self.chunk_size:
            self.context.log.info(f"ext.{self.ext_table}: {self.total:,} rows")
        self.buffer.clear()


class _CountedMember:
    def __init__(self, handle):
        self.handle, self.bytes_read = handle, 0

    def read(self, size=-1):
        data = self.handle.read(size)
        self.bytes_read += len(data)
        return data


def stream_member(handle, dataset: LPR3SourceDataset, writer, plan):
    counted = _CountedMember(handle)
    reader = open_member_csv(counted, plan["encoding"])
    header = next(reader, None)
    if header is None:
        raise ValueError(f"{dataset.source_prefix}: missing data header")
    if tuple(header) != dataset.source_columns:
        raise ValueError(f"{dataset.source_prefix}: data header differs from validated Metadata")
    audit_index = next(
        (
            i
            for i, name in enumerate(header)
            if name.casefold() in ("dw_id_audit_insert", "dw_id_audit_inserted")
        ),
        None,
    )
    audits = set()
    rows = 0
    stamp = plan["identity"].replace("T", " ")
    for values in reader:
        if len(values) != len(dataset.columns):
            raise ValueError(f"{dataset.source_prefix}: invalid field count at data row {rows+1}")
        rows += 1
        for column, value in zip(dataset.columns, values, strict=True):
            if not valid_format(column, value):
                raise ValueError(
                    f"{dataset.source_prefix}.{column.name}: invalid format/width at data row {rows}"
                )
        if has_mojibake("".join(values)) and any(has_mojibake(value) for value in values):
            raise ValueError(f"{dataset.source_prefix}: encoding corruption at data row {rows}")
        if audit_index is not None and values[audit_index]:
            audits.add(int(values[audit_index]))
        writer.add((plan["kadence"], stamp, plan["cutoff"], *values))
    return {
        "rows": rows,
        "bytes": counted.bytes_read,
        "audit_min": min(audits) if audits else None,
        "audit_distinct": len(audits),
    }


def load_group(conn, context, plan, prefixes):
    selected = [SOURCES_BY_PREFIX[p] for p in prefixes if p in plan["available"]]
    if not selected:
        return {}
    if archive_fingerprint(plan["path"]) != plan["fingerprint"]:
        raise ValueError("LPR3 delivery changed after preflight")
    for dataset in selected:
        execute_statements(conn, create_ext_sql(dataset))
    writers = {d.source_prefix: _ExtWriter(conn, d, context) for d in selected}

    def load():
        stats = {}
        members = iter_members(plan["path"])
        try:
            for name, handle in members:
                match = MEMBER_RE.fullmatch(name)
                if not match or match["metadata"] or match["table"] not in writers:
                    continue
                prefix = match["table"]
                if prefix in stats or name != plan["available"][prefix]["filename"]:
                    raise ValueError("LPR3 data member differs from preflight")
                started = time.monotonic()
                stats[prefix] = stream_member(
                    handle, SOURCES_BY_PREFIX[prefix], writers[prefix], plan
                )
                for measure in ("rows", "bytes"):
                    if stats[prefix][measure] != plan["available"][prefix][measure]:
                        label = "row" if measure == "rows" else "byte"
                        raise ValueError(
                            f"LPR3 extract {label} counts differ from the validated ExportLog"
                        )
                writers[prefix].flush()
                seconds = time.monotonic() - started
                stats[prefix]["seconds"] = round(seconds, 3)
                stats[prefix]["rows_per_second"] = round(
                    stats[prefix]["rows"] / max(seconds, 1e-9), 1
                )
                context.log.info(
                    f"LPR3 extract {prefix}: {stats[prefix]['rows']} rows in {seconds:.3f}s"
                )
        except (UnicodeError, csv.Error):
            raise ValueError("LPR3 CSV decoding failed; source values suppressed") from None
        finally:
            members.close()
        expected = {plan["available"][p]["filename"]: plan["available"][p]["rows"] for p in writers}
        actual = {plan["available"][p]["filename"]: item["rows"] for p, item in stats.items()}
        if export_log_deviations(expected, actual):
            raise ValueError("LPR3 extract row counts differ from the validated ExportLog")
        if archive_fingerprint(plan["path"]) != plan["fingerprint"]:
            raise ValueError("LPR3 delivery changed during extraction")
        return stats

    result = run_writers(writers, load)
    conn.commit()
    return result


def write_manifest(conn, plan, groups):
    all_stats = {}
    for group in groups:
        if set(all_stats) & set(group):
            raise ValueError("LPR3 extract groups overlap")
        all_stats.update(group)
    if set(all_stats) != set(plan["available"]):
        raise ValueError("LPR3 manifest cannot publish incomplete extract groups")
    cursor = conn.cursor()
    try:
        sql = f"INSERT INTO [ext].[{MANIFEST_TABLE}] VALUES ({','.join('?' for _ in range(15))})"
        for prefix, stats in sorted(all_stats.items()):
            control = plan["available"][prefix]
            cursor.execute(
                sql,
                (
                    plan["run_id"],
                    prefix,
                    SOURCES_BY_PREFIX[prefix].ext_table,
                    plan["filename"],
                    plan["kadence"],
                    plan["identity"].replace("T", " "),
                    plan["cutoff"],
                    stats["rows"],
                    control["rows"],
                    control["bytes"],
                    stats["audit_min"],
                    stats["audit_distinct"],
                    DELIVERY_UNIT,
                    plan["identity"],
                    int(plan["advance_checkpoint"]),
                ),
            )
        conn.commit()
    finally:
        cursor.close()


@dg.asset(
    group_name="lpr3_extract",
    compute_kind="csv",
    tags={"domain": "lpr3", "layer": "extract", "source": "lpr3"},
    description=(
        "Select the newest SDS LPR3 Grunddata delivery (weekly U or monthly M full snapshot) "
        "newer than the DFM delivery checkpoint, or with delivery_on_or_before the newest "
        "delivery on or before that date regardless of the checkpoint, and validate its "
        "Metadata and ExportLog before any ext table is written. Without a new delivery the "
        "manifest is emptied."
    ),
)
def lpr3_leverance_plan(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: LPR3ExtractConfig,
) -> dict:
    archives = discover_archives(lpr3_dir(config.base_dir))
    checkpoint = DfmManager(mssql_target_staging, context).get_checkpoint(
        DELIVERY_SCHEMA, DELIVERY_UNIT, target_type=DELIVERY_TARGET_TYPE
    )
    pin = date.fromisoformat(config.delivery_on_or_before) if config.delivery_on_or_before else None
    selected = select_delivery(archives, checkpoint, pin)
    if selected is None:
        with mssql_target_staging.get_connection() as conn:
            with claim_run(conn, context):
                execute_statements(conn, manifest_create_sql())
        context.log.info("No new LPR3 delivery; manifest empty and ext data unchanged")
        return {}
    with mssql_target_staging.get_connection() as conn:
        previous = read_last_cutoff(conn)
    started = time.monotonic()
    plan = preflight(selected, previous, source_encoding(ENCODING_ENV))
    context.add_output_metadata({"preflight_seconds": time.monotonic() - started})
    plan.update(run_id=context.run_id, advance_checkpoint=pin is None)
    with mssql_target_staging.get_connection() as conn:
        with claim_run(conn, context):
            # Another run may have archived a newer cutoff during our preflight.
            check_cutoff(plan["cutoff"], read_last_cutoff(conn))
            latest_checkpoint = DfmManager(mssql_target_staging, context).get_checkpoint(
                DELIVERY_SCHEMA, DELIVERY_UNIT, target_type=DELIVERY_TARGET_TYPE
            )
            if pin is None and not is_new_delivery(latest_checkpoint, selected.identity):
                execute_statements(conn, manifest_create_sql())
                context.log.info(
                    "LPR3 delivery was archived by another run during preflight; no-op"
                )
                return {}
            execute_statements(conn, manifest_create_sql())
            for prefix in plan["missing"]:
                execute_statements(
                    conn, f"DROP TABLE IF EXISTS [ext].[{SOURCES_BY_PREFIX[prefix].ext_table}]"
                )
    if plan["missing"]:
        context.log.warning(
            f"LPR3 missing source tables; merge/deletes must be skipped: {plan['missing']}"
        )
    return plan


def _group_asset(index):
    @dg.asset(
        name=f"ext_lpr3_group_{index}",
        group_name="lpr3_extract",
        compute_kind="csv",
        tags={"domain": "lpr3", "layer": "extract", "source": "lpr3"},
        description=(
            f"Stream LPR3 table group {index} from the selected tar.bz2 delivery 1:1 into its "
            "ext tables, validated against the delivery's Metadata and ExportLog."
        ),
    )
    def group(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        lpr3_leverance_plan: dict,
    ) -> dict:
        if not lpr3_leverance_plan:
            return {}
        if lpr3_leverance_plan["run_id"] != context.run_id:
            raise ValueError("LPR3 extract plan belongs to a different Dagster run")
        start = time.monotonic()
        targets = {
            SOURCES_BY_PREFIX[p].ext_table: p
            for p in GROUPS[index]
            if p in lpr3_leverance_plan["available"]
        }
        with (
            DfmManager(mssql_target_staging, context).target_runs(
                "ext",
                targets,
                dagster_run_id=context.run_id,
                mode="full_load",
                **share_connection(os.path.dirname(lpr3_leverance_plan["path"])),
            ) as logged_rows,
            mssql_target_staging.get_connection() as conn,
        ):
            with owned_landing(conn, context.run_id):
                result = load_group(conn, context, lpr3_leverance_plan, GROUPS[index])
            logged_rows.update(
                {SOURCES_BY_PREFIX[p].ext_table: stats["rows"] for p, stats in result.items()}
            )
        context.add_output_metadata(
            {
                "rows": sum(s["rows"] for s in result.values()),
                "seconds": time.monotonic() - start,
                "tables": dg.MetadataValue.json(result),
            }
        )
        return result

    return group


_group_assets = [_group_asset(index) for index in range(len(GROUPS))]


@dg.asset(
    group_name="lpr3_extract",
    compute_kind="csv",
    tags={"domain": "lpr3", "layer": "extract", "source": "lpr3"},
    description=(
        "Publish the run's LPR3 manifest after every table group has loaded; the archive "
        "assets read only this manifest."
    ),
)
def ext_lpr3(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    lpr3_leverance_plan: dict,
    ext_lpr3_group_0: dict,
    ext_lpr3_group_1: dict,
    ext_lpr3_group_2: dict,
    ext_lpr3_group_3: dict,
):
    if not lpr3_leverance_plan:
        return dg.MaterializeResult(metadata={"no_op": True})
    if lpr3_leverance_plan["run_id"] != context.run_id:
        raise ValueError("LPR3 manifest plan belongs to a different Dagster run")
    with mssql_target_staging.get_connection() as conn:
        with owned_landing(conn, context.run_id):
            write_manifest(
                conn,
                lpr3_leverance_plan,
                (ext_lpr3_group_0, ext_lpr3_group_1, ext_lpr3_group_2, ext_lpr3_group_3),
            )
    return dg.MaterializeResult(
        metadata={
            "tables": len(lpr3_leverance_plan["available"]),
            "missing": lpr3_leverance_plan["missing"],
            "identity": lpr3_leverance_plan["identity"],
        }
    )


@dg.asset(
    group_name="lpr3_freshness",
    compute_kind="csv",
    tags={"domain": "lpr3", "layer": "extract", "source": "lpr3"},
    description=(
        f"Staleness alarm for the LPR3 share: fails when the newest delivery is more than "
        f"{MAX_DELIVERY_AGE_DAYS} days old. Reads file names only and is not a dependency "
        f"of the load."
    ),
)
def lpr3_leverance_alder(config: LPR3ExtractConfig):
    archives = discover_archives(lpr3_dir(config.base_dir))
    if not archives:
        raise ValueError("No LPR3 deliveries found")
    age = (date.today() - archives[-1].timestamp.date()).days
    if age > MAX_DELIVERY_AGE_DAYS:
        raise ValueError(
            f"Newest LPR3 delivery is {age} days old; maximum is {MAX_DELIVERY_AGE_DAYS}"
        )
    return dg.MaterializeResult(metadata={"age_days": age, "maximum_days": MAX_DELIVERY_AGE_DAYS})


lpr3_extract_assets = [lpr3_leverance_plan, *_group_assets, ext_lpr3, lpr3_leverance_alder]
