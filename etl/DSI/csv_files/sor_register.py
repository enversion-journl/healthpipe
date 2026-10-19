# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SOR Register extraction asset — the SINGLE download of SOR source data.

Downloads the official SOR CSV from Sundhedsdatastyrelsen and writes it 1:1
(at entity-VERSION grain, keyed code + from_date) to ext.sor_register. The
arc_sor_register asset then upserts ext into the persistent arc.sor_register
archive, which cs_sor (and from there fhir_organization_sor / dim.Organisation)
consumes. There is deliberately no second SOR download anywhere else.

Two extraction modes (SORExtractConfig.load_type):
- "current":        SorEntity_daily.csv — current + future entity versions
- "all_historical": Sor_complete.zip → SOREntity.csv — every version ever

The pipeline is fully streaming — download to disk, iterate CSV rows straight
out of the (zip) file, transform/dedup per row, insert in chunks. Only the
(code, from_date) key set is held in memory; the all_historical file is far
too large to materialize as Python dicts (it OOM-kills the run container).

Data source: https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/
Documentation: https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/docs/SOR2-CSV-dokumentation.html
"""

import csv
import io
import os
import shutil
import zipfile
from collections.abc import Iterable, Iterator
from urllib.request import Request, urlopen

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import http_source
from core.org_config import DATA_DIR
from core.resources import MSSQLResource
from dagster import AssetExecutionContext
from pydantic import Field

BASE_URL = "https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00"
DAILY_CSV_URL = f"{BASE_URL}/data/SorEntity_daily.csv"
COMPLETE_ZIP_URL = f"{BASE_URL}/data/Sor_complete.zip"

PERSIST_DIR = os.path.join(DATA_DIR, "sor_raw")

LOAD_TYPE_CURRENT = "current"
LOAD_TYPE_ALL_HISTORICAL = "all_historical"
# Legacy aliases accepted for backwards-compatible run configs.
_LOAD_TYPE_ALIASES = {"daily": LOAD_TYPE_CURRENT, "full": LOAD_TYPE_ALL_HISTORICAL}

_DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Dagster-SOR-Loader/1.0)",
    "Accept": "*/*",
}

INSERT_CHUNK_SIZE = 1000

# Version key columns + raw attribute columns. from_date/to_date are the raw
# version validity bounds; cs_sor projects the CURRENT version's to_date as the
# closingDate property (openingDate = FirstFromDate = first version's start).
EXT_COLUMNS = [
    "code",
    "from_date",
    "to_date",
    "display",
    "parent_code",
    "entityType",
    "entityTypeId",
    "municipality",
    "region",
    "owner",
    "ownerId",
    "cvrNumber",
    "website",
    "email",
    "phone",
    "address",
    "postalCode",
    "city",
    "openingDate",
]

CSV_TO_EXT_MAP = {
    "SorId": "code",
    "FromDate": "from_date",
    "ToDate": "to_date",
    "EntityName": "display",
    "ParentSorId": "parent_code",
    "EntityTypeName": "entityType",
    "EntityTypeId": "entityTypeId",
    "PostalAddressMunicipalityCode": "municipality",
    "PostalAddressRegionCode": "region",
    "InstitutionOwnerEntityName": "owner",
    "InstitutionOwnerSorId": "ownerId",
    "InstitutionOwnerCvrNumberId": "cvrNumber",
    "VirtualAddressWebsite": "website",
    "VirtualAddressEmailAddress": "email",
    "VirtualAddressTelephoneNumber": "phone",
    "PostalAddressStreetName": "address",
    "PostalAddressPostCodeId": "postalCode",
    "PostalAddressDistrictName": "city",
    "FirstFromDate": "openingDate",
}

_SOR_ID_CSV_COLUMNS = {
    "SorId",
    "ParentSorId",
    "EntityTypeId",
    "InstitutionOwnerSorId",
    "InstitutionOwnerCvrNumberId",
}


def clean_sor_id(value: str) -> str:
    """Remove Excel formula wrapping: =\"21000016006\" -> 21000016006."""
    if not value:
        return ""
    value = value.strip()
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]
    return value.strip()


def _download_to_file(url: str, file_path: str, timeout: int) -> int:
    """Stream a URL to disk without holding the payload in memory. Returns size."""
    os.makedirs(PERSIST_DIR, exist_ok=True)
    req = Request(url, headers=_DOWNLOAD_HEADERS)
    with urlopen(req, timeout=timeout) as response, open(file_path, "wb") as f:
        shutil.copyfileobj(response, f)
    return os.path.getsize(file_path)


def _iter_daily_rows(context: dg.AssetExecutionContext) -> Iterator[dict[str, str]]:
    """Download SorEntity_daily.csv and yield parsed rows (streaming)."""
    context.log.info(f"Downloading current SOR data from {DAILY_CSV_URL}")
    file_path = os.path.join(PERSIST_DIR, "SorEntity_daily.csv")
    size = _download_to_file(DAILY_CSV_URL, file_path, timeout=120)
    context.log.info(f"Saved daily CSV to {file_path} ({size} bytes)")

    with open(file_path, encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f, delimiter=";")


def _iter_complete_rows(context: dg.AssetExecutionContext) -> Iterator[dict[str, str]]:
    """Download Sor_complete.zip and yield SOREntity.csv rows (streaming).

    The CSV is read directly out of the zip member — it is never decompressed
    to a string or materialized as a row list.
    """
    context.log.info(f"Downloading complete SOR dataset from {COMPLETE_ZIP_URL}")
    zip_path = os.path.join(PERSIST_DIR, "Sor_complete.zip")
    size = _download_to_file(COMPLETE_ZIP_URL, zip_path, timeout=300)
    context.log.info(f"Saved complete ZIP to {zip_path} ({size} bytes)")

    with zipfile.ZipFile(zip_path) as zf:
        with zf.open("SOREntity.csv") as member:
            text = io.TextIOWrapper(member, encoding="utf-8-sig", newline="")
            yield from csv.DictReader(text, delimiter=";")


def _transform_row(raw: dict[str, str]) -> dict[str, str] | None:
    """Map one SOR CSV row to an ext.sor_register row; None if SorId is empty."""
    code = clean_sor_id(raw.get("SorId", ""))
    if not code:
        return None

    ext_row: dict[str, str] = {}
    for csv_col, ext_col in CSV_TO_EXT_MAP.items():
        value = raw.get(csv_col, "")
        if csv_col in _SOR_ID_CSV_COLUMNS:
            value = clean_sor_id(value)
        ext_row[ext_col] = value.strip() if value else ""
    return ext_row


def _iter_ext_rows(
    raw_rows: Iterable[dict[str, str]], stats: dict[str, int]
) -> Iterator[dict[str, str]]:
    """Transform SOR CSV rows 1:1 into ext.sor_register version-grain rows.

    No dedup-to-latest here — every version row is kept so the archive can
    preserve full history. Rows are only deduplicated on the version key
    (code, from_date) itself, keeping the first occurrence, so the arc MERGE
    never sees duplicate source keys. Only the key set is held in memory.

    Mutates ``stats`` (keys: skipped, duplicates, codes = distinct SOR codes).
    """
    seen: set[tuple[str, str]] = set()
    codes: set[str] = set()

    for raw in raw_rows:
        ext_row = _transform_row(raw)
        if ext_row is None:
            stats["skipped"] += 1
            continue

        key = (ext_row["code"], ext_row["from_date"])
        if key in seen:
            stats["duplicates"] += 1
            continue
        seen.add(key)
        codes.add(ext_row["code"])
        yield ext_row

    stats["codes"] = len(codes)


def _transform_to_ext_format(
    raw_rows: list[dict[str, str]], context: dg.AssetExecutionContext
) -> list[dict[str, str]]:
    """List-based wrapper around _iter_ext_rows (tests and small inputs).

    The asset itself streams — it never materializes the full row list.
    """
    stats = {"skipped": 0, "duplicates": 0}
    ext_rows = list(_iter_ext_rows(raw_rows, stats))
    if stats["skipped"]:
        context.log.warning(f"Skipped {stats['skipped']} rows with empty SOR ID")
    if stats["duplicates"]:
        context.log.warning(
            f"Collapsed {stats['duplicates']} rows with duplicate (SorId, FromDate) "
            f"— kept first occurrence"
        )
    context.log.info(f"Transformed {len(ext_rows)} version rows to ext format")
    return ext_rows


def _write_to_ext_table(
    conn, ext_rows: Iterable[dict[str, str]], context: dg.AssetExecutionContext
) -> int:
    """Drop/create ext.sor_register and insert rows in streaming chunks."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS [ext].[sor_register]")

        col_defs = []
        for col in EXT_COLUMNS:
            if col == "code":
                col_defs.append(f"[{col}] NVARCHAR(100) NOT NULL")
            elif col == "from_date":
                col_defs.append(f"[{col}] NVARCHAR(50) NOT NULL")
            elif col == "to_date":
                col_defs.append(f"[{col}] NVARCHAR(50) NULL")
            else:
                col_defs.append(f"[{col}] NVARCHAR(500) NULL")
        col_defs.append("[dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
        col_defs.append("[dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()")

        ddl = f"CREATE TABLE [ext].[sor_register] (\n    {','.join(col_defs)}\n)"
        cursor.execute(ddl)

        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        placeholders = ", ".join(["?"] * len(EXT_COLUMNS))
        col_names = ", ".join(f"[{c}]" for c in EXT_COLUMNS)
        insert_sql = f"INSERT INTO [ext].[sor_register] ({col_names}) VALUES ({placeholders})"

        total_inserted = 0
        buffer: list[tuple] = []

        def _flush() -> None:
            nonlocal total_inserted
            if buffer:
                cursor.executemany(insert_sql, buffer)
                total_inserted += len(buffer)
                buffer.clear()

        for row in ext_rows:
            buffer.append(
                tuple(
                    # NOT NULL key columns keep '' (part of the arc PK);
                    # attribute columns store NULL for empty values.
                    (row.get(col, "") or ("" if col in ("code", "from_date") else None))
                    for col in EXT_COLUMNS
                )
            )
            if len(buffer) >= INSERT_CHUNK_SIZE:
                _flush()
                if total_inserted % 100_000 == 0:
                    context.log.info(f"  ... {total_inserted} rows inserted")
        _flush()

        conn.commit()
        context.log.info(f"Inserted {total_inserted} rows into ext.sor_register")
        return total_inserted
    finally:
        cursor.close()


class SORExtractConfig(dg.Config):
    """Runtime configuration for SOR register extraction."""

    load_type: str = Field(
        default=LOAD_TYPE_CURRENT,
        description=(
            'Which SOR file to download. Must be one of: "current" '
            "(SorEntity_daily.csv — snapshot of current + future entity versions; "
            'the daily default), "all_historical" (Sor_complete.zip — every entity '
            "version ever published; run once to backfill arc.sor_register). "
            'Legacy aliases "daily"/"full" are accepted.'
        ),
    )


@dg.asset(
    name="extract_sor_register",
    group_name="codesystem_extract",
    compute_kind="python",
    tags={"domain": "codesystem", "layer": "extract", "source": "sor"},
    description=(
        "Download the SOR register CSV from Sundhedsdatastyrelsen (the single SOR "
        "download) and load it 1:1 at version grain into ext.sor_register. "
        "Modes: 'current' (daily snapshot) / 'all_historical' (complete history)."
    ),
)
def extract_sor_register(
    context: AssetExecutionContext,
    config: SORExtractConfig,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    load_type = _LOAD_TYPE_ALIASES.get(config.load_type, config.load_type)
    if load_type == LOAD_TYPE_CURRENT:
        source_url = DAILY_CSV_URL
        raw_rows = _iter_daily_rows(context)
    elif load_type == LOAD_TYPE_ALL_HISTORICAL:
        source_url = COMPLETE_ZIP_URL
        raw_rows = _iter_complete_rows(context)
    else:
        raise ValueError(
            f"Invalid load_type: {config.load_type}. "
            f"Must be '{LOAD_TYPE_CURRENT}' or '{LOAD_TYPE_ALL_HISTORICAL}'"
        )

    stats = {"skipped": 0, "duplicates": 0, "codes": 0}
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ext",
        "sor_register",
        dagster_run_id=context.run_id,
        mode="full_load",
        **http_source(source_url),
    )
    try:
        with mssql_target_staging.get_connection() as conn:
            row_count = _write_to_ext_table(conn, _iter_ext_rows(raw_rows, stats), context)
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    if stats["skipped"]:
        context.log.warning(f"Skipped {stats['skipped']} rows with empty SOR ID")
    if stats["duplicates"]:
        context.log.warning(
            f"Collapsed {stats['duplicates']} rows with duplicate (SorId, FromDate) "
            f"— kept first occurrence"
        )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "unique_codes": dg.MetadataValue.int(stats["codes"]),
            "skipped_rows": dg.MetadataValue.int(stats["skipped"]),
            "duplicate_version_keys": dg.MetadataValue.int(stats["duplicates"]),
            "load_type": dg.MetadataValue.text(load_type),
            "source": dg.MetadataValue.text("sor-filer.sundhedsdata.dk"),
        }
    )
