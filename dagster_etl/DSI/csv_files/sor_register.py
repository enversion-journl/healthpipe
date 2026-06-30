# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SOR Register extraction asset.

Downloads the official SOR CSV from Sundhedsdatastyrelsen and writes
to ext.sor_register in wide format (Code, Display, ParentCode + property columns)
for consumption by the cs_sor CodeSystem builder.

Data source: https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/
Documentation: https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/docs/SOR2-CSV-dokumentation.html
"""

import csv
import io
import os
import zipfile
from urllib.request import Request, urlopen

import dagster as dg
from core.org_config import DATA_DIR
from core.resources import MSSQLResource
from dagster import AssetExecutionContext

BASE_URL = "https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00"
DAILY_CSV_URL = f"{BASE_URL}/data/SorEntity_daily.csv"
COMPLETE_ZIP_URL = f"{BASE_URL}/data/Sor_complete.zip"

PERSIST_DIR = os.path.join(DATA_DIR, "sor_raw")

EXT_COLUMNS = [
    "Code",
    "Display",
    "ParentCode",
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
    "closingDate",
]

CSV_TO_EXT_MAP = {
    "SorId": "Code",
    "EntityName": "Display",
    "ParentSorId": "ParentCode",
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
    "ToDate": "closingDate",
}


def _clean_sor_id(value: str) -> str:
    """Remove Excel formula wrapping: =\"21000016006\" -> 21000016006."""
    if not value:
        return ""
    value = value.strip()
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]
    return value.strip()


def _download_daily(context: dg.AssetExecutionContext) -> list[dict[str, str]]:
    """Download SorEntity_daily.csv and return parsed rows."""
    context.log.info(f"Downloading daily SOR data from {DAILY_CSV_URL}")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Dagster-SOR-Loader/1.0)", "Accept": "*/*"}
    req = Request(DAILY_CSV_URL, headers=headers)

    with urlopen(req, timeout=120) as response:
        csv_content = response.read().decode("utf-8-sig")

    os.makedirs(PERSIST_DIR, exist_ok=True)
    file_path = os.path.join(PERSIST_DIR, "SorEntity_daily.csv")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(csv_content)
    context.log.info(f"Saved daily CSV to {file_path} ({len(csv_content)} bytes)")

    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
    rows = list(reader)
    context.log.info(f"Parsed {len(rows)} rows from daily CSV")
    return rows


def _download_complete(context: dg.AssetExecutionContext) -> list[dict[str, str]]:
    """Download Sor_complete.zip, extract SOREntity.csv, and return parsed rows."""
    context.log.info(f"Downloading complete SOR dataset from {COMPLETE_ZIP_URL}")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Dagster-SOR-Loader/1.0)", "Accept": "*/*"}
    req = Request(COMPLETE_ZIP_URL, headers=headers)

    with urlopen(req, timeout=300) as response:
        zip_content = response.read()

    os.makedirs(PERSIST_DIR, exist_ok=True)
    zip_path = os.path.join(PERSIST_DIR, "Sor_complete.zip")
    with open(zip_path, "wb") as f:
        f.write(zip_content)
    context.log.info(f"Saved complete ZIP to {zip_path} ({len(zip_content)} bytes)")

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        with zf.open("SOREntity.csv") as csv_file:
            csv_content = csv_file.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
    rows = list(reader)
    context.log.info(f"Parsed {len(rows)} rows from complete dataset")
    return rows


def _deduplicate_by_latest(
    rows: list[dict[str, str]], context: dg.AssetExecutionContext
) -> list[dict[str, str]]:
    """Keep only the latest version of each SOR ID (highest FromDate)."""
    sor_versions: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        sor_id = _clean_sor_id(row.get("SorId", ""))
        if sor_id:
            sor_versions.setdefault(sor_id, []).append(row)

    duplicates = sum(1 for v in sor_versions.values() if len(v) > 1)
    if duplicates:
        context.log.warning(f"Found {duplicates} SOR IDs with multiple versions, keeping latest")

    result: list[dict[str, str]] = []
    for versions in sor_versions.values():
        if len(versions) == 1:
            result.append(versions[0])
        else:
            best = max(versions, key=lambda r: r.get("FromDate", "") or "")
            result.append(best)

    context.log.info(
        f"Deduplication: {len(rows)} rows → {len(result)} unique SOR IDs "
        f"({len(rows) - len(result)} duplicates removed)"
    )
    return result


def _transform_to_ext_format(
    raw_rows: list[dict[str, str]], context: dg.AssetExecutionContext
) -> list[dict[str, str]]:
    """Transform SOR CSV rows into ext.sor_register wide format."""
    ext_rows: list[dict[str, str]] = []
    skipped = 0

    for raw in raw_rows:
        code = _clean_sor_id(raw.get("SorId", ""))
        if not code:
            skipped += 1
            continue

        ext_row: dict[str, str] = {}
        for csv_col, ext_col in CSV_TO_EXT_MAP.items():
            value = raw.get(csv_col, "")
            if csv_col in ("SorId", "ParentSorId", "EntityTypeId", "InstitutionOwnerSorId"):
                value = _clean_sor_id(value)
            elif csv_col == "InstitutionOwnerCvrNumberId":
                value = _clean_sor_id(value)
            ext_row[ext_col] = value.strip() if value else ""

        ext_rows.append(ext_row)

    if skipped:
        context.log.warning(f"Skipped {skipped} rows with empty SOR ID")
    context.log.info(f"Transformed {len(ext_rows)} rows to ext format")
    return ext_rows


def _write_to_ext_table(
    conn, ext_rows: list[dict[str, str]], context: dg.AssetExecutionContext
) -> int:
    """Drop/create ext.sor_register and insert all rows."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS [ext].[sor_register]")

        col_defs = []
        for col in EXT_COLUMNS:
            if col == "Code":
                col_defs.append(f"[{col}] NVARCHAR(100) NOT NULL")
            else:
                col_defs.append(f"[{col}] NVARCHAR(500) NULL")
        col_defs.append("[DW_Timestamp_Insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
        col_defs.append("[DW_Timestamp_Update] DATETIME2(0) NOT NULL DEFAULT GETDATE()")

        ddl = f"CREATE TABLE [ext].[sor_register] (\n" f"    {','.join(col_defs)}\n" f")"
        cursor.execute(ddl)

        placeholders = ", ".join(["?"] * len(EXT_COLUMNS))
        col_names = ", ".join(f"[{c}]" for c in EXT_COLUMNS)
        insert_sql = f"INSERT INTO [ext].[sor_register] ({col_names}) VALUES ({placeholders})"

        chunk_size = 1000
        total_inserted = 0
        for i in range(0, len(ext_rows), chunk_size):
            chunk = ext_rows[i : i + chunk_size]
            rows_to_insert = [
                tuple(row.get(col, "") or None for col in EXT_COLUMNS) for row in chunk
            ]
            cursor.executemany(insert_sql, rows_to_insert)
            total_inserted += len(chunk)

        conn.commit()
        context.log.info(f"Inserted {total_inserted} rows into ext.sor_register")
        return total_inserted
    finally:
        cursor.close()


class SORExtractConfig(dg.Config):
    """Runtime configuration for SOR register extraction.

    Attributes:
        load_type: 'daily' for active/future entities only, 'full' for complete history
    """

    load_type: str = "daily"


@dg.asset(
    name="extract_sor_register",
    group_name="codesystem_extract",
    compute_kind="python",
    tags={"domain": "codesystem", "layer": "extract", "source": "sor"},
    description=(
        "Download SOR register CSV from Sundhedsdatastyrelsen and load into ext.sor_register. "
        "Produces wide-format rows (Code, Display, ParentCode + property columns) "
        "for the cs_sor CodeSystem builder."
    ),
)
def extract_sor_register(
    context: AssetExecutionContext,
    config: SORExtractConfig,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    if config.load_type == "daily":
        raw_rows = _download_daily(context)
    elif config.load_type == "full":
        raw_rows = _download_complete(context)
    else:
        raise ValueError(f"Invalid load_type: {config.load_type}. Must be 'daily' or 'full'")

    deduped = _deduplicate_by_latest(raw_rows, context)
    ext_rows = _transform_to_ext_format(deduped, context)

    with mssql_target_staging.get_connection() as conn:
        row_count = _write_to_ext_table(conn, ext_rows, context)

    active = sum(1 for r in ext_rows if not r.get("closingDate"))
    closed = len(ext_rows) - active

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "load_type": dg.MetadataValue.text(config.load_type),
            "active_count": dg.MetadataValue.int(active),
            "closed_count": dg.MetadataValue.int(closed),
            "source": dg.MetadataValue.text("sor-filer.sundhedsdata.dk"),
        }
    )
