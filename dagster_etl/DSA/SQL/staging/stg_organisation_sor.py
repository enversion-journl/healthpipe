# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SOR staging asset - Downloads and parses SOR CSV data.

Based on official SOR documentation:
https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/docs/SOR2-CSV-dokumentation.html
"""

import csv
import io
import os
import tempfile
import zipfile
from datetime import datetime
from typing import Any
from urllib.request import Request, urlopen

import dagster as dg
from core.org_config import DATA_DIR

# SOR data URLs
BASE_URL = "https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00"
DAILY_CSV_URL = f"{BASE_URL}/data/SorEntity_daily.csv"
COMPLETE_ZIP_URL = f"{BASE_URL}/data/Sor_complete.zip"


class OrganisationSORConfig(dg.Config):
    """Runtime configuration for SOR data loading.

    Attributes:
        load_type: Load type - 'daily' for incremental, 'full' for complete refresh
        persist_files: If True, save downloaded files to temp directory
        temp_dir: Temporary directory for downloaded files (use workspace path for persistence)
    """

    load_type: str = "daily"
    persist_files: bool = True
    temp_dir: str = os.path.join(DATA_DIR, "sor_raw")


def parse_sor_date(date_str: str) -> datetime | None:
    """
    Parse SOR date string to datetime.

    SOR dates are in format: YYYY-MM-DD HH:MM:SS
    """
    if not date_str or date_str.strip() == "":
        return None

    try:
        # Try full datetime format
        return datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            # Try date only format
            return datetime.strptime(date_str.strip(), "%Y-%m-%d")
        except ValueError:
            return None


def clean_sor_id(value: str) -> str:
    """Clean SOR ID from Excel formula format.

    Removes Excel formula wrapping and quotes from SOR IDs.
    Format in CSV: ="21000016006"

    Args:
        value: Raw value from CSV

    Returns:
        Cleaned SOR ID string
    """
    if not value:
        return ""

    # Remove whitespace
    value = value.strip()

    # Remove Excel formula format: ="..." -> ...
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]

    return value.strip()


def determine_status(to_date_str: str) -> str:
    """
    Determine organisation status based on ToDate.

    Rules per documentation:
    - If ToDate is undefined/empty or in the future: "Active"
    - If ToDate is in the past: "Closed"
    """
    if not to_date_str or to_date_str.strip() == "":
        return "Active"

    to_date = parse_sor_date(to_date_str)
    if not to_date:
        return "Active"

    if to_date > datetime.now():
        return "Active"
    else:
        return "Closed"


def deduplicate_sor_by_latest(
    rows: list[dict[str, Any]], context: dg.AssetExecutionContext, source_description: str
) -> list[dict[str, Any]]:
    """
    Remove duplicate SORId entries, keeping only the latest version (highest FromDate).

    This handles cases where the same SORId appears multiple times in the source file.

    Args:
        rows: All rows from SOR CSV
        context: Dagster context for logging
        source_description: Description for logging (e.g., "daily" or "complete")

    Returns:
        Deduplicated list with only latest version of each SOR ID
    """
    # Group by SORId
    sor_versions: dict[str, list[dict]] = {}

    for row in rows:
        # Note: column name is 'SorId', not 'SORId'
        sor_id = clean_sor_id(row.get("SorId", ""))
        if sor_id:
            if sor_id not in sor_versions:
                sor_versions[sor_id] = []
            sor_versions[sor_id].append(row)

    # Check for duplicates
    duplicates = {
        sor_id: versions for sor_id, versions in sor_versions.items() if len(versions) > 1
    }

    if duplicates:
        context.log.warning(
            f"Found {len(duplicates)} SOR IDs with multiple versions in {source_description} file. "
            f"Examples: {list(duplicates.keys())[:5]}"
        )

        # Log details for first few duplicates
        for sor_id, versions in list(duplicates.items())[:3]:
            from_dates = [v.get("FromDate", "N/A") for v in versions]
            context.log.warning(
                f"  SORId {sor_id} has {len(versions)} versions with FromDates: {from_dates}"
            )
    else:
        context.log.info(f"No duplicate SOR IDs found in {source_description} file")

    # Keep only the version with highest FromDate for each SORId
    deduplicated_rows = []

    for sor_id, versions in sor_versions.items():
        if len(versions) == 1:
            deduplicated_rows.append(versions[0])
        else:
            # Sort by FromDate (newest first), then take the first
            sorted_versions = sorted(
                versions,
                key=lambda r: parse_sor_date(r.get("FromDate", "")) or datetime.min,
                reverse=True,
            )

            deduplicated_rows.append(sorted_versions[0])

            context.log.debug(
                f"Kept version with FromDate={sorted_versions[0].get('FromDate')} "
                f"for SORId {sor_id}"
            )

    context.log.info(
        f"Deduplication: {len(rows)} total rows → {len(deduplicated_rows)} unique SOR IDs "
        f"({len(rows) - len(deduplicated_rows)} duplicates removed)"
    )

    return deduplicated_rows


def download_sor_daily(
    temp_dir: str, context: dg.AssetExecutionContext
) -> tuple[str, list[dict[str, Any]]]:
    """
    Download SorEntity_daily.csv.

    Note: Even though this is the "daily" file with only active/future entities,
    we still check for and handle duplicate SORIds (keeping latest FromDate).
    """
    context.log.info(f"Downloading daily SOR data from {DAILY_CSV_URL}")

    headers = {"User-Agent": "Mozilla/5.0 (compatible; Dagster-SOR-Loader/1.0)", "Accept": "*/*"}

    req = Request(DAILY_CSV_URL, headers=headers)

    with urlopen(req, timeout=120) as response:
        csv_content = response.read().decode("utf-8-sig")  # utf-8-sig strips BOM automatically

    # Save to temp directory (overwrite existing file)
    file_path = os.path.join(temp_dir, "SorEntity_daily.csv")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(csv_content)

    context.log.info(f"Saved daily CSV to {file_path} ({len(csv_content)} bytes)")

    # Parse CSV (delimiter is semicolon)
    csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
    all_rows = list(csv_reader)

    context.log.info(f"Parsed {len(all_rows)} rows from daily CSV")

    # Deduplicate (keep latest FromDate per SORId)
    deduplicated_rows = deduplicate_sor_by_latest(all_rows, context, "daily")

    return file_path, deduplicated_rows


def download_sor_complete(
    temp_dir: str, context: dg.AssetExecutionContext
) -> tuple[str, list[dict[str, Any]]]:
    """
    Download Sor_complete.zip and extract SOREntity.csv (full history).

    Returns only the latest version of each SOR ID (highest FromDate).
    """
    context.log.info(f"Downloading complete SOR dataset from {COMPLETE_ZIP_URL}")

    headers = {"User-Agent": "Mozilla/5.0 (compatible; Dagster-SOR-Loader/1.0)", "Accept": "*/*"}

    req = Request(COMPLETE_ZIP_URL, headers=headers)

    with urlopen(req, timeout=300) as response:
        zip_content = response.read()

    # Save ZIP file (overwrite existing file)
    zip_path = os.path.join(temp_dir, "Sor_complete.zip")
    with open(zip_path, "wb") as f:
        f.write(zip_content)

    context.log.info(f"Saved complete ZIP to {zip_path} ({len(zip_content)} bytes)")

    # Extract SOREntity.csv from ZIP
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        context.log.info(f"ZIP contains files: {zf.namelist()}")

        # Extract SOREntity.csv (note: capital letters in complete dataset)
        with zf.open("SOREntity.csv") as csv_file:
            csv_content = csv_file.read().decode("utf-8-sig")  # utf-8-sig strips BOM automatically

        # Save extracted CSV (overwrite existing file)
        csv_path = os.path.join(temp_dir, "SOREntity.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_content)

        context.log.info(f"Extracted CSV to {csv_path} ({len(csv_content)} bytes)")

    # Parse CSV
    csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
    all_rows = list(csv_reader)

    context.log.info(f"Parsed {len(all_rows)} total rows from complete dataset (includes history)")

    # Deduplicate to latest versions only
    deduplicated_rows = deduplicate_sor_by_latest(all_rows, context, "complete")

    return csv_path, deduplicated_rows


@dg.asset(
    name="stg_organisation_sor",
    group_name="shared_organisation_staging",
    io_manager_key="io_manager",
    compute_kind="python",
)
def stg_organisation_sor(
    context: dg.AssetExecutionContext,
    config: OrganisationSORConfig,
) -> list[dict[str, Any]]:
    """
    Download and parse SOR organisation data from Sundhedsdatastyrelsen.

    Handles duplicate SORIds by keeping only the latest version (highest FromDate).
    This applies to both daily and complete files.

    Data source: https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/
    Documentation: https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/docs/SOR2-CSV-dokumentation.html

    Runtime Config:
        - load_type: 'daily' or 'full'
        - persist_files: Save CSV/ZIP files to disk
        - temp_dir: Where to save files

    SOR CSV Columns (only relevant columns extracted):
        - SORId: SOR code (e.g., "265731000016003")
        - EntityName: Organisation name
        - ParentSorId: Parent SOR ID (for hierarchy)
        - FromDate: Version start date (used for deduplication)
        - ToDate: Version end date (used to determine Active/Closed status)

    Returns:
        List of dicts with SOR organisation data (deduplicated, latest versions only)
    """

    # Create temp directory if needed
    if config.persist_files:
        os.makedirs(config.temp_dir, exist_ok=True)
        context.log.info(f"Files will be saved to {config.temp_dir}")
    else:
        # Use Python's temporary directory (auto-cleanup)
        config.temp_dir = tempfile.mkdtemp(prefix="dagster_sor_")
        context.log.info(f"Using temporary directory: {config.temp_dir}")

    # Download based on load type
    if config.load_type == "daily":
        file_path, sor_rows = download_sor_daily(config.temp_dir, context)
    elif config.load_type == "full":
        file_path, sor_rows = download_sor_complete(config.temp_dir, context)
    else:
        raise ValueError(f"Invalid load_type: {config.load_type}. Must be 'daily' or 'full'")

    # Clean and enrich data
    enriched_rows = []

    for row in sor_rows:
        # Clean SorId (note: column name is 'SorId', not 'SORId')
        # Remove Excel formatting if present: "=""123""" -> 123
        sor_id = clean_sor_id(row.get("SorId", ""))

        if not sor_id:
            continue

        # Clean ParentSorId (note: column name is 'ParentSorId', not 'ParentSORId')
        parent_sor_id = clean_sor_id(row.get("ParentSorId", ""))

        # Determine status from ToDate
        status = determine_status(row.get("ToDate", ""))

        # Build enriched row (only columns we need)
        # Note: We normalize the output to use 'SORId' (all caps) for consistency with FHIR usage
        enriched_row = {
            "SORId": sor_id,  # Normalized from CSV's 'SorId'
            "EntityName": row.get("EntityName", "").strip(),
            "EntityTypeName": row.get("EntityTypeName", "").strip() or None,  # Organisation type
            "ParentSorId": parent_sor_id if parent_sor_id else None,  # Matches CSV column name
            "FromDate": row.get("FromDate", ""),
            "ToDate": row.get("ToDate", ""),
            "Status": status,  # Computed field
        }

        enriched_rows.append(enriched_row)

    context.log.info(f"Processed {len(enriched_rows)} SOR organisations")

    # Log statistics
    status_counts = {"Active": 0, "Closed": 0}
    has_parent_count = 0

    for row in enriched_rows:
        status_counts[row["Status"]] = status_counts.get(row["Status"], 0) + 1

        if row.get("ParentSorId"):
            has_parent_count += 1

    context.log.info(f"Status distribution: {status_counts}")
    context.log.info(f"Organisations with hierarchy: {has_parent_count}/{len(enriched_rows)}")

    return enriched_rows
