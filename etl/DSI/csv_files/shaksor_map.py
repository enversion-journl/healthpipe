# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SHAK/SOR map extraction asset.

The official ``Sor_complete.zip`` from Sundhedsdatastyrelsen contains
``SHAKSORMap.csv`` alongside ``SOREntity.csv``. That file is the authoritative
crosswalk between SHAK (Sygehus-Afdelingsklassifikation) codes and SOR
organisation codes, and also carries the SHAK master attributes.

This asset downloads the complete ZIP (reusing the copy persisted by the SOR
extract when present), keeps the single newest *direct* mapping per SHAK code,
and writes ``ext.shaksor_map`` in wide format. Two downstream builders consume
it: ``cs_shak`` (the SHAK CodeSystem) and ``cm_shak_to_sor`` (the SHAK->SOR
ConceptMap).

Data source: https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/
"""

import csv
import io
import os
import zipfile
from urllib.request import Request, urlopen

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import http_source
from core.resources import MSSQLResource
from dagster import AssetExecutionContext
from DSA.FHIR.utils.code_normalization import normalize_shak_code, normalize_sor_code
from DSI.csv_files.sor_register import COMPLETE_ZIP_URL, PERSIST_DIR

SHAKSOR_CSV_NAME = "SHAKSORMap.csv"

# Property columns carried on each SHAK concept (ext.shaksor_map wide format).
# validFrom/validTo are emitted as Date properties; the rest as String.
EXT_PROPERTY_COLUMNS = [
    "institutionTypeId",
    "institutionType",
    "ownerTypeId",
    "ownerType",
    "mainSpecialityId",
    "mainSpeciality",
    "speciality1Id",
    "speciality1",
    "speciality2Id",
    "speciality2",
    "speciality3Id",
    "speciality3",
    "validFrom",
    "validTo",
]

# SorDisplay is the SOR organisation name carried straight from SHAKSORMap so the
# SHAK->SOR ConceptMap keeps a target display even for CLOSED SOR entities (which
# are absent from the daily-loaded SOR codesystem / ref.Organisation).
# isDirect records whether the selected mapping row was direct — selection prefers
# direct rows, but a SHAK code with only indirect candidates falls back to the
# newest indirect one (kept so every SHAK code stays in the codesystem/conceptmap;
# the ConceptMap downgrades such rows to relationship 'inexact').
EXT_COLUMNS = ["code", "display", "SorCode", "SorDisplay", "isDirect", *EXT_PROPERTY_COLUMNS]

# SHAKSORMap.csv column -> ext.shaksor_map column.
CSV_TO_EXT_MAP = {
    "ShakName": "display",
    "SorEntityName": "SorDisplay",
    "ShakInstitutionTypeId": "institutionTypeId",
    "ShakInstitutionTypeName": "institutionType",
    "ShakOwnerTypeId": "ownerTypeId",
    "ShakOwnerTypeName": "ownerType",
    "ShakMainSpecialityId": "mainSpecialityId",
    "ShakMainSpecialityName": "mainSpeciality",
    "ShakSpeciality1Id": "speciality1Id",
    "ShakSpeciality1Name": "speciality1",
    "ShakSpeciality2Id": "speciality2Id",
    "ShakSpeciality2Name": "speciality2",
    "ShakSpeciality3Id": "speciality3Id",
    "ShakSpeciality3Name": "speciality3",
    "ShakDateFrom": "validFrom",
    "ShakDateTo": "validTo",
}


def _clean_excel(value: str | None) -> str:
    """Remove Excel formula wrapping: ``="7951000016004"`` -> ``7951000016004``."""
    if not value:
        return ""
    value = value.strip()
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]
    return value.strip()


def _is_direct(row: dict[str, str]) -> bool:
    return (row.get("IsDirectSorShakMapping") or "").strip().lower() == "true"


def _selection_key(row: dict[str, str]) -> tuple:
    """Rank candidate rows for a SHAK code: prefer the newest *direct* mapping.

    Order (descending): direct over indirect, currently-open (empty ToDate)
    over closed, then latest FromDate, then latest UpdatedAt.
    """
    return (
        1 if _is_direct(row) else 0,
        1 if not (row.get("ToDate") or "").strip() else 0,
        (row.get("FromDate") or "").strip(),
        (row.get("UpdatedAt") or "").strip(),
    )


def select_latest_direct(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Collapse all mapping versions to one row per SHAK code.

    Direct mappings always outrank indirect ones, so the newest DIRECT row wins
    whenever one exists. A SHAK code with only indirect candidates falls back to
    its newest indirect row rather than being dropped — the flag is carried to
    ext.shaksor_map as isDirect and downgraded downstream.
    """
    best: dict[str, tuple[tuple, dict[str, str]]] = {}
    for row in rows:
        shak = normalize_shak_code(_clean_excel(row.get("ShakId")))
        if not shak:
            continue
        key = _selection_key(row)
        current = best.get(shak)
        if current is None or key > current[0]:
            best[shak] = (key, row)
    return [normalized for _, normalized in best.values()]


def transform_to_ext_format(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Map the selected SHAKSORMap rows into ext.shaksor_map wide format.

    Rows without a SHAK code or a SOR target are dropped (both are required to
    be a usable concept + mapping).
    """
    ext_rows: list[dict[str, str]] = []
    for row in rows:
        code = normalize_shak_code(_clean_excel(row.get("ShakId")))
        sor_code = normalize_sor_code(_clean_excel(row.get("SorId")))
        if not code or not sor_code:
            continue

        ext_row: dict[str, str] = {col: "" for col in EXT_COLUMNS}
        ext_row["code"] = code
        ext_row["SorCode"] = sor_code
        ext_row["isDirect"] = "True" if _is_direct(row) else "False"
        for csv_col, ext_col in CSV_TO_EXT_MAP.items():
            value = (row.get(csv_col) or "").strip()
            ext_row[ext_col] = value
        if not ext_row["display"]:
            ext_row["display"] = code
        ext_rows.append(ext_row)
    return ext_rows


def _read_shaksor_rows(context: AssetExecutionContext) -> list[dict[str, str]]:
    """Return parsed SHAKSORMap.csv rows, reusing the persisted ZIP when present."""
    zip_path = os.path.join(PERSIST_DIR, "Sor_complete.zip")
    if os.path.exists(zip_path):
        context.log.info(f"Reusing persisted SOR complete ZIP at {zip_path}")
        with open(zip_path, "rb") as f:
            zip_content = f.read()
    else:
        context.log.info(f"Downloading complete SOR dataset from {COMPLETE_ZIP_URL}")
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; Dagster-SOR-Loader/1.0)",
            "Accept": "*/*",
        }
        req = Request(COMPLETE_ZIP_URL, headers=headers)
        with urlopen(req, timeout=300) as response:
            zip_content = response.read()
        os.makedirs(PERSIST_DIR, exist_ok=True)
        with open(zip_path, "wb") as f:
            f.write(zip_content)
        context.log.info(f"Saved complete ZIP to {zip_path} ({len(zip_content)} bytes)")

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        with zf.open(SHAKSOR_CSV_NAME) as csv_file:
            csv_content = csv_file.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
    rows = list(reader)
    context.log.info(f"Parsed {len(rows)} rows from {SHAKSOR_CSV_NAME}")
    return rows


def _write_to_ext_table(
    conn, ext_rows: list[dict[str, str]], context: AssetExecutionContext
) -> int:
    """Drop/create ext.shaksor_map and insert all rows."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS [ext].[shaksor_map]")

        col_defs = []
        for col in EXT_COLUMNS:
            if col == "code":
                col_defs.append(f"[{col}] NVARCHAR(100) NOT NULL")
            elif col == "SorCode":
                col_defs.append(f"[{col}] NVARCHAR(100) NOT NULL")
            else:
                col_defs.append(f"[{col}] NVARCHAR(500) NULL")
        col_defs.append("[dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
        col_defs.append("[dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()")

        cursor.execute(f"CREATE TABLE [ext].[shaksor_map] (\n    {','.join(col_defs)}\n)")

        placeholders = ", ".join(["?"] * len(EXT_COLUMNS))
        col_names = ", ".join(f"[{c}]" for c in EXT_COLUMNS)
        insert_sql = f"INSERT INTO [ext].[shaksor_map] ({col_names}) VALUES ({placeholders})"

        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        chunk_size = 1000
        total_inserted = 0
        for i in range(0, len(ext_rows), chunk_size):
            chunk = ext_rows[i : i + chunk_size]
            to_insert = [tuple(row.get(col, "") or None for col in EXT_COLUMNS) for row in chunk]
            cursor.executemany(insert_sql, to_insert)
            total_inserted += len(chunk)

        conn.commit()
        context.log.info(f"Inserted {total_inserted} rows into ext.shaksor_map")
        return total_inserted
    finally:
        cursor.close()


@dg.asset(
    name="extract_shak_register",
    group_name="codesystem_extract",
    compute_kind="python",
    tags={"domain": "codesystem", "layer": "extract", "source": "shak"},
    description=(
        "Download SHAKSORMap.csv from the Sundhedsdatastyrelsen SOR complete ZIP "
        "and load the newest direct SHAK->SOR mapping per SHAK code into "
        "ext.shaksor_map (wide format) for the cs_shak CodeSystem and "
        "cm_shak_to_sor ConceptMap builders."
    ),
)
def extract_shak_register(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ext",
        "shaksor_map",
        dagster_run_id=context.run_id,
        mode="full_load",
        **http_source(COMPLETE_ZIP_URL),
    )
    try:
        raw_rows = _read_shaksor_rows(context)
        selected = select_latest_direct(raw_rows)
        ext_rows = transform_to_ext_format(selected)
        context.log.info(
            f"Selected {len(selected)} newest-direct SHAK rows, "
            f"{len(ext_rows)} usable concepts after target validation"
        )

        with mssql_target_staging.get_connection() as conn:
            row_count = _write_to_ext_table(conn, ext_rows, context)
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "raw_rows": dg.MetadataValue.int(len(raw_rows)),
            "distinct_shak": dg.MetadataValue.int(len(selected)),
            "source": dg.MetadataValue.text("sor-filer.sundhedsdata.dk"),
        }
    )
