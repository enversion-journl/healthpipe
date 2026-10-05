# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
UNSPSC udbudskategori infrastructure (DSI/DSA Layer)

Self-contained infrastructure for RH's manually maintained UNSPSC udbudskategori
flat file:

- ``staging_infra_udbudskategori`` creates the archive table
  ``arc.unspsc_udbudskategori`` (DDL in tables/udbudskategori).
- ``staging_infra_udbudskategori_seed`` bulk-loads the committed CSV into that
  table, guarded by an empty-table check (loads only when empty; a manual DROP
  forces a reload after the ~yearly CSV update).

Both assets stay in the ``staging_infrastructure`` group so they continue to run
as part of the staging infrastructure setup job.
"""

import csv
from pathlib import Path

import dagster as dg
from core.resources import MSSQLResource
from infrastructure.utils.ddl_loader import execute_sql_files_from_directory

# RH UNSPSC udbudskategori seed (manuelt vedligeholdt CSV → arc-arkiv).
UDBUD_ARC_TABLE = "arc.unspsc_udbudskategori"
UDBUD_CSV_DELIMITER = ";"
UDBUD_CSV_PATH = (
    Path(__file__).resolve().parents[2] / "DSI" / "csv_files" / "data" / "unspsc_udbudskategori.csv"
)
# Udbudskategori_niv5 droppes bevidst (altid tom i kilden).
UDBUD_COLUMNS = [
    "UNSPSC_Nr",
    "UNSPSC_Tekst",
    "Udbudstype",
    "Udbudskategori_niv1",
    "Udbudskategori_niv2",
    "Udbudskategori_niv3",
    "Udbudskategori_niv4",
]


def _read_udbud_csv() -> list[dict]:
    """Read the committed udbudskategori CSV into a list of dicts (UDBUD_COLUMNS)."""
    with UDBUD_CSV_PATH.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=UDBUD_CSV_DELIMITER)
        return [{col: row.get(col) for col in UDBUD_COLUMNS} for row in reader]


@dg.asset(
    name="staging_infra_udbudskategori",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "udbudskategori"},
    description="Create RH UNSPSC udbudskategori archive table (arc.unspsc_udbudskategori).",
)
def staging_infra_udbudskategori(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create arc.unspsc_udbudskategori archive infrastructure."""
    results = execute_sql_files_from_directory(
        mssql_target_staging, "tables/udbudskategori", context
    )
    context.log.info("✅ UNSPSC udbudskategori archive infrastructure created")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "arc.unspsc_udbudskategori",
        }
    )


@dg.asset(
    name="staging_infra_udbudskategori_seed",
    group_name="staging_infrastructure",
    deps=["staging_infra_udbudskategori"],
    compute_kind="mssql",
    tags={"layer": "staging", "type": "infrastructure", "domain": "udbudskategori"},
    description=(
        "Seed arc.unspsc_udbudskategori from the committed RH CSV. Empty-table "
        "guard: loads only when the table is empty; a manual DROP forces reload "
        "after a CSV update (file changes ~once a year)."
    ),
)
def staging_infra_udbudskategori_seed(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Empty-table-guarded bulk load of the udbudskategori CSV into arc."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [arc].[unspsc_udbudskategori]")
            existing = cursor.fetchone()[0]
            if existing > 0:
                context.log.info(
                    f"arc.unspsc_udbudskategori already has {existing} rows — "
                    f"skipping CSV load (DROP the table to force reload)."
                )
                return dg.MaterializeResult(metadata={"skipped": True, "existing_rows": existing})

            rows = _read_udbud_csv()
            placeholders = ", ".join(["?"] * len(UDBUD_COLUMNS))
            col_names = ", ".join(f"[{c}]" for c in UDBUD_COLUMNS)
            insert_sql = (
                f"INSERT INTO [arc].[unspsc_udbudskategori] ({col_names}) "
                f"VALUES ({placeholders})"
            )
            chunk_size = 1000
            total = 0
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                to_insert = [
                    tuple((row.get(col) or "").strip() or None for col in UDBUD_COLUMNS)
                    for row in chunk
                ]
                cursor.executemany(insert_sql, to_insert)
                total += len(chunk)
            conn.commit()
        finally:
            cursor.close()

    context.log.info(f"Seeded arc.unspsc_udbudskategori with {total} rows from CSV.")
    return dg.MaterializeResult(
        metadata={
            "skipped": False,
            "rows_inserted": total,
            "source_file": UDBUD_CSV_PATH.name,
        }
    )
