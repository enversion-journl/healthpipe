# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ean NemHandel extraction asset.

Fetches GLN participant data from the NemHandelsregisteret API and writes
to ext.ean_nemhandel for consumption by the cs_ean CodeSystem builder.

Data source: https://api.nemhandel.dk/nemhandel-api/search/receiverList
"""

import dagster as dg
import requests
from core.resources import MSSQLResource
from dagster import AssetExecutionContext

NEMHANDEL_RECEIVER_LIST_URL = "https://api.nemhandel.dk/nemhandel-api/search/receiverList"

EXT_COLUMNS = ["participantID", "participantName", "participantOwnerId"]

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Dagster-NemHandel-Loader/1.0)",
    "Accept": "application/json",
}


def fetch_nemhandel_participants(
    context: AssetExecutionContext | None = None,
    timeout_seconds: int = 60,
) -> list[dict[str, str]]:
    """Fetch GLN participant rows from NemHandelsregisteret API.

    Returns dicts with participantID, participantName, participantOwnerId (GLN list only).
    """
    log = context.log if context else None
    try:
        response = requests.get(
            NEMHANDEL_RECEIVER_LIST_URL,
            headers=DEFAULT_HEADERS,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        msg = f"NemHandel API fetch failed: {e}"
        if log:
            log.warning(msg)
        else:
            print(f"Warning: {msg}")
        return []

    participants: list[dict[str, str]] = []
    gln_block = data.get("GLN") if isinstance(data, dict) else None
    if not isinstance(gln_block, list):
        if log:
            log.warning("NemHandel API response missing GLN list")
        return []

    for p in gln_block:
        if not isinstance(p, dict):
            continue
        pid = p.get("participantID")
        if pid is None or str(pid).strip() == "":
            continue
        participants.append(
            {
                "participantID": str(pid).strip(),
                "participantName": str(p.get("participantName", "") or "").strip(),
                "participantOwnerId": str(p.get("participantOwnerId", "") or "").strip(),
            }
        )

    # Latest wins if the API ever returns duplicate GLNs
    by_id: dict[str, dict[str, str]] = {}
    for row in participants:
        by_id[row["participantID"]] = row

    result = list(by_id.values())
    if log:
        log.info(f"NemHandel GLN participants: {len(result)} unique participantIDs")
    return result


def rows_for_ext(participants: list[dict[str, str]]) -> list[dict[str, str]]:
    """Normalize participant dicts for ext.ean_nemhandel insert (string values)."""
    return [
        {
            "participantID": p["participantID"],
            "participantName": p.get("participantName", ""),
            "participantOwnerId": p.get("participantOwnerId", ""),
        }
        for p in participants
    ]


def _write_to_ext_table(
    conn,
    ext_rows: list[dict[str, str]],
    context: AssetExecutionContext,
) -> int:
    """Recreate ext.ean_nemhandel and bulk-insert participant rows."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS [ext].[ean_nemhandel]")

        ddl = """
        CREATE TABLE [ext].[ean_nemhandel] (
            [participantID]       NVARCHAR(100) NOT NULL,
            [participantName]     NVARCHAR(512)   NULL,
            [participantOwnerId]  NVARCHAR(100)   NULL,
            [dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE(),
            [dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()
        );
        """
        cursor.execute(ddl)

        placeholders = ", ".join(["?"] * len(EXT_COLUMNS))
        col_names = ", ".join(f"[{c}]" for c in EXT_COLUMNS)
        insert_sql = f"INSERT INTO [ext].[ean_nemhandel] ({col_names}) VALUES ({placeholders})"

        chunk_size = 2000
        total_inserted = 0
        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        for i in range(0, len(ext_rows), chunk_size):
            chunk = ext_rows[i : i + chunk_size]
            rows_to_insert = [
                (
                    row.get("participantID", "") or "",
                    row.get("participantName", "") or None,
                    row.get("participantOwnerId", "") or None,
                )
                for row in chunk
            ]
            cursor.executemany(insert_sql, rows_to_insert)
            total_inserted += len(chunk)

        conn.commit()
        context.log.info(f"Inserted {total_inserted} rows into ext.ean_nemhandel")
        return total_inserted
    finally:
        cursor.close()


@dg.asset(
    name="extract_ean_nemhandel",
    group_name="codesystem_extract",
    compute_kind="python",
    tags={"domain": "codesystem", "layer": "extract", "source": "nemhandel"},
    description=(
        "Fetch GLN receiver list from api.nemhandel.dk and load ext.ean_nemhandel "
        "for cs_ean (participantID, participantName, participantOwnerId)."
    ),
)
def extract_ean_nemhandel(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Populate ext.ean_nemhandel from NemHandelsregisteret."""
    participants = fetch_nemhandel_participants(context=context)
    ext_rows = rows_for_ext(participants)

    with mssql_target_staging.get_connection() as conn:
        row_count = _write_to_ext_table(conn, ext_rows, context)

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "source_url": dg.MetadataValue.text(NEMHANDEL_RECEIVER_LIST_URL),
            "unique_participants": dg.MetadataValue.int(len(participants)),
        }
    )
