# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The return leg: read OS2mo's organisation back into the warehouse.

We deliver codesystems to Magenta as CSV files (see ``DSO/export/os2mo``), and
their import builds OS2mo's organisation from them. What comes back is not the
same thing we sent: OS2mo is where the organisation is enriched — with the
classes, associations and attributes Magenta and their users maintain — and
that enrichment is worth having in the warehouse.

Two assets, the ordinary DSI shape:

``ext_os2mo_org_unit``
    A 1:1 landing of OS2mo's org units, read through the same ``OS2moResource``
    the delivery uploads through.

``arc_os2mo_org_unit``
    The persistent archive. Every read is a genuine full snapshot, so the merge
    detects deletes and soft-deletes what OS2mo no longer returns — the same
    choice as ``arc.epji_location``.

``user_key`` carries our SOR code, which is what makes an archived row joinable
back to ``codesystem.SOR*``. It is landed CLEANED — the phase-1 units carry it
Excel-wrapped (``="<code>"``) in OS2mo and would otherwise join to nothing —
while ``user_key_raw`` keeps exactly what OS2mo holds.

Units that are no longer valid are landed too, with their ``validity_to``: the
read covers every unit OS2mo has ever held, so a closed unit stays in the
archive as closed rather than being soft-deleted.
"""

from typing import Any

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource, OS2moResource
from utils.os2mo.org_units import (
    ORG_UNITS_PAGE_QUERY,
    ORG_UNITS_PAGE_SIZE,
    extract_org_units_page,
)
from utils.sor_codes import clean_sor_id

__all__ = [
    "EXT_COLUMNS",
    "OS2MO_ORG_UNIT_ARCHIVE_CONFIG",
    "arc_os2mo_org_unit",
    "ext_os2mo_org_unit",
    "flatten_org_unit",
    "os2mo_extract_assets",
]

# Version key columns first, then the attributes. Kept flat and all-NVARCHAR
# for the same reason ext.sor_register is: ext is a faithful landing of what the
# source said, and typing decisions belong downstream.
EXT_COLUMNS = [
    "uuid",
    "validity_from",
    "validity_to",
    "user_key",
    "user_key_raw",
    "name",
    "parent_uuid",
    "parent_user_key",
    "org_unit_type_uuid",
    "org_unit_type_name",
]

_KEY_COLUMNS = ("uuid", "validity_from")

INSERT_CHUNK_SIZE = 1000


def flatten_org_unit(unit: dict[str, Any]) -> dict[str, str]:
    """Flatten one nested GraphQL org unit into the ext column layout.

    Parent and unit type arrive as ``*_response`` wrappers — ``{uuid, current}``
    — because the flat ``parent`` / ``unit_type`` fields are deprecated in the
    v30 schema. The uuid sits on the wrapper; the human-readable bits sit on
    ``current``, which is null for a unit with no parent (the root).
    """
    parent = unit.get("parent_response") or {}
    parent_current = parent.get("current") or {}
    unit_type = unit.get("unit_type_response") or {}
    unit_type_current = unit_type.get("current") or {}
    validity = unit.get("validity") or {}

    return {
        "uuid": str(unit.get("uuid") or ""),
        "validity_from": str(validity.get("from") or ""),
        "validity_to": str(validity.get("to") or ""),
        "user_key": clean_sor_id(str(unit.get("user_key") or "")),
        "user_key_raw": str(unit.get("user_key") or ""),
        "name": str(unit.get("name") or ""),
        "parent_uuid": str(parent.get("uuid") or ""),
        "parent_user_key": clean_sor_id(str(parent_current.get("user_key") or "")),
        "org_unit_type_uuid": str(unit_type.get("uuid") or ""),
        "org_unit_type_name": str(unit_type_current.get("name") or ""),
    }


def _write_to_ext_table(
    conn,
    ext_rows: list[dict[str, str]],
    context: dg.AssetExecutionContext,
) -> int:
    """Drop/create ext.os2mo_org_unit and insert the snapshot in chunks."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS [ext].[os2mo_org_unit]")

        col_defs = []
        for col in EXT_COLUMNS:
            if col in _KEY_COLUMNS:
                col_defs.append(f"[{col}] NVARCHAR(50) NOT NULL")
            elif col in ("validity_to", "parent_uuid", "org_unit_type_uuid"):
                col_defs.append(f"[{col}] NVARCHAR(50) NULL")
            elif col in ("user_key", "user_key_raw", "parent_user_key"):
                col_defs.append(f"[{col}] NVARCHAR(100) NULL")
            else:
                col_defs.append(f"[{col}] NVARCHAR(500) NULL")
        col_defs.append("[dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
        col_defs.append("[dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()")

        cursor.execute(f"CREATE TABLE [ext].[os2mo_org_unit] (\n    {','.join(col_defs)}\n)")

        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        placeholders = ", ".join(["?"] * len(EXT_COLUMNS))
        col_names = ", ".join(f"[{c}]" for c in EXT_COLUMNS)
        insert_sql = f"INSERT INTO [ext].[os2mo_org_unit] ({col_names}) VALUES ({placeholders})"

        total_inserted = 0
        for start in range(0, len(ext_rows), INSERT_CHUNK_SIZE):
            chunk = ext_rows[start : start + INSERT_CHUNK_SIZE]
            cursor.executemany(
                insert_sql,
                [
                    tuple(
                        # Key columns keep "" (they are part of the arc PK);
                        # attributes store NULL for empty values.
                        row.get(col, "") or ("" if col in _KEY_COLUMNS else None)
                        for col in EXT_COLUMNS
                    )
                    for row in chunk
                ],
            )
            total_inserted += len(chunk)

        conn.commit()
        context.log.info(f"Inserted {total_inserted} rows into ext.os2mo_org_unit")
        return total_inserted
    finally:
        cursor.close()


@dg.asset(
    name="ext_os2mo_org_unit",
    group_name="os2mo_import",
    compute_kind="graphql",
    tags={"domain": "organisation", "layer": "extract", "source": "os2mo"},
    description=(
        "1:1 landing of OS2mo's org units (ext.os2mo_org_unit), read over the "
        "pinned GraphQL endpoint. Full snapshot on every run."
    ),
)
def ext_os2mo_org_unit(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    os2mo: OS2moResource,
) -> dg.MaterializeResult:
    """Read every org unit OS2mo holds into ext."""
    units = list(
        os2mo.paginate(
            ORG_UNITS_PAGE_QUERY,
            {"limit": ORG_UNITS_PAGE_SIZE},
            extract_org_units_page,
            operation="org_units",
        )
    )
    context.log.info(f"Read {len(units)} org units from {os2mo.graphql_url}")

    if not units:
        raise dg.Failure(
            description=(
                "OS2mo returned no org units. An empty full snapshot would "
                "soft-delete the entire archive, so the extract stops here."
            )
        )

    ext_rows = [flatten_org_unit(unit) for unit in units]
    with mssql_target_staging.get_connection() as conn:
        inserted = _write_to_ext_table(conn, ext_rows, context)

    with_user_key = sum(1 for row in ext_rows if row["user_key"])

    return dg.MaterializeResult(
        metadata={
            "rows_inserted": inserted,
            "units_with_user_key": with_user_key,
            "target_table": "ext.os2mo_org_unit",
            "graphql_url": os2mo.graphql_url,
        }
    )


OS2MO_ORG_UNIT_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="os2mo_org_unit",
    target_schema="arc",
    target_table="os2mo_org_unit",
    merge_keys=_KEY_COLUMNS,
    non_key_columns=tuple(c for c in EXT_COLUMNS if c not in _KEY_COLUMNS),
    strategy="upsert",
    update_only_on_change=True,
    # Every read is a complete snapshot of current OS2mo state, so a unit that
    # stops appearing really is gone — soft-delete it rather than leaving a
    # stale row that looks current.
    detect_deletes=True,
    upstream_is_full_load=True,
    # ext is rebuilt from a keyed GraphQL response, so (uuid, validity_from) is
    # unique by construction and the cold-start fast path is safe.
    enable_bulk_insert_on_first_load=True,
)


@dg.asset(
    name="arc_os2mo_org_unit",
    group_name="os2mo_import",
    compute_kind="mssql",
    deps=["ext_os2mo_org_unit", "staging_infra_os2mo_archive"],
    tags={"domain": "organisation", "layer": "archive", "source": "os2mo"},
    description=(
        "UPSERT ext.os2mo_org_unit → arc.os2mo_org_unit (persistent archive of "
        "OS2mo's organisation). Unit versions absent from the latest snapshot "
        "— removed units and superseded versions alike — are soft-deleted."
    ),
)
def arc_os2mo_org_unit(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Merge the OS2mo snapshot into the persistent archive."""
    result = execute_archive_merge(
        context, mssql_target_staging, OS2MO_ORG_UNIT_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd into arc.os2mo_org_unit — inserted={result.rows_inserted}, "
        f"updated={result.rows_updated}; mode={config.mode}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.os2mo_org_unit",
            target_table="arc.os2mo_org_unit",
        )
    )


os2mo_extract_assets = [ext_os2mo_org_unit, arc_os2mo_org_unit]
