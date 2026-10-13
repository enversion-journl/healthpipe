# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.RekvirentUndtagelse — ILS exclusion list for reelt forbrug.

Full rebuild each run from arc.ils_rekvundtagne (manual list, ~130 rows).
Preserves orphans (no match in arc.ils_rekv / map.Rekvirent) with
dw_ek_Rekvirent NULL. CleanCode is SGH_{sgh}_RNR_{rnr} with a raw rnr.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.ref.rekvirent_undtagelse_sql import REBUILD_SQL, REF_TABLE


@dg.asset(
    name="ref_RekvirentUndtagelse",
    group_name="erp_ref",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["arc_ils_rekvundtagne"]),
        dg.AssetKey(["arc_ils_rekv"]),
        dg.AssetKey(["map_Rekvirent"]),
        dg.AssetKey(["staging_infra_ref"]),
    ],
    tags={"domain": "erp", "layer": "ref", "entity": "RekvirentUndtagelse"},
    description=(
        "ref.RekvirentUndtagelse — full rebuild of ILS rekvUndtagne exclusion "
        "list with raw CleanCode, navn COALESCE(rekv, undtagne), and "
        "nullable dw_ek_Rekvirent for orphans."
    ),
)
def ref_RekvirentUndtagelse(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Full-rebuild ref.RekvirentUndtagelse from archive + map."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(f"Rebuilding {REF_TABLE} from arc.ils_rekvundtagne...")
            cursor.execute(REBUILD_SQL)
            inserted = cursor.rowcount

            cursor.execute(f"SELECT COUNT(*) FROM {REF_TABLE}")
            total = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM {REF_TABLE} WHERE [dw_ek_Rekvirent] IS NULL")
            orphan_count = cursor.fetchone()[0]

            conn.commit()
            context.log.info(
                f"{REF_TABLE}: {inserted} rows inserted, {total} total "
                f"({orphan_count} orphans without dw_ek_Rekvirent)"
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "table": "ref.RekvirentUndtagelse",
            "rows_inserted": inserted,
            "total_rows": total,
            "orphan_rows": orphan_count,
        }
    )
