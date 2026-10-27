# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ConceptMap assets: curated CSV -> SOR.

The four ERP conceptmaps (ean/lager/rekvirent/okosted -> SOR) are built
exclusively from the curated CSV files in ``DSI/csv_files/data/`` (loaded by
the ``ext_csv_*`` extract assets). A mapping exists if and only if it has a
CSV row; new source codes stay unmapped until someone adds a row.

Each run MERGEs the full CSV snapshot with ``delete_stale=True``, so rows
removed from a CSV — including leftovers from the retired fuzzy-matching
builders — are deleted from the conceptmap table.
"""

from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSA.ConceptMap.utils import (
    conceptmap_rows_from_csv,
    merge_conceptmap_table,
    update_conceptmap_dw_ek,
)

# (asset/CSV name, conceptmap table)
_CSV_CONCEPTMAPS: list[tuple[str, str]] = [
    ("ean_to_sor", "EANToSOR"),
    ("lager_to_sor", "LagerToSOR"),
    ("rekvirent_to_sor", "RekvirentToSOR"),
    ("okosted_to_sor", "OkostedToSOR"),
]


def _build_csv_sor_conceptmap_asset(name: str, table: str):
    @dg.asset(
        name=f"cm_{name}",
        ins={"csv_rows": dg.AssetIn(f"ext_csv_{name}")},
        group_name="conceptmap",
        compute_kind="mssql",
        tags={"domain": "conceptmap", "layer": "conceptmap", "module": name},
        description=(
            f"MERGE the curated {name}.csv mappings into conceptmap.{table} "
            "(full snapshot, stale rows deleted)"
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        csv_rows: list[dict[str, Any]],
        mssql_target_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        rows = conceptmap_rows_from_csv(csv_rows)
        context.log.info(f"Built {len(rows)} mappings from {len(csv_rows)} CSV rows")

        # delete_stale=True would wipe the whole table on an empty snapshot; a
        # missing/empty/misread CSV must fail the run instead.
        if not rows:
            raise dg.Failure(
                f"{name}.csv produced no valid mappings — refusing to run the "
                f"full-snapshot MERGE against conceptmap.{table}"
            )

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                result = merge_conceptmap_table(cursor, table, rows, delete_stale=True)
                update_conceptmap_dw_ek(cursor, table)
                conn.commit()
            finally:
                cursor.close()

        context.log.info(
            f"conceptmap.{table}: {result['inserted']} inserted, "
            f"{result['updated']} updated, {result.get('deleted', 0)} deleted"
        )
        return dg.MaterializeResult(metadata=result)

    return _asset


cm_ean_to_sor = _build_csv_sor_conceptmap_asset(*_CSV_CONCEPTMAPS[0])
cm_lager_to_sor = _build_csv_sor_conceptmap_asset(*_CSV_CONCEPTMAPS[1])
cm_rekvirent_to_sor = _build_csv_sor_conceptmap_asset(*_CSV_CONCEPTMAPS[2])
cm_okosted_to_sor = _build_csv_sor_conceptmap_asset(*_CSV_CONCEPTMAPS[3])

csv_sor_conceptmap_assets = [
    cm_ean_to_sor,
    cm_lager_to_sor,
    cm_rekvirent_to_sor,
    cm_okosted_to_sor,
]
