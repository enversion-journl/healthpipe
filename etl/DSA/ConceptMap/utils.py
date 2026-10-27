# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared utilities for ConceptMap assets.

Provides the merge_conceptmap_table function that handles the temp-table + MERGE
pattern for conceptmap tables, the conceptmap_rows_from_csv converter for the
curated CSV-based ConceptMaps, plus hierarchy helpers shared with the security
layer (SOR expansion).
"""

import re
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any


def conceptmap_rows_from_csv(rows: list[dict[str, Any]]) -> list[tuple]:
    """Convert DSI ``ext_csv_*`` CSV rows (dicts) to conceptmap MERGE tuples.

    Returns ``(source_code, source_display, relationship, target_code,
    target_display)`` tuples. Rows missing source_code, target_code or
    relationship are skipped; duplicate (source_code, target_code) pairs keep
    the first occurrence (the conceptmap tables have that pair as PK).
    """
    results: list[tuple] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        source_code = str(row.get("source_code") or "").strip()
        target_code = str(row.get("target_code") or "").strip()
        relationship = str(row.get("relationship") or "").strip()
        if not source_code or not target_code or not relationship:
            continue
        if (source_code, target_code) in seen:
            continue
        seen.add((source_code, target_code))
        results.append(
            (
                source_code,
                str(row.get("source_display") or "").strip() or None,
                relationship,
                target_code,
                str(row.get("target_display") or "").strip() or None,
            )
        )
    return results


def find_all_descendants(root_code: str, hierarchy: dict[str, list[str]]) -> set[str]:
    descendants: set[str] = {root_code}

    def _traverse(code: str) -> None:
        for child in hierarchy.get(code, []):
            if child not in descendants:
                descendants.add(child)
                _traverse(child)

    _traverse(root_code)
    return descendants


def rows_to_hierarchy_dict(rows: list[tuple]) -> dict[str, list[str]]:
    hierarchy: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        hierarchy[str(r[0])].append(str(r[1]))
    return dict(hierarchy)


def merge_conceptmap_table(
    cursor,
    table_name: str,
    rows: list[tuple],
    *,
    delete_stale: bool = False,
) -> dict:
    """
    MERGE mapping rows into conceptmap.{table_name}.

    Each row is a tuple: (source_code, source_display, relationship, target_code, target_display)

    Args:
        delete_stale: When True, target rows whose (source_code, target_code) pair
            is absent from ``rows`` are deleted (full-snapshot semantics). Use
            when the builder emits the complete mapping set each run — e.g.
            SHAKToSOR, where a SHAK code that gets a new SOR target would
            otherwise leave its old (source_code, old target_code) row behind.

    Returns:
        Dict with insert/update (and delete, when delete_stale) counts and total.
    """
    cursor.execute("DROP TABLE IF EXISTS #tmp_mapping;")
    cursor.execute(
        """
        CREATE TABLE #tmp_mapping (
            [source_code]    NVARCHAR(120) COLLATE DATABASE_DEFAULT NOT NULL,
            [source_display] NVARCHAR(500) COLLATE DATABASE_DEFAULT NULL,
            [relationship]  NVARCHAR(50)  COLLATE DATABASE_DEFAULT NOT NULL,
            [target_code]    NVARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL,
            [target_display] NVARCHAR(500) COLLATE DATABASE_DEFAULT NULL,
            PRIMARY KEY ([source_code], [target_code])
        );
    """
    )

    if rows:
        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True
        for i in range(0, len(rows), 1000):
            cursor.executemany(
                "INSERT INTO #tmp_mapping VALUES (?, ?, ?, ?, ?)",
                rows[i : i + 1000],
            )

    delete_clause = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if delete_stale else ""
    cursor.execute(
        f"""
        MERGE INTO [conceptmap].[{table_name}] AS target
        USING #tmp_mapping AS source
        ON target.[source_code] = source.[source_code]
           AND target.[target_code] = source.[target_code]
        WHEN MATCHED THEN
            UPDATE SET
                target.[source_display] = source.[source_display],
                target.[relationship] = source.[relationship],
                target.[target_display] = source.[target_display],
                target.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([source_code], [source_display], [relationship],
                    [target_code], [target_display],
                    [dw_timestamp_insert], [dw_timestamp_update])
            VALUES (source.[source_code], source.[source_display], source.[relationship],
                    source.[target_code], source.[target_display],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause}
        OUTPUT $action;
    """
    )
    merge_results = cursor.fetchall()
    inserted = sum(1 for r in merge_results if r[0] == "INSERT")
    updated = sum(1 for r in merge_results if r[0] == "UPDATE")

    cursor.execute("DROP TABLE IF EXISTS #tmp_mapping;")

    result = {
        "inserted": inserted,
        "updated": updated,
        "total_mappings": len(rows),
        "table": f"conceptmap.{table_name}",
    }
    if delete_stale:
        result["deleted"] = sum(1 for r in merge_results if r[0] == "DELETE")
    return result


# ---------------------------------------------------------------------------
# DW_EK resolution (join to map.* after MERGE; NULL until map rows exist)
# ---------------------------------------------------------------------------

_CONCEPTMAP_DW_EK_SQL_PATH = Path(__file__).resolve().parent / "conceptmap_dw_ek_resolution.sql"
_CONCEPTMAP_TABLE_HEADER = re.compile(r"^\s*--\s*conceptmap_table:\s*([a-zA-Z0-9_]+)\s*$")


@lru_cache(maxsize=1)
def _load_conceptmap_dw_ek_update_sql() -> dict[str, str]:
    """Parse ``conceptmap_dw_ek_resolution.sql`` into per-table UPDATE batches."""
    if not _CONCEPTMAP_DW_EK_SQL_PATH.is_file():
        raise FileNotFoundError(
            f"Missing ConceptMap SQL file: {_CONCEPTMAP_DW_EK_SQL_PATH}",
        )
    text = _CONCEPTMAP_DW_EK_SQL_PATH.read_text(encoding="utf-8")
    blocks: dict[str, list[str]] = {}
    current: str | None = None
    for line in text.splitlines():
        m = _CONCEPTMAP_TABLE_HEADER.match(line)
        if m:
            current = m.group(1)
            blocks[current] = []
            continue
        if current is not None:
            blocks[current].append(line)
    return {k: "\n".join(v).strip() for k, v in blocks.items() if v}


def sql_update_conceptmap_dw_ek(table_name: str) -> str:
    """Return the UPDATE batch that resolves DW_EK columns for ``conceptmap.{table_name}``."""
    mapping = _load_conceptmap_dw_ek_update_sql()
    if table_name not in mapping:
        allowed = ", ".join(sorted(mapping))
        raise ValueError(f"Unknown conceptmap table_name={table_name!r}. Allowed: {allowed}")
    return mapping[table_name].strip()


def update_conceptmap_dw_ek(cursor, table_name: str) -> None:
    """Populate DW_EK columns on ``conceptmap.{table_name}`` from map tables (no commit)."""
    cursor.execute(sql_update_conceptmap_dw_ek(table_name))
