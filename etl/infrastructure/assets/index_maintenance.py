# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Index Maintenance for ERP Tables and the flat fhir tables that carry dw-keys

Auto-discovers all map/dim/fct tables from the map_*, dim_*, fct_*
files in the DSA SQL folders so adding a new table automatically includes it
in index maintenance -- no manual list to update. The fhir tables come from
DW_KEYS_CONFIG: their first dw-key pass splits pages on staging and, through the
DSO sync, on DSO.

Checks each index via sys.dm_db_index_physical_stats (SAMPLED) and rebuilds, offline,
the indexes with at least 1,000 pages that have more than 30% fragmentation, less than
75% page density, or a fill factor and more than 5% fragmentation; smaller indexes report
a high fragmentation that a rebuild does not lower. A rebuild sets fill factor 90, which
SQL Server keeps for the index, so an index that fragments keeps free space for new keys;
an index led by a date/time column only grows at the end and gets fill factor 100.
Afterwards every statistic with modifications gets UPDATE STATISTICS; a rebuild refreshes
only the statistics of the index it rebuilds.

The ERP asset depends on the facts but no job selects it, so it runs only when
materialised by hand; the fhir asset runs after the DSO syncs of its tables.
"""

import pathlib
from collections import Counter
from typing import NamedTuple

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, MaterializeResult, asset
from DSA.FHIR.SqlOnFhir.DWKeys_dfm import DW_KEYS_CONFIG
from DSA.FHIR.utils.sql_on_fhir.dw_keys_factory import GROUP_NAME as FHIR_DW_GROUP

_SQL_DIR = pathlib.Path(__file__).parents[2] / "DSA" / "SQL"

FRAG_THRESHOLD_REBUILD = 30.0
FRAG_THRESHOLD_WITH_FILL_FACTOR = 5.0
MIN_PAGE_DENSITY = 75.0
MIN_PAGE_COUNT = 1000
FILL_FACTOR = 90
ASCENDING_KEY_TYPES = frozenset(
    {"date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"}
)


def discover_erp_tables() -> list[str]:
    """Discover ERP tables from dim/, map/, fct/ file naming convention.

    Scans for map_*.py, dim_*.py, and fct_*.py files. Post-overhaul the stem
    after the layer prefix is already the final PascalCase table name (acronyms
    preserved), so it is used verbatim.

    Returns sorted list like ["[dim].[Kreditor]", "[fct].[Postering]", "[map].[Leverandoervare]"].
    """
    dir_prefixes = {
        _SQL_DIR / "map": {"map_": "map"},
        _SQL_DIR / "dim": {"dim_": "dim"},
        _SQL_DIR / "fct": {"fct_": "fct"},
    }
    tables = []
    for directory, prefixes in dir_prefixes.items():
        for prefix, schema in prefixes.items():
            for f in directory.glob(f"{prefix}*.py"):
                name = f.stem.removeprefix(prefix)
                tables.append(f"[{schema}].[{name}]")
    return sorted(tables)


ERP_TABLES_TO_MAINTAIN = discover_erp_tables()


class IndexStats(NamedTuple):
    name: str
    pages: int
    fragmentation: float
    density: float
    fill_factor: int
    leading_key_type: str


def _index_stats(cursor, table: str) -> list[IndexStats]:
    """Leaf-level size, fragmentation and page density per enabled index; empty when missing."""
    cursor.execute(
        f"""
        SELECT i.name, SUM(ps.page_count), MAX(ps.avg_fragmentation_in_percent),
               SUM(ps.avg_page_space_used_in_percent * ps.page_count)
                   / NULLIF(SUM(ps.page_count), 0),
               MAX(i.fill_factor), MAX(t.name)
        FROM sys.dm_db_index_physical_stats(
            DB_ID(), OBJECT_ID(N'{table}'), NULL, NULL, 'SAMPLED'
        ) ps
        JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id
        JOIN sys.index_columns ic
          ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal = 1
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.types t ON t.user_type_id = c.user_type_id
        WHERE ps.index_id > 0
          AND ps.alloc_unit_type_desc = 'IN_ROW_DATA'
          AND i.is_disabled = 0
          AND OBJECT_ID(N'{table}') IS NOT NULL
        GROUP BY i.name
        ORDER BY i.name
        """
    )
    return [
        IndexStats(str(name), int(pages), float(frag), float(density or 100.0), int(fill), str(key))
        for name, pages, frag, density, fill, key in cursor.fetchall()
    ]


def _rebuild_reason(index: IndexStats) -> str | None:
    if index.pages < MIN_PAGE_COUNT:
        return None
    if index.fragmentation > FRAG_THRESHOLD_REBUILD:
        return f"frag={index.fragmentation:.1f}%"
    if index.density < MIN_PAGE_DENSITY:
        return f"density={index.density:.1f}%"
    if 0 < index.fill_factor < 100 and index.fragmentation > FRAG_THRESHOLD_WITH_FILL_FACTOR:
        return f"frag={index.fragmentation:.1f}% at fill factor {index.fill_factor}"
    return None


def _modified_statistics(cursor, table: str) -> list[str]:
    cursor.execute(
        f"""
        SELECT s.name
        FROM sys.stats s
        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
        WHERE s.object_id = OBJECT_ID(N'{table}')
          AND sp.modification_counter > 0
          AND NOT EXISTS (
              SELECT 1 FROM sys.indexes i
              WHERE i.object_id = s.object_id AND i.index_id = s.stats_id AND i.is_disabled = 1
          )
        ORDER BY s.name
        """
    )
    return [str(name) for (name,) in cursor.fetchall()]


def _maintain_table(cursor, conn, table: str) -> str:
    """REBUILD each large index that needs it, then update modified statistics."""
    indexes = _index_stats(cursor, table)
    if not indexes:
        return "missing/no-indexes"
    rebuilt = []
    for index in indexes:
        reason = _rebuild_reason(index)
        if reason is None:
            continue
        fill_factor = 100 if index.leading_key_type in ASCENDING_KEY_TYPES else FILL_FACTOR
        cursor.execute(
            f"ALTER INDEX [{index.name}] ON {table} REBUILD WITH (FILLFACTOR = {fill_factor});"
        )
        conn.commit()
        rebuilt.append(f"{index.name} (pages={index.pages}, {reason})")
    statistics = _modified_statistics(cursor, table)
    for name in statistics:
        cursor.execute(f"UPDATE STATISTICS {table} ([{name}]);")
        conn.commit()
    action = "REBUILD " + ", ".join(rebuilt) if rebuilt else "skip"
    return f"{action}; statistics updated: {len(statistics)}"


def _maintain_tables(
    context: AssetExecutionContext,
    database: MSSQLResource,
    tables: list[str],
) -> tuple[dict[str, str], list[str]]:
    """Maintain each table on one database; a failing table is logged and skipped."""
    actions: dict[str, str] = {}
    errors: list[str] = []
    with database.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for table in tables:
                try:
                    actions[table] = _maintain_table(cursor, conn, table)
                    context.log.info(f"{table}: {actions[table]}")
                except Exception as e:
                    error_msg = f"Failed on {table}: {e}"
                    errors.append(error_msg)
                    context.log.warning(error_msg)
                    actions[table] = f"ERROR: {e}"
                    try:
                        conn.rollback()
                    except Exception:
                        pass
        finally:
            cursor.close()
    return actions, errors


def _action_counts(actions: dict[str, str]) -> Counter[str]:
    return Counter(action.split()[0].rstrip(":") for action in actions.values())


@asset(
    group_name="erp_maintenance",
    compute_kind="mssql",
    deps=[
        "fct_Fakturalinje",
        "fct_Postering",
        "fct_Faktura",
        "fct_Vareudlevering",
    ],
    tags={
        "domain": "erp",
        "layer": "maintenance",
        "module": "datawarehouse",
    },
    description=(
        "Index and statistics maintenance on all map/dim/fct tables. REBUILD each index with "
        "at least 1,000 pages that is fragmented or sparse, with fill factor 90; then update "
        "modified statistics."
    ),
)
def erp_index_maintenance(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Rebuild indexes and update statistics on ERP tables."""
    context.log.info(
        f"Starting index maintenance for {len(ERP_TABLES_TO_MAINTAIN)} tables "
        f"(rebuild >{FRAG_THRESHOLD_REBUILD}% or <{MIN_PAGE_DENSITY}% density, "
        f"at least {MIN_PAGE_COUNT} pages)"
    )
    actions, errors = _maintain_tables(context, mssql_target_staging, ERP_TABLES_TO_MAINTAIN)
    counts = _action_counts(actions)
    tables_rebuilt = counts["REBUILD"]
    tables_skipped = counts["skip"]
    tables_missing = counts["missing/no-indexes"]
    context.log.info(
        f"Index maintenance complete: {tables_rebuilt} rebuilt, {tables_skipped} skipped, "
        f"{tables_missing} missing, {len(errors)} errors"
    )

    return MaterializeResult(
        metadata={
            "tables_rebuilt": tables_rebuilt,
            "tables_skipped": tables_skipped,
            "tables_missing": tables_missing,
            "errors": "\n".join(errors) if errors else "none",
            "tables_discovered": len(ERP_TABLES_TO_MAINTAIN),
            "tables": ", ".join(ERP_TABLES_TO_MAINTAIN),
            "actions": "\n".join(f"  {t}: {a}" for t, a in sorted(actions.items())),
        }
    )


FHIR_DW_KEYS_TABLES_TO_MAINTAIN = sorted(f"[fhir].[{cfg.table}]" for cfg in DW_KEYS_CONFIG.values())


@asset(
    name="fhir_dw_keys_index_maintenance",
    group_name=FHIR_DW_GROUP,
    compute_kind="mssql",
    deps=[f"dso_sync_fhir_{name}_dfm" for name in DW_KEYS_CONFIG],
    tags={"domain": "fhir", "layer": "maintenance"},
    description=(
        "Index and statistics maintenance on the flat fhir tables that carry dw-keys, on "
        "staging and DSO. REBUILD each index with at least 1,000 pages that is fragmented or "
        "sparse, with fill factor 90; then update modified statistics."
    ),
)
def fhir_dw_keys_index_maintenance(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> MaterializeResult:
    """Rebuild indexes and update statistics on the dw-key tables."""
    metadata: dict[str, str | int] = {"tables": ", ".join(FHIR_DW_KEYS_TABLES_TO_MAINTAIN)}
    all_errors: list[str] = []
    for label, database in (("staging", mssql_target_staging), ("dso", mssql_target_dso)):
        actions, errors = _maintain_tables(context, database, FHIR_DW_KEYS_TABLES_TO_MAINTAIN)
        counts = _action_counts(actions)
        metadata[f"{label}_tables_rebuilt"] = counts["REBUILD"]
        metadata[f"{label}_actions"] = "\n".join(f"  {t}: {a}" for t, a in sorted(actions.items()))
        all_errors += [f"{label}: {e}" for e in errors]
    metadata["errors"] = "\n".join(all_errors) if all_errors else "none"
    return MaterializeResult(metadata=metadata)
