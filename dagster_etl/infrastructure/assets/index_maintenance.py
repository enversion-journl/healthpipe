# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Index Maintenance for ERP Tables

Auto-discovers all map/dim/fct tables from the MapUpd_*, DimUpd_*, FctUpd_*
files in the DSA SQL folders so adding a new table automatically includes it
in index maintenance -- no manual list to update.

Checks fragmentation via sys.dm_db_index_physical_stats before deciding to
REBUILD (>30%), REORGANIZE (10-30%), or skip (<10%).

Runs after all facts are complete (facts transitively depend on dims and maps).
"""

import pathlib

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, MaterializeResult, asset

_SQL_DIR = pathlib.Path(__file__).parents[2] / "DSA" / "SQL"
_ACRONYMS = {"ean": "EAN"}

FRAG_THRESHOLD_REBUILD = 30.0
FRAG_THRESHOLD_REORGANIZE = 10.0


def _to_pascal(suffix: str) -> str:
    """Convert underscore-delimited suffix to PascalCase with acronym support.

    Examples:
        'faktura' -> 'Faktura'
        'ean'                -> 'EAN'
        'postering'          -> 'Postering'
    """
    return "".join(_ACRONYMS.get(p, p.title()) for p in suffix.split("_"))


def discover_erp_tables() -> list[str]:
    """Discover ERP tables from dim/, map/, fct/ file naming convention.

    Scans for MapUpd_*.py, DimUpd_*.py, and FctUpd_*.py files and converts
    each filename into a fully-qualified SQL table identifier.

    Returns sorted list like ["[dim].[Kreditor]", "[fct].[Postering]", "[map].[Vare]"].
    """
    dir_prefixes = {
        _SQL_DIR / "map": {"MapUpd_": "map"},
        _SQL_DIR / "dim": {"DimUpd_": "dim"},
        _SQL_DIR / "fct": {"FctUpd_": "fct"},
    }
    tables = []
    for directory, prefixes in dir_prefixes.items():
        for prefix, schema in prefixes.items():
            for f in directory.glob(f"{prefix}*.py"):
                name = _to_pascal(f.stem.removeprefix(prefix))
                tables.append(f"[{schema}].[{name}]")
    return sorted(tables)


ERP_TABLES_TO_MAINTAIN = discover_erp_tables()


def _get_table_fragmentation(cursor, table: str) -> float | None:
    """Return the max avg_fragmentation_in_percent across all indexes on a table.

    Returns None if the table does not exist or has no indexes.
    """
    cursor.execute(
        f"""
        SELECT MAX(ps.avg_fragmentation_in_percent)
        FROM sys.dm_db_index_physical_stats(
            DB_ID(), OBJECT_ID(N'{table}'), NULL, NULL, 'LIMITED'
        ) ps
        WHERE ps.index_id > 0
        """
    )
    row = cursor.fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return None


@asset(
    group_name="erp_maintenance",
    compute_kind="mssql",
    deps=[
        "fct_fakturalinje",
        "fct_postering",
        "fct_faktura",
        "fct_vareudlevering",
    ],
    tags={
        "domain": "erp",
        "layer": "maintenance",
        "module": "datawarehouse",
    },
    description=(
        "Fragmentation-aware index maintenance on all ERP map/dim/fct tables. "
        "REBUILD >30%, REORGANIZE 10-30%, skip <10%."
    ),
)
def erp_index_maintenance(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Check fragmentation and rebuild/reorganize indexes on ERP tables."""

    tables_rebuilt = 0
    tables_reorganized = 0
    tables_skipped = 0
    tables_missing = 0
    errors: list[str] = []
    actions: dict[str, str] = {}

    context.log.info(
        f"Starting index maintenance for {len(ERP_TABLES_TO_MAINTAIN)} tables "
        f"(rebuild >{FRAG_THRESHOLD_REBUILD}%, reorganize >{FRAG_THRESHOLD_REORGANIZE}%)"
    )

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for table in ERP_TABLES_TO_MAINTAIN:
                try:
                    frag = _get_table_fragmentation(cursor, table)

                    if frag is None:
                        tables_missing += 1
                        actions[table] = "missing/no-indexes"
                        context.log.info(f"{table}: not found or no indexes, skipping")
                        continue

                    if frag > FRAG_THRESHOLD_REBUILD:
                        cursor.execute(f"ALTER INDEX ALL ON {table} REBUILD;")
                        conn.commit()
                        tables_rebuilt += 1
                        actions[table] = f"REBUILD (frag={frag:.1f}%)"
                        context.log.info(f"{table}: REBUILD (fragmentation {frag:.1f}%)")

                    elif frag > FRAG_THRESHOLD_REORGANIZE:
                        cursor.execute(f"ALTER INDEX ALL ON {table} REORGANIZE;")
                        conn.commit()
                        tables_reorganized += 1
                        actions[table] = f"REORGANIZE (frag={frag:.1f}%)"
                        context.log.info(f"{table}: REORGANIZE (fragmentation {frag:.1f}%)")

                    else:
                        tables_skipped += 1
                        actions[table] = f"skip (frag={frag:.1f}%)"
                        context.log.info(f"{table}: fragmentation {frag:.1f}%, skipping")

                except Exception as e:
                    error_msg = f"Failed on {table}: {e}"
                    errors.append(error_msg)
                    context.log.warning(error_msg)
                    actions[table] = f"ERROR: {e}"
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            context.log.info(
                f"Index maintenance complete: {tables_rebuilt} rebuilt, "
                f"{tables_reorganized} reorganized, {tables_skipped} skipped, "
                f"{tables_missing} missing, {len(errors)} errors"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "tables_rebuilt": tables_rebuilt,
            "tables_reorganized": tables_reorganized,
            "tables_skipped": tables_skipped,
            "tables_missing": tables_missing,
            "errors": "\n".join(errors) if errors else "none",
            "tables_discovered": len(ERP_TABLES_TO_MAINTAIN),
            "tables": ", ".join(ERP_TABLES_TO_MAINTAIN),
            "actions": "\n".join(f"  {t}: {a}" for t, a in sorted(actions.items())),
        }
    )
