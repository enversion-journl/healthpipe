# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Metadata-driven data quality checks for the DW dimensional model.

Three checks:
1. dq_null_check — no NULLs in any DW_EK_ column across fct/dim/map
2. dq_fact_dim_referential_check — fact FK values exist in target dim
3. dq_dim_map_referential_check — dim FK values exist in target map
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.checks.discovery import (
    discover_dw_ek_columns,
    get_existing_tables,
    get_primary_key_columns,
    sentinel_literal,
)

_CHECKED_SCHEMAS = ["fct", "dim", "map"]

_DW_QUALITY_KEY = dg.AssetKey("dw_quality")


@dg.asset(
    name="dw_quality",
    group_name="data_quality",
    compute_kind="mssql",
    description="Anchor asset for cross-domain DW quality checks. Materialize to run checks.",
)
def dw_quality() -> None:
    pass


@dg.asset_check(
    name="dq_null_check",
    asset=_DW_QUALITY_KEY,
    description=(
        "Verify no DW_EK_ columns contain NULL values across fct/dim/map schemas.\n\n"
        "**Outputs:**\n\n"
        "- **passed**: `{count, tables}` — tables where no DW_EK_ column contains NULLs\n"
        "- **failed**: `{count, tables: {schema.table: {column: null_count}}}` — columns with NULLs"
    ),
    compute_kind="mssql",
)
def dq_null_check(mssql_target_staging: MSSQLResource) -> dg.AssetCheckResult:
    """Scan all DW_EK_ columns in fct/dim/map for NULL values."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            columns = discover_dw_ek_columns(cursor, _CHECKED_SCHEMAS)

            # Group columns by (schema, table)
            table_columns: dict[tuple[str, str], list[str]] = defaultdict(list)
            for col in columns:
                table_columns[(col["schema"], col["table"])].append(col["column"])

            failed_tables = {}
            passed_tables: list[str] = []
            for (schema, table), cols in sorted(table_columns.items()):
                where_clause = " OR ".join(f"[{c}] IS NULL" for c in cols)
                cursor.execute(f"SELECT TOP 1 1 FROM [{schema}].[{table}] WHERE {where_clause}")
                if cursor.fetchone() is not None:
                    # Count nulls per column in a single aggregation query
                    case_exprs = ", ".join(
                        f"SUM(CASE WHEN [{c}] IS NULL THEN 1 ELSE 0 END)" for c in cols
                    )
                    cursor.execute(f"SELECT {case_exprs} FROM [{schema}].[{table}]")
                    counts = cursor.fetchone()
                    null_cols = {c: counts[i] for i, c in enumerate(cols) if counts[i] > 0}
                    if null_cols:
                        failed_tables[f"{schema}.{table}"] = null_cols
                    else:
                        passed_tables.append(f"{schema}.{table}")
                else:
                    passed_tables.append(f"{schema}.{table}")
        finally:
            cursor.close()

    check_passed = len(failed_tables) == 0
    metadata: dict[str, Any] = {
        "passed": dg.MetadataValue.json(
            {
                "count": len(passed_tables),
                "tables": passed_tables,
            }
        ),
        "failed": dg.MetadataValue.json(
            {
                "count": len(failed_tables),
                "tables": failed_tables,
            }
        ),
    }

    return dg.AssetCheckResult(passed=check_passed, metadata=metadata)


def _check_referential_integrity(
    cursor,
    source_schema: str,
    target_schema: str,
) -> tuple[dict[str, dict[str, int]], list[str], list[str], list[str], int]:
    """Check that FK values in source_schema tables exist in target_schema tables.

    Returns (orphans, unresolvable, skipped_pk_columns, passed_columns, total_columns).
    """
    columns = discover_dw_ek_columns(cursor, [source_schema])
    target_tables = get_existing_tables(cursor, target_schema)

    pk_cache: dict[tuple[str, str], set[str]] = {}
    orphans: dict[str, dict[str, int]] = {}
    unresolvable: list[str] = []
    skipped_pk_columns: list[str] = []
    passed_columns: list[str] = []

    for col in columns:
        schema, table, column = col["schema"], col["table"], col["column"]
        parsed_name = col["dim_name"]

        pk_key = (schema, table)
        if pk_key not in pk_cache:
            pk_cache[pk_key] = get_primary_key_columns(cursor, schema, table)
        is_pk = column in pk_cache[pk_key]

        if parsed_name not in target_tables:
            if is_pk:
                skipped_pk_columns.append(f"{schema}.{table}.{column}")
                continue
            unresolvable.append(f"{schema}.{table}.{column} -> {target_schema}.{parsed_name}")
            continue

        sentinel = sentinel_literal(col["data_type"])
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM [{schema}].[{table}] src
            WHERE src.[{column}] != {sentinel}
              AND NOT EXISTS (
                SELECT 1 FROM [{target_schema}].[{parsed_name}] tgt
                WHERE tgt.[DW_EK_{parsed_name}] = src.[{column}]
              )
            """
        )
        orphan_count = cursor.fetchone()[0]
        if orphan_count > 0:
            table_key = f"{schema}.{table}"
            if table_key not in orphans:
                orphans[table_key] = {}
            orphans[table_key][f"{column} -> {target_schema}.{parsed_name}"] = orphan_count
        else:
            passed_columns.append(f"{schema}.{table}.{column} -> {target_schema}.{parsed_name}")

    return orphans, unresolvable, skipped_pk_columns, passed_columns, len(columns)


def _build_referential_result(
    orphans: dict[str, dict[str, int]],
    unresolvable: list[str],
    skipped_pk_columns: list[str],
    passed_columns: list[str],
    total_columns: int,
) -> dg.AssetCheckResult:
    """Build an AssetCheckResult from referential integrity check results."""
    check_passed = len(orphans) == 0 and len(unresolvable) == 0
    metadata: dict[str, Any] = {
        "passed": dg.MetadataValue.json(
            {
                "count": len(passed_columns),
                "columns": passed_columns,
            }
        ),
        "failed": dg.MetadataValue.json(
            {
                "orphaned_references": orphans,
                "unresolvable_columns": unresolvable,
            }
        ),
        "skipped_pk_columns": dg.MetadataValue.json(skipped_pk_columns),
    }
    return dg.AssetCheckResult(passed=check_passed, metadata=metadata)


@dg.asset_check(
    name="dq_fact_dim_referential_check",
    asset=_DW_QUALITY_KEY,
    description=(
        "Verify all DW_EK_ FK values in fact tables exist in target dimension tables.\n\n"
        "**Outputs:**\n\n"
        "- **passed**: `{count, columns}` — FK columns where all values resolve to the target table\n"
        "- **failed**: `{orphaned_references, unresolvable_columns}` — broken FKs and missing target tables\n"
        "- **skipped_pk_columns**: PK columns excluded from the check (self-referencing identity keys)"
    ),
    compute_kind="mssql",
)
def dq_fact_dim_referential_check(
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    """Check referential integrity: fct.*.DW_EK_{dim} -> dim.{dim}.DW_EK_{dim}."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            result = _check_referential_integrity(cursor, "fct", "dim")
        finally:
            cursor.close()
    return _build_referential_result(*result)


_PRE_STG_FAKTURALINJE = "[stg].[pre_fakturalinje_mysupply]"


@dg.asset_check(
    name="dq_faktura_postering_match_check",
    asset=_DW_QUALITY_KEY,
    description=(
        "Observability for Faktura–Postering bridge quality: how many (CVR, FakturaNr) "
        "invoices in pre-staging are built from more than one MySupply header (header "
        "collapse before bridge matching). Non-failing: surfaced for match-rate context.\n\n"
        "**Outputs:**\n\n"
        "- **total_invoices**: distinct (CleanCode_Kreditor, CleanCode_Faktura) combos\n"
        "- **multi_header_invoices**: combos fed by >1 distinct header\n"
        "- **max_headers_per_invoice**: largest header fan-out onto a single invoice"
    ),
    compute_kind="mssql",
)
def dq_faktura_postering_match_check(
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    """Surface header -> (CVR, FakturaNr) collapse from pre-staging (always passes)."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                WITH per_invoice AS (
                    SELECT
                        CleanCode_Kreditor,
                        CleanCode_Faktura,
                        COUNT(DISTINCT statistic_invoice_header_id) AS header_count
                    FROM {_PRE_STG_FAKTURALINJE}
                    WHERE CleanCode_Faktura IS NOT NULL AND CleanCode_Faktura <> ''
                      AND CleanCode_Kreditor IS NOT NULL AND CleanCode_Kreditor <> ''
                    GROUP BY CleanCode_Kreditor, CleanCode_Faktura
                )
                SELECT
                    COUNT(*) AS total_invoices,
                    SUM(CASE WHEN header_count > 1 THEN 1 ELSE 0 END) AS multi_header_invoices,
                    MAX(header_count) AS max_headers_per_invoice
                FROM per_invoice
                """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()

    total_invoices = row[0] or 0
    multi_header_invoices = row[1] or 0
    max_headers_per_invoice = row[2] or 0

    metadata: dict[str, Any] = {
        "total_invoices": dg.MetadataValue.int(total_invoices),
        "multi_header_invoices": dg.MetadataValue.int(multi_header_invoices),
        "max_headers_per_invoice": dg.MetadataValue.int(max_headers_per_invoice),
    }

    return dg.AssetCheckResult(passed=True, metadata=metadata)


@dg.asset_check(
    name="dq_fakturalinje_postering_match_check",
    asset=_DW_QUALITY_KEY,
    description=(
        "Measure Fakturalinje -> Postering match rate via DW_EK_Faktura and "
        "[ref].[Faktura_To_Postering] (newest postering per invoice). Non-failing: "
        "surfaced for observability.\n\n"
        "**Outputs:**\n\n"
        "- **total_lines**: rows in fct.Fakturalinje\n"
        "- **lines_unknown_faktura**: DW_EK_Faktura = -1 (cannot match by design)\n"
        "- **lines_no_postering_match**: lines with a real Faktura but no bridge row"
    ),
    compute_kind="mssql",
)
def dq_fakturalinje_postering_match_check(
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    """Report Fakturalinje -> bridge Postering match rate (always passes)."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                WITH linje AS (
                    SELECT
                        f.DW_EK_Faktura,
                        COALESCE(b.DW_EK_Postering, -1) AS DW_EK_Postering_Bridge
                    FROM [fct].[Fakturalinje] f
                    LEFT JOIN [ref].[Faktura_To_Postering] b
                        ON b.DW_EK_Faktura = f.DW_EK_Faktura
                        AND b.is_newest_postering_for_faktura = 1
                        AND b.DW_IsDeleted = 0
                        AND f.DW_EK_Faktura <> -1
                )
                SELECT
                    COUNT(*) AS total_lines,
                    SUM(CASE WHEN DW_EK_Faktura = -1 THEN 1 ELSE 0 END)
                        AS lines_unknown_faktura,
                    SUM(CASE WHEN DW_EK_Faktura <> -1
                              AND DW_EK_Postering_Bridge = -1 THEN 1 ELSE 0 END)
                        AS lines_no_postering_match
                FROM linje
                """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()

    total_lines = row[0] or 0
    lines_unknown_faktura = row[1] or 0
    lines_no_postering_match = row[2] or 0

    metadata: dict[str, Any] = {
        "total_lines": dg.MetadataValue.int(total_lines),
        "lines_unknown_faktura": dg.MetadataValue.int(lines_unknown_faktura),
        "lines_no_postering_match": dg.MetadataValue.int(lines_no_postering_match),
    }

    return dg.AssetCheckResult(passed=True, metadata=metadata)


@dg.asset_check(
    name="dq_dim_map_referential_check",
    asset=_DW_QUALITY_KEY,
    description=(
        "Verify all DW_EK_ FK values in dimension tables exist in target map tables.\n\n"
        "**Outputs:**\n\n"
        "- **fk_columns_checked**: number of FK columns validated\n"
        "- **orphaned_references**: `{dim.table: {column -> map.target: count}}` for broken FKs\n"
        "- **unresolvable_columns**: FK columns where the target map table does not exist\n"
        "- **skipped_pk_columns**: PK columns excluded from the check (self-referencing identity keys)"
    ),
    compute_kind="mssql",
)
def dq_dim_map_referential_check(
    mssql_target_staging: MSSQLResource,
) -> dg.AssetCheckResult:
    """Check referential integrity: dim.*.DW_EK_{map} -> map.{map}.DW_EK_{map}."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            result = _check_referential_integrity(cursor, "dim", "map")
        finally:
            cursor.close()
    return _build_referential_result(*result)
