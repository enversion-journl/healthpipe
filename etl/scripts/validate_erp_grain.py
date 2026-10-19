#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ERP Grain Validation Script

Validates granularity assumptions across ERP source, map, dim, and fct tables.
Outputs results to a timestamped markdown report under validation_reports/.

Usage:
    cd etl
    python -m scripts.validate_erp_grain \\
        --partition-key 2025-01-15

    # Monthly partition (limits source scans to one month)
    python -m scripts.validate_erp_grain \\
        --partition-key 2025-01

    # With scheduled-catchup mode (25-hour window)
    python -m scripts.validate_erp_grain \\
        --partition-key 2025-01-15 --scheduled-catchup

    # Custom output path and failure sample size
    python -m scripts.validate_erp_grain \\
        --partition-key 2025-01-15 --output /tmp/grain.md --max-fail-samples 20

Environment:
    Reads DB credentials from .env (same vars used by Dagster resources).
    Requires: MSSQL_TARGET_STAGING_*, MYSUPPLY_PROD_MSSQL_*, SYDAX_PROD_MSSQL_*, ILS_PROD_MSSQL_*
"""

from __future__ import annotations

import argparse
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pymssql

_UTC = ZoneInfo("UTC")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

for candidate in (PROJECT_ROOT / ".env", PROJECT_ROOT.parent / ".env"):
    if candidate.exists():
        load_dotenv(candidate, override=True)
        break

from DSI.connectors.mssql.sql_builders import (
    get_month_date_range,
    is_monthly_partition,
)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    entity: str
    layer: str
    check_name: str
    passed: bool
    total_rows: int = 0
    duplicate_count: int = 0
    orphan_count: int = 0
    detail: str = ""
    samples: list[str] = field(default_factory=list)


@dataclass
class GrainSpec:
    """Declarative grain specification for one target table."""

    entity: str
    schema: str
    table: str
    grain_columns: list[str]
    cleancode_column: str | None = None
    staging_table: str | None = None
    staging_grain_columns: list[str] | None = None
    map_table: str | None = None
    map_grain_column: str | None = None
    source_system: str | None = None


# ---------------------------------------------------------------------------
# Grain catalog
# ---------------------------------------------------------------------------

MAP_SPECS: list[GrainSpec] = [
    GrainSpec("Aftale", "map", "Aftale", ["dw_ek_Aftale"], "clean_code_Aftale"),
    GrainSpec(
        "AftaleVarePris",
        "map",
        "AftaleVarePris",
        ["dw_ek_AftaleVarePris"],
        "clean_code_AftaleVarePris",
    ),
    GrainSpec("Faktura", "map", "Faktura", ["dw_ek_Faktura"], "clean_code_Faktura"),
    GrainSpec(
        "Fakturalinje",
        "map",
        "Fakturalinje",
        ["dw_ek_Fakturalinje"],
        "clean_code_Fakturalinje",
    ),
    GrainSpec("Kontoplan", "map", "Kontoplan", ["dw_ek_Kontoplan"], "clean_code_Kontoplan"),
    GrainSpec(
        "ILSLeverandoer",
        "map",
        "ILSLeverandoer",
        ["dw_ek_ILSLeverandoer"],
        "clean_code_ILSLeverandoer",
    ),
    GrainSpec("Kreditor", "map", "Kreditor", ["dw_ek_Kreditor"], "clean_code_Kreditor"),
    GrainSpec("Lager", "map", "Lager", ["dw_ek_Lager"], "clean_code_Lager"),
    GrainSpec("Postering", "map", "Postering", ["dw_ek_Postering"], "clean_code_Postering"),
    GrainSpec("Rekvirent", "map", "Rekvirent", ["dw_ek_Rekvirent"], "clean_code_Rekvirent"),
    GrainSpec(
        "Vareudlevering",
        "map",
        "Vareudlevering",
        ["dw_ek_Vareudlevering"],
        "clean_code_Vareudlevering",
    ),
    GrainSpec(
        "Leverandoervare",
        "map",
        "Leverandoervare",
        ["dw_ek_Leverandoervare"],
        "clean_code_Leverandoervare",
    ),
]

DIM_SPECS: list[GrainSpec] = [
    GrainSpec(
        "Aftale",
        "dim",
        "Aftale",
        ["dw_ek_Aftale"],
        "clean_code_Aftale",
        map_table="map.Aftale",
        map_grain_column="dw_ek_Aftale",
    ),
    GrainSpec(
        "Faktura",
        "dim",
        "Faktura",
        ["dw_ek_Faktura"],
        map_table="map.Faktura",
        map_grain_column="dw_ek_Faktura",
    ),
    GrainSpec(
        "Fakturalinje",
        "dim",
        "Fakturalinje",
        ["dw_ek_Fakturalinje"],
        map_table="map.Fakturalinje",
        map_grain_column="dw_ek_Fakturalinje",
    ),
    GrainSpec(
        "Kontoplan",
        "dim",
        "Kontoplan",
        ["dw_ek_Kontoplan"],
        "clean_code_Kontoplan",
        map_table="map.Kontoplan",
        map_grain_column="dw_ek_Kontoplan",
    ),
    GrainSpec(
        "ILSLeverandoer",
        "dim",
        "ILSLeverandoer",
        ["dw_ek_ILSLeverandoer"],
        "clean_code_ILSLeverandoer",
        map_table="map.ILSLeverandoer",
        map_grain_column="dw_ek_ILSLeverandoer",
    ),
    GrainSpec(
        "Kreditor",
        "dim",
        "Kreditor",
        ["dw_ek_Kreditor"],
        "clean_code_Kreditor",
        map_table="map.Kreditor",
        map_grain_column="dw_ek_Kreditor",
    ),
    GrainSpec(
        "Lager",
        "dim",
        "Lager",
        ["dw_ek_Lager"],
        "clean_code_Lager",
        map_table="map.Lager",
        map_grain_column="dw_ek_Lager",
    ),
    GrainSpec(
        "Postering",
        "dim",
        "Postering",
        ["dw_ek_Postering"],
        "clean_code_Postering",
        map_table="map.Postering",
        map_grain_column="dw_ek_Postering",
    ),
    GrainSpec(
        "Rekvirent",
        "dim",
        "Rekvirent",
        ["dw_ek_Rekvirent"],
        "clean_code_Rekvirent",
        map_table="map.Rekvirent",
        map_grain_column="dw_ek_Rekvirent",
    ),
    GrainSpec(
        "Vareudlevering",
        "dim",
        "Vareudlevering",
        ["dw_ek_Vareudlevering"],
        "clean_code_Vareudlevering",
        map_table="map.Vareudlevering",
        map_grain_column="dw_ek_Vareudlevering",
    ),
    GrainSpec(
        "Leverandoervare",
        "dim",
        "Leverandoervare",
        ["dw_ek_Leverandoervare"],
        "clean_code_Leverandoervare",
        map_table="map.Leverandoervare",
        map_grain_column="dw_ek_Leverandoervare",
    ),
]

FCT_SPECS: list[GrainSpec] = [
    GrainSpec(
        "Faktura",
        "fct",
        "Faktura",
        ["dw_ek_Faktura"],
        staging_table="stg.fct_Faktura_mysupply",
        staging_grain_columns=["clean_code_Faktura"],
        source_system="MySupply",
    ),
    GrainSpec(
        "Fakturalinje",
        "fct",
        "Fakturalinje",
        ["clean_code_Fakturalinje"],
        staging_table="stg.fct_Fakturalinje_mysupply",
        staging_grain_columns=["clean_code_Fakturalinje"],
        source_system="MySupply",
        # Transaction dim: dw_ek_Fakturalinje is resolved from map.Fakturalinje
        # (shared with dim.Fakturalinje).
        map_table="map.Fakturalinje",
        map_grain_column="dw_ek_Fakturalinje",
    ),
    GrainSpec(
        "Postering",
        "fct",
        "Postering",
        ["ref_ledgertrans_id"],
        staging_table="stg.fct_Postering_sydax",
        staging_grain_columns=["ref_ledgertrans_id"],
        source_system="SydAX",
    ),
    GrainSpec(
        "Vareudlevering",
        "fct",
        "Vareudlevering",
        ["clean_code_Vareudlevering"],
        staging_table="stg.fct_Vareudlevering_ils",
        staging_grain_columns=["clean_code_Vareudlevering"],
        source_system="ILS",
        # Transaction dim: dw_ek_Vareudlevering is resolved from map.Vareudlevering
        # (shared with dim.Vareudlevering).
        map_table="map.Vareudlevering",
        map_grain_column="dw_ek_Vareudlevering",
    ),
    GrainSpec(
        "AftaleVarePris",
        "fct",
        "AftaleVarePris",
        ["bk_prrowid", "clean_code_AftaleVarePris"],
        staging_table="stg.fct_AftaleVarePris_ils",
        staging_grain_columns=["bk_prrowid", "clean_code_AftaleVarePris"],
    ),
    # bridge.AftaleLager: grain haandhaeves af et unikt indeks (ikke PK'en,
    # som er dw_sk-identity) — valideres derfor her ligesom fakta-grains.
    # bridge.LeverandoervareILSVare behoever ingen spec: dens grain ER PK'en.
    GrainSpec(
        "AftaleLager",
        "bridge",
        "AftaleLager",
        ["clean_code_Aftale", "sgh_raw"],
        staging_table="stg.bridge_AftaleLager",
        staging_grain_columns=["clean_code_Aftale", "sgh_raw"],
    ),
]

SOURCE_PARTITION_CHECKS: list[dict] = [
    {
        "entity": "Finanspostering (SydAX)",
        "db": "sydax",
        "schema": "Finans",
        "table": "Finanspostering",
        "partition_column": "Oprettelsestidspunkt",
        "is_string_date": False,
    },
    {
        "entity": "Invoice Header (MySupply)",
        "db": "mysupply",
        "schema": "dbo",
        "table": "statistic_invoice_header",
        "partition_column": "FakturaDato",
        "is_string_date": True,
    },
    {
        "entity": "Vareudlevering (ILS)",
        "db": "ils",
        "schema": "dbo",
        "table": "udlev",
        "partition_columns": ["pro2created", "pro2modified"],
        "is_string_date": False,
    },
]


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


@contextmanager
def _connect_target():
    conn = pymssql.connect(
        server=os.getenv("MSSQL_TARGET_STAGING_HOST", "localhost"),
        port=int(os.getenv("MSSQL_TARGET_STAGING_PORT", "1433")),
        user=os.getenv("MSSQL_TARGET_STAGING_USER", "sa"),
        password=os.getenv("MSSQL_TARGET_STAGING_PASSWORD", ""),
        database=os.getenv("MSSQL_TARGET_STAGING_DB", "DW"),
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _connect_source(db_key: str):
    env_map = {
        "mysupply": (
            "MYSUPPLY_PROD_MSSQL_SERVER",
            "MYSUPPLY_PROD_MSSQL_PORT",
            "MYSUPPLY_PROD_MSSQL_USERNAME",
            "MYSUPPLY_PROD_MSSQL_PASSWORD",
            "MYSUPPLY_PROD_MSSQL_DATABASE",
        ),
        "sydax": (
            "SYDAX_PROD_MSSQL_SERVER",
            "SYDAX_PROD_MSSQL_PORT",
            "SYDAX_PROD_MSSQL_USERNAME",
            "SYDAX_PROD_MSSQL_PASSWORD",
            "SYDAX_PROD_MSSQL_DATABASE",
        ),
        "ils": (
            "ILS_PROD_MSSQL_SERVER",
            "ILS_PROD_MSSQL_PORT",
            "ILS_PROD_MSSQL_USERNAME",
            "ILS_PROD_MSSQL_PASSWORD",
            "ILS_PROD_MSSQL_DATABASE",
        ),
    }
    host_var, port_var, user_var, pwd_var, db_var = env_map[db_key]
    conn = pymssql.connect(
        server=os.getenv(host_var, "localhost"),
        port=int(os.getenv(port_var, "1433")),
        user=os.getenv(user_var, "sa"),
        password=os.getenv(pwd_var, ""),
        database=os.getenv(db_var, ""),
    )
    try:
        yield conn
    finally:
        conn.close()


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(
        "SELECT OBJECT_ID(%s, 'U')",
        (f"[{schema}].[{table}]",),
    )
    return cursor.fetchone()[0] is not None


def _column_exists(cursor, schema: str, table: str, column: str) -> bool:
    cursor.execute(
        "SELECT COL_LENGTH(%s, %s)",
        (f"[{schema}].[{table}]", column),
    )
    return cursor.fetchone()[0] is not None


# ---------------------------------------------------------------------------
# Partition predicate builder (mirrors DSI loader logic)
# ---------------------------------------------------------------------------


def _build_date_filter(
    column: str,
    partition_key: str,
    is_string_date: bool = False,
    scheduled_catchup: bool = False,
) -> str:
    monthly = is_monthly_partition(partition_key)
    if monthly:
        start, end = get_month_date_range(partition_key)
        if is_string_date:
            return f"[{column}] >= '{start}' AND [{column}] <= '{end}'"
        return f"CAST([{column}] AS DATE) >= '{start}' AND " f"CAST([{column}] AS DATE) <= '{end}'"

    if scheduled_catchup:
        from datetime import timedelta

        today = datetime.strptime(partition_key, "%Y-%m-%d")
        yesterday = today - timedelta(days=1)
        if is_string_date:
            return f"[{column}] >= '{yesterday.strftime('%Y-%m-%d')}'"
        start_ts = yesterday.strftime("%Y-%m-%d") + " 01:00:00"
        return f"[{column}] >= '{start_ts}'"

    if is_string_date:
        return f"[{column}] = '{partition_key}'"
    return f"CAST([{column}] AS DATE) = '{partition_key}'"


# ---------------------------------------------------------------------------
# Check runners
# ---------------------------------------------------------------------------


def _run_uniqueness_check(
    cursor,
    spec: GrainSpec,
    max_samples: int,
) -> CheckResult:
    """Check that grain columns are unique within a table."""
    fq_table = f"[{spec.schema}].[{spec.table}]"
    grain_cols = ", ".join(f"[{c}]" for c in spec.grain_columns)

    if not _table_exists(cursor, spec.schema, spec.table):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="uniqueness",
            passed=True,
            detail=f"Table {fq_table} does not exist (skipped)",
        )

    cursor.execute(f"SELECT COUNT(*) FROM {fq_table}")
    total_rows = cursor.fetchone()[0]

    dup_sql = (
        f"SELECT {grain_cols}, COUNT(*) AS cnt "
        f"FROM {fq_table} "
        f"GROUP BY {grain_cols} "
        f"HAVING COUNT(*) > 1"
    )
    cursor.execute(dup_sql)
    dups = cursor.fetchall()
    dup_count = len(dups)

    samples = []
    if dup_count > 0:
        for row in dups[:max_samples]:
            cols_str = " | ".join(str(v) for v in row)
            samples.append(cols_str)

    return CheckResult(
        entity=spec.entity,
        layer=spec.schema,
        check_name="uniqueness",
        passed=(dup_count == 0),
        total_rows=total_rows,
        duplicate_count=dup_count,
        detail=f"Grain: {', '.join(spec.grain_columns)}",
        samples=samples,
    )


def _run_cleancode_uniqueness(
    cursor,
    spec: GrainSpec,
    max_samples: int,
) -> CheckResult | None:
    """Check CleanCode column uniqueness (for map tables)."""
    if not spec.cleancode_column:
        return None

    fq_table = f"[{spec.schema}].[{spec.table}]"
    col = f"[{spec.cleancode_column}]"

    if not _table_exists(cursor, spec.schema, spec.table):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="cleancode_uniqueness",
            passed=True,
            detail=f"Table {fq_table} does not exist (skipped)",
        )

    cursor.execute(f"SELECT COUNT(*) FROM {fq_table}")
    total_rows = cursor.fetchone()[0]

    dup_sql = (
        f"SELECT {col}, COUNT(*) AS cnt "
        f"FROM {fq_table} "
        f"WHERE {col} IS NOT NULL "
        f"GROUP BY {col} "
        f"HAVING COUNT(*) > 1"
    )
    cursor.execute(dup_sql)
    dups = cursor.fetchall()
    dup_count = len(dups)

    samples = []
    for row in dups[:max_samples]:
        samples.append(f"{row[0]} (x{row[1]})")

    return CheckResult(
        entity=spec.entity,
        layer=spec.schema,
        check_name="cleancode_uniqueness",
        passed=(dup_count == 0),
        total_rows=total_rows,
        duplicate_count=dup_count,
        detail=f"CleanCode: {spec.cleancode_column}",
        samples=samples,
    )


def _run_cross_layer_check(
    cursor,
    spec: GrainSpec,
    max_samples: int,
) -> CheckResult | None:
    """Check that dim/fct keys exist in their parent map table."""
    if not spec.map_table or not spec.map_grain_column:
        return None

    map_schema, map_tbl = spec.map_table.split(".")
    fq_target = f"[{spec.schema}].[{spec.table}]"
    fq_map = f"[{map_schema}].[{map_tbl}]"
    col = spec.map_grain_column

    if not _table_exists(cursor, spec.schema, spec.table):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="cross_layer_coverage",
            passed=True,
            detail=f"Table {fq_target} does not exist (skipped)",
        )
    if not _table_exists(cursor, map_schema, map_tbl):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="cross_layer_coverage",
            passed=True,
            detail=f"Map table {fq_map} does not exist (skipped)",
        )

    orphan_sql = (
        f"SELECT d.[{col}] "
        f"FROM {fq_target} d "
        f"WHERE d.[{col}] <> -1 "
        f"  AND NOT EXISTS ("
        f"    SELECT 1 FROM {fq_map} m WHERE m.[{col}] = d.[{col}]"
        f"  )"
    )
    cursor.execute(orphan_sql)
    orphans = cursor.fetchall()
    orphan_count = len(orphans)

    samples = []
    for row in orphans[:max_samples]:
        samples.append(str(row[0]))

    return CheckResult(
        entity=spec.entity,
        layer=spec.schema,
        check_name="cross_layer_coverage",
        passed=(orphan_count == 0),
        orphan_count=orphan_count,
        detail=f"{fq_target}.{col} -> {fq_map}.{col}",
        samples=samples,
    )


def _run_fct_dim_parity_check(
    cursor,
    spec: GrainSpec,
    max_samples: int,
) -> CheckResult | None:
    """Check 1:1 PK parity between fct and dim for transaction dimensions.

    Both layers share the same dw_ek_* PK resolved from map.* — every
    non-sentinel fct key must exist in dim and vice versa.
    """
    if not spec.map_table or not spec.map_grain_column:
        return None

    col = spec.map_grain_column
    fq_fct = f"[{spec.schema}].[{spec.table}]"
    fq_dim = f"[dim].[{spec.table}]"

    if not _table_exists(cursor, spec.schema, spec.table):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="fct_dim_parity",
            passed=True,
            detail=f"Table {fq_fct} does not exist (skipped)",
        )
    if not _table_exists(cursor, "dim", spec.table):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="fct_dim_parity",
            passed=True,
            detail=f"Table {fq_dim} does not exist (skipped)",
        )

    fct_orphan_sql = (
        f"SELECT f.[{col}] "
        f"FROM {fq_fct} f "
        f"WHERE f.[{col}] <> -1 "
        f"  AND NOT EXISTS ("
        f"    SELECT 1 FROM {fq_dim} d WHERE d.[{col}] = f.[{col}]"
        f"  )"
    )
    cursor.execute(fct_orphan_sql)
    fct_orphans = cursor.fetchall()

    dim_orphan_sql = (
        f"SELECT d.[{col}] "
        f"FROM {fq_dim} d "
        f"WHERE d.[{col}] <> -1 "
        f"  AND NOT EXISTS ("
        f"    SELECT 1 FROM {fq_fct} f WHERE f.[{col}] = d.[{col}]"
        f"  )"
    )
    cursor.execute(dim_orphan_sql)
    dim_orphans = cursor.fetchall()

    orphan_count = len(fct_orphans) + len(dim_orphans)
    samples: list[str] = []
    for row in fct_orphans[:max_samples]:
        samples.append(f"fct->{col}={row[0]} missing in dim")
    remaining = max(0, max_samples - len(samples))
    for row in dim_orphans[:remaining]:
        samples.append(f"dim->{col}={row[0]} missing in fct")

    return CheckResult(
        entity=spec.entity,
        layer=spec.schema,
        check_name="fct_dim_parity",
        passed=(orphan_count == 0),
        orphan_count=orphan_count,
        detail=f"{fq_fct}.{col} <-> {fq_dim}.{col} (1:1 transaction dim)",
        samples=samples,
    )


def _run_fct_stg_coverage(
    cursor,
    spec: GrainSpec,
    max_samples: int,
) -> CheckResult | None:
    """Check that fact grain keys match staging grain keys (row count comparison)."""
    if not spec.staging_table or not spec.staging_grain_columns:
        return None

    fq_target = f"[{spec.schema}].[{spec.table}]"
    stg_schema, stg_tbl = spec.staging_table.split(".")

    if not _table_exists(cursor, spec.schema, spec.table):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="fct_stg_coverage",
            passed=True,
            detail=f"Table {fq_target} does not exist (skipped)",
        )
    if not _table_exists(cursor, stg_schema, stg_tbl):
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name="fct_stg_coverage",
            passed=True,
            detail=f"Staging table {spec.staging_table} does not exist (skipped)",
        )

    # COUNT DISTINCT tager kun EET udtryk i T-SQL — for et fler-kolonne grain
    # (fx AftaleVarePris' bk_prrowid + clean_code_AftaleVarePris) skal
    # kolonnerne CONCAT'es i stedet for at staa som en kommasepareret liste
    # (som ville vaere ugyldig T-SQL: "COUNT(DISTINCT [a], [b])"). Samme
    # argument for WHERE-NULL-filteret: "WHERE [a], [b] IS NOT NULL" er
    # ugyldig syntaks — hver kolonne skal have sit eget IS NOT NULL, AND'et
    # sammen.
    if len(spec.staging_grain_columns) > 1:
        stg_distinct_expr = (
            "CONCAT(" + ", '|', ".join(f"[{c}]" for c in spec.staging_grain_columns) + ")"
        )
    else:
        stg_distinct_expr = f"[{spec.staging_grain_columns[0]}]"
    stg_not_null_where = " AND ".join(f"[{c}] IS NOT NULL" for c in spec.staging_grain_columns)

    cursor.execute(
        f"SELECT COUNT(DISTINCT {stg_distinct_expr}) FROM [{stg_schema}].[{stg_tbl}] "
        f"WHERE {stg_not_null_where}"
    )
    stg_distinct = cursor.fetchone()[0]

    fct_exclude_predicates = []
    if spec.grain_columns[0].startswith("DW_EK_"):
        fct_exclude_predicates.append(f"[{spec.grain_columns[0]}] <> -1")
    # Soft-delete-eksklusion: ingen af scriptets andre (enkelt-kolonne)
    # coverage-tjek filtrerer dw_is_deleted i dag, saa denne tilfoejes KUN
    # her, betinget paa at kolonnen rent faktisk findes paa fact-tabellen.
    # Uden filteret ville post-re-key dw_is_deleted=1-residue (fx
    # AftaleVarePris's bk_prrowid-kildepraefiks-aendring, spec 2026-08-31, som
    # efterlader gamle raekker soft-deleted under det gamle noegleformat)
    # garantere et mismatch mod staging, der kun baerer det NYE format.
    if _column_exists(cursor, spec.schema, spec.table, "dw_is_deleted"):
        fct_exclude_predicates.append("[dw_is_deleted] = 0")
    fct_exclude = f" WHERE {' AND '.join(fct_exclude_predicates)}" if fct_exclude_predicates else ""
    cursor.execute(f"SELECT COUNT(*) FROM {fq_target}{fct_exclude}")
    fct_count = cursor.fetchone()[0]

    passed = fct_count == stg_distinct
    detail = f"fct={fct_count}, stg_distinct={stg_distinct}"
    if not passed:
        diff = fct_count - stg_distinct
        detail += f" (diff={diff:+d})"

    return CheckResult(
        entity=spec.entity,
        layer=spec.schema,
        check_name="fct_stg_coverage",
        passed=passed,
        total_rows=fct_count,
        detail=f"Grain: {', '.join(spec.grain_columns)} vs staging {spec.staging_table} ({detail})",
    )


def _run_source_partition_count(
    partition_key: str,
    scheduled_catchup: bool,
    check_cfg: dict,
) -> CheckResult:
    """Count rows in a source table limited by partition predicate."""
    entity = check_cfg["entity"]
    db_key = check_cfg["db"]
    schema = check_cfg["schema"]
    table = check_cfg["table"]
    is_string = check_cfg.get("is_string_date", False)

    cols = check_cfg.get("partition_columns")
    single_col = check_cfg.get("partition_column")

    if cols:
        predicates = [
            f"({_build_date_filter(c, partition_key, is_string, scheduled_catchup)})" for c in cols
        ]
        where = " OR ".join(predicates)
    elif single_col:
        where = _build_date_filter(single_col, partition_key, is_string, scheduled_catchup)
    else:
        where = "1=1"

    sql = f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE {where}"

    try:
        with _connect_source(db_key) as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                count = cur.fetchone()[0]
            finally:
                cur.close()
    except Exception as exc:
        return CheckResult(
            entity=entity,
            layer="source",
            check_name="partition_rowcount",
            passed=True,
            detail=f"Connection failed for {db_key}: {exc}",
        )

    return CheckResult(
        entity=entity,
        layer="source",
        check_name="partition_rowcount",
        passed=True,
        total_rows=count,
        detail=f"[{schema}].[{table}] WHERE {where}",
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def _safe_run(cursor, fn, spec: GrainSpec, max_samples: int) -> CheckResult | None:
    """Run a check function, catching SQL errors so one failure doesn't abort the run."""
    try:
        return fn(cursor, spec, max_samples)
    except Exception as exc:
        return CheckResult(
            entity=spec.entity,
            layer=spec.schema,
            check_name=fn.__name__.replace("_run_", ""),
            passed=False,
            detail=f"ERROR: {exc}",
        )


def run_all_checks(
    partition_key: str,
    scheduled_catchup: bool = False,
    max_samples: int = 10,
) -> list[CheckResult]:
    results: list[CheckResult] = []

    # 1) Source partition row counts
    print("  [1/4] Source partition row counts...")
    for cfg in SOURCE_PARTITION_CHECKS:
        r = _run_source_partition_count(partition_key, scheduled_catchup, cfg)
        results.append(r)
        status = "PASS" if r.passed else "FAIL"
        print(f"    {status}  {r.entity}: {r.total_rows} rows")

    # 2-4) Target DW checks
    with _connect_target() as conn:
        cursor = conn.cursor()
        try:
            print("  [2/4] Map grain checks...")
            for spec in MAP_SPECS:
                r = _safe_run(cursor, _run_uniqueness_check, spec, max_samples)
                results.append(r)
                _print_check(r)

                cc = _safe_run(cursor, _run_cleancode_uniqueness, spec, max_samples)
                if cc:
                    results.append(cc)
                    _print_check(cc)

            print("  [3/4] Dimension grain + cross-layer checks...")
            for spec in DIM_SPECS:
                r = _safe_run(cursor, _run_uniqueness_check, spec, max_samples)
                results.append(r)
                _print_check(r)

                xl = _safe_run(cursor, _run_cross_layer_check, spec, max_samples)
                if xl:
                    results.append(xl)
                    _print_check(xl)

            print("  [4/4] Fact grain + staging coverage checks...")
            for spec in FCT_SPECS:
                r = _safe_run(cursor, _run_uniqueness_check, spec, max_samples)
                results.append(r)
                _print_check(r)

                cov = _safe_run(cursor, _run_fct_stg_coverage, spec, max_samples)
                if cov:
                    results.append(cov)
                    _print_check(cov)

                # Transaction-dim facts (map_table/map_grain_column set) must have
                # their PK resolve into the shared map table, e.g. fct.Fakturalinje
                # -> map.Fakturalinje.
                xl = _safe_run(cursor, _run_cross_layer_check, spec, max_samples)
                if xl:
                    results.append(xl)
                    _print_check(xl)

                parity = _safe_run(cursor, _run_fct_dim_parity_check, spec, max_samples)
                if parity:
                    results.append(parity)
                    _print_check(parity)
        finally:
            cursor.close()
    return results


def _print_check(r: CheckResult) -> None:
    status = "PASS" if r.passed else "FAIL"
    dup_info = f" dupes={r.duplicate_count}" if r.duplicate_count else ""
    orphan_info = f" orphans={r.orphan_count}" if r.orphan_count else ""
    print(f"    {status}  [{r.layer}] {r.entity}.{r.check_name}{dup_info}{orphan_info}")


# ---------------------------------------------------------------------------
# Markdown report generation
# ---------------------------------------------------------------------------


def generate_report(
    results: list[CheckResult],
    partition_key: str,
    scheduled_catchup: bool,
    output_path: Path,
) -> None:
    now = datetime.now(tz=_UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    lines: list[str] = []

    lines.append("# ERP Grain Validation Report\n")
    lines.append(f"- **Generated**: {now}")
    lines.append(f"- **Partition key**: `{partition_key}`")
    lines.append(f"- **Scheduled catchup**: {scheduled_catchup}")
    lines.append(f"- **Total checks**: {total}")
    lines.append(f"- **Passed**: {passed}")
    lines.append(f"- **Failed**: {failed}")
    lines.append("")

    if failed > 0:
        lines.append("## Critical Failures\n")
        lines.append("| Layer | Entity | Check | Detail |")
        lines.append("|-------|--------|-------|--------|")
        for r in results:
            if not r.passed:
                lines.append(f"| {r.layer} | {r.entity} | {r.check_name} | {r.detail} |")
        lines.append("")

    # Group results by layer
    for layer_label, layer_key in [
        ("Source Partition Row Counts", "source"),
        ("Map Tables (map)", "map"),
        ("Dimension Tables (dim)", "dim"),
        ("Fact Tables (fct)", "fct"),
    ]:
        layer_results = [r for r in results if r.layer == layer_key]
        if not layer_results:
            continue

        lines.append(f"## {layer_label}\n")
        lines.append("| Entity | Check | Status | Rows | Dupes | Orphans | Detail |")
        lines.append("|--------|-------|--------|------|-------|---------|--------|")
        for r in layer_results:
            status = "PASS" if r.passed else "**FAIL**"
            rows = str(r.total_rows) if r.total_rows > 0 else ("0" if r.total_rows == 0 else "-")
            dupes = str(r.duplicate_count) if r.duplicate_count > 0 else "-"
            orphans = str(r.orphan_count) if r.orphan_count > 0 else "-"
            detail_escaped = r.detail.replace("|", "\\|")
            lines.append(
                f"| {r.entity} | {r.check_name} | {status} | {rows} | {dupes} | {orphans} | {detail_escaped} |"
            )
        lines.append("")

        # Show samples for failures
        failed_with_samples = [r for r in layer_results if not r.passed and r.samples]
        if failed_with_samples:
            lines.append(f"### Failure Samples ({layer_label})\n")
            for r in failed_with_samples:
                lines.append(f"**{r.entity} - {r.check_name}** ({r.detail}):\n")
                lines.append("```")
                for s in r.samples:
                    lines.append(s)
                lines.append("```\n")

    report_text = "\n".join(lines)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_text, encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate ERP grain assumptions and generate markdown report.",
    )
    parser.add_argument(
        "--partition-key",
        required=True,
        help="Partition key in YYYY-MM-DD (daily) or YYYY-MM (monthly) format.",
    )
    parser.add_argument(
        "--scheduled-catchup",
        action="store_true",
        default=False,
        help="Use scheduled-catchup mode (25-hour window) for source partition predicates.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Custom output path for the markdown report.",
    )
    parser.add_argument(
        "--max-fail-samples",
        type=int,
        default=10,
        help="Maximum number of sample rows to include for each failing check.",
    )
    args = parser.parse_args()

    pk = args.partition_key
    is_monthly = is_monthly_partition(pk)
    mode = "monthly" if is_monthly else "daily"
    print(f"ERP Grain Validation ({mode} partition: {pk})")
    print("=" * 60)

    results = run_all_checks(
        partition_key=pk,
        scheduled_catchup=args.scheduled_catchup,
        max_samples=args.max_fail_samples,
    )

    if args.output:
        out_path = Path(args.output)
    else:
        ts = datetime.now(tz=_UTC).strftime("%Y%m%d_%H%M%S")
        out_path = PROJECT_ROOT / "validation_reports" / f"erp_grain_report_{ts}.md"

    generate_report(results, pk, args.scheduled_catchup, out_path)

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print("=" * 60)
    print(f"Results: {passed}/{total} passed, {failed} failed")
    print(f"Report: {out_path}")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
