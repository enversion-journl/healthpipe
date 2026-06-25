# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Monthly partition completeness checks for ERP fact tables.

Compares row counts grouped by month across three tiers (Source -> DSA -> DSO)
for fct.Faktura, fct.Fakturalinje, fct.Postering, fct.Vareudlevering,
fct.Finanskladdelinje, and fct.Budget.

Detects:
- Data loss in staging (DSA count != Source count)
- Sync gaps (DSO count != DSA count)
- Months present in one tier but missing from another
"""

from __future__ import annotations

from typing import Any

import dagster as dg
from core.resources import (
    ILSMSSQLResource,
    MSSQLResource,
    MySupplyMSSQLResource,
    SydaxMSSQLResource,
)

_DW_QUALITY_KEY = dg.AssetKey("dw_quality")


def _fetch_monthly_counts(conn, query: str) -> dict[int, int]:
    """Execute a GROUP BY month query and return {month_key: row_count}."""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return {int(row[0]): int(row[1]) for row in cursor.fetchall() if row[0] is not None}
    finally:
        cursor.close()


def _fetch_unknown_date_count(conn, schema: str, table: str, date_column: str) -> int:
    """Count rows where the date key is -1 (unknown/unmapped date)."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE [{date_column}] = -1")
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def _compare_monthly_counts(
    source_resource,
    source_query: str,
    staging_resource: MSSQLResource,
    dso_resource: MSSQLResource,
    fct_schema: str,
    fct_table: str,
    date_column: str,
) -> dg.AssetCheckResult:
    """Compare monthly row counts across Source, DSA, and DSO for a fact table.

    The DSA/DSO query groups by date_column / 100 to convert YYYYMMDD -> YYYYMM.
    """
    dw_query = (
        f"SELECT [{date_column}] / 100 AS month_key, COUNT(*) AS cnt "
        f"FROM [{fct_schema}].[{fct_table}] "
        f"WHERE [{date_column}] > 0 "
        f"GROUP BY [{date_column}] / 100"
    )

    with source_resource.get_connection() as conn:
        source_counts = _fetch_monthly_counts(conn, source_query)

    with staging_resource.get_connection() as conn:
        dsa_counts = _fetch_monthly_counts(conn, dw_query)
        dsa_unknown = _fetch_unknown_date_count(conn, fct_schema, fct_table, date_column)

    with dso_resource.get_connection() as conn:
        dso_counts = _fetch_monthly_counts(conn, dw_query)
        dso_unknown = _fetch_unknown_date_count(conn, fct_schema, fct_table, date_column)

    all_months = sorted(set(source_counts) | set(dsa_counts) | set(dso_counts))

    passed_months: list[str] = []
    failed_months: list[dict[str, Any]] = []

    for month in all_months:
        src = source_counts.get(month, 0)
        dsa = dsa_counts.get(month, 0)
        dso = dso_counts.get(month, 0)

        if src == dsa == dso:
            passed_months.append(str(month))
        else:
            entry: dict[str, Any] = {"month": str(month)}
            if src != dsa:
                entry["source"] = src
                entry["dsa"] = dsa
                entry["source_dsa_diff"] = src - dsa
            if dsa != dso:
                entry["dsa"] = dsa
                entry["dso"] = dso
                entry["dsa_dso_diff"] = dsa - dso
            if month not in source_counts:
                entry["note"] = "missing_from_source"
            elif month not in dsa_counts:
                entry["note"] = "missing_from_dsa"
            elif month not in dso_counts:
                entry["note"] = "missing_from_dso"
            failed_months.append(entry)

    check_passed = len(failed_months) == 0
    metadata: dict[str, Any] = {
        "summary": dg.MetadataValue.json(
            {
                "table": f"{fct_schema}.{fct_table}",
                "months_checked": len(all_months),
                "months_passed": len(passed_months),
                "months_failed": len(failed_months),
                "dsa_unknown_date_rows": dsa_unknown,
                "dso_unknown_date_rows": dso_unknown,
                "source_total": sum(source_counts.values()),
                "dsa_total": sum(dsa_counts.values()),
                "dso_total": sum(dso_counts.values()),
            }
        ),
        "passed": dg.MetadataValue.json(
            {
                "count": len(passed_months),
                "months": passed_months,
            }
        ),
        "failed": dg.MetadataValue.json(
            {
                "count": len(failed_months),
                "months": failed_months,
            }
        ),
    }

    return dg.AssetCheckResult(passed=check_passed, metadata=metadata)


# ---------------------------------------------------------------------------
# fct.Faktura — Source: MySupply statistic_invoice_header, key: FakturaDato
# ---------------------------------------------------------------------------

_FAKTURA_SOURCE_QUERY = """
SELECT
    CONVERT(INT, FORMAT(TRY_CONVERT(DATETIME2, FakturaDato), 'yyyyMM')) AS month_key,
    COUNT(*) AS cnt
FROM dbo.statistic_invoice_header
WHERE FakturaDato IS NOT NULL
  AND TRY_CONVERT(DATETIME2, FakturaDato) IS NOT NULL
GROUP BY FORMAT(TRY_CONVERT(DATETIME2, FakturaDato), 'yyyyMM')
"""


@dg.asset_check(
    name="dq_partition_completeness_faktura",
    asset=_DW_QUALITY_KEY,
    description=(
        "Compare monthly row counts for fct.Faktura across Source (MySupply), DSA, and DSO.\n\n"
        "Groups by FakturaDato (source) / DW_EK_Dato_Faktura (DW).\n\n"
        "**Outputs:**\n\n"
        "- **summary**: totals and unknown-date row counts\n"
        "- **passed**: months where all three tiers match\n"
        "- **failed**: months with mismatches, including diff values"
    ),
    compute_kind="mssql",
)
def dq_partition_completeness_faktura(
    mysupply_mssql: MySupplyMSSQLResource,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.AssetCheckResult:
    """Monthly partition completeness: fct.Faktura (MySupply -> DSA -> DSO)."""
    return _compare_monthly_counts(
        source_resource=mysupply_mssql,
        source_query=_FAKTURA_SOURCE_QUERY,
        staging_resource=mssql_target_staging,
        dso_resource=mssql_target_dso,
        fct_schema="fct",
        fct_table="Faktura",
        date_column="DW_EK_Dato_Faktura",
    )


# ---------------------------------------------------------------------------
# fct.Fakturalinje — Source: MySupply invoice_line joined to header for date
# ---------------------------------------------------------------------------

_FAKTURALINJE_SOURCE_QUERY = """
SELECT
    CONVERT(INT, FORMAT(TRY_CONVERT(DATETIME2, h.FakturaDato), 'yyyyMM')) AS month_key,
    COUNT(*) AS cnt
FROM dbo.statistic_invoice_line l
JOIN dbo.statistic_invoice_header h
    ON l.statistic_invoice_header_id = h.id
WHERE h.FakturaDato IS NOT NULL
  AND TRY_CONVERT(DATETIME2, h.FakturaDato) IS NOT NULL
GROUP BY FORMAT(TRY_CONVERT(DATETIME2, h.FakturaDato), 'yyyyMM')
"""


@dg.asset_check(
    name="dq_partition_completeness_fakturalinje",
    asset=_DW_QUALITY_KEY,
    description=(
        "Compare monthly row counts for fct.Fakturalinje across Source (MySupply), DSA, "
        "and DSO.\n\n"
        "Groups by FakturaDato via invoice header join (source) / DW_EK_Dato_Faktura (DW).\n\n"
        "**Outputs:**\n\n"
        "- **summary**: totals and unknown-date row counts\n"
        "- **passed**: months where all three tiers match\n"
        "- **failed**: months with mismatches, including diff values"
    ),
    compute_kind="mssql",
)
def dq_partition_completeness_fakturalinje(
    mysupply_mssql: MySupplyMSSQLResource,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.AssetCheckResult:
    """Monthly partition completeness: fct.Fakturalinje (MySupply -> DSA -> DSO)."""
    return _compare_monthly_counts(
        source_resource=mysupply_mssql,
        source_query=_FAKTURALINJE_SOURCE_QUERY,
        staging_resource=mssql_target_staging,
        dso_resource=mssql_target_dso,
        fct_schema="fct",
        fct_table="Fakturalinje",
        date_column="DW_EK_Dato_Faktura",
    )


# ---------------------------------------------------------------------------
# fct.Postering — Source: SydAX Finans.Finanspostering, key: Bogfoeringsdato
# ---------------------------------------------------------------------------

_POSTERING_SOURCE_QUERY = """
SELECT
    CONVERT(INT, FORMAT(Bogfoeringsdato, 'yyyyMM')) AS month_key,
    COUNT(*) AS cnt
FROM Finans.Finanspostering
WHERE Bogfoeringsdato IS NOT NULL
  AND Kildesystem = 'SydAX'
GROUP BY FORMAT(Bogfoeringsdato, 'yyyyMM')
"""


@dg.asset_check(
    name="dq_partition_completeness_postering",
    asset=_DW_QUALITY_KEY,
    description=(
        "Compare monthly row counts for fct.Postering across Source (SydAX), DSA, and DSO.\n\n"
        "Groups by Bogfoeringsdato (source) / DW_EK_Dato_Bogfoering (DW).\n\n"
        "**Outputs:**\n\n"
        "- **summary**: totals and unknown-date row counts\n"
        "- **passed**: months where all three tiers match\n"
        "- **failed**: months with mismatches, including diff values"
    ),
    compute_kind="mssql",
)
def dq_partition_completeness_postering(
    sydax_mssql: SydaxMSSQLResource,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.AssetCheckResult:
    """Monthly partition completeness: fct.Postering (SydAX -> DSA -> DSO)."""
    return _compare_monthly_counts(
        source_resource=sydax_mssql,
        source_query=_POSTERING_SOURCE_QUERY,
        staging_resource=mssql_target_staging,
        dso_resource=mssql_target_dso,
        fct_schema="fct",
        fct_table="Postering",
        date_column="DW_EK_Dato_Bogfoering",
    )


# ---------------------------------------------------------------------------
# fct.Vareudlevering — Source: ILS dbo.udlev, key: udldto (business delivery date)
# ---------------------------------------------------------------------------

_UDLEVERING_SOURCE_QUERY = """
SELECT
    CONVERT(INT, FORMAT(TRY_CONVERT(DATETIME2, udldto), 'yyyyMM')) AS month_key,
    COUNT(*) AS cnt
FROM dbo.udlev
WHERE udldto IS NOT NULL
  AND TRY_CONVERT(DATETIME2, udldto) IS NOT NULL
GROUP BY FORMAT(TRY_CONVERT(DATETIME2, udldto), 'yyyyMM')
"""


@dg.asset_check(
    name="dq_partition_completeness_vareudlevering",
    asset=_DW_QUALITY_KEY,
    description=(
        "Compare monthly row counts for fct.Vareudlevering across Source (ILS), DSA, and DSO.\n\n"
        "Groups by udldto delivery date (source) / DW_EK_Dato_Vareudlevering (DW).\n\n"
        "Note: ILS extraction partitions by pro2created/pro2modified, but the fact table\n"
        "stores the business delivery date (udldto). Counts are compared on this business\n"
        "date, not the extraction partition window.\n\n"
        "**Outputs:**\n\n"
        "- **summary**: totals and unknown-date row counts\n"
        "- **passed**: months where all three tiers match\n"
        "- **failed**: months with mismatches, including diff values"
    ),
    compute_kind="mssql",
)
def dq_partition_completeness_vareudlevering(
    ils_mssql: ILSMSSQLResource,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.AssetCheckResult:
    """Monthly partition completeness: fct.Vareudlevering (ILS -> DSA -> DSO)."""
    return _compare_monthly_counts(
        source_resource=ils_mssql,
        source_query=_UDLEVERING_SOURCE_QUERY,
        staging_resource=mssql_target_staging,
        dso_resource=mssql_target_dso,
        fct_schema="fct",
        fct_table="Vareudlevering",
        date_column="DW_EK_Dato_Vareudlevering",
    )


# ---------------------------------------------------------------------------
# fct.Finanskladdelinje — Source: SydAX Finans.Finanskladdelinje, key: dato
# ---------------------------------------------------------------------------

_FINANSKLADDELINJE_SOURCE_QUERY = """
SELECT
    CONVERT(INT, FORMAT(TRY_CONVERT(DATETIME2, dato), 'yyyyMM')) AS month_key,
    COUNT(*) AS cnt
FROM Finans.Finanskladdelinje
WHERE dato IS NOT NULL
  AND TRY_CONVERT(DATETIME2, dato) IS NOT NULL
  AND Kildesystem = 'SydAX'
GROUP BY FORMAT(TRY_CONVERT(DATETIME2, dato), 'yyyyMM')
"""


@dg.asset_check(
    name="dq_partition_completeness_finanskladdelinje",
    asset=_DW_QUALITY_KEY,
    description=(
        "Compare monthly row counts for fct.Finanskladdelinje across Source (SydAX), DSA, "
        "and DSO.\n\n"
        "Groups by dato (source) / DW_EK_Dato_Linje (DW).\n\n"
        "Source is filtered to Kildesystem = 'SydAX' to match the enriched extraction.\n\n"
        "**Outputs:**\n\n"
        "- **summary**: totals and unknown-date row counts\n"
        "- **passed**: months where all three tiers match\n"
        "- **failed**: months with mismatches, including diff values"
    ),
    compute_kind="mssql",
)
def dq_partition_completeness_finanskladdelinje(
    sydax_mssql: SydaxMSSQLResource,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.AssetCheckResult:
    """Monthly partition completeness: fct.Finanskladdelinje (SydAX -> DSA -> DSO)."""
    return _compare_monthly_counts(
        source_resource=sydax_mssql,
        source_query=_FINANSKLADDELINJE_SOURCE_QUERY,
        staging_resource=mssql_target_staging,
        dso_resource=mssql_target_dso,
        fct_schema="fct",
        fct_table="Finanskladdelinje",
        date_column="DW_EK_Dato_Linje",
    )


# ---------------------------------------------------------------------------
# fct.Budget — Source: SydAX Finans.Budgetpost, key: Budgetdato
# ---------------------------------------------------------------------------

_BUDGET_SOURCE_QUERY = """
SELECT
    CONVERT(INT, FORMAT(TRY_CONVERT(DATETIME2, Budgetdato), 'yyyyMM')) AS month_key,
    COUNT(*) AS cnt
FROM Finans.Budgetpost
WHERE Budgetdato IS NOT NULL
  AND TRY_CONVERT(DATETIME2, Budgetdato) IS NOT NULL
  AND Kildesystem = 'SydAX'
GROUP BY FORMAT(TRY_CONVERT(DATETIME2, Budgetdato), 'yyyyMM')
"""


@dg.asset_check(
    name="dq_partition_completeness_budget",
    asset=_DW_QUALITY_KEY,
    description=(
        "Compare monthly row counts for fct.Budget across Source (SydAX), DSA, and DSO.\n\n"
        "Groups by Budgetdato (source) / DW_EK_Dato_Budget (DW).\n\n"
        "Source is filtered to Kildesystem = 'SydAX' to match the enriched extraction.\n\n"
        "**Outputs:**\n\n"
        "- **summary**: totals and unknown-date row counts\n"
        "- **passed**: months where all three tiers match\n"
        "- **failed**: months with mismatches, including diff values"
    ),
    compute_kind="mssql",
)
def dq_partition_completeness_budget(
    sydax_mssql: SydaxMSSQLResource,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.AssetCheckResult:
    """Monthly partition completeness: fct.Budget (SydAX -> DSA -> DSO)."""
    return _compare_monthly_counts(
        source_resource=sydax_mssql,
        source_query=_BUDGET_SOURCE_QUERY,
        staging_resource=mssql_target_staging,
        dso_resource=mssql_target_dso,
        fct_schema="fct",
        fct_table="Budget",
        date_column="DW_EK_Dato_Budget",
    )
