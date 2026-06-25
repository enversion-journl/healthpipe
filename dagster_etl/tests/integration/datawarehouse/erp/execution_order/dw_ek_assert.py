# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Assertion helpers and DW_EK coverage map for execution-order tests.

Declarative coverage of every DW_EK_* column in every MySupply / SydAx-fed
table.  Each FK column is annotated with the map/dim it references so that
``assert_keys_resolved`` can validate join integrity, not just != -1.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

TEST_NAMESPACE_PREFIX = "TEST-EXEC-"
TEST_CVR_PREFIX = "999"
TEST_LEDGERTRANS_MIN = 9_900_000_000


@dataclass(frozen=True)
class FKColumn:
    """One foreign-key column on a target table."""

    column: str
    ref_schema: str
    ref_table: str
    ref_pk: str
    is_date_key: bool = False
    always_minus_one: bool = False


@dataclass(frozen=True)
class TableCoverage:
    schema: str
    table: str
    pk_column: str
    fk_columns: tuple[FKColumn, ...]
    namespace_column: str | None = None


COVERAGE: tuple[TableCoverage, ...] = (
    # ── Facts ─────────────────────────────────────────────────────────
    TableCoverage(
        schema="fct",
        table="Faktura",
        pk_column="DW_EK_Faktura",
        namespace_column="FakturaNr",
        fk_columns=(
            FKColumn("DW_EK_Faktura", "map", "Faktura", "DW_EK_Faktura"),
            FKColumn("DW_EK_Kreditor", "map", "Kreditor", "DW_EK_Kreditor"),
            FKColumn("DW_EK_EAN_Regningsmodtager", "map", "EAN", "DW_EK_EAN"),
            # DW_EK_Postering / DW_EK_Kontoplan removed (finding 2.2): no fact-to-fact
            # FK on fct.Faktura; the account/posting link is exposed via vw_fct.Faktura.
            FKColumn("DW_EK_Dato_Faktura", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Ordre", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Betaling", "", "", "", is_date_key=True),
        ),
    ),
    TableCoverage(
        schema="fct",
        table="Fakturalinje",
        pk_column="DW_EK_Fakturalinje",
        namespace_column="FakturaNr",
        fk_columns=(
            FKColumn("DW_EK_Kreditor", "map", "Kreditor", "DW_EK_Kreditor"),
            FKColumn("DW_EK_Vare", "map", "Vare", "DW_EK_Vare"),
            FKColumn("DW_EK_Faktura", "map", "Faktura", "DW_EK_Faktura"),
            # DW_EK_Kontoplan removed (finding 2.2): no fact-to-fact FK on
            # fct.Fakturalinje; exposed via vw_fct.Fakturalinje.DW_EK_Kontoplan_Postering.
            FKColumn("DW_EK_EAN_Regningsmodtager", "map", "EAN", "DW_EK_EAN"),
            FKColumn("DW_EK_Dato_Faktura", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Ordre", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Betaling", "", "", "", is_date_key=True),
            FKColumn(
                "DW_EK_Dato_Modtagelse",
                "",
                "",
                "",
                is_date_key=True,
                always_minus_one=True,
            ),
        ),
    ),
    TableCoverage(
        schema="fct",
        table="Postering",
        pk_column="ref_LedgertransId",
        namespace_column="FakturaNr",
        fk_columns=(
            FKColumn("DW_EK_Postering", "map", "Postering", "DW_EK_Postering"),
            FKColumn("DW_EK_Kreditor", "map", "Kreditor", "DW_EK_Kreditor"),
            FKColumn("DW_EK_Kontoplan", "map", "Kontoplan", "DW_EK_Kontoplan"),
            FKColumn("DW_EK_Faktura", "map", "Faktura", "DW_EK_Faktura"),
            FKColumn("DW_EK_Okosted", "map", "Okosted", "DW_EK_Okosted"),
            FKColumn(
                "DW_EK_Finanskladdelinje",
                "map",
                "Finanskladdelinje",
                "DW_EK_Finanskladdelinje",
            ),
            FKColumn("DW_EK_Dato_Godkendelse", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Lukket", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_TransOprettet", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Forfald", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_JournalFaktura", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_SidstUdlignet", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Transaktion", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Bogfoering", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_PostOprettet", "", "", "", is_date_key=True),
        ),
    ),
    TableCoverage(
        schema="fct",
        table="Budget",
        pk_column="DW_EK_Budget",
        namespace_column="bk_ref_LedgerbudgetId",
        fk_columns=(
            FKColumn("DW_EK_Budget", "map", "Budget", "DW_EK_Budget"),
            FKColumn(
                "DW_EK_BudgetEgenskaber",
                "map",
                "BudgetEgenskaber",
                "DW_EK_BudgetEgenskaber",
            ),
            FKColumn("DW_EK_Kontoplan", "map", "Kontoplan", "DW_EK_Kontoplan"),
            FKColumn("DW_EK_Okosted", "map", "Okosted", "DW_EK_Okosted"),
            FKColumn("DW_EK_Dato_Budget", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_BudgetAdopted", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_BudgetOprettet", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_BudgetAendret", "", "", "", is_date_key=True),
        ),
    ),
    TableCoverage(
        schema="fct",
        table="Finanskladdelinje",
        pk_column="DW_EK_Finanskladdelinje",
        namespace_column="bk_ref_LedgertransId",
        fk_columns=(
            FKColumn(
                "DW_EK_Finanskladdelinje",
                "map",
                "Finanskladdelinje",
                "DW_EK_Finanskladdelinje",
            ),
            FKColumn(
                "DW_EK_Finanskladde",
                "map",
                "Finanskladde",
                "DW_EK_Finanskladde",
            ),
            FKColumn("DW_EK_Kontoplan", "map", "Kontoplan", "DW_EK_Kontoplan"),
            FKColumn("DW_EK_Okosted", "map", "Okosted", "DW_EK_Okosted"),
            FKColumn("DW_EK_Dato_Linje", "", "", "", is_date_key=True),
            FKColumn("DW_EK_Dato_Oprettet", "", "", "", is_date_key=True),
        ),
    ),
    # ── Dimensions ────────────────────────────────────────────────────
    TableCoverage(
        schema="dim",
        table="Faktura",
        pk_column="DW_EK_Faktura",
        namespace_column=None,
        fk_columns=(FKColumn("DW_EK_Faktura", "map", "Faktura", "DW_EK_Faktura"),),
    ),
    TableCoverage(
        schema="dim",
        table="Fakturalinje",
        pk_column="DW_EK_Fakturalinje",
        namespace_column=None,
        fk_columns=(
            FKColumn(
                "DW_EK_Fakturalinje",
                "map",
                "Fakturalinje",
                "DW_EK_Fakturalinje",
            ),
        ),
    ),
    TableCoverage(
        schema="dim",
        table="Postering",
        pk_column="DW_EK_Postering",
        namespace_column=None,
        fk_columns=(FKColumn("DW_EK_Postering", "map", "Postering", "DW_EK_Postering"),),
    ),
    TableCoverage(
        schema="dim",
        table="Kreditor",
        pk_column="DW_EK_Kreditor",
        namespace_column=None,
        fk_columns=(FKColumn("DW_EK_Kreditor", "map", "Kreditor", "DW_EK_Kreditor"),),
    ),
    TableCoverage(
        schema="dim",
        table="Kontoplan",
        pk_column="DW_EK_Kontoplan",
        namespace_column=None,
        fk_columns=(FKColumn("DW_EK_Kontoplan", "map", "Kontoplan", "DW_EK_Kontoplan"),),
    ),
    TableCoverage(
        schema="dim",
        table="Finanskladde",
        pk_column="DW_EK_Finanskladde",
        namespace_column=None,
        fk_columns=(
            FKColumn(
                "DW_EK_Finanskladde",
                "map",
                "Finanskladde",
                "DW_EK_Finanskladde",
            ),
        ),
    ),
    TableCoverage(
        schema="dim",
        table="BudgetEgenskaber",
        pk_column="DW_EK_BudgetEgenskaber",
        namespace_column=None,
        fk_columns=(
            FKColumn(
                "DW_EK_BudgetEgenskaber",
                "map",
                "BudgetEgenskaber",
                "DW_EK_BudgetEgenskaber",
            ),
        ),
    ),
)

# ── Comprehensive DW table lists for cleanup ─────────────────────────

EXT_TABLES = (
    ("ext", "statistic_invoice_header_mysupply"),
    ("ext", "metadata_mysupply"),
    ("ext", "statistic_invoice_line_mysupply"),
    ("ext", "statistic_invoice_header_allowance_charge_mysupply"),
    ("ext", "statistic_invoice_line_allowance_charge_mysupply"),
    ("ext", "finanspostering_enriched"),
    ("ext", "finanskladdelinje_enriched"),
    ("ext", "budgetpost_enriched"),
    ("ext", "finanskladde_sydax"),
    ("ext", "finanskladdelinje_sydax"),
    ("ext", "kreditor_sydax"),
    ("ext", "finanskonto_sydax"),
)

STG_TABLES = (
    ("stg", "fct_faktura_mysupply"),
    ("stg", "fct_fakturalinje_mysupply"),
    ("stg", "fct_postering_sydax"),
    ("stg", "fct_budget_sydax"),
    ("stg", "fct_finanskladdelinje_sydax"),
    ("stg", "dim_faktura_mysupply"),
    ("stg", "dim_fakturalinje_mysupply"),
    ("stg", "dim_kreditor_mysupply"),
    ("stg", "dim_kreditor_sydax"),
    ("stg", "dim_kreditor_ils"),
    ("stg", "dim_kontoplan_sydax"),
    ("stg", "dim_postering_sydax"),
    ("stg", "dim_finanskladde_sydax"),
    ("stg", "dim_BudgetEgenskaber_sydax"),
    ("stg", "dim_ean"),
    ("stg", "dim_okosted"),
)

MAP_TABLES = (
    ("map", "Faktura"),
    ("map", "Fakturalinje"),
    ("map", "Postering"),
    ("map", "Kreditor"),
    ("map", "Kontoplan"),
    ("map", "Vare"),
    ("map", "EAN"),
    ("map", "Okosted"),
    ("map", "Finanskladde"),
    ("map", "Finanskladdelinje"),
    ("map", "BudgetEgenskaber"),
    ("map", "Budget"),
)

DIM_TABLES = (
    ("dim", "Faktura"),
    ("dim", "Fakturalinje"),
    ("dim", "Postering"),
    ("dim", "Kreditor"),
    ("dim", "Kontoplan"),
    ("dim", "Finanskladde"),
    ("dim", "BudgetEgenskaber"),
)

FCT_TABLES = (
    ("fct", "Fakturalinje"),
    ("fct", "Faktura"),
    ("fct", "Postering"),
    ("fct", "Budget"),
    ("fct", "Finanskladdelinje"),
)

REF_TABLES = (("ref", "Faktura_To_Postering"),)

ALL_DW_TABLES = FCT_TABLES + REF_TABLES + DIM_TABLES + MAP_TABLES + STG_TABLES + EXT_TABLES


# ── Cleanup ───────────────────────────────────────────────────────────


def truncate_all_dw_tables(conn) -> None:
    """Truncate every DW table used by the ERP pipeline, in FK-safe order."""
    cursor = conn.cursor()
    try:
        for schema, table in ALL_DW_TABLES:
            if schema == "dim":
                pk = f"DW_EK_{table}"
                cursor.execute(
                    f"""
                    IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NOT NULL
                        DELETE FROM [{schema}].[{table}] WHERE [{pk}] > 0
                """
                )
            else:
                cursor.execute(
                    f"""
                    IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NOT NULL
                    BEGIN
                        BEGIN TRY TRUNCATE TABLE [{schema}].[{table}] END TRY
                        BEGIN CATCH DELETE FROM [{schema}].[{table}] END CATCH
                    END
                """
                )
        conn.commit()
    finally:
        cursor.close()


# ── Snapshot ──────────────────────────────────────────────────────────


@dataclass
class DWSnapshot:
    """Frozen DW_EK_* state across all in-scope tables for one namespace."""

    rows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def table_rows(self, schema: str, table: str) -> list[dict[str, Any]]:
        return self.rows.get(f"{schema}.{table}", [])


def _table_namespace_filter(tc: TableCoverage) -> str | None:
    """Canonical test-namespace WHERE fragment for a covered table.

    Each table filters on its OWN namespace column — a caller-supplied
    FakturaNr filter must never be applied to e.g. fct.Budget, which has no
    such column.
    """
    if tc.namespace_column is None:
        return None
    filters = {
        "FakturaNr": f"[FakturaNr] LIKE '{TEST_NAMESPACE_PREFIX}%'",
        "bk_ref_LedgerbudgetId": (
            f"TRY_CAST([bk_ref_LedgerbudgetId] AS BIGINT) >= {TEST_LEDGERTRANS_MIN}"
        ),
        "bk_ref_LedgertransId": (
            f"TRY_CAST([bk_ref_LedgertransId] AS BIGINT) >= {TEST_LEDGERTRANS_MIN}"
        ),
    }
    return filters[tc.namespace_column]


def snapshot(conn, namespace_filter_sql: str | None = None) -> DWSnapshot:
    """Dump every DW_EK_* column for the test namespace across all tables.

    ``namespace_filter_sql`` acts as an opt-in switch: when truthy, each
    table is filtered with its own canonical namespace filter.
    """
    snap = DWSnapshot()
    cursor = conn.cursor()
    try:
        for tc in COVERAGE:
            fq = f"[{tc.schema}].[{tc.table}]"
            cols = [tc.pk_column] + [fk.column for fk in tc.fk_columns]
            if tc.namespace_column:
                cols.append(tc.namespace_column)
            col_list = ", ".join(f"[{c}]" for c in cols)

            table_filter = _table_namespace_filter(tc)
            sql = f"SELECT {col_list} FROM {fq}"
            if namespace_filter_sql and table_filter:
                sql += f" WHERE {table_filter}"
            elif tc.schema == "dim":
                pk = f"DW_EK_{tc.table}"
                sql += f" WHERE [{pk}] > 0"

            try:
                cursor.execute(sql)
                desc = [d[0] for d in cursor.description]
                rows = [dict(zip(desc, row, strict=False)) for row in cursor.fetchall()]
                snap.rows[f"{tc.schema}.{tc.table}"] = rows
            except Exception:
                logger.warning("Could not snapshot %s — table may not exist", fq)
                snap.rows[f"{tc.schema}.{tc.table}"] = []
        return snap
    finally:
        cursor.close()


# ── Assertions ────────────────────────────────────────────────────────


def assert_keys_resolved(
    conn,
    schema: str,
    table: str,
    namespace_filter_sql: str | None = None,
    expect_unresolved: set[str] | None = None,
) -> dict[str, int]:
    """Assert every DW_EK FK column joins to an existing row and != -1.

    Returns a dict ``{column: minus_one_count}`` for diagnostic use.
    ``expect_unresolved`` names columns that are allowed to be -1.
    """
    expect_unresolved = expect_unresolved or set()

    tc = _find_coverage(schema, table)
    table_filter = _table_namespace_filter(tc)
    if table_filter is None and tc.schema == "dim":
        # Exclude the conventional DW_EK = -1 UNKNOWN member row.
        table_filter = f"[DW_EK_{tc.table}] > 0"
        namespace_filter_sql = namespace_filter_sql or "dim-unknown-member"

    cursor = conn.cursor()
    results: dict[str, int] = {}
    try:
        for fk in tc.fk_columns:
            if fk.always_minus_one or fk.is_date_key:
                continue

            fq = f"[{tc.schema}].[{tc.table}]"
            where = f"WHERE {table_filter}" if namespace_filter_sql and table_filter else ""

            cursor.execute(f"SELECT COUNT(*) FROM {fq} {where}")
            total = cursor.fetchone()[0]
            if total == 0:
                results[fk.column] = 0
                continue

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {fq} t
                {where}
                {"AND" if where else "WHERE"} t.[{fk.column}] = -1
            """
            )
            minus_one = cursor.fetchone()[0]
            results[fk.column] = minus_one

            if fk.column in expect_unresolved:
                continue

            if minus_one > 0:
                raise AssertionError(
                    f"{fq}.{fk.column}: {minus_one}/{total} rows have -1 "
                    f"(unresolved FK to [{fk.ref_schema}].[{fk.ref_table}])"
                )

            if fk.ref_schema and fk.ref_table:
                # Qualify filter columns with the t alias — the joined map
                # table may carry a column of the same name.
                where_t = (
                    where.replace("[DW_EK_", "t.[DW_EK_")
                    .replace("[FakturaNr]", "t.[FakturaNr]")
                    .replace("[bk_ref_", "t.[bk_ref_")
                )
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {fq} t
                    LEFT JOIN [{fk.ref_schema}].[{fk.ref_table}] r
                        ON r.[{fk.ref_pk}] = t.[{fk.column}]
                    {where_t}
                    {"AND" if where_t else "WHERE"} r.[{fk.ref_pk}] IS NULL
                      AND t.[{fk.column}] <> -1
                """
                )
                dangling = cursor.fetchone()[0]
                if dangling > 0:
                    raise AssertionError(
                        f"{fq}.{fk.column}: {dangling}/{total} rows have dangling FK "
                        f"(value exists but no matching row in "
                        f"[{fk.ref_schema}].[{fk.ref_table}])"
                    )

        return results
    finally:
        cursor.close()


def assert_date_keys_not_minus_one(
    conn,
    schema: str,
    table: str,
    namespace_filter_sql: str | None = None,
    expect_minus_one: set[str] | None = None,
) -> None:
    """Assert date-key columns are populated (!= -1) for seeded rows."""
    expect_minus_one = expect_minus_one or set()
    tc = _find_coverage(schema, table)
    table_filter = _table_namespace_filter(tc)

    cursor = conn.cursor()
    try:
        for fk in tc.fk_columns:
            if not fk.is_date_key or fk.always_minus_one:
                continue
            if fk.column in expect_minus_one:
                continue

            fq = f"[{tc.schema}].[{tc.table}]"
            where = f"WHERE {table_filter}" if namespace_filter_sql and table_filter else ""

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {fq}
                {where}
                {"AND" if where else "WHERE"} [{fk.column}] = -1
            """
            )
            minus_one = cursor.fetchone()[0]
            if minus_one > 0:
                raise AssertionError(f"{fq}.{fk.column}: {minus_one} rows have date key = -1")
    finally:
        cursor.close()


def assert_key_stable(
    before: DWSnapshot,
    after: DWSnapshot,
    schema: str,
    table: str,
    business_key: str,
) -> None:
    """Assert DW_EK identity unchanged between two snapshots.

    Compares rows matched by *business_key* column value; raises if any
    DW_EK_* column changed value.
    """
    tc = _find_coverage(schema, table)
    key = f"{schema}.{table}"

    before_idx: dict[Any, dict[str, Any]] = {
        r[business_key]: r for r in before.table_rows(schema, table)
    }
    after_idx: dict[Any, dict[str, Any]] = {
        r[business_key]: r for r in after.table_rows(schema, table)
    }

    ek_cols = [tc.pk_column] + [
        fk.column for fk in tc.fk_columns if not fk.is_date_key and not fk.always_minus_one
    ]

    for bk_val, before_row in before_idx.items():
        if bk_val not in after_idx:
            continue
        after_row = after_idx[bk_val]
        for col in ek_cols:
            if col not in before_row or col not in after_row:
                continue
            if before_row[col] != after_row[col]:
                raise AssertionError(
                    f"{key} row {business_key}={bk_val}: "
                    f"{col} changed from {before_row[col]} to {after_row[col]}"
                )


def assert_row_count(conn, schema: str, table: str, expected: int) -> None:
    cursor = conn.cursor()
    try:
        fq = f"[{schema}].[{table}]"
        where = ""
        if schema == "dim":
            pk = f"DW_EK_{table}"
            where = f" WHERE [{pk}] > 0"
        cursor.execute(f"SELECT COUNT(*) FROM {fq}{where}")
        actual = cursor.fetchone()[0]
        assert actual == expected, f"{fq}: expected {expected} rows, got {actual}"
    finally:
        cursor.close()


def get_row_count(conn, schema: str, table: str) -> int:
    cursor = conn.cursor()
    try:
        fq = f"[{schema}].[{table}]"
        where = ""
        if schema == "dim":
            pk = f"DW_EK_{table}"
            where = f" WHERE [{pk}] > 0"
        cursor.execute(f"SELECT COUNT(*) FROM {fq}{where}")
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def assert_full_coverage_pass(
    conn,
    namespace_filter_sql: str | None = None,
    expect_unresolved: dict[str, set[str]] | None = None,
    skip_empty_tables: bool = True,
) -> dict[str, dict[str, int]]:
    """Run assert_keys_resolved for every table in COVERAGE.

    ``expect_unresolved`` maps ``"schema.table"`` -> set of FK column names
    that are allowed to be -1.

    Returns ``{table: {column: minus_one_count}}``.
    """
    expect_unresolved = expect_unresolved or {}
    all_results: dict[str, dict[str, int]] = {}
    errors: list[str] = []

    for tc in COVERAGE:
        key = f"{tc.schema}.{tc.table}"
        expected_unresolved = expect_unresolved.get(key, set())

        if skip_empty_tables:
            count = get_row_count(conn, tc.schema, tc.table)
            if count == 0:
                logger.info("Skipping empty table %s", key)
                continue

        try:
            result = assert_keys_resolved(
                conn,
                tc.schema,
                tc.table,
                namespace_filter_sql=namespace_filter_sql,
                expect_unresolved=expected_unresolved,
            )
            all_results[key] = result
        except AssertionError as e:
            errors.append(str(e))

    if errors:
        raise AssertionError(
            f"Coverage pass failed with {len(errors)} error(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    return all_results


# ── Helpers ───────────────────────────────────────────────────────────


def _find_coverage(schema: str, table: str) -> TableCoverage:
    for tc in COVERAGE:
        if tc.schema == schema and tc.table == table:
            return tc
    raise ValueError(f"No coverage entry for [{schema}].[{table}]")
