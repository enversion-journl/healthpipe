# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg
from core.resources import MySupplyMSSQLResource


def _observe_table_metadata(
    context: dg.AssetExecutionContext,
    mysupply_mssql: MySupplyMSSQLResource,
    table_name: str,
) -> dg.ObserveResult:
    """Observe table metadata without partitioning - gets total row count and column schema."""
    # Parse schema and table name
    schema_table = table_name.split(".")
    schema_name = schema_table[0] if len(schema_table) == 2 else "dbo"
    table_name = schema_table[1] if len(schema_table) == 2 else schema_table[0]

    with mysupply_mssql.get_connection() as conn:
        with conn.cursor() as cursor:
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {table_name};"
            cursor.execute(count_query)
            (count,) = cursor.fetchone()

            # Get column information using string formatting (to avoid parameter binding issues)
            columns_query = f"""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(columns_query)
            columns_info = []

            for row in cursor.fetchall():
                col_name, data_type = row
                columns_info.append({"name": col_name, "type": data_type})

    return dg.ObserveResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "table": dg.MetadataValue.text(table_name),
            "column_count": dg.MetadataValue.int(len(columns_info)),
            "columns": dg.MetadataValue.json(columns_info),
        }
    )


# Using AssetSpec to declare external assets, which is the modern approach
# for sources not materialized by Dagster. This provides a clear signal to the planner.

mysupply_statistic_invoice_header_source = dg.AssetSpec(
    key="mysupply_statistic_invoice_header_source",
    group_name="mysupply_source",
    description="The dbo.statistic_invoice_header table in mySupply MSSQL database.",
)

mysupply_statistic_invoice_line_source = dg.AssetSpec(
    key="mysupply_statistic_invoice_line_source",
    group_name="mysupply_source",
    description="The dbo.statistic_invoice_line table in mySupply MSSQL database.",
)

mysupply_metadata_source = dg.AssetSpec(
    key="mysupply_metadata_source",
    group_name="mysupply_source",
    description="The dbo.metadata table in mySupply MSSQL database.",
)
