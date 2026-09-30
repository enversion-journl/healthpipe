# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg
from core.resources import SydaxMSSQLResource


def _observe_table_metadata(
    context: dg.AssetExecutionContext, sydax_mssql: SydaxMSSQLResource, table_name: str
) -> dg.ObserveResult:
    """Observe table metadata without partitioning - gets total row count and column schema."""
    # Parse schema and table name
    schema_table = table_name.split(".")
    schema_name = schema_table[0] if len(schema_table) == 2 else "dbo"
    table_name = schema_table[1] if len(schema_table) == 2 else schema_table[0]

    with sydax_mssql.get_connection() as conn:
        with conn.cursor() as cursor:
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
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
            "table": dg.MetadataValue.text(f"{schema_name}.{table_name}"),
            "column_count": dg.MetadataValue.int(len(columns_info)),
            "columns": dg.MetadataValue.json(columns_info),
        }
    )


# Using AssetSpec to declare external assets, which is the modern approach
# for sources not materialized by Dagster. This provides a clear signal to the planner.

# Finans schema tables
sydax_finanspostering_kreditor_trans_relation_source = dg.AssetSpec(
    key="sydax_finanspostering_kreditor_trans_relation_source",
    group_name="sydax_source",
    description="The Finans.FinansposteringKreditorTransRelation table in SYDAX MSSQL database.",
)

sydax_finanskonto_source = dg.AssetSpec(
    key="sydax_finanskonto_source",
    group_name="sydax_source",
    description="The Finans.Finanskonto table in SYDAX MSSQL database.",
)

sydax_finanspostering_source = dg.AssetSpec(
    key="sydax_finanspostering_source",
    group_name="sydax_source",
    description="The Finans.Finanspostering table in SYDAX MSSQL database.",
)

# Afdeling table for organization hierarchy
sydax_afdeling_source = dg.AssetSpec(
    key="sydax_afdeling_source",
    group_name="sydax_source",
    description="The Finans.Afdeling table in SYDAX MSSQL database - contains organizational hierarchy.",
)

# Kreditor schema tables
sydax_kreditortransaktion_source = dg.AssetSpec(
    key="sydax_kreditortransaktion_source",
    group_name="sydax_source",
    description="The Kreditor.Kreditortransaktion table in SYDAX MSSQL database.",
)

sydax_kreditor_source = dg.AssetSpec(
    key="sydax_kreditor_source",
    group_name="sydax_source",
    description="The Kreditor.Kreditor table in SYDAX MSSQL database.",
)
