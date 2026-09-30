# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg
from core.resources import ILSMSSQLResource


def _observe_table_metadata(
    context: dg.AssetExecutionContext, ils_mssql: ILSMSSQLResource, table_name: str
) -> dg.ObserveResult:
    """Observe table metadata without partitioning - gets total row count and column schema."""
    # Parse schema and table name
    schema_table = table_name.split(".")
    schema_name = schema_table[0] if len(schema_table) == 2 else "dbo"
    table_name = schema_table[1] if len(schema_table) == 2 else schema_table[0]

    with ils_mssql.get_connection() as conn:
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

# ILS dbo schema tables
ils_vare_source = dg.AssetSpec(
    key="ils_vare_source",
    group_name="ils_source",
    description="The dbo.vare table in ILS MSSQL database.",
)

ils_vareart_source = dg.AssetSpec(
    key="ils_vareart_source",
    group_name="ils_source",
    description="The dbo.vareart table in ILS MSSQL database.",
)

ils_varegrp_source = dg.AssetSpec(
    key="ils_varegrp_source",
    group_name="ils_source",
    description="The dbo.varegrp table in ILS MSSQL database.",
)

ils_varetype_source = dg.AssetSpec(
    key="ils_varetype_source",
    group_name="ils_source",
    description="The dbo.varetype table in ILS MSSQL database.",
)

ils_lev_source = dg.AssetSpec(
    key="ils_lev_source",
    group_name="ils_source",
    description="The dbo.lev table in ILS MSSQL database.",
)

ils_lafth_source = dg.AssetSpec(
    key="ils_lafth_source",
    group_name="ils_source",
    description="The dbo.lafth table in ILS MSSQL database.",
)

ils_laftd_source = dg.AssetSpec(
    key="ils_laftd_source",
    group_name="ils_source",
    description="The dbo.laftd table in ILS MSSQL database.",
)

ils_laftdpris_source = dg.AssetSpec(
    key="ils_laftdpris_source",
    group_name="ils_source",
    description="The dbo.laftdpris table in ILS MSSQL database.",
)

ils_laftdstam_source = dg.AssetSpec(
    key="ils_laftdstam_source",
    group_name="ils_source",
    description="The dbo.laftdstam table in ILS MSSQL database.",
)

ils_udlev_source = dg.AssetSpec(
    key="ils_udlev_source",
    group_name="ils_source",
    description="The dbo.udlev table in ILS MSSQL database.",
)
