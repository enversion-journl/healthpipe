# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg
from core.resources import MSSQLResource


def _observe_table_metadata(
    context: dg.AssetExecutionContext, mssql_source_dso: MSSQLResource, table_name: str
) -> dg.ObserveResult:
    """Observe table metadata without partitioning - gets total row count and column schema."""
    # Parse schema and table name
    schema_table = table_name.split(".")
    schema_name = schema_table[0] if len(schema_table) == 2 else "dbo"
    table_name = schema_table[1] if len(schema_table) == 2 else schema_table[0]
    with mssql_source_dso.get_connection() as conn:
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

mssql_patient_columna_source = dg.AssetSpec(
    key="mssql_patient_columna_source",
    group_name="mssql_source",
    description="The dbo.PATIENT_OPLYSNINGER_COLUMNA table in MSSQL.",
)

mssql_patient_officielle_source = dg.AssetSpec(
    key="mssql_patient_officielle_source",
    group_name="mssql_source",
    description="The dbo.PATIENT_OPLYSNINGER_OFFICIELLE table in MSSQL.",
)

mssql_episode_of_care_source = dg.AssetSpec(
    key="mssql_episode_of_care_source",
    group_name="mssql_source",
    description="The dbo.EPISODE_OF_CARE table in MSSQL.",
)

mssql_encounter_source = dg.AssetSpec(
    key="mssql_encounter_source",
    group_name="mssql_source",
    description="The dbo.ENCOUNTER table in MSSQL.",
)

mssql_eventmarker_source = dg.AssetSpec(
    key="mssql_eventmarker_source",
    group_name="mssql_source",
    description="The dbo.EVENTMARKER table in MSSQL.",
)

mssql_location_source = dg.AssetSpec(
    key="mssql_location_source",
    group_name="mssql_source",
    description="The dbo.LOCATION table in MSSQL, containing patient stay information.",
)

mssql_healthissue_source = dg.AssetSpec(
    key="mssql_healthissue_source",
    group_name="mssql_source",
    description="The dbo.HEALTHISSUE table in MSSQL, containing patient health problems.",
)
