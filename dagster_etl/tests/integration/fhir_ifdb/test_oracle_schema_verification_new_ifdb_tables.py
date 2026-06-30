# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Oracle schema verification for newly added IFDB extraction configs.

This test compares configured extraction columns against live Oracle metadata
for the new tables introduced in the IFDB FHIR pipeline expansion:
- AKTIVITETSRESULTAT
- DIAGNOSE
- ADMINISTRATIVE_PROCEDURE
- RIS_YDELSE

The test is read-only and requires real Oracle access.
"""

from __future__ import annotations

import pytest
from DSI.extract.IFDB.administrative_procedure import ADMINISTRATIVE_PROCEDURE_CONFIG
from DSI.extract.IFDB.aktivitetsresultat import AKTIVITETSRESULTAT_CONFIG
from DSI.extract.IFDB.diagnose import DIAGNOSE_CONFIG
from DSI.extract.IFDB.ris_ydelse import RIS_YDELSE_CONFIG


def _split_table_name(qualified_table: str) -> tuple[str, str]:
    """
    Split fully-qualified table name ('SCHEMA.TABLE') into parts.
    """
    if "." not in qualified_table:
        raise ValueError(f"Expected qualified table name 'SCHEMA.TABLE', got: {qualified_table}")
    schema, table = qualified_table.split(".", 1)
    return schema.upper(), table.upper()


def _fetch_table_columns(cursor, owner: str, table: str) -> dict[str, str]:
    """
    Return {COLUMN_NAME: DATA_TYPE} for a table from Oracle metadata.
    """
    # owner/table come from hardcoded config constants in this test module.
    # Keep a simple safety check before embedding.
    if not owner.replace("_", "").isalnum() or not table.replace("_", "").isalnum():
        raise ValueError(f"Unexpected owner/table format: owner={owner}, table={table}")

    cursor.execute(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{owner}'
          AND TABLE_NAME = '{table}'
        ORDER BY COLUMN_ID
        """
    )
    return {row[0].upper(): row[1].upper() for row in cursor.fetchall()}


@pytest.mark.integration
@pytest.mark.real_resources
@pytest.mark.read_only
@pytest.mark.parametrize(
    ("config_name", "config"),
    [
        ("AKTIVITETSRESULTAT", AKTIVITETSRESULTAT_CONFIG),
        ("DIAGNOSE", DIAGNOSE_CONFIG),
        ("ADMINISTRATIVE_PROCEDURE", ADMINISTRATIVE_PROCEDURE_CONFIG),
        ("RIS_YDELSE", RIS_YDELSE_CONFIG),
    ],
)
def test_new_ifdb_extraction_config_matches_oracle_schema(
    config_name, config, real_ifdb_oracle_resource
):
    """
    Verify that every configured extraction column exists in Oracle metadata.

    Also verifies the configured partition column exists.
    """
    owner, table = _split_table_name(config.table)
    configured_columns = [col.upper() for col in config.columns]
    configured_set = set(configured_columns)
    partition_column = config.partition_column.upper()

    with real_ifdb_oracle_resource.get_connection() as conn:
        cursor = conn.cursor()
        oracle_columns = _fetch_table_columns(cursor, owner, table)
        cursor.close()

    assert oracle_columns, (
        f"[{config_name}] No metadata found in Oracle for {owner}.{table}. "
        "Check schema/table name and Oracle permissions."
    )

    oracle_set = set(oracle_columns.keys())

    missing_in_oracle = sorted(configured_set - oracle_set)
    assert not missing_in_oracle, (
        f"[{config_name}] Config columns missing in Oracle ({owner}.{table}): "
        f"{missing_in_oracle}"
    )

    assert partition_column in oracle_set, (
        f"[{config_name}] Partition column '{partition_column}' not found in Oracle "
        f"for {owner}.{table}"
    )


@pytest.mark.integration
@pytest.mark.real_resources
@pytest.mark.read_only
@pytest.mark.parametrize(
    ("config_name", "config"),
    [
        ("AKTIVITETSRESULTAT", AKTIVITETSRESULTAT_CONFIG),
        ("DIAGNOSE", DIAGNOSE_CONFIG),
        ("ADMINISTRATIVE_PROCEDURE", ADMINISTRATIVE_PROCEDURE_CONFIG),
        ("RIS_YDELSE", RIS_YDELSE_CONFIG),
    ],
)
def test_partition_column_has_supported_type(config_name, config, real_ifdb_oracle_resource):
    """
    Verify partition column datatype is suitable for partition filtering.
    """
    owner, table = _split_table_name(config.table)
    partition_column = config.partition_column.upper()
    supported_types = {"DATE", "TIMESTAMP", "TIMESTAMP(6)", "NUMBER", "NUMBER(19)"}

    with real_ifdb_oracle_resource.get_connection() as conn:
        cursor = conn.cursor()
        oracle_columns = _fetch_table_columns(cursor, owner, table)
        cursor.close()

    assert (
        partition_column in oracle_columns
    ), f"[{config_name}] Partition column '{partition_column}' missing in {owner}.{table}"

    datatype = oracle_columns[partition_column]
    # Accept broad categories from ALL_TAB_COLUMNS.DATA_TYPE
    assert any(datatype.startswith(t.split("(")[0]) for t in supported_types), (
        f"[{config_name}] Partition column '{partition_column}' has unexpected datatype "
        f"'{datatype}' in {owner}.{table}. Expected date/timestamp/number-like type."
    )
