# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""An extract run records its source connection in meta.DFMRunLog and logs the run as JSON.

Runs against the devcontainer mssql with the real DDL and migration. healthpipe-infrastructure points the
same python_logs handler at the container's stdout, which LogScale collects.
Requires USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true.
"""

import json
import os

import dagster as dg
import pytest
from core.resources import MSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets
from infrastructure.utils.ddl_loader import execute_sql, load_sql_file

pytestmark = [pytest.mark.integration, pytest.mark.real_resources]

SOURCE_SCHEMA = "dfmrunlogtest"
SOURCE_TABLE = "kilde"
TARGET_TABLE = "dfmrunlogtest_kilde"
SOURCE_ROWS = 3
DDL = (
    "tables/meta/02_dfm_state.sql",
    "tables/meta/03_dfm_run_log.sql",
    "migrations/20261005_01_dfm_run_log_source_columns.sql",
)


@pytest.fixture
def staging(real_mssql_target_staging_resource):
    resource = real_mssql_target_staging_resource
    if os.getenv("ENABLE_TEST_WRITES", "false").lower() != "true":
        pytest.skip("requires ENABLE_TEST_WRITES=true")
    if resource.host != "mssql" or resource.database != "staging":
        pytest.fail("This destructive fixture requires local mssql/staging")

    for ddl in DDL:
        execute_sql(resource, load_sql_file(ddl))

    def cleanup():
        with resource.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS [ext].[{TARGET_TABLE}]")
            cursor.execute(f"DROP TABLE IF EXISTS [{SOURCE_SCHEMA}].[{SOURCE_TABLE}]")
            cursor.execute(f"DROP SCHEMA IF EXISTS [{SOURCE_SCHEMA}]")
            cursor.execute("DELETE FROM [meta].[DFMRunLog] WHERE [table_name] = ?", (TARGET_TABLE,))
            conn.commit()
            cursor.close()

    cleanup()
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA [{SOURCE_SCHEMA}]")
        cursor.execute(
            f"CREATE TABLE [{SOURCE_SCHEMA}].[{SOURCE_TABLE}] "
            "([id] INT NOT NULL, [navn] NVARCHAR(50) NOT NULL)"
        )
        cursor.executemany(
            f"INSERT INTO [{SOURCE_SCHEMA}].[{SOURCE_TABLE}] ([id], [navn]) VALUES (?, ?)",
            [(i, f"række {i}") for i in range(1, SOURCE_ROWS + 1)],
        )
        conn.commit()
        cursor.close()
    yield resource
    cleanup()


def test_extract_records_source_connection_in_dfm_run_log(staging, tmp_path):
    handler_output = tmp_path / "stdout.log"
    [asset] = create_dfm_extraction_assets(
        [
            DfmExtractConfig(
                source_schema=SOURCE_SCHEMA,
                source_table=SOURCE_TABLE,
                target_table=TARGET_TABLE,
                is_full_load=True,
                description="DFMRunLog e2e test table.",
            )
        ],
        source_resource_key="dfmrunlogtest_mssql",
        source_resource_type=MSSQLResource,
        source_system="DFMRUNLOGTEST",
        group_name="dfmrunlogtest",
        domain_tag="test",
    )
    source = MSSQLResource(
        host=staging.host,
        port=staging.port,
        database=staging.database,
        user=staging.user,
        password=staging.password,
    )
    python_logs = {
        "dagster_handler_config": {
            "handlers": {
                "containerStdout": {
                    "class": "logging.FileHandler",
                    "filename": str(handler_output),
                    "mode": "a",
                    "level": "DEBUG",
                    "formatter": "json",
                }
            },
            "formatters": {"json": {"()": "dagster._loggers.JsonLogFormatter"}},
        }
    }

    with dg.instance_for_test(overrides={"python_logs": python_logs}) as instance:
        result = dg.materialize(
            [asset],
            resources={"dfmrunlogtest_mssql": source, "mssql_target_staging": staging},
            instance=instance,
        )

    assert result.success
    with staging.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT [status], [rows_affected], [source_user], [source_host], [source_table] "
            "FROM [meta].[DFMRunLog] WHERE [table_name] = ? AND [dagster_run_id] = ?",
            (TARGET_TABLE, result.run_id),
        )
        row = tuple(cursor.fetchone())
        cursor.close()
    source_table = f"{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    assert row == ("success", SOURCE_ROWS, staging.user, staging.host, source_table)
    messages = [
        json.loads(line)["dagster_meta"].get("orig_message") or ""
        for line in handler_output.read_text().splitlines()
    ]
    [run] = [json.loads(message) for message in messages if '"event": "dfm_run"' in message]
    assert run["status"] == "success"
    assert run["rows_affected"] == SOURCE_ROWS
    assert (run["source_user"], run["source_host"], run["source_table"]) == row[2:]
