# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Extract assets record their source connection in meta.DFMRunLog and log the run as JSON.

DfmManager is real; the source and target databases are fakes, so the test runs locally and in CI.
The JSON line goes through Dagster's python_logs handlers, which healthpipe-infrastructure points at the
container's stdout for LogScale.
"""

import json
from contextlib import contextmanager
from types import SimpleNamespace

import dagster as dg
import pytest
from core.resources import IFDBOracleResource, ILSMSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets
from DSI.connectors.mssql.models import TableLoadResult

pytestmark = [pytest.mark.unit, pytest.mark.integration]

ROWS = [("1", "a"), ("2", "b"), ("3", "c")]


class FakeSourceDatabase:
    host = "fake-source-host"
    user = "fake-service-account"

    def __init__(self):
        self._rows = list(ROWS)
        self.description = [(name, None, None, None, None, None, None) for name in ("ID", "NAVN")]

    def execute(self, sql, *params):
        pass

    def fetchmany(self, size):
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def close(self):
        pass

    @contextmanager
    def get_connection(self, timeout: int = 0):
        yield SimpleNamespace(cursor=lambda: self)


class FakeTargetDatabase:
    """Answers DfmManager's SQL and records every statement with its parameters."""

    host = "fake-target-host"

    def __init__(self):
        self.statements: list[tuple[str, tuple]] = []
        self.rows_written = 0
        self.rowcount = 0

    def execute(self, sql, *params):
        self.statements.append((sql, params[0] if params else ()))

    def fetchone(self):
        last_sql = self.statements[-1][0]
        if "OUTPUT INSERTED" in last_sql:
            return (7, "2026-10-05 10:00:00.000")
        if "COUNT(*)" in last_sql:
            return (self.rows_written,)
        return None

    def bulkcopy(self, table, data, **kwargs):
        self.rows_written += len(data)
        return {"rows_copied": len(data)}

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        return self

    @contextmanager
    def get_connection(self, timeout: int = 0):
        yield self


class FakeMssqlExtractor:
    def __init__(self, source_conn, target_conn, **kwargs):
        pass

    def stream_extract_and_insert(self, table_config, partition_key):
        return TableLoadResult(
            source_table="dbo.kilde", target_table="ext.ils_kilde", row_count=len(ROWS)
        )


CASES = {
    "mssql": dict(
        config=DfmExtractConfig(
            source_schema="dbo", source_table="kilde", target_table="ils_kilde", is_full_load=True
        ),
        resource_type=ILSMSSQLResource,
    ),
    "oracle": dict(
        config=DfmExtractConfig(
            source_schema="IFDB",
            source_table="KILDE",
            target_table="ifdb_kilde",
            is_full_load=True,
            columns=("ID", "NAVN"),
            column_types={"ID": "NVARCHAR(10) NOT NULL", "NAVN": "NVARCHAR(10) NULL"},
        ),
        resource_type=IFDBOracleResource,
    ),
}


@pytest.mark.parametrize("dialect", list(CASES))
def test_extract_records_source_connection_and_logs_dfm_run(dialect, monkeypatch, tmp_path):
    import DSI.connectors.mssql.extractor as mssql_extractor

    monkeypatch.setattr(mssql_extractor, "MssqlExtractor", FakeMssqlExtractor)
    case = CASES[dialect]
    config = case["config"]
    [asset] = create_dfm_extraction_assets(
        [config],
        source_resource_key="source_db",
        source_resource_type=case["resource_type"],
        source_system="TEST",
        group_name="dfmrunlogtest",
        domain_tag="test",
        dialect=dialect,
    )
    handler_output = tmp_path / "stdout.log"
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
    target = FakeTargetDatabase()

    with dg.instance_for_test(overrides={"python_logs": python_logs}) as instance:
        result = dg.materialize(
            [asset],
            resources={"source_db": FakeSourceDatabase(), "mssql_target_staging": target},
            instance=instance,
        )

    assert result.success
    source_table = f"{config.source_schema}.{config.source_table}"
    [insert_params] = [
        params for sql, params in target.statements if "INSERT INTO [meta].[DFMRunLog]" in sql
    ]
    assert insert_params[-3:] == ("fake-service-account", "fake-source-host", source_table)
    messages = [
        json.loads(line)["dagster_meta"].get("orig_message") or ""
        for line in handler_output.read_text().splitlines()
    ]
    [run] = [json.loads(message) for message in messages if '"event": "dfm_run"' in message]
    assert run["status"] == "success"
    assert run["rows_affected"] == len(ROWS)
    assert run["table_name"] == config.target_table
    assert run["source_user"] == "fake-service-account"
    assert run["source_host"] == "fake-source-host"
    assert run["source_table"] == source_table
    assert run["dagster_run_id"] == result.run_id
