# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the Oracle-DFM ext write path (bulkcopy + arraysize).

AB#29470: `_execute_dfm_oracle_extract` writes to ext.* via mssql-python's
bulk-copy protocol; the legacy executemany loop is reachable only via the
ext_insert_method=executemany run tag. Covers:

  1. Happy path — rows flow through cursor.bulkcopy() with no audit columns,
     one persistent bulk-copy connection, arraysize/prefetchrows/LOB-handler set.
  2. Failure semantics — any bulk-copy error fails the asset (no silent
     executemany degradation); timeouts are owned by bulkcopy_insert_with_retry.
  3. Run-tag escape hatch — ext_insert_method=executemany is the only route to
     the legacy path.
  4. bulkcopy_insert extra_columns parameter (audit columns default, opt-out).
"""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from DSI.connectors.extract_config import DfmExtractConfig

pytestmark = pytest.mark.unit


COLUMNS = ("OBJEKT_ID", "XML")

EXTRACT_CONFIG = DfmExtractConfig(
    source_schema="IFDB",
    source_table="TESTTABLE",
    target_table="ifdb_testtable",
    delta_column="OPDATERET_TID",
    checkpoint_type="datetime",
    checkpoint_from=("arc", "ifdb_testtable"),
    columns=COLUMNS,
    column_types={
        "OBJEKT_ID": "NVARCHAR(50) NOT NULL",
        "XML": "NVARCHAR(MAX) NULL",
    },
    description="Test table.",
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeLog:
    def __init__(self):
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def info(self, msg):
        self.infos.append(str(msg))

    def warning(self, msg):
        self.warnings.append(str(msg))

    def error(self, msg):
        self.errors.append(str(msg))


class FakeContext:
    def __init__(self, tags: dict[str, str] | None = None):
        self.log = FakeLog()
        self.run_id = "test-run-id"
        self._tags = tags or {}
        self.run = SimpleNamespace(tags=self._tags)

    def has_tag(self, key: str) -> bool:
        return key in self._tags


class FakeDfmManager:
    """Stands in for core.dfm_manager.DfmManager (monkeypatched in)."""

    instances: list["FakeDfmManager"] = []

    def __init__(self, resource, context):
        self.completed: list[tuple] = []
        self.failed: list[tuple] = []
        FakeDfmManager.instances.append(self)

    @contextmanager
    def connection(self):
        yield

    def start_dfm_run(self, layer, table, dagster_run_id, mode, checkpoint_from):
        return SimpleNamespace(checkpoint=None, log_id=42)

    def get_checkpoint(self, schema, table):
        return None

    def complete_dfm_run(self, log_id, rows_affected):
        self.completed.append((log_id, rows_affected))

    def fail_dfm_run(self, log_id, error_message):
        self.failed.append((log_id, error_message))


class FakeOracleCursor:
    def __init__(self, batches: list[list[tuple]]):
        self._batches = list(batches)
        self.description = [(c, None, None, None, None, None, None) for c in COLUMNS]
        self.executed: list[str] = []
        self.arraysize = 100  # driver default
        self.prefetchrows = 2  # driver default

    def execute(self, query):
        self.executed.append(query)

    def fetchmany(self, n):
        if self._batches:
            return self._batches.pop(0)
        return []

    def close(self):
        pass


class FakeOracleResource:
    def __init__(self, batches: list[list[tuple]]):
        self.cursor = FakeOracleCursor(batches)

    @contextmanager
    def get_connection(self):
        yield SimpleNamespace(cursor=lambda: self.cursor)


class FakeMssqlServer:
    """Shared state across all fake MSSQL connections/cursors."""

    def __init__(self):
        self.executed_sql: list[str] = []
        self.executemany_calls: list[tuple[str, list]] = []
        self.bulkcopy_calls: list[dict] = []
        self.connections_opened = 0
        # Rows "committed" via bulkcopy or executemany.
        self.committed_rows = 0
        # Exceptions to raise from successive bulkcopy calls (None = succeed).
        self.bulkcopy_errors: list[Exception | None] = []


class FakeMssqlCursor:
    def __init__(self, server: FakeMssqlServer):
        self.server = server
        self._result = None

    def execute(self, sql, *params):
        self.server.executed_sql.append(sql)
        if sql.strip().upper().startswith("SELECT COUNT(*)"):
            self._result = (self.server.committed_rows,)

    def executemany(self, sql, seq):
        self.server.executemany_calls.append((sql, list(seq)))
        self.server.committed_rows += len(seq)

    def fetchone(self):
        return self._result

    def bulkcopy(self, table, data, batch_size, timeout, table_lock, column_mappings):
        self.server.bulkcopy_calls.append(
            {
                "table": table,
                "rows": list(data),
                "batch_size": batch_size,
                "table_lock": table_lock,
                "column_mappings": list(column_mappings),
            }
        )
        if self.server.bulkcopy_errors:
            err = self.server.bulkcopy_errors.pop(0)
            if err is not None:
                raise err
        self.server.committed_rows += len(data)
        return {"rows_copied": len(data)}

    def close(self):
        pass


class FakeMssqlConnection:
    def __init__(self, server: FakeMssqlServer):
        self.server = server

    def cursor(self):
        return FakeMssqlCursor(self.server)

    def commit(self):
        pass

    def close(self):
        pass


class FakeMssqlResource:
    def __init__(self, server: FakeMssqlServer):
        self.server = server

    @contextmanager
    def get_connection(self, timeout: int = 0):
        self.server.connections_opened += 1
        yield FakeMssqlConnection(self.server)


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _run_extract(monkeypatch, *, batches, tags=None, server=None):
    import core.dfm_manager
    from DSI.connectors.factory import _execute_dfm_oracle_extract

    FakeDfmManager.instances = []
    monkeypatch.setattr(core.dfm_manager, "DfmManager", FakeDfmManager)

    server = server or FakeMssqlServer()
    context = FakeContext(tags=tags)
    result = _execute_dfm_oracle_extract(
        context,
        FakeOracleResource(batches),
        FakeMssqlResource(server),
        SimpleNamespace(mode="delta"),
        EXTRACT_CONFIG,
        "IFDB",
        lambda run_config, checkpoint, cfg, upper_bound: None,
    )
    return result, server, context


def _meta(result, key):
    value = result.metadata[key]
    return getattr(value, "value", value)


_LONG_XML = "<xml>" + "æøå-" * 2000 + "</xml>"  # > 4000 chars, NVARCHAR(MAX)


# ---------------------------------------------------------------------------
# 1. Happy path
# ---------------------------------------------------------------------------


class TestBulkcopyHappyPath:
    def test_rows_flow_through_bulkcopy_without_audit_columns(self, monkeypatch):
        batches = [
            [("1", _LONG_XML), ("2", None)],
            [("3", "<xml>ok</xml>")],
        ]
        result, server, context = _run_extract(monkeypatch, batches=batches)

        assert _meta(result, "row_count") == 3
        assert _meta(result, "insert_method") == "bulkcopy"
        assert len(server.bulkcopy_calls) == 2
        assert server.executemany_calls == []
        # Column mapping is exactly the config columns — no audit columns.
        assert server.bulkcopy_calls[0]["column_mappings"] == list(COLUMNS)
        assert server.bulkcopy_calls[0]["table"] == "ext.ifdb_testtable"
        # NVARCHAR(MAX) payload passes through untruncated.
        assert server.bulkcopy_calls[0]["rows"][0][1] == _LONG_XML

    def test_one_persistent_bulkcopy_connection(self, monkeypatch):
        batches = [[("1", "a")], [("2", "b")], [("3", "c")]]
        result, server, _ = _run_extract(monkeypatch, batches=batches)

        # One connection for DDL + one persistent bulk-copy connection.
        assert server.connections_opened == 2
        assert _meta(result, "row_count") == 3

    def test_oracle_cursor_arraysize_and_prefetchrows(self, monkeypatch):
        import core.dfm_manager
        from DSI.connectors.factory import _execute_dfm_oracle_extract

        monkeypatch.setattr(core.dfm_manager, "DfmManager", FakeDfmManager)
        oracle = FakeOracleResource([[("1", "a")]])
        _execute_dfm_oracle_extract(
            FakeContext(),
            oracle,
            FakeMssqlResource(FakeMssqlServer()),
            SimpleNamespace(mode="delta"),
            EXTRACT_CONFIG,
            "IFDB",
            lambda run_config, checkpoint, cfg, upper_bound: None,
        )
        assert oracle.cursor.arraysize == 10_000
        assert oracle.cursor.prefetchrows == 10_001
        # No cursor-level output type handler: it would replace the resource's
        # connection-level handler (encoding + LOB-inline bindings).
        assert not hasattr(oracle.cursor, "outputtypehandler")

    def test_resource_handler_maps_strings_and_lobs(self):
        import oracledb
        from core.resources import _ifdb_oracle_output_type_handler

        class VarRecorder:
            arraysize = 10_000

            def var(self, db_type, arraysize=None, encoding_errors=None):
                return (db_type, arraysize, encoding_errors)

        cursor = VarRecorder()
        # Strings keep the Windows-1252 dirty-byte protection.
        for str_type in (
            oracledb.DB_TYPE_VARCHAR,
            oracledb.DB_TYPE_CHAR,
            oracledb.DB_TYPE_LONG,
            oracledb.DB_TYPE_NVARCHAR,
            oracledb.DB_TYPE_NCHAR,
        ):
            meta = SimpleNamespace(type_code=str_type)
            assert _ifdb_oracle_output_type_handler(cursor, meta) == (str, 10_000, "replace")
        # LOBs fetch inline (no per-row locator round trips).
        for lob_type, expected in (
            (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_LONG),
            (oracledb.DB_TYPE_NCLOB, oracledb.DB_TYPE_LONG),
        ):
            meta = SimpleNamespace(type_code=lob_type)
            assert _ifdb_oracle_output_type_handler(cursor, meta) == (
                expected,
                10_000,
                "replace",
            )
        meta = SimpleNamespace(type_code=oracledb.DB_TYPE_BLOB)
        assert _ifdb_oracle_output_type_handler(cursor, meta) == (
            oracledb.DB_TYPE_LONG_RAW,
            10_000,
            None,
        )
        # Everything else keeps the driver default binding.
        meta = SimpleNamespace(type_code=oracledb.DB_TYPE_NUMBER)
        assert _ifdb_oracle_output_type_handler(cursor, meta) is None

    def test_values_are_stringified(self, monkeypatch):
        batches = [[(99276330680.0, "x")]]
        _, server, _ = _run_extract(monkeypatch, batches=batches)
        assert server.bulkcopy_calls[0]["rows"][0][0] == "99276330680"

    def test_dfm_run_completed_with_row_count(self, monkeypatch):
        _run_extract(monkeypatch, batches=[[("1", "a"), ("2", "b")]])
        assert FakeDfmManager.instances[0].completed == [(42, 2)]


# ---------------------------------------------------------------------------
# 2. Failure semantics
# ---------------------------------------------------------------------------


class TestBulkcopyFailureFailsAsset:
    def test_driver_error_fails_the_asset_without_executemany(self, monkeypatch):
        server = FakeMssqlServer()
        server.bulkcopy_errors = [RuntimeError("unsupported value shape")]
        batches = [
            [("1", "a"), ("2", "b")],
            [("3", "c")],
        ]
        with pytest.raises(RuntimeError, match="unsupported value shape"):
            _run_extract(monkeypatch, batches=batches, server=server)

        # No silent degradation: nothing was inserted via executemany, and
        # the DFM run is marked failed.
        assert server.executemany_calls == []
        assert FakeDfmManager.instances[0].failed
        assert not FakeDfmManager.instances[0].completed

    def test_timeout_error_propagates(self, monkeypatch):
        import core.dfm_manager
        import DSI.connectors.mssql.bulk_copy as bulk_copy_module
        from DSI.connectors.factory import _execute_dfm_oracle_extract

        FakeDfmManager.instances = []
        monkeypatch.setattr(core.dfm_manager, "DfmManager", FakeDfmManager)

        def _raise_timeout(*args, **kwargs):
            raise RuntimeError("bulk-copy timeout: deadline exceeded")

        monkeypatch.setattr(bulk_copy_module, "bulkcopy_insert_with_retry", _raise_timeout)

        server = FakeMssqlServer()
        with pytest.raises(RuntimeError, match="deadline"):
            _execute_dfm_oracle_extract(
                FakeContext(),
                FakeOracleResource([[("1", "a")]]),
                FakeMssqlResource(server),
                SimpleNamespace(mode="delta"),
                EXTRACT_CONFIG,
                "IFDB",
                lambda run_config, checkpoint, cfg, upper_bound: None,
            )
        # No executemany fallback for timeouts.
        assert server.executemany_calls == []
        assert FakeDfmManager.instances[0].failed


# ---------------------------------------------------------------------------
# 3. Run-tag escape hatch (the only route to executemany)
# ---------------------------------------------------------------------------


class TestRunTagEscapeHatch:
    def test_executemany_tag_forces_legacy_path(self, monkeypatch):
        batches = [[("1", "a"), ("2", "b")]]
        result, server, _ = _run_extract(
            monkeypatch, batches=batches, tags={"ext_insert_method": "executemany"}
        )
        assert _meta(result, "insert_method") == "executemany"
        assert server.bulkcopy_calls == []
        inserted = [row for _, rows in server.executemany_calls for row in rows]
        assert inserted == [("1", "a"), ("2", "b")]

    def test_unknown_tag_value_uses_bulkcopy(self, monkeypatch):
        result, server, context = _run_extract(
            monkeypatch, batches=[[("1", "a")]], tags={"ext_insert_method": "banana"}
        )
        assert _meta(result, "insert_method") == "bulkcopy"
        assert len(server.bulkcopy_calls) == 1
        assert any("Unknown" in w for w in context.log.warnings)


# ---------------------------------------------------------------------------
# 4. bulkcopy_insert extra_columns parameter
# ---------------------------------------------------------------------------


class TestBulkcopyInsertExtraColumns:
    def _call(self, extra_columns=None):
        from DSI.connectors.mssql.bulk_copy import bulkcopy_insert
        from DSI.connectors.mssql.models import TableConfig

        server = FakeMssqlServer()
        config = TableConfig(
            output_name="ext_t",
            source_schema="s",
            source_table="t",
            target_table="t",
            description="",
        )
        kwargs = {} if extra_columns is None else {"extra_columns": extra_columns}
        count = bulkcopy_insert(
            FakeContext(),
            FakeMssqlConnection(server),
            config,
            ["A", "B"],
            [("1", "2")],
            **kwargs,
        )
        return count, server

    def test_default_appends_audit_columns(self):
        count, server = self._call()
        assert count == 1
        assert server.bulkcopy_calls[0]["column_mappings"] == [
            "A",
            "B",
            "SourceSystem",
            "dw_load_timestamp",
            "dw_partition_date",
        ]

    def test_empty_extra_columns_maps_source_columns_only(self):
        count, server = self._call(extra_columns=())
        assert count == 1
        assert server.bulkcopy_calls[0]["column_mappings"] == ["A", "B"]


# ---------------------------------------------------------------------------
# 5. Timeout duplicate guard (expected_rows_before)
# ---------------------------------------------------------------------------


class TestTimeoutDuplicateGuard:
    """On a client timeout the server may still have committed the atomic
    batch; with expected_rows_before the recovery reads the truth instead of
    re-inserting (duplicates) or failing a healthy run."""

    def _helper_args(self, server):
        from DSI.connectors.mssql.models import TableConfig

        config = TableConfig(
            output_name="ext_t",
            source_schema="s",
            source_table="t",
            target_table="t",
            description="",
        )
        return config, FakeMssqlConnection(server), FakeMssqlResource(server)

    def test_committed_batch_is_banked_not_reinserted(self):
        from DSI.connectors.mssql.bulk_copy import bulkcopy_insert_with_retry

        server = FakeMssqlServer()
        server.committed_rows = 100  # caller's running total
        data = [(str(i), "x") for i in range(600)]

        def timeout_then_commit(*a, **k):
            # Simulate: server committed the batch, client saw a timeout.
            server.committed_rows += len(data)
            raise RuntimeError("bulk-copy timeout: deadline exceeded")

        config, conn, resource = self._helper_args(server)
        original_bulkcopy = FakeMssqlCursor.bulkcopy
        FakeMssqlCursor.bulkcopy = staticmethod(timeout_then_commit)
        try:
            count, _ = bulkcopy_insert_with_retry(
                FakeContext(),
                conn,
                config,
                ["A", "B"],
                data,
                mssql_target_resource=resource,
                extra_columns=(),
                expected_rows_before=100,
            )
        finally:
            FakeMssqlCursor.bulkcopy = original_bulkcopy
        assert count == len(data)
        # Exactly one committed copy of the batch — no duplicates.
        assert server.committed_rows == 100 + len(data)

    def test_uncommitted_timeout_still_splits_and_retries(self):
        from DSI.connectors.mssql.bulk_copy import bulkcopy_insert_with_retry

        server = FakeMssqlServer()
        server.committed_rows = 0
        data = [(str(i), "x") for i in range(1200)]
        # First call times out with nothing committed; split halves succeed.
        server.bulkcopy_errors = [RuntimeError("bulk-copy timeout: deadline exceeded")]

        config, conn, resource = self._helper_args(server)
        count, _ = bulkcopy_insert_with_retry(
            FakeContext(),
            conn,
            config,
            ["A", "B"],
            data,
            mssql_target_resource=resource,
            extra_columns=(),
            expected_rows_before=0,
        )
        assert count == len(data)
        assert server.committed_rows == len(data)

    def test_inconsistent_count_fails_loud(self):
        import pytest as _pytest
        from DSI.connectors.mssql.bulk_copy import bulkcopy_insert_with_retry

        server = FakeMssqlServer()
        server.committed_rows = 50  # neither base (100) nor base+chunk
        data = [(str(i), "x") for i in range(600)]
        server.bulkcopy_errors = [RuntimeError("bulk-copy timeout: deadline exceeded")]

        config, conn, resource = self._helper_args(server)
        with _pytest.raises(RuntimeError, match="inconsistent target"):
            bulkcopy_insert_with_retry(
                FakeContext(),
                conn,
                config,
                ["A", "B"],
                data,
                mssql_target_resource=resource,
                extra_columns=(),
                expected_rows_before=100,
            )
