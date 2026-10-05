# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Real-DB end-to-end: production parallel_stream_extract_and_insert."""

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.real_resources]


def _require_real_db():
    if (
        os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() != "true"
        or os.getenv("ENABLE_TEST_WRITES", "false").lower() != "true"
    ):
        pytest.skip("requires USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true")


def test_parallel_extract_transactional(monkeypatch):
    _require_real_db()
    monkeypatch.setenv("MYSUPPLY_MAX_SOURCE_CONNS", "4")
    from core.resources import MSSQLResource
    from dagster import build_asset_context
    from DSI.connectors.mssql.models import TableConfig
    from DSI.connectors.mssql.parallel_extract import parallel_stream_extract_and_insert

    res = MSSQLResource(
        host=os.getenv("MSSQL_SOURCE_STAGING_HOST", "localhost"),
        port=int(os.getenv("MSSQL_SOURCE_STAGING_PORT", "1433")),
        database=os.getenv("MSSQL_SOURCE_STAGING_DB", "DW"),
        user=os.getenv("MSSQL_SOURCE_STAGING_USER", "sa"),
        password=os.getenv("MSSQL_SOURCE_STAGING_PASSWORD", ""),
    )
    with res.get_connection() as c:
        cur = c.cursor()
        cur.execute("IF OBJECT_ID('dbo.pe_src') IS NOT NULL DROP TABLE dbo.pe_src")
        cur.execute("CREATE TABLE dbo.pe_src (Id INT NOT NULL PRIMARY KEY, V NVARCHAR(50))")
        cur.executemany(
            "INSERT INTO dbo.pe_src (Id, V) VALUES (?, ?)", [(i, f"v{i}") for i in range(1, 251)]
        )
        c.commit()
        cur.close()

    cfg = TableConfig(
        output_name="pe",
        source_schema="dbo",
        source_table="pe_src",
        target_table="pe_dst",
        description="d",
        is_full_load=True,
        parallel_workers=4,
    )
    result = parallel_stream_extract_and_insert(build_asset_context(), res, res, cfg, "MySupply")
    assert result.row_count == 250

    with res.get_connection() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT_BIG(*) FROM ext.pe_dst")
        assert int(cur.fetchall()[0][0]) == 250
        cur.execute("DROP TABLE IF EXISTS ext.pe_dst")
        cur.execute("IF OBJECT_ID('dbo.pe_src') IS NOT NULL DROP TABLE dbo.pe_src")
        c.commit()
        cur.close()


def test_parallel_extract_streams_progress_to_parent_log(monkeypatch):
    """The fix for the 'looks frozen' UI: spawn workers stream progress through a
    queue and the PARENT re-emits it via context.log. This runs the real spawn
    pool against the real DB and asserts a per-window progress line actually
    reached the parent logger (what Dagster captures), not just worker stdout."""
    _require_real_db()
    monkeypatch.setenv("MYSUPPLY_MAX_SOURCE_CONNS", "4")
    from types import SimpleNamespace

    from core.resources import MSSQLResource
    from DSI.connectors.mssql.models import TableConfig
    from DSI.connectors.mssql.parallel_extract import parallel_stream_extract_and_insert

    captured: list[tuple[str, str]] = []

    class _RecordingLog:
        def info(self, m, *a, **k):
            captured.append(("info", str(m)))

        def warning(self, m, *a, **k):
            captured.append(("warning", str(m)))

        def error(self, m, *a, **k):
            captured.append(("error", str(m)))

    ctx = SimpleNamespace(log=_RecordingLog())

    res = MSSQLResource(
        host=os.getenv("MSSQL_SOURCE_STAGING_HOST", "localhost"),
        port=int(os.getenv("MSSQL_SOURCE_STAGING_PORT", "1433")),
        database=os.getenv("MSSQL_SOURCE_STAGING_DB", "DW"),
        user=os.getenv("MSSQL_SOURCE_STAGING_USER", "sa"),
        password=os.getenv("MSSQL_SOURCE_STAGING_PASSWORD", ""),
    )
    with res.get_connection() as c:
        cur = c.cursor()
        cur.execute("IF OBJECT_ID('dbo.pe_prog') IS NOT NULL DROP TABLE dbo.pe_prog")
        cur.execute("CREATE TABLE dbo.pe_prog (Id INT NOT NULL PRIMARY KEY, V NVARCHAR(50))")
        cur.executemany(
            "INSERT INTO dbo.pe_prog (Id, V) VALUES (?, ?)", [(i, f"v{i}") for i in range(1, 301)]
        )
        c.commit()
        cur.close()

    cfg = TableConfig(
        output_name="pe",
        source_schema="dbo",
        source_table="pe_prog",
        target_table="pe_prog_dst",
        description="d",
        is_full_load=True,
        parallel_workers=3,
    )
    result = parallel_stream_extract_and_insert(ctx, res, res, cfg, "MySupply")
    assert result.row_count == 300

    # The whole point: worker progress reached the PARENT logger.
    progress = [m for (lvl, m) in captured if "chunk" in m]
    assert progress, f"no worker progress reached the parent log; captured={captured[-6:]}"
    assert any("w" in m for m in progress)  # carries a window label

    with res.get_connection() as c:
        cur = c.cursor()
        cur.execute("DROP TABLE IF EXISTS ext.pe_prog_dst")
        cur.execute("IF OBJECT_ID('dbo.pe_prog') IS NOT NULL DROP TABLE dbo.pe_prog")
        c.commit()
        cur.close()
