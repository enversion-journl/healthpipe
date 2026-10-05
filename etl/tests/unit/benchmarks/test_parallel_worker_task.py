# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""build_window_task carries the semaphore identity + source system."""

import pytest
from DSI.connectors.mssql.parallel_extract import build_window_task


@pytest.mark.unit
def test_build_window_task_includes_all_fields():
    task = build_window_task(
        source_conn_str="S",
        target_conn_str="T",
        select_sql="SELECT 1",
        target_table="bench_t",
        column_names=["a"],
        bcp_limits=[None],
        chunk_size=10_000,
        table_lock=True,
        load_timestamp=None,
        source_system="Sydax",
        source_key="sydax",
        source_capacity=16,
    )
    assert task["source_key"] == "sydax"
    assert task["source_capacity"] == 16
    assert task["source_system"] == "Sydax"
    assert task["select_sql"] == "SELECT 1"
    assert task["target_table"] == "bench_t"
    assert task["chunk_size"] == 10_000
    assert task["table_lock"] is True
    assert task["column_names"] == ["a"]
    assert task["bcp_limits"] == [None]
    assert task["source_conn_str"] == "S" and task["target_conn_str"] == "T"
