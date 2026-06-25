# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Gating predicate + source budget resolution."""

import pytest
from DSI.connectors.mssql.models import TableConfig
from DSI.connectors.mssql.parallel_extract import (
    MAX_SOURCE_CONNECTIONS,
    clamp_worker_count,
    should_run_parallel,
    source_budget,
)


def _cfg(workers):
    return TableConfig(
        output_name="o",
        source_schema="dbo",
        source_table="t",
        target_table="t",
        description="d",
        parallel_workers=workers,
    )


@pytest.mark.unit
def test_parallel_only_when_workers_full_load_and_mssql_python(monkeypatch):
    monkeypatch.delenv("EXT_PARALLEL_DISABLE", raising=False)
    assert should_run_parallel(_cfg(8), is_full_load=True, is_mssql_python=True)
    assert not should_run_parallel(_cfg(1), is_full_load=True, is_mssql_python=True)
    assert not should_run_parallel(_cfg(8), is_full_load=False, is_mssql_python=True)
    assert not should_run_parallel(_cfg(8), is_full_load=True, is_mssql_python=False)


@pytest.mark.unit
def test_kill_switch_forces_serial(monkeypatch):
    monkeypatch.setenv("EXT_PARALLEL_DISABLE", "true")
    assert not should_run_parallel(_cfg(8), is_full_load=True, is_mssql_python=True)


@pytest.mark.unit
def test_source_budget_defaults_and_env(monkeypatch):
    monkeypatch.delenv("MYSUPPLY_MAX_SOURCE_CONNS", raising=False)
    assert source_budget("MySupply") == 16
    monkeypatch.setenv("MYSUPPLY_MAX_SOURCE_CONNS", "10")
    assert source_budget("MySupply") == 10


@pytest.mark.unit
def test_max_source_connections_default_is_sixteen():
    # Env-tunable (EXT_MAX_SOURCE_CONNECTIONS); default raised 8 -> 16 to let the
    # benchmark probe beyond the previous sweet spot.
    assert MAX_SOURCE_CONNECTIONS == 16


@pytest.mark.unit
def test_clamp_worker_count_clamps_to_max():
    assert clamp_worker_count(_cfg(4)) == 4
    assert clamp_worker_count(_cfg(16)) == 16  # within the (raised) cap
    assert clamp_worker_count(_cfg(32)) == MAX_SOURCE_CONNECTIONS == 16  # clamped
    assert clamp_worker_count(_cfg(1)) == 1
