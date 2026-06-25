# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""run_parallel_extract_if_enabled: runs + emits metadata only when the gate passes."""

import DSI.connectors.mssql.asset_factory as af
import pytest
from DSI.connectors.mssql.models import TableConfig, TableLoadResult


class _Log:
    info = warning = error = staticmethod(lambda *a, **k: None)


class _Ctx:
    def __init__(self):
        self.log = _Log()
        self.metadata = None

    def add_output_metadata(self, md):
        self.metadata = md


class _Res:
    pass


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
def test_runs_and_emits_metadata_when_gate_passes(monkeypatch):
    called = {}

    def _fake_parallel(context, source_resource, target_resource, config, source_system):
        called["yes"] = True
        return TableLoadResult("s.t", "ext.t", 9)

    monkeypatch.setattr(af, "parallel_stream_extract_and_insert", _fake_parallel)
    ctx = _Ctx()
    handled = af.run_parallel_extract_if_enabled(
        context=ctx,
        source_resource=_Res(),
        target_resource=_Res(),
        config=_cfg(8),
        source_system="MySupply",
        is_full_load=True,
        is_mssql_python=True,
    )
    assert handled is True
    assert called.get("yes") is True
    assert ctx.metadata["row_count"] == 9


@pytest.mark.unit
def test_returns_false_when_gate_fails_on_single_worker():
    handled = af.run_parallel_extract_if_enabled(
        context=_Ctx(),
        source_resource=_Res(),
        target_resource=_Res(),
        config=_cfg(1),
        source_system="MySupply",
        is_full_load=True,
        is_mssql_python=True,
    )
    assert handled is False


@pytest.mark.unit
def test_returns_false_when_not_full_load():
    handled = af.run_parallel_extract_if_enabled(
        context=_Ctx(),
        source_resource=_Res(),
        target_resource=_Res(),
        config=_cfg(8),
        source_system="MySupply",
        is_full_load=False,
        is_mssql_python=True,
    )
    assert handled is False
