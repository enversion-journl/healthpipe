# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Checkpoint-ownership contracts for the delta-driven Vare/ILSVare flow.

Exactly one asset advances each DFM checkpoint the entity-delta hub reads;
staging assets and the hub itself only READ. A second writer — or a reader
that starts advancing — silently shrinks the hub's delta windows and holes
the delta.
"""

import importlib
import inspect

import pytest

pytestmark = pytest.mark.unit


def _src(module: str) -> str:
    return inspect.getsource(importlib.import_module(module))


def test_dims_and_bridge_own_their_checkpoints():
    assert 'set_checkpoint("dim", "Leverandoervare"' in _src("DSA.SQL.dim.dim_Leverandoervare")
    assert 'set_checkpoint("dim", "ILSVare"' in _src("DSA.SQL.dim.dim_ILSVare")
    assert 'set_checkpoint("bridge", "LeverandoervareILSVare"' in _src(
        "DSA.SQL.bridge.bridge_LeverandoervareILSVare"
    )


def test_hub_and_staging_only_read_checkpoints():
    for module in (
        "DSA.SQL.staging.pre_stg_Leverandoervare_ILSVare_delta_entiteter",
        "DSA.SQL.staging.stg_dim_Leverandoervare",
        "DSA.SQL.staging.stg_dim_ILSVare",
        "DSA.SQL.staging.stg_bridge_Leverandoervare_ILSVare",
    ):
        assert "set_checkpoint(" not in _src(module), module


def test_dim_ilsvare_reads_delta_staging_not_legacy_table():
    # forbudt kilde: fuld-rebuild-tabellen stg.dim_ILSVare_ils findes ikke
    # laengere og maa ikke genopstaa som kilde for dim.ILSVare
    src = _src("DSA.SQL.dim.dim_ILSVare")
    assert "[stg].[dim_ILSVare]" in src
    assert "[stg].[dim_ILSVare_ils]" not in src
