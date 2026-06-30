# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Structural/wiring tests for the Vareudlevering DFM migration (US 28691).

These assert the static shape of the migrated pipeline — dependency graph,
group membership, source-string repoints, checkpoint-owner calls, job
membership. Runtime row-count parity is an operational gate, not a CI test.
"""

from __future__ import annotations

import inspect


def test_pre_stg_module_imports():
    from DSA.SQL.staging import pre_stg_vareudlevering_ils

    assert pre_stg_vareudlevering_ils is not None


def test_pre_stg_reads_archive_not_partition_extract():
    src = inspect.getsource(
        __import__("DSA.SQL.staging.pre_stg_vareudlevering_ils", fromlist=["x"])
    )
    assert "[arc].[ils_udlev]" in src
    assert "[ext].[udlev_ils]" not in src
    assert "arc_ils_udlev" in src
    assert "ext_udlev_ils_daily" not in src
    assert "ext_udlev_ils_monthly" not in src
    assert "DfmManager" in src
    assert 'group_name="pre_staging_vareudlevering"' in src
    assert "config: RunModeConfig" in src


def test_stg_dim_wrapped_in_dfm():
    src = inspect.getsource(
        __import__("DSA.SQL.staging.stg_dim_vareudlevering_ils", fromlist=["x"])
    )
    assert 'group_name="staging_vareudlevering"' in src
    assert "DfmManager" in src
    assert "config: RunModeConfig" in src
    assert "start_dfm_run(" in src


def test_stg_fct_repoints_lev_to_archive():
    src = inspect.getsource(
        __import__("DSA.SQL.staging.stg_fct_vareudlevering_ils", fromlist=["x"])
    )
    assert "[arc].[ils_lev]" in src
    assert "[ext].[lev_ils]" not in src
    assert 'AssetKey(["ext_lev_ils"])' not in src
    assert 'group_name="staging_vareudlevering"' in src
    assert "DfmManager" in src
    assert "config: RunModeConfig" in src


def test_map_wrapped_in_dfm():
    src = inspect.getsource(__import__("DSA.SQL.map.MapUpd_vareudlevering", fromlist=["x"]))
    assert 'group_name="maps_vareudlevering"' in src
    assert "DfmManager" in src
    assert "start_dfm_run(" in src
    assert "config: RunModeConfig" in src


def test_dim_is_checkpoint_owner():
    src = inspect.getsource(
        __import__("DSA.SQL.dim.DimUpd_vareudlevering_egenskaber", fromlist=["x"])
    )
    assert 'group_name="dimensions_vareudlevering"' in src
    assert "start_dfm_run(" in src
    assert '"VareudleveringEgenskaber"' in src
    assert "set_checkpoint(" in src
    assert "should_advance_checkpoint" in src
    assert "config: RunModeConfig" in src


def test_fct_is_checkpoint_owner():
    src = inspect.getsource(__import__("DSA.SQL.fct.FctUpd_vareudlevering", fromlist=["x"]))
    assert 'group_name="facts_vareudlevering"' in src
    assert "start_dfm_run(" in src
    assert "set_checkpoint(" in src
    assert "should_advance_checkpoint" in src
    assert "config: RunModeConfig" in src


def test_dso_sync_assets_in_vareudlevering_group():
    dim_src = inspect.getsource(__import__("DSO.load.sync_dim", fromlist=["x"]))
    fct_src = inspect.getsource(__import__("DSO.load.sync_fct", fromlist=["x"]))
    assert 'name="dso_sync_dim_vareudlevering_egenskaber"' in dim_src
    assert 'name="dso_sync_fct_vareudlevering"' in fct_src
    assert dim_src.count('group_name="dso_sync_vareudlevering"') >= 1
    assert fct_src.count('group_name="dso_sync_vareudlevering"') >= 1


def test_groups_in_erp_daily_dfm_job():
    src = inspect.getsource(__import__("jobs.erp_daily_dfm_jobs", fromlist=["x"]))
    for group in (
        "pre_staging_vareudlevering",
        "staging_vareudlevering",
        "maps_vareudlevering",
        "dimensions_vareudlevering",
        "facts_vareudlevering",
        "dso_sync_vareudlevering",
    ):
        assert f'"{group}"' in src, f"{group} not wired into erp_daily_dfm_job"


def test_dev_vareudlevering_job_removed():
    import jobs.erp_jobs as ej

    assert not hasattr(ej, "erp_dev_vareudlevering_job")
    assert "erp_dev_vareudlevering_job" not in ej.__all__


def test_dq_check_migrated_to_archive():
    src = inspect.getsource(
        __import__("DSA.SQL.checks.partition_completeness_checks", fromlist=["x"])
    )
    assert "dq_archive_completeness_vareudlevering" in src
    assert "dq_partition_completeness_vareudlevering" not in src
    assert "arc.ils_udlev" in src

    defs_src = inspect.getsource(__import__("definitions", fromlist=["x"]))
    assert "dq_partition_completeness_vareudlevering" not in defs_src
    assert "dq_archive_completeness_vareudlevering" in defs_src
