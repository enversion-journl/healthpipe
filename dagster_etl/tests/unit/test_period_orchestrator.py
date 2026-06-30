# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the period orchestrator.

Tests:
  - generate_date_partitions() for all granularities and edge cases
  - build_period_run_config() config introspection
  - _run_partitions_sequentially() with mocked Dagster instance/job
  - resolve_job_def() resolution logic
"""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.SQL.dfm.period_orchestrator import (
    DatePartition,
    PartitionedPeriodConfig,
    ResolvedJob,
    _run_partitions_sequentially,
    build_period_run_config,
    generate_date_partitions,
    resolve_job_def,
)

# ===========================================================================
# generate_date_partitions — month
# ===========================================================================


class TestPartitionsByMonth:
    def test_full_months(self):
        result = generate_date_partitions("2025-01-01", "2025-03-31", "month")
        assert result == [
            DatePartition("2025-01-01", "2025-01-31"),
            DatePartition("2025-02-01", "2025-02-28"),
            DatePartition("2025-03-01", "2025-03-31"),
        ]

    def test_leap_year_february(self):
        result = generate_date_partitions("2024-02-01", "2024-02-29", "month")
        assert result == [DatePartition("2024-02-01", "2024-02-29")]

    def test_partial_first_month(self):
        result = generate_date_partitions("2025-01-15", "2025-03-31", "month")
        assert result[0] == DatePartition("2025-01-15", "2025-01-31")
        assert len(result) == 3

    def test_partial_last_month(self):
        result = generate_date_partitions("2025-01-01", "2025-03-15", "month")
        assert result[-1] == DatePartition("2025-03-01", "2025-03-15")

    def test_single_month(self):
        result = generate_date_partitions("2025-06-01", "2025-06-30", "month")
        assert result == [DatePartition("2025-06-01", "2025-06-30")]

    def test_cross_year_boundary(self):
        result = generate_date_partitions("2024-11-01", "2025-02-28", "month")
        assert len(result) == 4
        assert result[0] == DatePartition("2024-11-01", "2024-11-30")
        assert result[1] == DatePartition("2024-12-01", "2024-12-31")
        assert result[2] == DatePartition("2025-01-01", "2025-01-31")
        assert result[3] == DatePartition("2025-02-01", "2025-02-28")

    def test_single_day_range(self):
        result = generate_date_partitions("2025-06-15", "2025-06-15", "month")
        assert result == [DatePartition("2025-06-15", "2025-06-15")]


# ===========================================================================
# generate_date_partitions — day
# ===========================================================================


class TestPartitionsByDay:
    def test_three_days(self):
        result = generate_date_partitions("2025-01-01", "2025-01-03", "day")
        assert result == [
            DatePartition("2025-01-01", "2025-01-01"),
            DatePartition("2025-01-02", "2025-01-02"),
            DatePartition("2025-01-03", "2025-01-03"),
        ]

    def test_single_day(self):
        result = generate_date_partitions("2025-06-15", "2025-06-15", "day")
        assert result == [DatePartition("2025-06-15", "2025-06-15")]


# ===========================================================================
# generate_date_partitions — week
# ===========================================================================


class TestPartitionsByWeek:
    def test_full_weeks(self):
        result = generate_date_partitions("2025-01-06", "2025-01-19", "week")
        assert result == [
            DatePartition("2025-01-06", "2025-01-12"),
            DatePartition("2025-01-13", "2025-01-19"),
        ]

    def test_partial_first_week(self):
        result = generate_date_partitions("2025-01-08", "2025-01-19", "week")
        assert result[0] == DatePartition("2025-01-08", "2025-01-12")

    def test_partial_last_week(self):
        result = generate_date_partitions("2025-01-06", "2025-01-15", "week")
        assert result[-1] == DatePartition("2025-01-13", "2025-01-15")


# ===========================================================================
# generate_date_partitions — quarter
# ===========================================================================


class TestPartitionsByQuarter:
    def test_full_year_as_quarters(self):
        result = generate_date_partitions("2025-01-01", "2025-12-31", "quarter")
        assert len(result) == 4
        assert result[0] == DatePartition("2025-01-01", "2025-03-31")
        assert result[1] == DatePartition("2025-04-01", "2025-06-30")
        assert result[2] == DatePartition("2025-07-01", "2025-09-30")
        assert result[3] == DatePartition("2025-10-01", "2025-12-31")

    def test_partial_quarter(self):
        result = generate_date_partitions("2025-02-15", "2025-05-10", "quarter")
        assert result[0] == DatePartition("2025-02-15", "2025-03-31")
        assert result[1] == DatePartition("2025-04-01", "2025-05-10")


# ===========================================================================
# generate_date_partitions — year
# ===========================================================================


class TestPartitionsByYear:
    def test_three_full_years(self):
        result = generate_date_partitions("2023-01-01", "2025-12-31", "year")
        assert len(result) == 3
        assert result[0] == DatePartition("2023-01-01", "2023-12-31")
        assert result[1] == DatePartition("2024-01-01", "2024-12-31")
        assert result[2] == DatePartition("2025-01-01", "2025-12-31")

    def test_partial_years(self):
        result = generate_date_partitions("2023-06-15", "2025-03-10", "year")
        assert result[0] == DatePartition("2023-06-15", "2023-12-31")
        assert result[-1] == DatePartition("2025-01-01", "2025-03-10")


# ===========================================================================
# generate_date_partitions — validation
# ===========================================================================


class TestPartitionValidation:
    def test_reversed_range_raises(self):
        with pytest.raises(ValueError, match="after"):
            generate_date_partitions("2025-12-31", "2025-01-01", "month")

    def test_unknown_granularity_raises(self):
        with pytest.raises(ValueError, match="Unknown granularity"):
            generate_date_partitions("2025-01-01", "2025-12-31", "decade")


# ===========================================================================
# build_period_run_config
# ===========================================================================


def _make_mock_job_def(node_names_with_config: list[str], node_names_without: list[str]):
    """Create a mock JobDefinition with specified nodes."""
    nodes = []
    for name in node_names_with_config:
        node = MagicMock()
        node.name = name
        config_type = MagicMock()
        config_type.fields = {"mode": ..., "date_from": ..., "date_to": ..., "delta_column": ...}
        node.config_schema.config_type = config_type
        nodes.append(node)

    for name in node_names_without:
        node = MagicMock()
        node.name = name
        node.config_schema.config_type = None
        nodes.append(node)

    job_def = MagicMock()
    job_def.all_node_defs = nodes
    return job_def


class TestBuildPeriodRunConfig:
    def test_sets_config_on_configurable_ops(self):
        job_def = _make_mock_job_def(
            ["ext_encounter", "arc_encounter"],
            ["flow_start"],
        )
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(job_def, partition)

        assert "ops" in result
        assert "ext_encounter" in result["ops"]
        assert "arc_encounter" in result["ops"]
        assert "flow_start" not in result["ops"]

    def test_period_config_values(self):
        job_def = _make_mock_job_def(["my_asset"], [])
        partition = DatePartition("2025-06-01", "2025-06-30")
        result = build_period_run_config(job_def, partition)

        cfg = result["ops"]["my_asset"]["config"]
        assert cfg["mode"] == "period"
        assert cfg["date_from"] == "2025-06-01"
        assert cfg["date_to"] == "2025-06-30"
        assert cfg["advance_checkpoint"] is False

    def test_advance_checkpoint_true(self):
        job_def = _make_mock_job_def(["my_asset"], [])
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(job_def, partition, advance_checkpoint=True)

        assert result["ops"]["my_asset"]["config"]["advance_checkpoint"] is True

    def test_advance_checkpoint_false(self):
        job_def = _make_mock_job_def(["my_asset"], [])
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(job_def, partition, advance_checkpoint=False)

        assert result["ops"]["my_asset"]["config"]["advance_checkpoint"] is False

    def test_delta_column_included_when_set(self):
        job_def = _make_mock_job_def(["my_asset"], [])
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(job_def, partition, delta_column="MOD_TS")

        assert result["ops"]["my_asset"]["config"]["delta_column"] == "MOD_TS"

    def test_delta_column_absent_when_none(self):
        job_def = _make_mock_job_def(["my_asset"], [])
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(job_def, partition, delta_column=None)

        assert "delta_column" not in result["ops"]["my_asset"]["config"]

    def test_empty_ops_when_no_configurable_nodes(self):
        job_def = _make_mock_job_def([], ["flow_start", "infra_setup"])
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(job_def, partition)

        assert result["ops"] == {}


def _make_job_def_with_flow_start():
    """Mock JobDefinition with one RunModeConfig op and one FlowStartConfig op."""
    mode_node = MagicMock()
    mode_node.name = "ext_encounter"
    mode_ct = MagicMock()
    mode_ct.fields = {"mode": ..., "date_from": ..., "date_to": ...}
    mode_node.config_schema.config_type = mode_ct

    flow_node = MagicMock()
    flow_node.name = "dfm_ifdb_flow_start"
    flow_ct = MagicMock()
    flow_ct.fields = {"flow_start_override": ...}
    flow_node.config_schema.config_type = flow_ct

    job_def = MagicMock()
    job_def.all_node_defs = [mode_node, flow_node]
    return job_def


class TestBuildPeriodRunConfigFlowStart:
    def test_flow_start_override_set_on_flow_start_op(self):
        job_def = _make_job_def_with_flow_start()
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(
            job_def, partition, flow_start_override="2025-06-30 23:59:59"
        )

        assert result["ops"]["dfm_ifdb_flow_start"]["config"] == {
            "flow_start_override": "2025-06-30 23:59:59"
        }
        # The RunModeConfig op still gets period config.
        assert result["ops"]["ext_encounter"]["config"]["mode"] == "period"

    def test_flow_start_op_skipped_when_no_override(self):
        job_def = _make_job_def_with_flow_start()
        partition = DatePartition("2025-01-01", "2025-01-31")
        result = build_period_run_config(job_def, partition)

        assert "dfm_ifdb_flow_start" not in result["ops"]
        assert "ext_encounter" in result["ops"]


# ===========================================================================
# _run_partitions_sequentially — helpers
# ===========================================================================


def _mock_config(**overrides) -> PartitionedPeriodConfig:
    defaults = dict(
        job_name="test_job",
        date_from="2025-01-01",
        date_to="2025-03-31",
        granularity="month",
        advance_checkpoint=True,
    )
    defaults.update(overrides)
    return PartitionedPeriodConfig(**defaults)


def _mock_context():
    ctx = MagicMock()
    ctx.run_id = "orchestrator-run-id"
    ctx.log = MagicMock()
    return ctx


def _mock_instance(success_sequence: list[bool]):
    """Create a mock instance where create_run_for_job returns runs."""
    instance = MagicMock()
    runs = []
    for i, _ in enumerate(success_sequence):
        run = MagicMock()
        run.run_id = f"child-run-{i}"
        runs.append(run)
    instance.create_run_for_job.side_effect = runs
    return instance, runs


def _mock_execution_results(success_sequence: list[bool]):
    """Create mock execution results to be returned by execute_run."""
    results = []
    for s in success_sequence:
        result = MagicMock()
        result.success = s
        results.append(result)
    return results


# ===========================================================================
# _run_partitions_sequentially — tests
# ===========================================================================


class TestRunPartitionsSequentially:
    """All tests patch _get_reconstructable_job so we don't need a real
    Dagster execution context — the returned mock is passed straight to
    the (also-patched) execute_run.
    """

    _RECON_PATCH = "DSA.SQL.dfm.period_orchestrator._get_reconstructable_job"
    _EXEC_PATCH = "DSA.SQL.dfm.period_orchestrator.execute_run"

    def test_all_succeed(self):
        config = _mock_config()
        context = _mock_context()
        instance, runs = _mock_instance([True, True, True])
        job_def = _make_mock_job_def(["op_a"], [])
        resolved = ResolvedJob(job_def, None)
        partitions = generate_date_partitions("2025-01-01", "2025-03-31", "month")

        exec_results = _mock_execution_results([True, True, True])

        with (
            patch(self._RECON_PATCH, return_value=MagicMock()),
            patch(self._EXEC_PATCH, side_effect=exec_results),
        ):
            results = _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        assert len(results) == 3
        assert all(r.success for r in results)

    def test_stops_on_first_failure(self):
        config = _mock_config()
        context = _mock_context()
        instance, runs = _mock_instance([True, False])
        job_def = _make_mock_job_def(["op_a"], [])
        resolved = ResolvedJob(job_def, None)
        partitions = generate_date_partitions("2025-01-01", "2025-03-31", "month")

        exec_results = _mock_execution_results([True, False])

        with (
            patch(self._RECON_PATCH, return_value=MagicMock()),
            patch(self._EXEC_PATCH, side_effect=exec_results),
        ):
            results = _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        assert len(results) == 2
        assert results[0].success is True
        assert results[1].success is False

    def test_exception_stops_and_records_failure(self):
        config = _mock_config()
        context = _mock_context()
        instance = MagicMock()
        instance.create_run_for_job.side_effect = RuntimeError("DB unavailable")
        job_def = _make_mock_job_def(["op_a"], [])
        resolved = ResolvedJob(job_def, None)
        partitions = generate_date_partitions("2025-01-01", "2025-03-31", "month")

        with patch(self._RECON_PATCH, return_value=MagicMock()):
            results = _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        assert len(results) == 1
        assert results[0].success is False
        assert "DB unavailable" in results[0].error

    def test_child_runs_tagged_correctly(self):
        config = _mock_config()
        context = _mock_context()
        instance, runs = _mock_instance([True])
        job_def = _make_mock_job_def(["op_a"], [])
        resolved = ResolvedJob(job_def, None)
        partitions = [DatePartition("2025-01-01", "2025-01-31")]

        exec_results = _mock_execution_results([True])

        with (
            patch(self._RECON_PATCH, return_value=MagicMock()),
            patch(self._EXEC_PATCH, side_effect=exec_results),
        ):
            _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        tags = instance.create_run_for_job.call_args.kwargs["tags"]
        assert tags["dagster/orchestrator"] == "period_orchestrator"
        assert tags["dagster/partition_start"] == "2025-01-01"
        assert tags["dagster/partition_end"] == "2025-01-31"
        assert tags["dagster/parent_run_id"] == "orchestrator-run-id"

    def test_advance_checkpoint_passed_to_run_config(self):
        config = _mock_config(advance_checkpoint=False)
        context = _mock_context()
        instance, runs = _mock_instance([True])
        job_def = _make_mock_job_def(["op_a"], [])
        resolved = ResolvedJob(job_def, None)
        partitions = [DatePartition("2025-01-01", "2025-01-31")]

        exec_results = _mock_execution_results([True])

        with (
            patch(self._RECON_PATCH, return_value=MagicMock()),
            patch(self._EXEC_PATCH, side_effect=exec_results),
        ):
            _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        run_config = instance.create_run_for_job.call_args.kwargs["run_config"]
        cfg = run_config["ops"]["op_a"]["config"]
        assert cfg["advance_checkpoint"] is False
        assert cfg["mode"] == "period"

    def test_flow_start_override_propagated_to_child_run(self):
        """config.flow_start_override reaches the child run's flow-start op."""
        config = _mock_config(flow_start_override="2025-06-30 23:59:59")
        context = _mock_context()
        instance, runs = _mock_instance([True])
        job_def = _make_job_def_with_flow_start()
        resolved = ResolvedJob(job_def, None)
        partitions = [DatePartition("2025-01-01", "2025-01-31")]

        exec_results = _mock_execution_results([True])

        with (
            patch(self._RECON_PATCH, return_value=MagicMock()),
            patch(self._EXEC_PATCH, side_effect=exec_results),
        ):
            _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        run_config = instance.create_run_for_job.call_args.kwargs["run_config"]
        assert run_config["ops"]["dfm_ifdb_flow_start"]["config"] == {
            "flow_start_override": "2025-06-30 23:59:59"
        }

    def test_asset_selection_passed_to_create_run(self):
        """When asset_names is used, asset_selection is passed to create_run_for_job."""
        config = _mock_config()
        context = _mock_context()
        instance, runs = _mock_instance([True])
        job_def = _make_mock_job_def(["op_a"], [])
        asset_keys = frozenset({dg.AssetKey("ext_enc"), dg.AssetKey("arc_enc")})
        resolved = ResolvedJob(job_def, asset_keys)
        partitions = [DatePartition("2025-01-01", "2025-01-31")]

        exec_results = _mock_execution_results([True])

        with (
            patch(self._RECON_PATCH, return_value=MagicMock()),
            patch(self._EXEC_PATCH, side_effect=exec_results),
        ):
            _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        assert instance.create_run_for_job.call_args.kwargs["asset_selection"] == asset_keys

    def test_no_asset_selection_for_named_job(self):
        """When job_name is used, asset_selection is None."""
        config = _mock_config()
        context = _mock_context()
        instance, runs = _mock_instance([True])
        job_def = _make_mock_job_def(["op_a"], [])
        resolved = ResolvedJob(job_def, None)
        partitions = [DatePartition("2025-01-01", "2025-01-31")]

        exec_results = _mock_execution_results([True])

        with (
            patch(self._RECON_PATCH, return_value=MagicMock()),
            patch(self._EXEC_PATCH, side_effect=exec_results),
        ):
            _run_partitions_sequentially(
                resolved=resolved,
                instance=instance,
                config=config,
                partitions=partitions,
                context=context,
            )

        assert instance.create_run_for_job.call_args.kwargs["asset_selection"] is None


# ===========================================================================
# resolve_job_def
# ===========================================================================


class TestResolveJobDef:
    def test_job_name_takes_precedence(self):
        """When job_name is set, resolve via get_job; asset_selection is None."""
        config = _mock_config(job_name="my_job", asset_names=["asset_a"])
        repo = MagicMock()
        sentinel_job = MagicMock(spec=dg.JobDefinition)
        repo.get_job.return_value = sentinel_job

        result = resolve_job_def(config, repo)

        repo.get_job.assert_called_once_with("my_job")
        assert result.job_def is sentinel_job
        assert result.asset_selection is None

    def test_asset_names_creates_implicit_job(self):
        """When only asset_names is set, resolve via implicit job with asset_selection."""
        config = _mock_config(job_name="", asset_names=["ext_encounter", "arc_encounter"])
        repo = MagicMock()
        sentinel_job = MagicMock(spec=dg.JobDefinition)
        repo.get_implicit_job_def_for_assets.return_value = sentinel_job

        result = resolve_job_def(config, repo)

        repo.get_implicit_job_def_for_assets.assert_called_once()
        assert result.job_def is sentinel_job
        assert result.asset_selection == frozenset(
            {dg.AssetKey("ext_encounter"), dg.AssetKey("arc_encounter")}
        )

    def test_asset_names_invalid_raises_failure(self):
        """When asset_names yields no implicit job, a Failure is raised."""
        config = _mock_config(job_name="", asset_names=["nonexistent"])
        repo = MagicMock()
        repo.get_implicit_job_def_for_assets.return_value = None

        with pytest.raises(dg.Failure, match="Could not create an implicit job"):
            resolve_job_def(config, repo)

    def test_neither_raises_failure(self):
        """When both job_name and asset_names are empty, a Failure is raised."""
        config = _mock_config(job_name="", asset_names=[])

        with pytest.raises(dg.Failure, match="Either job_name or asset_names"):
            resolve_job_def(config, MagicMock())
