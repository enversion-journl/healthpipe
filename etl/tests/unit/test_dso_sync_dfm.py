# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for DSO DFM sync helpers."""

from unittest.mock import MagicMock, patch

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmRunContext
from DSO.load.sync_configs import SyncTableConfig
from DSO.load.sync_dfm import (
    DSO_DFM_SCOPE,
    build_source_max_checkpoint_sql,
    finalize_dfm_run,
    resolve_dfm_mode,
    watermark_column_names,
)
from DSO.load.sync_utils import build_delta_merge_statement, build_watermark_coalesce_predicate


class TestResolveDfmMode:
    def test_watermark_defaults_to_delta(self):
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        assert resolve_dfm_mode(config) == "delta"

    def test_truncate_insert_maps_to_full_load(self):
        config = SyncTableConfig(
            schema="sec",
            table="RolleDomaene",
            primary_key=["a", "b"],
            mode="truncate_insert",
        )
        assert resolve_dfm_mode(config) == "full_load"

    def test_run_mode_config_override(self):
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        run_cfg = RunModeConfig(mode="full_load")
        assert resolve_dfm_mode(config, run_cfg) == "full_load"


class TestCheckpointSql:
    def test_build_source_max_checkpoint_sql(self):
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = ["id", "dw_timestamp_insert", "dw_timestamp_update"]
        sql = build_source_max_checkpoint_sql(config, columns)
        assert "MAX(COALESCE([dw_timestamp_update], [dw_timestamp_insert]))" in sql
        assert "FROM [dim].[Kreditor]" in sql
        assert "CONVERT(NVARCHAR(50)" in sql

    def test_watermark_column_names(self):
        config = SyncTableConfig(
            schema="dim",
            table="T",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_insert",
        )
        wm, ins = watermark_column_names(config, ["id", "dw_timestamp_insert"])
        assert wm == "dw_timestamp_insert"
        assert ins == "dw_timestamp_insert"

    def test_pinned_window_predicate(self):
        pred = build_watermark_coalesce_predicate(
            "dw_timestamp_update",
            "dw_timestamp_insert",
            lower_exclusive="2026-01-01 00:00:00.000",
            upper_inclusive="2026-02-01 12:00:00.000",
        )
        assert (
            "COALESCE([dw_timestamp_update], [dw_timestamp_insert]) > '2026-01-01 00:00:00.000'"
            in pred
        )
        assert (
            "COALESCE([dw_timestamp_update], [dw_timestamp_insert]) <= '2026-02-01 12:00:00.000'"
            in pred
        )

    def test_delta_merge_includes_upper_bound(self):
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = ["id", "dw_timestamp_insert", "dw_timestamp_update"]
        sql = build_delta_merge_statement(
            config,
            columns,
            last_sync_checkpoint="2026-01-01 00:00:00.000",
            checkpoint_upper_bound="2026-02-01 12:00:00.000",
            staging_db="TestDB",
        )
        assert "> '2026-01-01 00:00:00.000'" in sql
        assert "<= '2026-02-01 12:00:00.000'" in sql


class TestFinalizeDfmRun:
    def test_zero_rows_does_not_set_checkpoint(self):
        mock_dfm = MagicMock()
        run_ctx = DfmRunContext(log_id=1, checkpoint="2026-01-01 00:00:00")
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        finalize_dfm_run(
            mock_dfm,
            run_ctx,
            config,
            ["id", "dw_timestamp_update"],
            MagicMock(),
            rows_affected=0,
            dfm_mode="delta",
            checkpoint_upper_bound="2026-02-01 12:00:00",
        )
        mock_dfm.set_checkpoint.assert_not_called()
        mock_dfm.complete_dfm_run.assert_called_once_with(
            1, checkpoint_after="2026-01-01 00:00:00", rows_affected=0
        )

    def test_advances_checkpoint_from_pre_sampled_upper_bound(self):
        mock_dfm = MagicMock()
        run_ctx = DfmRunContext(log_id=2, checkpoint="2026-01-01 00:00:00")
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        with patch("DSO.load.sync_dfm.query_source_checkpoint") as mock_query:
            finalize_dfm_run(
                mock_dfm,
                run_ctx,
                config,
                ["id", "dw_timestamp_update"],
                MagicMock(),
                rows_affected=10,
                dfm_mode="delta",
                checkpoint_upper_bound="2026-02-01 12:00:00",
            )
            mock_query.assert_not_called()
        mock_dfm.set_checkpoint.assert_called_once_with(
            "dim",
            "Kreditor",
            value="2026-02-01 12:00:00",
            mode="delta",
            scope=DSO_DFM_SCOPE,
        )
        mock_dfm.complete_dfm_run.assert_called_once_with(
            2, checkpoint_after="2026-02-01 12:00:00", rows_affected=10
        )

    def test_null_upper_bound_keeps_checkpoint_in_run_log(self):
        mock_dfm = MagicMock()
        run_ctx = DfmRunContext(log_id=4, checkpoint="2026-01-01 00:00:00")
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        result = finalize_dfm_run(
            mock_dfm,
            run_ctx,
            config,
            ["id", "dw_timestamp_update"],
            MagicMock(),
            rows_affected=12,
            dfm_mode="delta",
            checkpoint_upper_bound=None,
        )
        assert result == "2026-01-01 00:00:00"
        mock_dfm.set_checkpoint.assert_not_called()
        mock_dfm.complete_dfm_run.assert_called_once_with(
            4, checkpoint_after="2026-01-01 00:00:00", rows_affected=12
        )
        mock_dfm._context.log.warning.assert_called_once()

    def test_non_watermark_only_completes_run(self):
        mock_dfm = MagicMock()
        run_ctx = DfmRunContext(log_id=3, checkpoint=None)
        config = SyncTableConfig(
            schema="sec",
            table="RolleDomaene",
            primary_key=["a", "b"],
            mode="truncate_insert",
        )
        finalize_dfm_run(
            mock_dfm,
            run_ctx,
            config,
            ["a", "b"],
            MagicMock(),
            rows_affected=5,
            dfm_mode="full_load",
        )
        mock_dfm.set_checkpoint.assert_not_called()
        mock_dfm.complete_dfm_run.assert_called_once_with(3, checkpoint_after=None, rows_affected=5)
