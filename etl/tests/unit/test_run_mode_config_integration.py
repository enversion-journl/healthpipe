# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests verifying RunModeConfig settings flow correctly through
execute_archive_merge and drive expected behaviour.

Coverage:
  1. advance_checkpoint=False prevents checkpoint advancement
  2. advance_checkpoint=True advances checkpoint (flow_start_key, checkpoint_query, fallback)
  3. full_load mode keeps delete detection enabled (when upstream_is_full_load=True)
  4. full_load mode on RunModeConfig also enables delete detection
  5. flow_start_key populates new_checkpoint from a sibling state row
  6. checkpoint_query determines new_checkpoint from SQL after MERGE
  7. Fallback to SYSDATETIME when no checkpoint_query and no flow_start_key
  8. ArchiveMergeConfig __post_init__ validates tuple fields
  9. validate_checkpoint_value accepts/rejects correct formats
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from core.archive_merge import execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from utils.sql_safety import validate_checkpoint_value

pytestmark = pytest.mark.unit

# ===========================================================================
# Helpers
# ===========================================================================


def _make_mssql_mock(*, ext_count=100, action_row=(5, 3, 0), checkpoint_row=None):
    """Build a MSSQLResource mock with configurable cursor responses."""
    mssql = MagicMock()
    cursor_mock = MagicMock()

    fetchone_sequence = []
    if ext_count is not None:
        fetchone_sequence.append((ext_count,))
    fetchone_sequence.append(action_row)
    if checkpoint_row is not None:
        fetchone_sequence.append(checkpoint_row)

    cursor_mock.fetchone = MagicMock(side_effect=fetchone_sequence)
    cursor_mock.nextset = MagicMock(return_value=True)
    cursor_mock.rowcount = ext_count or 0

    conn_mock = MagicMock()
    conn_mock.cursor.return_value = cursor_mock

    @contextmanager
    def _get_conn():
        yield conn_mock

    mssql.get_connection = _get_conn
    return mssql, cursor_mock


def _make_context():
    ctx = MagicMock()
    ctx.run_id = "test-run-mode-789"
    ctx.log = MagicMock()
    return ctx


BASE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="orders_delta",
    target_schema="arc",
    target_table="orders",
    merge_keys=("OrderID",),
    non_key_columns=("Amount", "Status"),
    strategy="upsert",
    detect_deletes=False,
)


# ===========================================================================
# advance_checkpoint=False — checkpoint must NOT be advanced
# ===========================================================================


class TestAdvanceCheckpointFalse:
    """When advance_checkpoint=False, dfm.set_checkpoint must not be called."""

    @patch("core.archive_merge.DfmManager")
    def test_no_checkpoint_call_on_advance_false(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = None
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(
            ext_count=None,
            action_row=(10, 0, 0),
            checkpoint_row=("2025-07-01 00:00:00",),
        )
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="orders_delta",
            target_schema="arc",
            target_table="orders",
            merge_keys=("OrderID",),
            non_key_columns=("Amount",),
            checkpoint_query="SELECT MAX([ModDate]) FROM [arc].[orders]",
        )
        run_config = RunModeConfig(mode="delta", advance_checkpoint=False)

        result = execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_not_called()
        mock_dfm.complete_dfm_run.assert_called_once()
        assert result.row_count == 10
        assert result.new_checkpoint == "2025-07-01 00:00:00"

    @patch("core.archive_merge.DfmManager")
    def test_advance_false_with_flow_start_still_no_checkpoint(self, MockDfmManager):
        """Even with flow_start_key providing a value, advance_checkpoint=False skips it."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=2, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 08:00:00"
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(5, 2, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="ifdb_encounter",
            target_schema="arc",
            target_table="ifdb_encounter",
            merge_keys=("EK",),
            non_key_columns=("Col1",),
            flow_start_key=("ext", "dfm_ifdb_flow_start"),
        )
        run_config = RunModeConfig(mode="delta", advance_checkpoint=False)

        result = execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_not_called()
        assert result.new_checkpoint == "2025-06-01 08:00:00"


# ===========================================================================
# advance_checkpoint=True — checkpoint IS advanced
# ===========================================================================


class TestAdvanceCheckpointTrue:
    """When advance_checkpoint=True (default), dfm.set_checkpoint must be called."""

    @patch("core.archive_merge.DfmManager")
    def test_advances_with_flow_start_key(self, MockDfmManager):
        """flow_start_key takes priority for the new checkpoint value."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=3, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-02 12:00:00"
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(8, 1, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="ifdb_encounter",
            target_schema="arc",
            target_table="ifdb_encounter",
            merge_keys=("EK",),
            non_key_columns=("Col1",),
            flow_start_key=("ext", "dfm_ifdb_flow_start"),
        )
        run_config = RunModeConfig(mode="delta", advance_checkpoint=True)

        result = execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_called_once_with(
            "arc", "ifdb_encounter", value="2025-06-02 12:00:00", mode="delta"
        )
        assert result.new_checkpoint == "2025-06-02 12:00:00"

    @patch("core.archive_merge.DfmManager")
    def test_advances_with_checkpoint_query(self, MockDfmManager):
        """checkpoint_query result becomes the new checkpoint."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=4, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = None
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(
            ext_count=None,
            action_row=(3, 2, 0),
            checkpoint_row=("2025-07-01 00:00:00",),
        )
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="vare_ils",
            target_schema="arc",
            target_table="vare_ils",
            merge_keys=("VareID",),
            non_key_columns=("Navn",),
            checkpoint_query="SELECT MAX([dw_timestamp_update]) FROM [arc].[vare_ils]",
        )
        run_config = RunModeConfig(mode="delta", advance_checkpoint=True)

        result = execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_called_once_with(
            "arc", "vare_ils", value="2025-07-01 00:00:00", mode="delta"
        )
        assert result.new_checkpoint == "2025-07-01 00:00:00"

    @patch("core.archive_merge.DfmManager")
    def test_falls_back_to_sysdatetime_when_no_query_no_flow_start(self, MockDfmManager):
        """Without checkpoint_query or flow_start_key, set_checkpoint uses SYSDATETIME."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=5, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-08-01 09:00:00.000"
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(4, 0, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="simple_table",
            target_schema="arc",
            target_table="simple_table",
            merge_keys=("ID",),
            non_key_columns=("Val",),
        )
        run_config = RunModeConfig(mode="delta", advance_checkpoint=True)

        execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_called_once_with("arc", "simple_table", mode="delta")


# ===========================================================================
# full_load mode enables delete detection
# ===========================================================================


class TestFullLoadDeleteDetection:
    """full_load mode (or upstream_is_full_load) keeps detect_deletes=True."""

    @patch("core.archive_merge.DfmManager")
    def test_full_load_mode_keeps_delete_detection(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=6, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = None
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=50, action_row=(40, 8, 2))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="categories",
            target_schema="arc",
            target_table="categories",
            merge_keys=("CatID",),
            non_key_columns=("name",),
            detect_deletes=True,
            upstream_is_full_load=False,
        )
        run_config = RunModeConfig(mode="full_load")

        result = execute_archive_merge(ctx, mssql, config, run_config)

        assert result.detect_deletes is True
        assert result.rows_soft_deleted == 2

    @patch("core.archive_merge.DfmManager")
    def test_upstream_is_full_load_keeps_delete_detection(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=7, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = None
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=25, action_row=(20, 3, 2))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="locations",
            target_schema="arc",
            target_table="locations",
            merge_keys=("LocID",),
            non_key_columns=("name",),
            detect_deletes=True,
            upstream_is_full_load=True,
        )
        run_config = RunModeConfig(mode="delta")

        result = execute_archive_merge(ctx, mssql, config, run_config)

        assert result.detect_deletes is True
        assert result.rows_soft_deleted == 2

    @patch("core.archive_merge.DfmManager")
    def test_delta_mode_without_full_load_disables_detection(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=8, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = None
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(10, 5, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="shipments",
            target_schema="arc",
            target_table="shipments",
            merge_keys=("ShipID",),
            non_key_columns=("Dest",),
            detect_deletes=True,
            upstream_is_full_load=False,
        )
        run_config = RunModeConfig(mode="delta")

        result = execute_archive_merge(ctx, mssql, config, run_config)

        assert result.detect_deletes is False


# ===========================================================================
# Period mode — verify it functions like delta but explicit date bounds
# ===========================================================================


class TestPeriodMode:
    """period mode still advances checkpoint when advance_checkpoint=True."""

    @patch("core.archive_merge.DfmManager")
    def test_period_mode_advances_checkpoint(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=9, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-03-31 23:59:59"
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(20, 0, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="invoices",
            target_schema="arc",
            target_table="invoices",
            merge_keys=("InvID",),
            non_key_columns=("Total",),
            flow_start_key=("ext", "dfm_flow_start"),
        )
        run_config = RunModeConfig(
            mode="period",
            date_from="2025-03-01",
            date_to="2025-03-31",
            advance_checkpoint=True,
        )

        result = execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_called_once_with(
            "arc", "invoices", value="2025-03-31 23:59:59", mode="period"
        )
        assert result.new_checkpoint == "2025-03-31 23:59:59"

    @patch("core.archive_merge.DfmManager")
    def test_period_mode_no_advance(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=10, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-03-31 23:59:59"
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(20, 0, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="invoices",
            target_schema="arc",
            target_table="invoices",
            merge_keys=("InvID",),
            non_key_columns=("Total",),
            flow_start_key=("ext", "dfm_flow_start"),
        )
        run_config = RunModeConfig(
            mode="period",
            date_from="2025-03-01",
            date_to="2025-03-31",
            advance_checkpoint=False,
        )

        execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_not_called()


# ===========================================================================
# ArchiveMergeConfig __post_init__ validation
# ===========================================================================


class TestArchiveMergeConfigValidation:
    """ArchiveMergeConfig rejects bare strings for tuple fields."""

    def test_merge_keys_as_string_raises(self):
        with pytest.raises(TypeError, match="merge_keys must be a tuple"):
            ArchiveMergeConfig(
                source_schema="ext",
                source_table="t",
                target_schema="arc",
                target_table="t",
                merge_keys="DW_EK_SpendAnalysisLine",
                non_key_columns=("Col1",),
            )

    def test_non_key_columns_as_string_raises(self):
        with pytest.raises(TypeError, match="non_key_columns must be a tuple"):
            ArchiveMergeConfig(
                source_schema="ext",
                source_table="t",
                target_schema="arc",
                target_table="t",
                merge_keys=("ID",),
                non_key_columns="SingleColumn",
            )

    def test_valid_tuples_accepted(self):
        cfg = ArchiveMergeConfig(
            source_schema="ext",
            source_table="t",
            target_schema="arc",
            target_table="t",
            merge_keys=("ID",),
            non_key_columns=("A", "B"),
        )
        assert cfg.merge_keys == ("ID",)
        assert cfg.non_key_columns == ("A", "B")

    def test_multi_element_tuples_accepted(self):
        cfg = ArchiveMergeConfig(
            source_schema="ext",
            source_table="t",
            target_schema="arc",
            target_table="t",
            merge_keys=("K1", "K2", "K3"),
            non_key_columns=("C1", "C2"),
        )
        assert len(cfg.merge_keys) == 3

    def test_error_message_includes_suggestion(self):
        with pytest.raises(TypeError, match=r"trailing comma"):
            ArchiveMergeConfig(
                source_schema="ext",
                source_table="t",
                target_schema="arc",
                target_table="t",
                merge_keys="MyKey",
                non_key_columns=("Col1",),
            )


# ===========================================================================
# validate_checkpoint_value — shared utility
# ===========================================================================


class TestValidateCheckpointValue:
    """validate_checkpoint_value accepts datetime/int and rejects other formats."""

    @pytest.mark.parametrize(
        "value",
        [
            "2025-01-01",
            "2025-12-31",
            "2025-01-01 00:00:00",
            "2025-06-08 14:30:00",
            "2025-01-01T12:00:00",
            "2025-01-01T12:00:00.123",
            "2025-01-01T12:00:00.123456",
            "2025-01-01 12:00",
        ],
    )
    def test_valid_datetimes(self, value):
        assert validate_checkpoint_value(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            "0",
            "1",
            "12345",
            "999999999",
            "-1",
            "-12345",
        ],
    )
    def test_valid_integers(self, value):
        assert validate_checkpoint_value(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            "'; DROP TABLE arc.test; --",
            "2025-01-01' OR 1=1--",
            "abc",
            "not_a_date",
            "",
            " ",
            "12.5",
            "2025/01/01",
            "2025-01-01; DROP TABLE x",
        ],
    )
    def test_invalid_filter_values_raise(self, value):
        with pytest.raises(ValueError, match="Invalid checkpoint value"):
            validate_checkpoint_value(value)


# ===========================================================================
# Error handling — fail_dfm_run called on exception
# ===========================================================================


class TestErrorHandling:
    """On MERGE failure, dfm.fail_dfm_run is called and exception re-raised."""

    @patch("core.archive_merge.DfmManager")
    def test_exception_calls_fail_dfm_run(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=11, checkpoint=None)
        MockDfmManager.return_value = mock_dfm

        mssql = MagicMock()
        cursor_mock = MagicMock()
        cursor_mock.execute.side_effect = RuntimeError("Connection lost")
        conn_mock = MagicMock()
        conn_mock.cursor.return_value = cursor_mock

        @contextmanager
        def _get_conn():
            yield conn_mock

        mssql.get_connection = _get_conn
        ctx = _make_context()

        with pytest.raises(RuntimeError, match="Connection lost"):
            execute_archive_merge(ctx, mssql, BASE_CONFIG, RunModeConfig(mode="delta"))

        mock_dfm.fail_dfm_run.assert_called_once_with(11, error_message="Connection lost")


# ===========================================================================
# Mode validation (strict RunModeConfig)
# ===========================================================================


class TestRunModeConfigValidation:
    """RunModeConfig now enforces strict mode constraints."""

    def test_invalid_mode_raises(self):
        with pytest.raises(Exception, match="Invalid mode"):
            RunModeConfig(mode="typo_mode")

    def test_ids_without_id_filter_values_or_filter_table_raises(self):
        with pytest.raises(Exception, match="id_filter_values or id_filter_table"):
            RunModeConfig(mode="ids", id_filter_column="ID")

    def test_ids_without_id_filter_column_raises(self):
        with pytest.raises(Exception, match="id_filter_column"):
            RunModeConfig(mode="ids", id_filter_values=["A"])

    def test_period_without_date_from_raises(self):
        with pytest.raises(Exception, match="date_from"):
            RunModeConfig(mode="period")

    def test_period_with_only_date_from_is_valid(self):
        cfg = RunModeConfig(mode="period", date_from="2025-01-01")
        assert cfg.date_from == "2025-01-01"
        assert cfg.date_to is None

    def test_valid_delta_mode(self):
        cfg = RunModeConfig(mode="delta")
        assert cfg.mode == "delta"

    def test_valid_full_load_mode(self):
        cfg = RunModeConfig(mode="full_load")
        assert cfg.mode == "full_load"

    def test_valid_ids_mode(self):
        cfg = RunModeConfig(mode="ids", id_filter_column="PK", id_filter_values=["1", "2"])
        assert cfg.mode == "ids"

    def test_valid_period_mode_with_both_dates(self):
        cfg = RunModeConfig(mode="period", date_from="2025-01-01", date_to="2025-01-31")
        assert cfg.date_from == "2025-01-01"
        assert cfg.date_to == "2025-01-31"


# ===========================================================================
# should_advance_checkpoint — mode-dependent defaults
# ===========================================================================


class TestShouldAdvanceCheckpoint:
    """should_advance_checkpoint resolves None → mode-appropriate default."""

    def test_delta_default_advances(self):
        cfg = RunModeConfig(mode="delta")
        assert cfg.should_advance_checkpoint is True

    def test_full_load_default_advances(self):
        cfg = RunModeConfig(mode="full_load")
        assert cfg.should_advance_checkpoint is True

    def test_period_default_does_not_advance(self):
        cfg = RunModeConfig(mode="period", date_from="2025-01-01")
        assert cfg.should_advance_checkpoint is False

    def test_ids_default_does_not_advance(self):
        cfg = RunModeConfig(mode="ids", id_filter_column="ID", id_filter_values=["X"])
        assert cfg.should_advance_checkpoint is False

    def test_period_explicit_true_advances(self):
        cfg = RunModeConfig(mode="period", date_from="2025-01-01", advance_checkpoint=True)
        assert cfg.should_advance_checkpoint is True

    def test_ids_explicit_true_advances(self):
        cfg = RunModeConfig(
            mode="ids", id_filter_column="ID", id_filter_values=["X"], advance_checkpoint=True
        )
        assert cfg.should_advance_checkpoint is True

    def test_delta_explicit_false_blocks(self):
        cfg = RunModeConfig(mode="delta", advance_checkpoint=False)
        assert cfg.should_advance_checkpoint is False

    @patch("core.archive_merge.DfmManager")
    def test_period_mode_default_none_blocks_checkpoint_in_merge(self, MockDfmManager):
        """period mode with default advance_checkpoint=None blocks advancement."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=20, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 12:00:00"
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(10, 0, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="enc",
            target_schema="arc",
            target_table="enc",
            merge_keys=("EK",),
            non_key_columns=("Col1",),
            flow_start_key=("ext", "flow_start"),
        )
        run_config = RunModeConfig(mode="period", date_from="2025-01-01", date_to="2025-06-01")

        execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_not_called()

    @patch("core.archive_merge.DfmManager")
    def test_ids_mode_default_none_blocks_checkpoint_in_merge(self, MockDfmManager):
        """ids mode with default advance_checkpoint=None blocks advancement."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=21, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 12:00:00"
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(3, 0, 0))
        ctx = _make_context()

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="enc",
            target_schema="arc",
            target_table="enc",
            merge_keys=("EK",),
            non_key_columns=("Col1",),
            flow_start_key=("ext", "flow_start"),
        )
        run_config = RunModeConfig(mode="ids", id_filter_column="EK", id_filter_values=["A", "B"])

        execute_archive_merge(ctx, mssql, config, run_config)

        mock_dfm.set_checkpoint.assert_not_called()
