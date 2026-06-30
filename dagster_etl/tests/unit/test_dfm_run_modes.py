# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for DFM run modes: build_oracle_where() and build_archive_merge_sql().

Tests the WHERE-clause generation for all RunModeConfig modes:
  - full_load: no WHERE clause
  - delta (date): lower-bound with TO_DATE
  - delta (date + upper_bound): lower + upper bound window
  - delta (lookback): subtracts INTERVAL from checkpoint
  - delta (first run / no checkpoint): no WHERE clause
  - period: date range with >= / < + 1
  - ids (inline values): IN (...) list
  - ids (filter table): IN (SELECT ... FROM ...)

Also tests the MERGE SQL builder:
  - basic upsert (no change guard)
  - update_only_on_change (NULL-safe change detection)
  - detect_deletes (soft-delete clause)
  - insert_only strategy
"""

import pytest
from core.archive_merge import build_archive_merge_sql
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.oracle.ifdb_extract import build_oracle_where

# ===========================================================================
# Fixtures
# ===========================================================================

DELTA_COL = "IFDB_MODIFICATION_DATETIME"

_ORACLE_EXTRACT_CFG = DfmExtractConfig(
    source_schema="IFDB",
    source_table="ENCOUNTER",
    target_table="encounter_ifdb",
    delta_column=DELTA_COL,
    checkpoint_type="datetime",
    checkpoint_from=("arc", "encounter_ifdb"),
    description="Test fixture for Oracle WHERE builder tests.",
)


def _cfg(mode: str = "delta", **kwargs) -> RunModeConfig:
    """Shorthand for creating a RunModeConfig with overrides."""
    return RunModeConfig(mode=mode, **kwargs)


def _arc_cfg(**overrides) -> ArchiveMergeConfig:
    """Minimal ArchiveMergeConfig for MERGE SQL tests."""
    defaults = dict(
        source_schema="ext",
        source_table="test_src",
        target_schema="arc",
        target_table="test_tgt",
        merge_keys=("ID",),
        non_key_columns=("COL_A", "COL_B"),
    )
    defaults.update(overrides)
    return ArchiveMergeConfig(**defaults)


# ===========================================================================
# full_load
# ===========================================================================


class TestFullLoadMode:
    """full_load returns no WHERE clause — extracts everything."""

    def test_returns_none(self):
        result = build_oracle_where(_cfg("full_load"), "2025-01-01 00:00:00", _ORACLE_EXTRACT_CFG)
        assert result is None

    def test_ignores_checkpoint(self):
        result = build_oracle_where(
            _cfg("full_load"), "2025-06-01 12:00:00.123456", _ORACLE_EXTRACT_CFG
        )
        assert result is None

    def test_ignores_upper_bound(self):
        result = build_oracle_where(
            _cfg("full_load"),
            "2025-01-01 00:00:00",
            _ORACLE_EXTRACT_CFG,
            upper_bound="2025-06-01 00:00:00",
        )
        assert result is None


# ===========================================================================
# delta — date checkpoint
# ===========================================================================


class TestDeltaDateMode:
    """delta mode with a datetime checkpoint produces lower-bound WHERE."""

    def test_basic_checkpoint(self):
        result = build_oracle_where(_cfg("delta"), "2025-05-08 10:30:00", _ORACLE_EXTRACT_CFG)
        assert result is not None
        assert ">" in result
        assert ">=" not in result
        assert "TO_DATE('2025-05-08 10:30:00'" in result
        assert DELTA_COL in result

    def test_strips_fractional_seconds(self):
        result = build_oracle_where(
            _cfg("delta"), "2025-05-08 10:30:00.123456", _ORACLE_EXTRACT_CFG
        )
        assert "2025-05-08 10:30:00'" in result
        assert ".123456" not in result

    def test_no_checkpoint_returns_none(self):
        result = build_oracle_where(_cfg("delta"), None, _ORACLE_EXTRACT_CFG)
        assert result is None

    def test_empty_checkpoint_returns_none(self):
        result = build_oracle_where(_cfg("delta"), "", _ORACLE_EXTRACT_CFG)
        assert result is None

    def test_delta_column_override(self):
        cfg = _cfg("delta", delta_column="MY_CUSTOM_TS")
        result = build_oracle_where(cfg, "2025-01-01 00:00:00", _ORACLE_EXTRACT_CFG)
        assert "MY_CUSTOM_TS" in result
        assert DELTA_COL not in result


# ===========================================================================
# delta — with upper bound (flow-start window)
# ===========================================================================


class TestDeltaWithUpperBound:
    """delta mode with upper_bound creates a bounded window."""

    def test_both_bounds(self):
        result = build_oracle_where(
            _cfg("delta"),
            "2025-05-01 00:00:00",
            _ORACLE_EXTRACT_CFG,
            upper_bound="2025-05-08 12:00:00",
        )
        assert result is not None
        assert " > " in result
        assert " <= " in result
        assert "AND" in result
        assert "2025-05-01 00:00:00" in result
        assert "2025-05-08 12:00:00" in result

    def test_upper_bound_strips_fractional(self):
        result = build_oracle_where(
            _cfg("delta"),
            "2025-05-01 00:00:00",
            _ORACLE_EXTRACT_CFG,
            upper_bound="2025-05-08 12:00:00.999999",
        )
        assert "2025-05-08 12:00:00'" in result
        assert ".999999" not in result

    def test_upper_bound_only_no_checkpoint(self):
        """First run with a flow-start: only upper bound, no lower bound."""
        result = build_oracle_where(
            _cfg("delta"),
            None,
            _ORACLE_EXTRACT_CFG,
            upper_bound="2025-05-08 12:00:00",
        )
        assert result is not None
        assert " <= " in result
        assert " > " not in result

    def test_upper_bound_column_uses_different_column(self):
        """upper_bound_column causes the upper bound to use a different column."""
        cfg = DfmExtractConfig(
            source_schema="IFDB",
            source_table="ENCOUNTER",
            target_table="encounter_ifdb",
            delta_column=DELTA_COL,
            checkpoint_type="datetime",
            upper_bound_column="EPJI_MODIFICATION_DATETIME",
            description="Test with upper_bound_column.",
        )
        result = build_oracle_where(
            _cfg("delta"),
            "2026-05-26 13:00:23",
            cfg,
            upper_bound="2026-05-27 05:00:24",
        )
        assert result is not None
        assert f"{DELTA_COL} > TO_DATE('2026-05-26 13:00:23'" in result
        assert "(EPJI_MODIFICATION_DATETIME <= TO_DATE('2026-05-27 05:00:24'" in result
        assert "OR EPJI_MODIFICATION_DATETIME IS NULL)" in result

    def test_upper_bound_column_not_used_for_lower_bound(self):
        """upper_bound_column does NOT affect the lower bound — delta_column is still used."""
        cfg = DfmExtractConfig(
            source_schema="IFDB",
            source_table="ENCOUNTER",
            target_table="encounter_ifdb",
            delta_column=DELTA_COL,
            checkpoint_type="datetime",
            upper_bound_column="EPJI_MODIFICATION_DATETIME",
            description="Test with upper_bound_column.",
        )
        result = build_oracle_where(
            _cfg("delta"),
            "2026-05-26 13:00:23",
            cfg,
            upper_bound="2026-05-27 05:00:24",
        )
        assert result.startswith(DELTA_COL)
        assert "EPJI_MODIFICATION_DATETIME <= TO_DATE(" in result
        assert "OR EPJI_MODIFICATION_DATETIME IS NULL" in result

    def test_upper_bound_column_without_upper_bound_value(self):
        """When no upper_bound is passed, upper_bound_column has no effect."""
        cfg = DfmExtractConfig(
            source_schema="IFDB",
            source_table="ENCOUNTER",
            target_table="encounter_ifdb",
            delta_column=DELTA_COL,
            checkpoint_type="datetime",
            upper_bound_column="EPJI_MODIFICATION_DATETIME",
            description="Test with upper_bound_column.",
        )
        result = build_oracle_where(
            _cfg("delta"),
            "2026-05-26 13:00:23",
            cfg,
        )
        assert "EPJI_MODIFICATION_DATETIME" not in result
        assert DELTA_COL in result


# ===========================================================================
# delta — lookback
# ===========================================================================


class TestDeltaLookback:
    """delta mode with lookback_days subtracts an INTERVAL."""

    def test_lookback_produces_interval(self):
        cfg = _cfg("delta", lookback_days=3)
        result = build_oracle_where(cfg, "2025-05-08 10:00:00", _ORACLE_EXTRACT_CFG)
        assert result is not None
        assert "INTERVAL '3' DAY" in result
        assert ">" in result

    def test_lookback_zero_is_ignored(self):
        cfg = _cfg("delta", lookback_days=0)
        result = build_oracle_where(cfg, "2025-05-08 10:00:00", _ORACLE_EXTRACT_CFG)
        assert "INTERVAL" not in result

    def test_lookback_with_upper_bound(self):
        cfg = _cfg("delta", lookback_days=7)
        result = build_oracle_where(
            cfg,
            "2025-05-01 00:00:00",
            _ORACLE_EXTRACT_CFG,
            upper_bound="2025-05-08 00:00:00",
        )
        assert "INTERVAL '7' DAY" in result
        assert "<=" in result
        assert "AND" in result


# ===========================================================================
# period
# ===========================================================================


class TestPeriodMode:
    """period mode creates a date range using >= and < + 1."""

    def test_basic_period(self):
        cfg = _cfg("period", date_from="2025-01-01", date_to="2025-01-31")
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert result is not None
        assert ">=" in result
        assert "< TO_DATE('2025-01-31'" in result
        assert "+ 1" in result
        assert "2025-01-01" in result

    def test_period_ignores_checkpoint(self):
        cfg = _cfg("period", date_from="2025-03-01", date_to="2025-03-31")
        result = build_oracle_where(cfg, "some-old-checkpoint", _ORACLE_EXTRACT_CFG)
        assert "some-old-checkpoint" not in result
        assert "2025-03-01" in result

    def test_period_uses_delta_column(self):
        cfg = _cfg("period", date_from="2025-01-01", date_to="2025-12-31")
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert DELTA_COL in result


# ===========================================================================
# ids — inline values
# ===========================================================================


class TestIdsInlineMode:
    """ids mode with id_filter_values creates an IN (...) list."""

    def test_single_id(self):
        cfg = _cfg("ids", id_filter_column="OBJECT_ID", id_filter_values=["ABC123"])
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert result is not None
        assert "OBJECT_ID IN" in result
        assert "'ABC123'" in result

    def test_multiple_ids(self):
        cfg = _cfg("ids", id_filter_column="OBJECT_ID", id_filter_values=["ID1", "ID2", "ID3"])
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert "'ID1'" in result
        assert "'ID2'" in result
        assert "'ID3'" in result

    def test_custom_id_filter_column(self):
        cfg = _cfg("ids", id_filter_column="PATIENT_ID", id_filter_values=["P1"])
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert "PATIENT_ID IN" in result
        assert "OBJECT_ID" not in result

    def test_sql_injection_escaped(self):
        cfg = _cfg("ids", id_filter_column="OBJECT_ID", id_filter_values=["O'Brien"])
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert "O''Brien" in result


# ===========================================================================
# ids — filter table
# ===========================================================================


class TestIdsFilterTableMode:
    """ids mode with id_filter_table creates a subquery IN (SELECT ...)."""

    def test_filter_table(self):
        cfg = _cfg("ids", id_filter_column="OBJECT_ID", id_filter_table="tmp.my_ids")
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert result is not None
        assert "IN" in result
        assert "SELECT flt.OBJECT_ID FROM tmp.my_ids flt" in result

    def test_filter_table_custom_column(self):
        cfg = _cfg("ids", id_filter_table="tmp.patients", id_filter_column="PATIENT_ID")
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert "PATIENT_ID IN" in result
        assert "SELECT flt.PATIENT_ID FROM tmp.patients flt" in result

    def test_filter_table_takes_precedence_over_values(self):
        cfg = _cfg(
            "ids",
            id_filter_column="OBJECT_ID",
            id_filter_table="tmp.ids",
            id_filter_values=["should_not_appear"],
        )
        result = build_oracle_where(cfg, None, _ORACLE_EXTRACT_CFG)
        assert "tmp.ids" in result
        assert "should_not_appear" not in result


# ===========================================================================
# Unknown / fallback mode
# ===========================================================================


class TestUnknownMode:
    """Unknown mode raises a validation error."""

    def test_unknown_mode_raises_validation_error(self):
        with pytest.raises(Exception, match="Invalid mode"):
            _cfg("unknown_mode")


# ===========================================================================
# MERGE SQL — basic upsert
# ===========================================================================


class TestMergeSqlUpsert:
    """build_archive_merge_sql() with default upsert strategy."""

    def test_contains_merge_statement(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "MERGE [arc].[test_tgt] AS tgt" in sql
        assert "USING [ext].[test_src] AS src" in sql

    def test_on_clause_uses_merge_keys(self):
        sql = build_archive_merge_sql(_arc_cfg(merge_keys=("K1", "K2")))
        assert "tgt.[K1] = src.[K1]" in sql
        assert "tgt.[K2] = src.[K2]" in sql

    def test_update_sets_non_key_columns(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "tgt.[COL_A] = src.[COL_A]" in sql
        assert "tgt.[COL_B] = src.[COL_B]" in sql

    def test_update_sets_timestamp(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "DW_Timestamp_Update" in sql
        assert "SYSDATETIME()" in sql

    def test_insert_includes_all_columns(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "[ID]" in sql
        assert "[COL_A]" in sql
        assert "[COL_B]" in sql
        assert "DW_Timestamp_Insert" in sql

    def test_no_change_guard_by_default(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "WHEN MATCHED THEN UPDATE SET" in sql
        assert "WHEN MATCHED AND" not in sql

    def test_no_soft_delete_by_default(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "NOT MATCHED BY SOURCE" not in sql


# ===========================================================================
# MERGE SQL — update_only_on_change
# ===========================================================================


class TestMergeSqlChangeGuard:
    """build_archive_merge_sql() with update_only_on_change=True."""

    def test_has_change_guard_condition(self):
        sql = build_archive_merge_sql(_arc_cfg(update_only_on_change=True))
        assert "WHEN MATCHED AND (" in sql

    def test_uses_null_safe_comparison(self):
        sql = build_archive_merge_sql(_arc_cfg(update_only_on_change=True))
        assert "ISNULL(NULLIF(tgt.[COL_A], src.[COL_A])" in sql
        assert "NULLIF(src.[COL_A], tgt.[COL_A])) IS NOT NULL" in sql

    def test_all_non_key_columns_checked(self):
        sql = build_archive_merge_sql(
            _arc_cfg(
                non_key_columns=("X", "Y", "Z"),
                update_only_on_change=True,
            )
        )
        for col in ("X", "Y", "Z"):
            assert f"NULLIF(tgt.[{col}], src.[{col}])" in sql

    def test_conditions_joined_with_or(self):
        sql = build_archive_merge_sql(
            _arc_cfg(
                non_key_columns=("A", "B"),
                update_only_on_change=True,
            )
        )
        assert " OR " in sql

    def test_still_updates_non_key_columns(self):
        sql = build_archive_merge_sql(_arc_cfg(update_only_on_change=True))
        assert "tgt.[COL_A] = src.[COL_A]" in sql
        assert "tgt.[COL_B] = src.[COL_B]" in sql


# ===========================================================================
# MERGE SQL — detect_deletes (soft-delete)
# ===========================================================================


class TestMergeSqlSoftDelete:
    """build_archive_merge_sql() with detect_deletes=True."""

    def test_has_not_matched_by_source_clause(self):
        sql = build_archive_merge_sql(_arc_cfg(detect_deletes=True))
        assert "WHEN NOT MATCHED BY SOURCE AND tgt.[DW_IsDeleted] = 0 THEN UPDATE SET" in sql

    def test_sets_soft_delete_column(self):
        sql = build_archive_merge_sql(_arc_cfg(detect_deletes=True))
        assert "tgt.[DW_IsDeleted] = 1" in sql

    def test_custom_soft_delete_column(self):
        sql = build_archive_merge_sql(_arc_cfg(detect_deletes=True, soft_delete_column="IsRemoved"))
        assert "tgt.[IsRemoved] = 1" in sql
        assert "AND tgt.[IsRemoved] = 0 THEN UPDATE SET" in sql
        assert "DW_IsDeleted" not in sql

    def test_skips_already_deleted_rows(self):
        """Guard ensures rows already soft-deleted are not re-updated."""
        sql = build_archive_merge_sql(_arc_cfg(detect_deletes=True))
        assert "AND tgt.[DW_IsDeleted] = 0 THEN" in sql

    def test_combined_change_guard_and_soft_delete(self):
        sql = build_archive_merge_sql(_arc_cfg(update_only_on_change=True, detect_deletes=True))
        assert "WHEN MATCHED AND (" in sql
        assert "WHEN NOT MATCHED BY SOURCE AND tgt.[DW_IsDeleted] = 0" in sql


# ===========================================================================
# MERGE SQL — source_delete_column (source-driven DW_IsDeleted)
# ===========================================================================


class TestMergeSqlSourceDeleteColumn:
    """build_archive_merge_sql() with source_delete_column set."""

    def test_update_sets_dw_is_deleted_from_source(self):
        sql = build_archive_merge_sql(_arc_cfg(source_delete_column="DELETED"))
        assert "tgt.[DW_IsDeleted] = CASE WHEN src.[DELETED]" in sql

    def test_insert_includes_dw_is_deleted(self):
        sql = build_archive_merge_sql(_arc_cfg(source_delete_column="DELETED"))
        assert "[DW_IsDeleted]" in sql
        assert "CASE WHEN src.[DELETED] IS NOT NULL AND src.[DELETED] NOT IN ('0', '0.0')" in sql

    def test_source_delete_zero_maps_to_zero(self):
        sql = build_archive_merge_sql(_arc_cfg(source_delete_column="SLETTET"))
        assert "THEN 1 ELSE 0 END" in sql

    def test_no_dw_is_deleted_without_source_delete_column(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "CASE WHEN src.[" not in sql

    def test_source_delete_with_detect_deletes(self):
        """source_delete_column and detect_deletes can coexist."""
        sql = build_archive_merge_sql(_arc_cfg(source_delete_column="DELETED", detect_deletes=True))
        assert "tgt.[DW_IsDeleted] = CASE WHEN src.[DELETED]" in sql
        assert "WHEN NOT MATCHED BY SOURCE" in sql

    def test_custom_soft_delete_column_name(self):
        sql = build_archive_merge_sql(
            _arc_cfg(source_delete_column="DELETED", soft_delete_column="IsRemoved")
        )
        assert "tgt.[IsRemoved] = CASE WHEN src.[DELETED]" in sql
        assert "[IsRemoved]" in sql


# ===========================================================================
# MERGE SQL — insert_only
# ===========================================================================


class TestMergeSqlInsertOnly:
    """build_archive_merge_sql() with strategy='insert_only'."""

    def test_uses_insert_not_merge(self):
        sql = build_archive_merge_sql(_arc_cfg(strategy="insert_only"))
        assert "INSERT INTO [arc].[test_tgt]" in sql
        assert "MERGE" not in sql

    def test_includes_all_columns(self):
        sql = build_archive_merge_sql(_arc_cfg(strategy="insert_only"))
        assert "[ID]" in sql
        assert "[COL_A]" in sql
        assert "[COL_B]" in sql

    def test_includes_timestamps(self):
        sql = build_archive_merge_sql(_arc_cfg(strategy="insert_only"))
        assert "DW_Timestamp_Insert" in sql
        assert "DW_Timestamp_Update" in sql
        assert "SYSDATETIME()" in sql

    def test_insert_only_ignores_track_actions(self):
        sql = build_archive_merge_sql(_arc_cfg(strategy="insert_only"), track_actions=True)
        assert "@_merge_actions" not in sql
        assert "OUTPUT $action" not in sql

    def test_insert_only_with_empty_merge_keys(self):
        """insert_only needs no merge_keys; columns live in non_key_columns and
        are still INSERTed (no MERGE/ON clause)."""
        cfg = _arc_cfg(
            strategy="insert_only",
            merge_keys=(),
            non_key_columns=("prrowid", "COL_A"),
        )
        sql = build_archive_merge_sql(cfg)
        assert "INSERT INTO [arc].[test_tgt]" in sql
        assert "MERGE" not in sql
        assert "[prrowid]" in sql
        assert "[COL_A]" in sql


class TestArchiveMergeConfigValidation:
    """__post_init__ guards on ArchiveMergeConfig."""

    def test_upsert_requires_non_empty_merge_keys(self):
        with pytest.raises(ValueError, match="requires non-empty merge_keys"):
            _arc_cfg(strategy="upsert", merge_keys=())

    def test_insert_only_allows_empty_merge_keys(self):
        # Should not raise.
        _arc_cfg(strategy="insert_only", merge_keys=(), non_key_columns=("prrowid",))


# ===========================================================================
# MERGE SQL — track_actions (OUTPUT $action)
# ===========================================================================


class TestMergeSqlTrackActions:
    """build_archive_merge_sql() with track_actions=True."""

    def test_declares_table_variable(self):
        sql = build_archive_merge_sql(_arc_cfg(), track_actions=True)
        assert "DECLARE @_merge_actions TABLE" in sql

    def test_has_output_clause(self):
        sql = build_archive_merge_sql(_arc_cfg(), track_actions=True)
        assert "OUTPUT $action INTO @_merge_actions" in sql

    def test_has_summary_select_without_soft_delete(self):
        """Without detect_deletes, soft_deleted column is always 0."""
        sql = build_archive_merge_sql(_arc_cfg(), track_actions=True)
        assert "'INSERT'" in sql
        assert "'UPDATE'" in sql
        assert "    0\n" in sql

    def test_still_contains_merge_body(self):
        sql = build_archive_merge_sql(_arc_cfg(), track_actions=True)
        assert "MERGE [arc].[test_tgt] AS tgt" in sql
        assert "USING [ext].[test_src] AS src" in sql

    def test_no_tracking_by_default(self):
        sql = build_archive_merge_sql(_arc_cfg())
        assert "@_merge_actions" not in sql
        assert "OUTPUT $action" not in sql

    def test_soft_delete_with_tracking_uses_is_soft_delete_column(self):
        """With detect_deletes, OUTPUT tracks soft-delete via inserted/deleted comparison."""
        sql = build_archive_merge_sql(_arc_cfg(detect_deletes=True), track_actions=True)
        assert "WHEN NOT MATCHED BY SOURCE" in sql
        assert "[is_soft_delete] BIT" in sql
        assert "inserted.[DW_IsDeleted] = 1" in sql
        assert "deleted.[DW_IsDeleted] = 0" in sql
        assert "[is_soft_delete] = 0 THEN 1 ELSE 0 END" in sql
        assert "[is_soft_delete] = 1 THEN 1 ELSE 0 END" in sql

    def test_source_delete_column_also_uses_soft_delete_tracking(self):
        """source_delete_column enables the soft-delete tracking path."""
        sql = build_archive_merge_sql(_arc_cfg(source_delete_column="DELETED"), track_actions=True)
        assert "[is_soft_delete] BIT" in sql
        assert "inserted.[DW_IsDeleted] = 1" in sql
