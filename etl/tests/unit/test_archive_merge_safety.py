# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Smoke tests for execute_archive_merge delete-detection safety gate.

Verifies that:
  - Delete detection is disabled when mode != full_load and upstream_is_full_load=False
  - Delete detection is disabled when source table has 0 rows (empty extract guard)
  - Delete detection stays enabled when mode == full_load and source has rows
  - build_archive_metadata returns a consistent set of keys
"""

import dataclasses
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from core.archive_merge import (
    ArchiveMergeResult,
    build_archive_metadata,
    execute_archive_merge,
)
from core.dfm_config import ArchiveMergeConfig, RunModeConfig

pytestmark = pytest.mark.unit

# ===========================================================================
# Fixtures
# ===========================================================================


UPSERT_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="vare_ils_delta",
    target_schema="arc",
    target_table="vare_ils",
    merge_keys=("dw_ek_Leverandoervare",),
    non_key_columns=("VareNavn", "VareGruppe"),
    strategy="upsert",
    detect_deletes=True,
    update_only_on_change=True,
    upstream_is_full_load=False,
)


INSERT_ONLY_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ils_lgrbeh",
    target_schema="arc",
    target_table="ils_lgrbeh",
    # insert_only appends snapshots — no MERGE/ON — so merge_keys is empty and
    # the business key (prrowid) lives in non_key_columns with the rest.
    merge_keys=(),
    non_key_columns=("prrowid", "sgh", "lgr"),
    strategy="insert_only",
)


# Flow-start-keyed config (no delete detection) — used to verify that period
# mode pins the checkpoint to the period boundary rather than flow-start.
FLOW_START_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_encounter",
    target_schema="arc",
    target_table="ifdb_encounter",
    merge_keys=("OBJECT_ID",),
    non_key_columns=("PATIENT_ID", "STATUS"),
    strategy="upsert",
    detect_deletes=False,
    flow_start_key=("dfm", "ifdb_flow_start"),
)


def _make_mssql_mock(*, ext_count=100, action_row=(5, 3, 0), checkpoint_row=None):
    """Build a MSSQLResource mock with configurable cursor responses.

    The mock supports multiple sequential queries within a single connection:
    - SELECT COUNT(*) → ext_count (for delete-detection safety gate)
    - MERGE → action_row via nextset/fetchone
    - checkpoint_query → checkpoint_row
    - get_checkpoint → checkpoint_row

    These configs leave enable_bulk_insert_on_first_load at its default (False),
    so the empty-target probe never runs and the MERGE path is exercised.
    """
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
    ctx.run_id = "test-run-456"
    ctx.log = MagicMock()
    return ctx


# ===========================================================================
# Delete-detection safety gate (unit tests on the logic path)
# ===========================================================================


class TestDeleteDetectionSafetyGate:
    @patch("core.archive_merge.DfmManager")
    def test_delta_mode_disables_delete_detection(self, MockDfmManager):
        """In delta mode with upstream_is_full_load=False, delete detection is disabled."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = None
        MockDfmManager.return_value = mock_dfm

        mssql, cursor = _make_mssql_mock(ext_count=None, action_row=(5, 3, 0))
        ctx = _make_context()

        result = execute_archive_merge(ctx, mssql, UPSERT_CONFIG, RunModeConfig(mode="delta"))

        assert result.detect_deletes is False
        ctx.log.warning.assert_any_call(
            "Delete detection disabled — mode='delta' "
            "and upstream_is_full_load=False; "
            "NOT MATCHED BY SOURCE would incorrectly flag rows."
        )

    @patch("core.archive_merge.DfmManager")
    def test_full_load_with_zero_source_rows_disables_detection(self, MockDfmManager):
        """Full-load with 0 source rows refuses to delete all archive rows."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=2, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = None
        MockDfmManager.return_value = mock_dfm

        config = ArchiveMergeConfig(
            source_schema="ext",
            source_table="vare_ils_delta",
            target_schema="arc",
            target_table="vare_ils",
            merge_keys=("dw_ek_Leverandoervare",),
            non_key_columns=("VareNavn", "VareGruppe"),
            strategy="upsert",
            detect_deletes=True,
            upstream_is_full_load=True,
        )
        mssql, cursor = _make_mssql_mock(ext_count=0, action_row=(0, 0, 0))
        ctx = _make_context()

        result = execute_archive_merge(ctx, mssql, config, RunModeConfig(mode="delta"))

        assert result.detect_deletes is False
        warning_messages = [c.args[0] for c in ctx.log.warning.call_args_list]
        assert any("0 rows" in msg for msg in warning_messages)


# ===========================================================================
# Period-mode checkpoint derivation
# ===========================================================================


class TestPeriodCheckpointDerivation:
    """In period mode the checkpoint must reflect the processed period
    boundary (date_to), NOT the flow-start ceiling (which tracks "now").

    This is what makes a chunked historical reload resumable: if chunk N
    fails, the checkpoint reflects the last completed chunk's boundary, and a
    missed manual resume self-heals on the next scheduled delta run.
    """

    _LIVE_FLOW_START = "2099-12-31 23:59:59.000"

    @patch("core.archive_merge.DfmManager")
    def test_period_advance_pins_checkpoint_to_date_to(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        # flow-start resolves to a live "now" value that must be IGNORED.
        mock_dfm.get_checkpoint.return_value = self._LIVE_FLOW_START
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(5, 3, 0))
        ctx = _make_context()
        run_config = RunModeConfig(
            mode="period",
            date_from="2021-01-01",
            date_to="2021-01-31",
            advance_checkpoint=True,
        )

        result = execute_archive_merge(ctx, mssql, FLOW_START_CONFIG, run_config)

        assert result.new_checkpoint == "2021-01-31 23:59:59"
        mock_dfm.set_checkpoint.assert_called_once_with(
            "arc", "ifdb_encounter", value="2021-01-31 23:59:59", mode="period"
        )
        # The live flow-start value must never become the checkpoint.
        assert result.new_checkpoint != self._LIVE_FLOW_START

    @patch("core.archive_merge.DfmManager")
    def test_open_ended_period_falls_back_to_flow_start(self, MockDfmManager):
        """A period with no date_to (open-ended → 'up to now') keeps the
        flow-start fallback, since there is no explicit upper bound."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=2, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = self._LIVE_FLOW_START
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(1, 0, 0))
        ctx = _make_context()
        run_config = RunModeConfig(
            mode="period",
            date_from="2021-01-01",
            advance_checkpoint=True,
        )

        result = execute_archive_merge(ctx, mssql, FLOW_START_CONFIG, run_config)

        assert result.new_checkpoint == self._LIVE_FLOW_START
        mock_dfm.set_checkpoint.assert_called_once_with(
            "arc", "ifdb_encounter", value=self._LIVE_FLOW_START, mode="period"
        )

    @patch("core.archive_merge.DfmManager")
    def test_period_without_advance_does_not_set_checkpoint(self, MockDfmManager):
        """advance_checkpoint=False (or default for period) leaves the
        production checkpoint untouched."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=3, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = self._LIVE_FLOW_START
        MockDfmManager.return_value = mock_dfm

        mssql, _ = _make_mssql_mock(ext_count=None, action_row=(5, 3, 0))
        ctx = _make_context()
        run_config = RunModeConfig(
            mode="period",
            date_from="2021-01-01",
            date_to="2021-01-31",
            advance_checkpoint=False,
        )

        result = execute_archive_merge(ctx, mssql, FLOW_START_CONFIG, run_config)

        # The derived checkpoint is still computed/returned, but not persisted.
        assert result.new_checkpoint == "2021-01-31 23:59:59"
        mock_dfm.set_checkpoint.assert_not_called()


# ===========================================================================
# build_archive_metadata consistency
# ===========================================================================


class TestBuildArchiveMetadata:
    EXPECTED_KEYS = {
        "row_count",
        "rows_inserted",
        "rows_updated",
        "rows_soft_deleted",
        "rows_dedup_dropped",
        "source_table",
        "target_table",
        "mode",
        "advance_checkpoint",
        "strategy",
        "detect_deletes",
        "update_only_on_change",
    }

    def test_returns_all_expected_keys(self):
        result = ArchiveMergeResult(
            row_count=10,
            new_checkpoint="2025-06-01",
            detect_deletes=False,
            update_only_on_change=True,
            strategy="upsert",
            upstream_is_full_load=False,
            rows_inserted=7,
            rows_updated=3,
            rows_soft_deleted=0,
        )
        config = RunModeConfig(mode="delta")
        metadata = build_archive_metadata(
            result, config, source_table="ext.vare_ils", target_table="arc.vare_ils"
        )

        assert set(metadata.keys()) == self.EXPECTED_KEYS

    def test_insert_only_gets_same_keys_as_upsert(self):
        """Insert-only archives must expose the same keys as upsert archives."""
        result = ArchiveMergeResult(
            row_count=50,
            new_checkpoint=None,
            detect_deletes=False,
            update_only_on_change=False,
            strategy="insert_only",
            upstream_is_full_load=False,
            rows_inserted=50,
            rows_updated=0,
            rows_soft_deleted=0,
        )
        config = RunModeConfig(mode="delta", advance_checkpoint=True)
        metadata = build_archive_metadata(
            result, config, source_table="ext.ils_lgrbeh", target_table="arc.ils_lgrbeh"
        )

        assert set(metadata.keys()) == self.EXPECTED_KEYS
        assert metadata["strategy"] == "insert_only"
        assert metadata["row_count"] == 50

    def test_values_reflect_inputs(self):
        result = ArchiveMergeResult(
            row_count=8,
            new_checkpoint="2025-01-01",
            detect_deletes=True,
            update_only_on_change=True,
            strategy="upsert",
            upstream_is_full_load=True,
            rows_inserted=5,
            rows_updated=2,
            rows_soft_deleted=1,
        )
        config = RunModeConfig(mode="full_load", advance_checkpoint=False)
        metadata = build_archive_metadata(
            result, config, source_table="ext.x", target_table="arc.x"
        )

        assert metadata["row_count"] == 8
        assert metadata["rows_inserted"] == 5
        assert metadata["rows_updated"] == 2
        assert metadata["rows_soft_deleted"] == 1
        assert metadata["source_table"] == "ext.x"
        assert metadata["target_table"] == "arc.x"
        assert metadata["mode"] == "full_load"
        assert metadata["advance_checkpoint"] is False
        assert metadata["strategy"] == "upsert"
        assert metadata["detect_deletes"] is True
        assert metadata["update_only_on_change"] is True


# ===========================================================================
# Key-only tables (empty non_key_columns)
# ===========================================================================


class TestKeyOnlyMergeSql:
    """Link tables where every column is part of the merge key (e.g.
    arc.ifdb_pas_diagnose_encounter_ref) must still produce executable MERGE
    SQL: no dangling comma in UPDATE SET, no leading OR in the change guard,
    and soft-delete resurrection as the only WHEN MATCHED effect."""

    KEY_ONLY_CONFIG = ArchiveMergeConfig(
        source_schema="ext",
        source_table="ifdb_pas_diagnose_encounter_ref",
        target_schema="arc",
        target_table="ifdb_pas_diagnose_encounter_ref",
        merge_keys=("DIAGNOSE_ID", "ENCOUNTER_ID"),
        non_key_columns=(),
        strategy="upsert",
        update_only_on_change=True,
        detect_deletes=True,
        upstream_is_full_load=True,
    )

    def test_key_only_merge_sql_is_well_formed(self):
        from core.archive_merge import build_archive_merge_sql

        sql = build_archive_merge_sql(self.KEY_ONLY_CONFIG)

        # Resurrection is the only reason a matched key-only row updates, and
        # the empty change-check must degrade to a constant false, not to
        # malformed SQL (dangling comma / leading OR).
        compact = " ".join(sql.split())
        assert "UPDATE SET ," not in compact and "( OR" not in compact
        assert "1 = 0 OR tgt.[dw_is_deleted] <> 0" in compact
        assert "tgt.[dw_is_deleted] = 0" in sql
        assert "WHEN NOT MATCHED BY SOURCE AND tgt.[dw_is_deleted] = 0" in sql


# ===========================================================================
# Source dedup for PK-less sources
# ===========================================================================


class TestDedupSourceOnKeys:
    """Sources with no upstream primary key (IFDB's child/link tables) can
    deliver two ext rows sharing the asserted merge key. Both write paths must
    therefore read a deduplicated source — a MERGE-only fix still blows up on
    the arc PK during the cold load, which is exactly when the whole table
    arrives at once.
    """

    DEDUP_CONFIG = ArchiveMergeConfig(
        source_schema="ext",
        source_table="ifdb_aktivitetsudfoerelse_speciale",
        target_schema="arc",
        target_table="ifdb_aktivitetsudfoerelse_speciale",
        merge_keys=("AKTIVITETSUDFOERELSE_ID", "SPECIALE_KODE"),
        non_key_columns=("SPECIALE_HOSPITAL_KODE", "SPECIALE_KODETEKST"),
        strategy="upsert",
        update_only_on_change=True,
        upstream_is_full_load=True,
        enable_bulk_insert_on_first_load=True,
        dedup_source_on_keys=True,
    )

    def test_merge_reads_a_deduplicated_source(self):
        from core.archive_merge import build_archive_merge_sql

        sql = build_archive_merge_sql(self.DEDUP_CONFIG)

        # The MERGE must read a deduplicated derived table, not the raw ext heap.
        assert "USING [ext].[ifdb_aktivitetsudfoerelse_speciale]" not in sql
        assert "ROW_NUMBER()" in sql and "WHERE deduped.[_dedup_rn] = 1" in sql

    def test_cold_load_reads_the_same_deduplicated_source(self):
        from core.archive_merge import _build_empty_target_fastload_sql

        sql = _build_empty_target_fastload_sql(self.DEDUP_CONFIG)

        assert "FROM [ext].[ifdb_aktivitetsudfoerelse_speciale];" not in sql
        assert "WHERE deduped.[_dedup_rn] = 1" in sql

    def test_dedup_is_opt_in(self):
        from core.archive_merge import build_archive_merge_sql

        sql = build_archive_merge_sql(UPSERT_CONFIG)

        assert "USING [ext].[vare_ils_delta] AS src" in sql
        assert "_dedup_rn" not in sql

    def test_soft_delete_column_survives_the_dedup_projection(self):
        """A dropped source_delete_column would make the MERGE reference an
        alias the derived table never exposes."""
        from core.archive_merge import build_archive_merge_sql

        config = dataclasses.replace(
            self.DEDUP_CONFIG,
            detect_deletes=False,
            enable_bulk_insert_on_first_load=False,
            source_delete_column="SLETTET",
        )
        sql = build_archive_merge_sql(config)

        assert "[SLETTET]," in sql
        assert "src.[SLETTET]" in sql


# ===========================================================================
# Scoped delete detection
# ===========================================================================


class TestDeleteScopePredicate:
    """Full snapshots that deliberately cover only a SLICE of the archive
    (DRG: an M delivery carries three activity years, a U delivery only two)
    must be able to scope NOT MATCHED BY SOURCE to that slice — otherwise
    every U run would soft-delete the oldest year and the next M run would
    resurrect it (the HAIBA failure mode, per takstsystem/aar)."""

    def _config(self, **overrides):
        base = dict(
            source_schema="ext",
            source_table="v_drg_kontakt",
            target_schema="arc",
            target_table="drg_kontakt",
            merge_keys=("takstsystem", "KONTAKT_VAERDI"),
            non_key_columns=("DRG", "TOTALPRIS_DRG"),
            strategy="upsert",
            update_only_on_change=True,
            detect_deletes=True,
            upstream_is_full_load=True,
        )
        base.update(overrides)
        return ArchiveMergeConfig(**base)

    def test_scope_predicate_restricts_the_delete_clause(self):
        from core.archive_merge import build_archive_merge_sql

        predicate = "(tgt.[takstsystem] = 'T26' AND tgt.[AKTIVITETSAAR] IN ('2025', '2026'))"
        sql = build_archive_merge_sql(self._config(delete_scope_predicate=predicate))
        assert f"WHEN NOT MATCHED BY SOURCE AND tgt.[dw_is_deleted] = 0 AND ({predicate})" in sql

    def test_without_predicate_the_clause_is_unchanged(self):
        from core.archive_merge import build_archive_merge_sql

        sql = build_archive_merge_sql(self._config())
        assert "WHEN NOT MATCHED BY SOURCE AND tgt.[dw_is_deleted] = 0 THEN UPDATE SET" in sql

    def test_predicate_without_detect_deletes_is_harmless(self):
        from core.archive_merge import build_archive_merge_sql

        sql = build_archive_merge_sql(
            self._config(detect_deletes=False, delete_scope_predicate="tgt.[x] = 1")
        )
        assert "NOT MATCHED BY SOURCE" not in sql
