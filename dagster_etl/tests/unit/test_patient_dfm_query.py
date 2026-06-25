# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for Patient_IFDB_dfm._build_patient_query (CPR dedup).

Verifies the whole-archive dedup behaviour: ranking must run over the entire
archive for every CPR present in the run window — not just the windowed rows —
so a touched-but-deleted duplicate cannot win over (and clobber) an active
patient that lies outside the window.
"""

from core.dfm_config import RunModeConfig
from DSA.FHIR.Patient.Patient_IFDB_dfm import _build_patient_query


def _cfg(mode: str = "delta", **kwargs) -> RunModeConfig:
    return RunModeConfig(mode=mode, **kwargs)


class TestBuildPatientQuery:
    def test_ranks_over_whole_archive_not_window(self):
        """The ROW_NUMBER ranking must read the full archive table, joined to
        the windowed CPRs — not a checkpoint-filtered `base` CTE."""
        sql = _build_patient_query("2025-06-01 00:00:00", _cfg("delta"))

        # Windowed CPRs feed a join back to the full archive table.
        assert "WITH windowed_cpr AS" in sql
        assert "INNER JOIN windowed_cpr w ON w.[CPR] = a.[CPR]" in sql
        assert "FROM [arc].[patient_ifdb] a" in sql
        # The ranking is over the joined archive rows `a`, not a windowed CTE.
        assert "PARTITION BY a.[CPR]" in sql

    def test_active_preferred_then_most_recent(self):
        sql = _build_patient_query("2025-06-01 00:00:00", _cfg("delta"))
        assert "CASE WHEN ISNULL(a.[SLETTET], '0') IN ('0', '0.0') THEN 0 ELSE 1 END" in sql
        assert "a.[DW_Timestamp_Update] DESC" in sql
        assert "WHERE _rn = 1" in sql

    def test_windowed_cpr_excludes_null_and_empty(self):
        sql = _build_patient_query("2025-06-01 00:00:00", _cfg("delta"))
        assert "[CPR] IS NOT NULL AND [CPR] != ''" in sql

    def test_no_cpr_rows_taken_from_window_only(self):
        """Rows without a CPR are emitted from the window, never deduped."""
        sql = _build_patient_query("2025-06-01 00:00:00", _cfg("delta"))
        assert "WHERE nc.[CPR] IS NULL OR nc.[CPR] = ''" in sql

    def test_delta_checkpoint_scopes_the_window(self):
        """The windowed-CPR set is checkpoint-filtered (delta), but the ranked
        archive scan is not."""
        sql = _build_patient_query("2025-06-01 00:00:00", _cfg("delta"))
        # Checkpoint comparison appears (window side), targeting the delta column.
        assert "> '2025-06-01 00:00:00'" in sql

    def test_full_load_has_no_checkpoint_filter(self):
        sql = _build_patient_query(None, _cfg("full_load"))
        assert "windowed_cpr" in sql
        assert "2025" not in sql  # no date literal injected in full_load

    def test_ids_mode_propagates_to_window(self):
        sql = _build_patient_query(
            None, _cfg("ids", id_filter_column="CPR", id_filter_values=["A", "B"])
        )
        assert "[CPR] IN (" in sql
        assert "'A'" in sql and "'B'" in sql
