# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM-wiring for the HAIBA assets: every layer logs its run in meta.DFMRunLog.

The failures these guard against are silent: an asset that stops logging
(no timing, no error trail in DFMRunLog), a mode that drifts away from
full_load (a partial ext soft-deletes whole HAI types downstream), or an
ext run that stops borrowing the arc owner's checkpoint. The extract and
rebuild remain log-only — dm owns no checkpoint (TRUNCATE + INSERT has no
delta position), and the delivery watermark lives in meta.HAIBALeverance.
"""

from unittest.mock import MagicMock

import dagster as dg
import DSA.SQL.dm.haiba as dm_haiba_module
import DSA.SQL.dm.haiba_kontakt as kontakt_module
import DSI.csv_files.haiba as ext_module
import pytest
from core.dfm_manager import CHECKPOINT_OWNER_SCHEMAS, SCHEMA_TO_TARGET_TYPE


def _ctx() -> dg.AssetExecutionContext:
    return dg.build_asset_context()


def _dfm(monkeypatch, module) -> MagicMock:
    dfm = MagicMock()
    dfm.start_dfm_run.return_value = MagicMock(log_id=117, checkpoint=None)
    monkeypatch.setattr(module, "DfmManager", lambda *a, **k: dfm)
    return dfm


def _result(rows: int = 42) -> dg.MaterializeResult:
    return dg.MaterializeResult(metadata={"row_count": dg.MetadataValue.int(rows)})


class TestSchemaContract:
    def test_dm_maps_to_datamart(self):
        assert SCHEMA_TO_TARGET_TYPE["dm"] == "datamart"

    def test_dm_is_not_a_checkpoint_owner(self):
        """The rebuild is TRUNCATE + INSERT — there is no delta position to
        own, so a dm run must stay log-only (checkpoint None)."""
        assert "dm" not in CHECKPOINT_OWNER_SCHEMAS


class TestExtractWiring:
    def test_logs_full_load_run_borrowing_arc_checkpoint(self, monkeypatch):
        dfm = _dfm(monkeypatch, ext_module)
        monkeypatch.setattr(ext_module, "_extract_schema", lambda *a, **k: _result(7))

        result = ext_module.ext_haiba_cases_ssi(
            _ctx(), MagicMock(), ext_module.HaibaExtractConfig()
        )

        args, kwargs = dfm.start_dfm_run.call_args
        assert args == ("ext", "haiba_cases_ssi")
        assert kwargs["checkpoint_from"] == ("arc", "haiba_cases_ssi")
        assert kwargs["mode"] == "full_load"
        dfm.complete_dfm_run.assert_called_once_with(117, rows_affected=7)
        assert result.metadata["row_count"].value == 7

    def test_failure_marks_run_failed_and_reraises(self, monkeypatch):
        dfm = _dfm(monkeypatch, ext_module)

        def _boom(*a, **k):
            raise ValueError("no usable delivery")

        monkeypatch.setattr(ext_module, "_extract_schema", _boom)

        with pytest.raises(ValueError, match="no usable delivery"):
            ext_module.ext_haiba_agg_infektion(_ctx(), MagicMock(), ext_module.HaibaExtractConfig())

        dfm.fail_dfm_run.assert_called_once()
        assert "no usable delivery" in dfm.fail_dfm_run.call_args.kwargs["error_message"]
        dfm.complete_dfm_run.assert_not_called()


class TestRebuildWiring:
    def test_logs_full_load_run_without_checkpoint(self, monkeypatch):
        dfm = _dfm(monkeypatch, dm_haiba_module)
        monkeypatch.setattr(dm_haiba_module, "_rebuild", lambda *a, **k: _result(3))

        dm_haiba_module.dm_haiba_agg_infektion(_ctx(), MagicMock())

        args, kwargs = dfm.start_dfm_run.call_args
        assert args == ("dm", "HAIBA_AggInfektion")
        assert kwargs["mode"] == "full_load"
        assert "checkpoint_from" not in kwargs
        dfm.complete_dfm_run.assert_called_once_with(117, rows_affected=3)

    def test_failure_marks_run_failed_and_reraises(self, monkeypatch):
        dfm = _dfm(monkeypatch, dm_haiba_module)

        def _boom(*a, **k):
            raise ValueError("row-count mismatch after rebuild")

        monkeypatch.setattr(dm_haiba_module, "_rebuild", _boom)

        with pytest.raises(ValueError, match="row-count mismatch"):
            dm_haiba_module.dm_haiba_cases_ssi(_ctx(), MagicMock())

        dfm.fail_dfm_run.assert_called_once()
        dfm.complete_dfm_run.assert_not_called()


class TestKontaktWiring:
    def test_logs_against_the_fact_table(self, monkeypatch):
        """Rebuild and stamp log two rows per publish for the same dm table,
        tied together by dagster_run_id — the stamp must not invent a
        separate table identity."""
        dfm = _dfm(monkeypatch, kontakt_module)
        monkeypatch.setattr(
            kontakt_module, "_stamp", lambda *a, **k: dg.MaterializeResult(metadata={})
        )

        kontakt_module.dm_haiba_cases_infektion_kontakt(_ctx(), MagicMock())

        args, kwargs = dfm.start_dfm_run.call_args
        assert args == ("dm", "HAIBA_CasesInfektion")
        assert kwargs["mode"] == "full_load"
        dfm.complete_dfm_run.assert_called_once_with(117)

    def test_failure_marks_run_failed_and_reraises(self, monkeypatch):
        dfm = _dfm(monkeypatch, kontakt_module)

        def _boom(*a, **k):
            raise RuntimeError("update failed")

        monkeypatch.setattr(kontakt_module, "_stamp", _boom)

        with pytest.raises(RuntimeError, match="update failed"):
            kontakt_module.dm_haiba_cases_ssi_kontakt(_ctx(), MagicMock())

        dfm.fail_dfm_run.assert_called_once()
        dfm.complete_dfm_run.assert_not_called()
