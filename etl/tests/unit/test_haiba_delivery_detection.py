# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for HAIBA delivery detection.

The dangerous failure here is not a missed skip — it is a PARTIAL ext: the
archive merges with detect_deletes on an unscoped NOT MATCHED BY SOURCE, so an
ext that carries only the types with a new delivery soft-deletes every other
type's history. The detection must therefore be all-or-nothing per ext table,
and these tests pin that contract on both sides of the manifest.
"""

import os
from datetime import date, datetime
from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.resources import FileShareResource
from DSI.connectors.delivery_detection import delivery_identity
from DSI.csv_files import haiba, haiba_archive
from DSI.csv_files.haiba import HaibaExtractConfig, file_mtime
from tests.unit.test_haiba_extract_guards import _AGG_HEADER, _write

pytestmark = pytest.mark.unit


def _write_delivery(agg_dir, hai_type: str, delivery: str):
    path = agg_dir / f"HAIBA_{hai_type}_agg_RegS_{delivery}.csv"
    _write(path, _AGG_HEADER, [f"{hai_type};2024-05-01;Kontaktansvar;;1083;;;;;;;1;2.5"])
    return path


class TestDetectionIsOffForDeliberateReloads:
    def test_on_for_a_normal_scheduled_run(self):
        assert haiba.detection_enabled(None, False) is True

    def test_off_when_a_delivery_is_pinned(self):
        """Detection on a pinned run would skip the pinned delivery itself."""
        assert haiba.detection_enabled(date(2025, 12, 27), False) is False

    def test_off_when_the_operator_forces_a_reload(self):
        assert haiba.detection_enabled(None, True) is False


class TestPendingTypes:
    """The identity comparison itself is pinned in test_delivery_detection.py;
    these cover only what pending_types adds: the per-type fan-out."""

    def test_checkpoints_are_per_type(self):
        """BACT being up to date must not hide a pending UTI delivery."""
        current = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 15, 7, 0, 0))
        new = delivery_identity(date(2026, 8, 22), datetime(2026, 8, 22, 7, 0, 0))
        pending = haiba.pending_types(
            {"BACT": current, "UTI": new}, {"BACT": current, "UTI": current}
        )
        assert pending == ["UTI"]

    def test_a_type_with_no_checkpoint_is_pending(self):
        """A brand-new HAI type is loaded without any configuration."""
        identity = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 15, 7, 0, 0))
        assert haiba.pending_types({"CDI_NEW": identity}, {"UTI": identity}) == ["CDI_NEW"]


class TestUnitAndIdentityFormat:
    def test_unit_is_hai_type_then_kind(self):
        """The persisted DFMState key: swapping the parts would orphan every
        existing checkpoint and re-ingest everything."""
        assert haiba.haiba_delivery_unit("SSI_HIP", "cases") == "haiba_leverance_SSI_HIP_cases"
        assert haiba.haiba_delivery_unit("BACT", "agg") == "haiba_leverance_BACT_agg"

    def test_file_mtime_is_truncated_to_whole_seconds(self, tmp_path):
        """Rounding instead of truncating would make the archive stamp a
        different identity than the extract compared against."""
        path = tmp_path / "f.csv"
        path.write_text("x")
        os.utime(path, (1755241200.987654, 1755241200.987654))
        stamp = file_mtime(str(path))
        assert stamp.microsecond == 0
        assert stamp == datetime.fromtimestamp(1755241200)


class TestExtractShortCircuit:
    """_extract_schema against a tmp share, with the DB side monkeypatched."""

    def _setup(self, tmp_path, monkeypatch):
        agg_dir = tmp_path / "HAIBAagg"
        agg_dir.mkdir()
        written = {"rows": [], "manifest": [], "ddl": []}

        def _capture_ext(conn, schema, rows, ctx):
            written["rows"].extend(rows)
            return len(written["rows"])

        monkeypatch.setattr(haiba, "_write_to_ext_table", _capture_ext)
        monkeypatch.setattr(haiba, "_arc_active_counts", lambda conn, schema: {})
        monkeypatch.setattr(
            haiba,
            "_write_manifest_rows",
            lambda conn, schema, files, mtimes: written["manifest"].extend(sorted(files)),
        )
        monkeypatch.setattr(
            haiba, "execute_statements", lambda conn, sql, **kw: written["ddl"].append(sql)
        )
        return agg_dir, written

    def test_no_new_delivery_skips_the_read_and_leaves_ext_alone(self, tmp_path, monkeypatch):
        agg_dir, written = self._setup(tmp_path, monkeypatch)
        path = _write_delivery(agg_dir, "UTI", "2026-08-15")
        identity = delivery_identity(date(2026, 8, 15), file_mtime(str(path)))
        monkeypatch.setattr(haiba, "_read_checkpoints", lambda *a, **k: {"UTI": identity})

        result = haiba._extract_schema(
            MagicMock(),
            MagicMock(),
            FileShareResource(base_dir=str(tmp_path)),
            haiba.AGG_INFEKTION,
            HaibaExtractConfig(),
        )

        assert result.metadata["besluttet"].value == "intet nyt"
        assert result.metadata["row_count"].value == 0
        assert written["rows"] == []
        assert written["manifest"] == []
        # The skip must still EMPTY the manifest: a standing manifest from the
        # previous run would make the archive re-merge and re-stamp it.
        assert any("leverance_manifest" in sql for sql in written["ddl"])

    def test_one_new_type_reloads_every_type(self, tmp_path, monkeypatch):
        """THE archive invariant: a partial ext must never arise from the
        short-circuit. BACT is current, UTI is new — both must be streamed."""
        agg_dir, written = self._setup(tmp_path, monkeypatch)
        bact = _write_delivery(agg_dir, "BACT", "2026-08-15")
        _write_delivery(agg_dir, "UTI", "2026-08-22")
        bact_identity = delivery_identity(date(2026, 8, 15), file_mtime(str(bact)))
        old_uti = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 15, 7, 0, 0))
        monkeypatch.setattr(
            haiba,
            "_read_checkpoints",
            lambda *a, **k: {"BACT": bact_identity, "UTI": old_uti},
        )

        result = haiba._extract_schema(
            MagicMock(),
            MagicMock(),
            FileShareResource(base_dir=str(tmp_path)),
            haiba.AGG_INFEKTION,
            HaibaExtractConfig(),
        )

        assert result.metadata["besluttet"].value == "indlaest"
        assert result.metadata["nye_leverancer"].value == ["UTI"]
        assert {row["HAI"] for row in written["rows"]} == {"BACT", "UTI"}
        assert written["manifest"] == ["BACT", "UTI"]

    def test_force_reload_ignores_the_checkpoint(self, tmp_path, monkeypatch):
        agg_dir, written = self._setup(tmp_path, monkeypatch)
        _write_delivery(agg_dir, "UTI", "2026-08-15")

        def _fail(*a, **k):
            raise AssertionError("force_reload must not consult the checkpoint")

        monkeypatch.setattr(haiba, "_read_checkpoints", _fail)

        result = haiba._extract_schema(
            MagicMock(),
            MagicMock(),
            FileShareResource(base_dir=str(tmp_path)),
            haiba.AGG_INFEKTION,
            HaibaExtractConfig(force_reload=True),
        )

        assert result.metadata["besluttet"].value == "indlaest"
        assert {row["HAI"] for row in written["rows"]} == {"UTI"}

    def test_uncheckpointable_hai_type_fails_before_anything_loads(self, tmp_path, monkeypatch):
        """A type name delivery_unit refuses must fail up front — failing at
        the archive's stamping step instead would leave the asset red with the
        rows already merged. force_reload proves the guard does not hide
        behind the detection branch."""
        agg_dir, written = self._setup(tmp_path, monkeypatch)
        _write_delivery(agg_dir, "SSI-HIP", "2026-08-15")

        with pytest.raises(ValueError, match="SSI-HIP"):
            haiba._extract_schema(
                MagicMock(),
                MagicMock(),
                FileShareResource(base_dir=str(tmp_path)),
                haiba.AGG_INFEKTION,
                HaibaExtractConfig(force_reload=True),
            )
        assert written["rows"] == []
        assert written["manifest"] == []


class TestArchiveSideOfTheContract:
    def _manifest_entry(self):
        return haiba_archive.HaibaDeliveryManifest(
            hai_type="UTI",
            kind="agg",
            filnavn="HAIBA_UTI_agg_RegS_2026-08-15.csv",
            leverance_dato=date(2026, 8, 15),
            fil_mtime=datetime(2026, 8, 15, 7, 0, 0),
        )

    def _setup_merge(self, monkeypatch):
        """Stub the merge path around a real advance_delivery_checkpoints."""
        monkeypatch.setattr(
            haiba_archive, "read_manifest", lambda mssql, schema: {"UTI": self._manifest_entry()}
        )
        monkeypatch.setattr(
            haiba_archive, "guard_delivery_direction", lambda *a, **k: ("2026-08-15", "2026-08-08")
        )
        monkeypatch.setattr(haiba_archive, "capture_agg_revisions", lambda *a, **k: 0)
        monkeypatch.setattr(haiba_archive, "execute_archive_merge", lambda *a, **k: MagicMock())
        monkeypatch.setattr(haiba_archive, "build_archive_metadata", lambda *a, **k: {})
        monkeypatch.setattr(haiba_archive, "_record_deliveries", lambda *a, **k: None)
        dfm = MagicMock()
        monkeypatch.setattr(haiba_archive, "DfmManager", lambda *a, **k: dfm)
        return dfm

    def test_empty_manifest_is_a_declared_no_op(self, monkeypatch):
        """No merge, no checkpoint movement — re-merging whatever ext still
        holds would re-present a stale snapshot as a fresh delivery."""
        monkeypatch.setattr(haiba_archive, "read_manifest", lambda mssql, schema: {})
        merged: list[str] = []
        monkeypatch.setattr(
            haiba_archive, "execute_archive_merge", lambda *a, **k: merged.append("merge")
        )
        dfm = MagicMock()
        dfm.start_dfm_run.return_value = MagicMock(log_id=1)
        monkeypatch.setattr(haiba_archive, "DfmManager", lambda *a, **k: dfm)

        result = haiba_archive._run_archive(
            MagicMock(),
            MagicMock(),
            haiba_archive.HaibaArchiveConfig(),
            haiba.AGG_INFEKTION,
            haiba_archive.AGG_INFEKTION_CONFIG,
        )

        assert result.metadata["besluttet"].value == "sprunget over"
        assert merged == []
        dfm.set_checkpoint.assert_not_called()
        dfm.complete_dfm_run.assert_called_once()

    def test_merge_advances_the_checkpoint_from_the_manifest(self, monkeypatch):
        """Reader and writer must agree on unit and identity — a mismatch means
        the extract either never skips or skips forever."""
        dfm = self._setup_merge(monkeypatch)

        result = haiba_archive._run_archive(
            MagicMock(),
            MagicMock(),
            haiba_archive.HaibaArchiveConfig(),
            haiba.AGG_INFEKTION,
            haiba_archive.AGG_INFEKTION_CONFIG,
        )

        dfm.set_checkpoint.assert_called_once_with(
            "dfm",
            "haiba_leverance_UTI_agg",
            value="2026-08-15|2026-08-15T07:00:00",
            mode="delta",
            target_type="archive",
        )
        assert result.metadata["besluttet"].value == "merget"

    def test_advance_checkpoint_false_blocks_the_stamping(self, monkeypatch):
        """The dry-run contract: advance_checkpoint=False must leave the
        delivery checkpoints unmoved, or the next extract skips the delivery."""
        dfm = self._setup_merge(monkeypatch)

        haiba_archive._run_archive(
            MagicMock(),
            MagicMock(),
            haiba_archive.HaibaArchiveConfig(advance_checkpoint=False),
            haiba.AGG_INFEKTION,
            haiba_archive.AGG_INFEKTION_CONFIG,
        )

        dfm.set_checkpoint.assert_not_called()

    def test_backfill_does_not_advance_the_checkpoint(self, monkeypatch):
        """Regressing a checkpoint to the pinned delivery would make the next
        run re-ingest the current one."""
        entry = self._manifest_entry()
        monkeypatch.setattr(haiba_archive, "read_manifest", lambda mssql, schema: {"UTI": entry})
        monkeypatch.setattr(
            haiba_archive, "guard_delivery_direction", lambda *a, **k: ("2026-08-15", "2026-08-22")
        )
        monkeypatch.setattr(
            haiba_archive, "_run_backfill", lambda *a, **k: dg.MaterializeResult(metadata={})
        )
        dfm = MagicMock()
        monkeypatch.setattr(haiba_archive, "DfmManager", lambda *a, **k: dfm)

        haiba_archive._run_archive(
            MagicMock(),
            MagicMock(),
            haiba_archive.HaibaArchiveConfig(backfill_historical=True),
            haiba.AGG_INFEKTION,
            haiba_archive.AGG_INFEKTION_CONFIG,
        )

        dfm.set_checkpoint.assert_not_called()
