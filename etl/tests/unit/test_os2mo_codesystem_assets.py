# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the export and upload steps — with DFM wiring.

Both DFM runs are log-only: `export` and `os2mo` are deliberately absent from
CHECKPOINT_OWNER_SCHEMAS, because a full load every night has no delta position
to hold. And only a REAL upload is logged under `os2mo`: meta.DFMRunLog answers
*did we deliver, and did it work*, and a dry run is not a delivery.
"""

import re
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from core.dfm_manager import CHECKPOINT_OWNER_SCHEMAS, SCHEMA_TO_TARGET_TYPE
from DSO.export.os2mo import codesystem_assets
from DSO.export.os2mo.codesystem_assets import (
    DELIVERY_DFM_SCHEMA,
    EXPORT_DFM_SCHEMA,
    OS2moExportConfig,
    OS2moUploadConfig,
    _build_delivery,
    _upload_delivery,
    csv_preview,
)
from DSO.export.os2mo.codesystems import OKOSTED_EXPORT
from utils.os2mo.csv_format import encode_csv

pytestmark = pytest.mark.unit

MODULE = "DSO.export.os2mo.codesystem_assets"

SOURCE_ROWS = [
    {"code": "RSD", "display": "Example Region", "parent_code": "", "valid_from": "2020-01-01"},
    {"code": "OUH", "display": "OUH", "parent_code": "RSD", "valid_from": "2020-01-01"},
]


def _build(tmp_path, source_rows=SOURCE_ROWS):
    with (
        patch(f"{MODULE}.DfmManager") as dfm_cls,
        patch(f"{MODULE}.read_source_rows", return_value=source_rows),
        patch(f"{MODULE}.write_export_table", side_effect=lambda m, s, rows, b: len(rows)),
    ):
        dfm = dfm_cls.return_value
        dfm.start_dfm_run.return_value = MagicMock(log_id=7)
        output = _build_delivery(
            dg.build_asset_context(),
            OKOSTED_EXPORT,
            MagicMock(),
            OS2moExportConfig(output_dir=str(tmp_path)),
        )
    return output.value, dfm, output.metadata


def _delivery():
    return {
        "filename": "rsyd_okosted_20261006_013000.csv",
        "csv": '"user_key"\r\n"RSD"\r\n',
        "rows": 1,
    }


def _upload(os2mo, **config):
    with patch(f"{MODULE}.DfmManager") as dfm_cls:
        dfm = dfm_cls.return_value
        dfm.start_dfm_run.return_value = MagicMock(log_id=9)
        result = _upload_delivery(
            dg.build_asset_context(),
            OKOSTED_EXPORT,
            _delivery(),
            os2mo,
            MagicMock(),
            OS2moUploadConfig(**config),
        )
    return result, dfm_cls, dfm


class TestReadExportSet:
    def test_codes_without_type_is_counted_and_rest_have_type(self):
        source = [
            {"code": "A", "display": "Rod", "type": "juridisk enhed", "has_properties": 1},
            {"code": "B", "display": "Rod uden type", "parent_code": "A", "has_properties": 1},
        ]
        with patch(f"{MODULE}.read_source_rows", return_value=source):
            export_set = codesystem_assets.read_export_set(MagicMock(), OKOSTED_EXPORT)

        assert export_set.codes_without_type == 1


class TestSchemaRegistration:
    def test_neither_is_a_checkpoint_owner(self):
        assert EXPORT_DFM_SCHEMA not in CHECKPOINT_OWNER_SCHEMAS
        assert DELIVERY_DFM_SCHEMA not in CHECKPOINT_OWNER_SCHEMAS

    def test_both_targets_self_map_so_usp_dfm_reset_needs_no_case_branch(self):
        for schema in (EXPORT_DFM_SCHEMA, DELIVERY_DFM_SCHEMA):
            assert SCHEMA_TO_TARGET_TYPE[schema] == schema


class TestBuild:
    def test_the_file_is_written_and_returned(self, tmp_path):
        delivery, _, _ = _build(tmp_path)

        assert re.fullmatch(r"rsyd_okosted_\d{8}_\d{6}\.csv", delivery["filename"])
        assert delivery["rows"] == 2
        # The local copy carries the delivered name and the delivered bytes.
        on_disk = (tmp_path / delivery["filename"]).read_bytes()
        assert on_disk == encode_csv(delivery["csv"])

    def test_codes_outside_the_tree_are_counted(self, tmp_path):
        cycle = [
            {"code": "A", "display": "A", "parent_code": "B"},
            {"code": "B", "display": "B", "parent_code": "A"},
        ]
        delivery, _, metadata = _build(tmp_path, source_rows=SOURCE_ROWS + cycle)

        assert delivery["rows"] == 2
        assert metadata["codes_unreachable"].value == 2

    def test_codes_gone_from_the_source_are_left_out_and_counted(self, tmp_path):
        gone = [{"code": "OLD", "display": "Old", "parent_code": "", "has_properties": 0}]
        delivery, _, metadata = _build(tmp_path, source_rows=SOURCE_ROWS + gone)

        assert "OLD" not in delivery["csv"]
        assert metadata["codes_gone"].value == 1
        assert metadata["codes_unreachable"].value == 0

    def test_unreadable_values_reach_the_metadata(self, tmp_path):
        odd = [{"code": "X", "display": "X", "parent_code": "RSD", "valid_to": "31/12/2025"}]
        _, _, metadata = _build(tmp_path, source_rows=SOURCE_ROWS + odd)
        assert metadata["unreadable_values"].value == {"valid_to": 1}

    def test_codes_without_type_reach_the_metadata(self, tmp_path):
        # SOURCE_ROWS carries no `type`, so both built rows are untyped.
        _, _, metadata = _build(tmp_path)
        assert metadata["codes_without_type"].value == 2
        assert "codes_with_mappings" not in metadata
        assert "map_slots" not in metadata

    def test_the_build_is_logged_under_export(self, tmp_path):
        _, dfm, _ = _build(tmp_path)

        assert dfm.start_dfm_run.call_args.args[:2] == (EXPORT_DFM_SCHEMA, "OkostedToOs2mo")
        dfm.complete_dfm_run.assert_called_once_with(7, rows_affected=2)

    def test_an_empty_set_is_refused_and_logged_as_failed(self, tmp_path):
        with (
            patch(f"{MODULE}.DfmManager") as dfm_cls,
            patch(f"{MODULE}.read_source_rows", return_value=[]),
            patch(f"{MODULE}.write_export_table") as write,
        ):
            dfm = dfm_cls.return_value
            dfm.start_dfm_run.return_value = MagicMock(log_id=7)
            with pytest.raises(dg.Failure, match="empty"):
                _build_delivery(
                    dg.build_asset_context(),
                    OKOSTED_EXPORT,
                    MagicMock(),
                    OS2moExportConfig(output_dir=str(tmp_path)),
                )

        dfm.fail_dfm_run.assert_called_once()
        dfm.complete_dfm_run.assert_not_called()
        write.assert_not_called()


class TestPreview:
    def test_the_build_shows_the_start_of_the_file(self, tmp_path):
        delivery, _, metadata = _build(tmp_path)
        preview = metadata["csv_preview"].value

        assert preview.startswith('```\n"kode";"kode_parent"')
        assert '"OUH"' in preview
        assert metadata["csv_filename"].value == delivery["filename"]

    def test_the_preview_is_capped_and_says_so(self):
        text = '"user_key"\r\n' + "".join(f'"{i}"\r\n' for i in range(25))
        preview = csv_preview(text, rows=20)

        assert '"19"' in preview and '"20"' not in preview
        assert preview.endswith("… 5 more rows")

    def test_a_short_file_is_shown_whole(self):
        text = '"user_key"\r\n"A"\r\n'
        assert csv_preview(text) == '```\n"user_key"\n"A"\n```'

    def test_an_embedded_newline_stays_on_its_record(self):
        text = '"name"\r\n"Afsnit\nNord"\r\n"Syd"\r\n'
        assert csv_preview(text, rows=1) == '```\n"name"\n"Afsnit\nNord"\n```\n… 1 more rows'


class TestUpload:
    def test_a_dry_run_sends_nothing_and_logs_nothing(self):
        os2mo = MagicMock()
        result, dfm_cls, _ = _upload(os2mo)

        os2mo.upload_file.assert_not_called()
        dfm_cls.assert_not_called()
        assert result.metadata["dry_run"] is True

    def test_a_dry_run_needs_no_os2mo_configuration(self):
        from core.resources import OS2moResource

        result, _, _ = _upload(OS2moResource())
        assert result.metadata["graphql_url"] == "not configured"

    def test_a_real_upload_sends_the_exact_bytes(self):
        os2mo = MagicMock()
        _upload(os2mo, dry_run=False)

        call = os2mo.upload_file.call_args
        assert call.args == ("rsyd_okosted_20261006_013000.csv", encode_csv(_delivery()["csv"]))
        assert call.kwargs == {"file_store": "INSERTS", "force": True}

    def test_a_real_upload_is_logged_under_os2mo(self):
        _, _, dfm = _upload(MagicMock(), dry_run=False)

        assert dfm.start_dfm_run.call_args.args[:2] == (DELIVERY_DFM_SCHEMA, "OkostedFile")
        dfm.complete_dfm_run.assert_called_once_with(9, rows_affected=1)

    def test_the_failure_reaches_the_run_log(self):
        os2mo = MagicMock()
        os2mo.upload_file.side_effect = dg.Failure(description="HTTP 500")

        with patch(f"{MODULE}.DfmManager") as dfm_cls:
            dfm = dfm_cls.return_value
            dfm.start_dfm_run.return_value = MagicMock(log_id=9)
            with pytest.raises(dg.Failure):
                _upload_delivery(
                    dg.build_asset_context(),
                    OKOSTED_EXPORT,
                    _delivery(),
                    os2mo,
                    MagicMock(),
                    OS2moUploadConfig(dry_run=False),
                )
        dfm.fail_dfm_run.assert_called_once()
        dfm.complete_dfm_run.assert_not_called()
