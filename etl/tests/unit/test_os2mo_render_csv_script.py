# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The local render script writes the same file the export asset builds."""

import re
from unittest.mock import MagicMock, patch

import pytest
from DSO.export.os2mo.codesystem_assets import ExportSet
from DSO.export.os2mo.codesystems import OKOSTED_EXPORT
from scripts import os2mo_render_csv
from utils.os2mo.csv_format import encode_csv, render_csv

pytestmark = pytest.mark.unit

ROWS = [
    {"kode": "RSD", "kode_parent": "", "navn": "Example Region", "type": "juridisk enhed"},
]


def _render(tmp_path, codesystems):
    export_set = ExportSet(rows=ROWS, codes_read=3, codes_gone=0, unreadable={})
    with patch.object(os2mo_render_csv, "read_export_set", return_value=export_set) as read:
        paths = os2mo_render_csv.render(MagicMock(), codesystems, tmp_path)
    return paths, read


def test_the_file_is_the_export_assets_bytes(tmp_path):
    paths, _ = _render(tmp_path, ["okosted"])

    assert len(paths) == 1
    assert re.fullmatch(r"rsyd_okosted_\d{8}_\d{6}\.csv", paths[0].name)
    assert paths[0].read_bytes() == encode_csv(render_csv(OKOSTED_EXPORT, ROWS))


def test_each_requested_codesystem_is_read_with_its_own_spec(tmp_path):
    _, read = _render(tmp_path, ["sor", "okosted"])
    assert [call.args[1].name for call in read.call_args_list] == ["SOR", "Okosted"]


def test_an_unknown_codesystem_is_refused():
    with pytest.raises(SystemExit):
        os2mo_render_csv.main(["--codesystem", "nope"])


def test_a_missing_staging_variable_is_named(monkeypatch):
    monkeypatch.delenv("MSSQL_TARGET_STAGING_PASSWORD", raising=False)
    with pytest.raises(SystemExit, match="MSSQL_TARGET_STAGING_PASSWORD"):
        os2mo_render_csv.staging_from_env()
