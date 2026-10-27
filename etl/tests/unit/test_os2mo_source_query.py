# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the codesystem reads and export.CodesystemToOs2mo.

The table is what anyone with staging access queries to answer "what are we
sending Magenta", so it must carry the same rows the file is rendered from and
replace ONE codesystem's rows per run without touching the others.
"""

import json
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from DSO.export.os2mo.codesystems import OKOSTED_EXPORT, SOR_EXPORT
from DSO.export.os2mo.source_query import (
    EXPORT_TABLE,
    EXPORT_TABLE_COLUMNS,
    _export_row_to_tuple,
    build_mapping_query,
    build_source_query,
    read_mappings,
    write_export_table,
)

pytestmark = pytest.mark.unit

BUILT_AT = datetime(2026, 10, 6, 1, 30, 0)

EXPORT_ROW = {
    "user_key": "4711",
    "parent_user_key": "OUH",
    "validity_from": "2021-03-01",
    "validity_to": "",
    "name": "Kardiologi",
    "org_unit_type": "",
    "active": "true",
    "deleted": "false",
    "created": "2024-02-01T02:00:00",
    "updated": "",
    "_mappings": [("12345", "SOR", "narrower")],
}


def _mock_mssql(rows=None):
    mssql = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mssql.get_connection.return_value.__enter__.return_value = conn
    return mssql, conn, cursor


class TestSourceQuery:
    def test_sor_reads_only_the_active_set(self):
        sql = build_source_query(SOR_EXPORT)
        assert "INNER JOIN [codesystem].[SORProperties] p_active" in sql
        assert "p_active.[value] = 'true'" in sql
        assert "[codesystem].[SORConcept] c" in sql

    def test_no_placeholder_survives_formatting(self):
        for spec in (SOR_EXPORT, OKOSTED_EXPORT):
            assert "{" not in build_source_query(spec)

    def test_a_second_parent_edge_cannot_duplicate_a_code(self):
        sql = build_source_query(OKOSTED_EXPORT)
        assert "SELECT TOP 1 hh.[parent_code]" in sql
        assert "LEFT JOIN [codesystem].[OkostedHierarchy]" not in sql


class TestMappings:
    def test_the_conceptmap_table_is_read(self):
        sql = build_mapping_query(OKOSTED_EXPORT.mappings[0])
        assert "[conceptmap].[OkostedToSOR]" in sql

    def test_rows_are_grouped_by_source_code_with_the_system(self):
        mssql, _, _ = _mock_mssql(
            [("4711", "12345", "narrower"), ("4711", "67890", "narrower"), ("OUH", "1", "")]
        )
        assert read_mappings(mssql, OKOSTED_EXPORT) == {
            "4711": [("12345", "SOR", "narrower"), ("67890", "SOR", "narrower")],
            "OUH": [("1", "SOR", "")],
        }

    def test_a_codesystem_without_mappings_reads_nothing(self):
        mssql, _, cursor = _mock_mssql()
        assert read_mappings(mssql, SOR_EXPORT) == {}
        cursor.execute.assert_not_called()


class TestExportTable:
    def test_a_row_is_converted_in_column_order(self):
        values = _export_row_to_tuple(OKOSTED_EXPORT, EXPORT_ROW, BUILT_AT)

        assert len(values) == len(EXPORT_TABLE_COLUMNS) + 1
        assert values[:3] == ("Okosted", "4711", "OUH")
        assert values[3] == date(2021, 3, 1)
        assert values[4] is None  # open-ended
        assert values[6] is None  # empty org_unit_type becomes NULL
        assert values[7:9] == (1, 0)  # active, deleted as bits
        assert values[9] == datetime(2024, 2, 1, 2, 0, 0)
        assert values[10] is None  # no updated
        assert json.loads(values[11]) == [
            {"code": "12345", "system": "SOR", "relationship": "narrower"}
        ]
        assert values[-1] == BUILT_AT

    def test_no_mappings_is_null_not_an_empty_list(self):
        row = {**EXPORT_ROW, "_mappings": []}
        assert _export_row_to_tuple(OKOSTED_EXPORT, row, BUILT_AT)[11] is None

    def test_only_this_codesystems_rows_are_replaced(self):
        mssql, _, cursor = _mock_mssql()
        write_export_table(mssql, OKOSTED_EXPORT, [EXPORT_ROW], BUILT_AT)

        delete = cursor.execute.call_args_list[0]
        assert delete.args[0] == f"DELETE FROM {EXPORT_TABLE} WHERE [codesystem] = ?"
        assert delete.args[1] == "Okosted"

    def test_it_commits_once_so_a_failure_leaves_the_previous_set(self):
        mssql, conn, _ = _mock_mssql()
        write_export_table(mssql, OKOSTED_EXPORT, [EXPORT_ROW] * 3, BUILT_AT)
        conn.commit.assert_called_once()

    def test_every_row_is_written(self):
        mssql, _, cursor = _mock_mssql()
        written = write_export_table(mssql, OKOSTED_EXPORT, [EXPORT_ROW] * 2500, BUILT_AT)

        assert written == 2500
        assert sum(len(call.args[1]) for call in cursor.executemany.call_args_list) == 2500
