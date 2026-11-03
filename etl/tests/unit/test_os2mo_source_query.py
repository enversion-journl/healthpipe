# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the codesystem reads and export.CodesystemToOs2mo.

The table is what anyone with staging access queries to answer "what are we
sending Magenta", so it must carry the same rows the file is rendered from and
replace ONE codesystem's rows per run without touching the others.

The specs used below are built locally rather than imported from
``DSO.export.os2mo.codesystems``, the same way ``test_os2mo_codesystem_export.py``
already does: that module's production specs are a downstream consumer,
exercised on their own terms elsewhere.
"""

from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from DSO.export.os2mo.source_query import (
    EXPORT_TABLE,
    EXPORT_TABLE_COLUMNS,
    _export_row_to_tuple,
    build_source_query,
    write_export_table,
)
from utils.os2mo.codesystem_export import CodesystemExportSpec

pytestmark = pytest.mark.unit

BUILT_AT = datetime(2026, 10, 6, 1, 30, 0)


def _spec(**overrides):
    defaults = dict(
        name="Okosted",
        asset_prefix="okosted",
        source_deps=("cs_okosted",),
        from_property="startsdate",
        to_property="enddate",
        type_property="enhedstype",
        status_active_property="aktiv",
        status_deleted_property="slettet",
    )
    defaults.update(overrides)
    return CodesystemExportSpec(**defaults)


OKOSTED_SPEC = _spec()

# A local stand-in for the production SOR spec (DSO.export.os2mo.codesystems):
# same property names, so test_sor_reads_only_the_scoped_set exercises the
# same shape of spec without depending on that consumer module.
SOR_SPEC = _spec(
    name="SOR",
    asset_prefix="sor",
    source_deps=("cs_sor",),
    from_property="openingDate",
    to_property="closingDate",
    type_property="entityType",
    scope_property="rsdOrganization",
    scope_value="true",
    status_active_property="activeOwnOrganization",
    status_deleted_property=None,
    root_code="6101000016001",
)

EXPORT_ROW = {
    "kode": "4711",
    "kode_parent": "OUH",
    "gyldig_fra": "2021-03-01",
    "gyldig_til": "",
    "navn": "Kardiologi",
    "type": "",
    "niveau": "2",
    "aktiv": "true",
    "dw_is_deleted": "false",
    "dw_timestamp_insert": "2024-02-01T02:00:00",
    "dw_timestamp_update": "",
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
    def test_sor_reads_only_the_scoped_set(self):
        sql = build_source_query(SOR_SPEC)
        assert "INNER JOIN [codesystem].[SORProperties] p_scope" in sql
        assert "p_scope.[property_code] = 'rsdOrganization'" in sql
        assert "p_scope.[value] = 'true'" in sql
        assert "[codesystem].[SORConcept] c" in sql

    def test_no_placeholder_survives_formatting(self):
        for spec in (SOR_SPEC, OKOSTED_SPEC):
            assert "{" not in build_source_query(spec)

    def test_a_second_parent_edge_cannot_duplicate_a_code(self):
        sql = build_source_query(OKOSTED_SPEC)
        assert "SELECT TOP 1 hh.[parent_code]" in sql
        assert "LEFT JOIN [codesystem].[OkostedHierarchy]" not in sql


class TestExportTable:
    def test_export_row_maps_file_columns_to_table_columns(self):
        row = {
            "kode": "FER_NIVEAU_1",
            "kode_parent": "",
            "gyldig_fra": "1900-01-01",
            "gyldig_til": "",
            "navn": "Feriefond",
            "type": "juridisk enhed",
            "niveau": "1",
            "aktiv": "true",
            "dw_is_deleted": "false",
            "dw_timestamp_insert": "2026-10-15T01:00:00",
            "dw_timestamp_update": "2026-10-15T01:00:00",
        }
        built_at = datetime(2026, 10, 15, 2, 0, 0)
        values = dict(
            zip(
                (*EXPORT_TABLE_COLUMNS, "dw_timestamp_insert"),
                _export_row_to_tuple(_spec(), row, built_at),
                strict=True,
            )
        )
        assert values["kode"] == "FER_NIVEAU_1"
        assert values["kode_parent"] is None  # '' -> NULL
        assert values["niveau"] == 1  # str -> INT
        assert values["aktiv"] == 1
        assert values["dw_is_deleted_codesystem"] == 0  # the file's dw_is_deleted
        assert values["dw_timestamp_insert_codesystem"] == datetime(2026, 10, 15, 1, 0, 0)
        assert values["dw_timestamp_insert"] == built_at  # the table's own build stamp

    def test_a_row_is_converted_in_column_order(self):
        values = _export_row_to_tuple(OKOSTED_SPEC, EXPORT_ROW, BUILT_AT)

        assert len(values) == len(EXPORT_TABLE_COLUMNS) + 1
        assert values[:3] == ("Okosted", "4711", "OUH")
        assert values[3] == date(2021, 3, 1)
        assert values[4] is None  # open-ended
        assert values[6] is None  # empty type becomes NULL
        assert values[7] == 2  # niveau as int
        assert values[8] == 1  # aktiv as bit
        assert values[9] == 0  # dw_is_deleted_codesystem as bit
        assert values[10] == datetime(2024, 2, 1, 2, 0, 0)
        assert values[11] is None  # no dw_timestamp_update_codesystem
        assert values[-1] == BUILT_AT

    def test_only_this_codesystems_rows_are_replaced(self):
        mssql, _, cursor = _mock_mssql()
        write_export_table(mssql, OKOSTED_SPEC, [EXPORT_ROW], BUILT_AT)

        delete = cursor.execute.call_args_list[0]
        assert delete.args[0] == f"DELETE FROM {EXPORT_TABLE} WHERE [codesystem] = ?"
        assert delete.args[1] == "Okosted"

    def test_it_commits_once_so_a_failure_leaves_the_previous_set(self):
        mssql, conn, _ = _mock_mssql()
        write_export_table(mssql, OKOSTED_SPEC, [EXPORT_ROW] * 3, BUILT_AT)
        conn.commit.assert_called_once()

    def test_every_row_is_written(self):
        mssql, _, cursor = _mock_mssql()
        written = write_export_table(mssql, OKOSTED_SPEC, [EXPORT_ROW] * 2500, BUILT_AT)

        assert written == 2500
        assert sum(len(call.args[1]) for call in cursor.executemany.call_args_list) == 2500
