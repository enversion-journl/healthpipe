# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The SOR-keyed Location is the merge of both archives: SOR wins, also from a deleted row."""

import sqlite3
from contextlib import contextmanager

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.Location.location_builder import build
from DSA.FHIR.Location.location_service import (
    build_ifdb_code_query,
    build_sor_code_query,
    read_contributions,
    write_codes,
)

pytestmark = [pytest.mark.unit, pytest.mark.location_test]

_SOR_ROWS = [
    # OID, SORKODE, AKTIV, ORG_TYPE, PARENT_ORGENHEDOID, GYLDIG_FRA_DATOTID, dw_is_deleted,
    # dw_timestamp_update
    ("oe-1", "100", "1", "afdeling", None, None, 0, "2026-10-01 03:00:00"),
    ("oe-3", "300", "0", "afsnit", "oe-1", "2020-03-01 00:00:00", 0, "2026-10-01 03:00:00"),
    ("oe-4", "400", "0", "klinik", None, None, 1, "2026-10-01 03:00:00"),
    ("oe-5a", "500", "0", "gammel", None, None, 1, "2026-10-02 03:00:00"),
    ("oe-5b", "500", "1", "ny", None, None, 0, "2026-10-01 03:00:00"),
    ("oe-6", "600", "1", "ambulatorium", None, None, 1, "2026-10-01 03:00:00"),
]
_IFDB_ROWS = [
    # OBJECT_ID, LOCATION_SORCODE, dw_is_deleted, dw_timestamp_update
    ("l-2", "200", 0, "2026-10-01 03:00:00"),
    ("l-3", "300", 0, "2026-10-01 03:00:00"),
    ("l-4", "400", 0, "2026-10-01 03:00:00"),
    ("l-6", "100", 1, "2026-10-02 03:00:00"),
    ("l-7", "700", 1, "2026-10-01 03:00:00"),
]


class _ArchiveOnSqlite:
    """The two archive tables on SQLite; the production queries run against them."""

    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.execute("ATTACH ':memory:' AS arc")
        self.db.execute(
            "CREATE TABLE arc.bom_ifdb_orgenhed_sor (OID TEXT, SORKODE TEXT, AKTIV TEXT, "
            "ORG_TYPE TEXT, PARENT_ORGENHEDOID TEXT, GYLDIG_FRA_DATOTID TEXT, "
            "GYLDIG_TIL_DATOTID TEXT, dw_is_deleted INT, dw_timestamp_update TEXT)"
        )
        self.db.executemany(
            "INSERT INTO arc.bom_ifdb_orgenhed_sor " "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            _SOR_ROWS,
        )
        self.db.execute(
            "CREATE TABLE arc.ifdb_location (OBJECT_ID TEXT, LOCATION_SORCODE TEXT, "
            "dw_is_deleted INT, dw_timestamp_update TEXT)"
        )
        self.db.executemany("INSERT INTO arc.ifdb_location VALUES (?, ?, ?, ?)", _IFDB_ROWS)

    @contextmanager
    def get_connection(self):
        yield self.db


@pytest.mark.parametrize(
    ("code", "status", "type_code", "part_of", "valid_from", "tags"),
    [
        pytest.param("100", "active", "afdeling", None, None, ["BOM_IFDB"], id="sor-only"),
        pytest.param("200", "active", None, None, None, ["IFDB"], id="ifdb-only"),
        pytest.param(
            "300",
            "inactive",
            "afsnit",
            "100",
            "2020-03-01",
            ["BOM_IFDB", "IFDB"],
            id="both-sor-wins",
        ),
        pytest.param(
            "400", "inactive", "klinik", None, None, ["BOM_IFDB", "IFDB"], id="sor-row-deleted"
        ),
        pytest.param(
            "600", "active", "ambulatorium", None, None, ["BOM_IFDB"], id="deleted-keeps-aktiv"
        ),
        pytest.param(
            "500", "active", "ny", None, None, ["BOM_IFDB"], id="live-sor-row-before-deleted"
        ),
    ],
)
def test_the_merged_location_takes_every_shared_field_from_sor(
    code, status, type_code, part_of, valid_from, tags
):
    location = build(read_contributions([code], _ArchiveOnSqlite())[code])

    assert [i.value for i in location.identifier] == [code]
    assert location.managingOrganization.identifier.value == code
    assert location.status == status
    assert (location.type[0].coding[0].code if location.type else None) == type_code
    assert (location.partOf.identifier.value if location.partOf else None) == part_of
    start = location.extension[0].valuePeriod.start if location.extension else None
    assert (start.date().isoformat() if start else None) == valid_from
    assert [t.code for t in location.meta.tag] == tags


def test_a_changed_code_in_neither_archive_fails_the_run():
    with pytest.raises(dg.Failure, match="in neither archive"):
        write_codes(None, _ArchiveOnSqlite(), None, ["999"], source_name="bom_ifdb_oracle")


@pytest.mark.parametrize(
    ("build_query", "code"),
    [
        pytest.param(build_sor_code_query, "500", id="sor"),
        pytest.param(build_ifdb_code_query, "100", id="ifdb"),
    ],
)
def test_a_soft_delete_after_the_checkpoint_selects_its_code(build_query, code):
    query = build_query("2026-10-01 12:00:00", RunModeConfig(mode="delta"))

    assert [raw for (raw,) in _ArchiveOnSqlite().db.execute(query)] == [code]


def test_a_changed_code_with_only_deleted_ifdb_rows_is_skipped():
    assert write_codes(None, _ArchiveOnSqlite(), None, ["700"], source_name="ifdb_oracle") == 0
