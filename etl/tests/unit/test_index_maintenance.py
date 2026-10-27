# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Index maintenance: which indexes are rebuilt and which statistics are updated."""

from unittest.mock import MagicMock

import pytest
from infrastructure.assets.index_maintenance import FILL_FACTOR, _maintain_table


class _MaintenanceCursor:
    """Answers the index and statistics queries with the given rows; records the writes."""

    def __init__(self, indexes: list[tuple], statistics: list[str]) -> None:
        self._indexes = indexes
        self._statistics = [(name,) for name in statistics]
        self._rows: list[tuple] = []
        self.writes: list[str] = []

    def execute(self, sql: str) -> None:
        if "dm_db_index_physical_stats" in sql:
            self._rows = self._indexes
        elif "dm_db_stats_properties" in sql:
            self._rows = self._statistics
        else:
            self.writes.append(sql)

    def fetchall(self) -> list[tuple]:
        return self._rows


def _rebuild(name: str, fill_factor: int = FILL_FACTOR) -> str:
    return f"ALTER INDEX [{name}] ON [fhir].[T] REBUILD WITH (FILLFACTOR = {fill_factor});"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("indexes", "rebuilt"),
    [
        ([], []),
        (
            [
                ("PK_T", 50_000, 5.0, 96.0, 0, "nvarchar"),
                ("IX_T_Patient", 20_000, 12.0, 92.0, 0, "nvarchar"),
            ],
            [],
        ),
        (
            [
                ("PK_T", 3, 50.0, 40.0, 0, "nvarchar"),
                ("IX_T_Small", 999, 98.0, 60.0, 0, "nvarchar"),
            ],
            [],
        ),
        (
            [
                ("PK_T", 50_000, 5.0, 96.0, 0, "nvarchar"),
                ("IX_T_Patient", 1_000, 34.4, 82.0, 0, "nvarchar"),
            ],
            ["IX_T_Patient"],
        ),
        (
            [
                ("PK_T", 60_000, 31.0, 89.0, 0, "nvarchar"),
                ("IX_T_Patient", 20_000, 80.0, 70.0, 0, "nvarchar"),
            ],
            ["PK_T", "IX_T_Patient"],
        ),
        ([("IX_T_Org", 20_000, 12.0, 74.0, 0, "nvarchar")], ["IX_T_Org"]),
        (
            [
                ("IX_T_Org", 20_000, 6.0, 85.0, 90, "nvarchar"),
                ("IX_T_Ts", 20_000, 6.0, 97.0, 0, "nvarchar"),
            ],
            ["IX_T_Org"],
        ),
    ],
    ids=[
        "missing",
        "below-threshold",
        "small-index",
        "one-index",
        "two-indexes",
        "sparse-index",
        "fill-factor-index-at-lower-threshold",
    ],
)
def test_only_large_indexes_that_need_it_are_rebuilt_one_by_one(
    indexes: list[tuple], rebuilt: list[str]
) -> None:
    """A small index stays fragmented after a rebuild, and ALL rewrites every index."""
    cursor = _MaintenanceCursor(indexes, statistics=[])
    _maintain_table(cursor, MagicMock(), "[fhir].[T]")
    assert cursor.writes == [_rebuild(name) for name in rebuilt]


@pytest.mark.unit
def test_an_index_led_by_a_timestamp_is_rebuilt_without_free_space() -> None:
    """New keys only land at the end, so fill factor 90 there leaves space that is never used."""
    cursor = _MaintenanceCursor(
        [("IX_T_Watermark", 13_000, 54.9, 80.0, 0, "datetime2")], statistics=[]
    )
    _maintain_table(cursor, MagicMock(), "[fhir].[T]")
    assert cursor.writes == [_rebuild("IX_T_Watermark", fill_factor=100)]


@pytest.mark.unit
def test_modified_statistics_are_updated_after_the_rebuilds() -> None:
    """A rebuild refreshes only its own index statistics; column statistics need the update."""
    cursor = _MaintenanceCursor(
        [("IX_T_Patient", 20_000, 40.0, 70.0, 0, "nvarchar")], statistics=["_WA_Sys_0003_ABC"]
    )
    action = _maintain_table(cursor, MagicMock(), "[fhir].[T]")
    assert cursor.writes == [
        _rebuild("IX_T_Patient"),
        "UPDATE STATISTICS [fhir].[T] ([_WA_Sys_0003_ABC]);",
    ]
    assert action.startswith("REBUILD ") and action.endswith("statistics updated: 1")
