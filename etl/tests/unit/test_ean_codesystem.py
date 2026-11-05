# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for cs_ean — the EAN (GLN) codesystem the OS2mo delivery reads.

The merge only upserts, so a partial NemHandel response deletes nothing, and it
only touches changed rows: the OS2mo EAN delivery reads participantName's
dw_timestamp_update as a renamed GLN's "last changed".
"""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.CodeSystem.ean.ean import build_ean_codesystem, cs_ean

pytestmark = pytest.mark.unit

MODULE = "DSA.CodeSystem.ean.ean"
COLUMNS = ["participantID", "participantName", "participantOwnerId"]
MERGE_RESULT = {
    f"{table}_{action}": 0
    for table in ("concepts", "properties", "hierarchy")
    for action in ("inserted", "updated")
}


def _mssql(rows):
    mssql = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.description = [(name,) for name in COLUMNS]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mssql.get_connection.return_value.__enter__.return_value = conn
    return mssql, conn


def _run(rows):
    mssql, conn = _mssql(rows)
    with patch(f"{MODULE}.merge_codesystem_tables", return_value=MERGE_RESULT) as merge:
        result = cs_ean(context=dg.build_asset_context(), mssql_target_staging=mssql)
    return result, merge, conn


def test_the_build_is_flat_with_name_and_owner_as_properties():
    concepts, properties, hierarchy = build_ean_codesystem(
        [("5790000000001", "Firma A", "DK12345678")], COLUMNS
    )
    assert concepts == [{"CODE": "5790000000001", "DISPLAY": "Firma A"}]
    assert {p["KEY"] for p in properties} == {"participantName", "participantOwnerId"}
    assert hierarchy == [{"CODE": "5790000000001", "PARENT_CODE": ""}]


def test_a_name_nemhandel_clears_is_cleared_too():
    """Both properties are always written, NULL when empty, so the upsert
    overwrites a name NemHandel no longer has."""
    concepts, properties, _ = build_ean_codesystem([("5790000000009", None, "")], COLUMNS)

    assert concepts == [{"CODE": "5790000000009", "DISPLAY": "ean 5790000000009"}]
    assert {(p["KEY"], p["VALUE"]) for p in properties} == {
        ("participantName", None),
        ("participantOwnerId", None),
    }


def test_the_merge_only_upserts_and_only_touches_changes():
    _, merge, conn = _run([("5790000000001", "Firma A", "DK12345678")])

    kwargs = merge.call_args.kwargs
    assert kwargs["update_only_on_change"] is True
    # Nothing is deleted: a partial NemHandel response must not remove the
    # properties of the GLNs it is missing.
    for flag in ("delete_stale", "delete_stale_properties", "delete_stale_hierarchy"):
        assert not kwargs.get(flag)
    conn.commit.assert_called_once()
