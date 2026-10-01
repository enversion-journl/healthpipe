# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for _persist_cpr_matched_identifiers.

Regression: when resolve_missing_patients CPR-matched an IFDB patient to an
existing FHIR patient, the mapping was held only in memory. Downstream
reference resolution consulted the identifier cache (built from FHIR state)
and failed to find the new IFDB PATIENT_ID because only the old CPR
identifier was on the resource → ValueError.

Fix: after CPR-match, the existing FHIR patient gets the new IFDB
PATIENT_ID added to its identifier array, atomically and idempotently.
"""

from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.FHIR.utils.ifdb.dependency_resolver import (
    _persist_cpr_matched_identifiers,
)
from utils.fhir.identifier_systems import SYSTEM_IFDB_PATIENT_ID

pytestmark = pytest.mark.unit


def _mock_context() -> MagicMock:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    return ctx


def _mock_fhir_database(update_rowcounts: list[int]) -> MagicMock:
    """Mock FHIRDatabaseResource whose cursor.execute advances rowcount."""
    fhir_database = MagicMock()

    cursor = MagicMock()
    rowcount_iter = iter(update_rowcounts)

    def _execute(*args, **kwargs):
        try:
            cursor.rowcount = next(rowcount_iter)
        except StopIteration:
            cursor.rowcount = 0

    cursor.execute.side_effect = _execute

    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = False

    conn = MagicMock()
    conn.cursor.return_value = cursor_cm

    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    fhir_database.get_connection.return_value = conn_cm

    fhir_database._cursor = cursor
    fhir_database._conn = conn
    return fhir_database


def test_empty_cpr_matched_is_noop():
    fhir_database = _mock_fhir_database([])
    added = _persist_cpr_matched_identifiers(
        cpr_matched={},
        fhir_database=fhir_database,
        context=_mock_context(),
    )
    assert added == 0
    fhir_database.get_connection.assert_not_called()


def test_single_cpr_match_issues_one_update():
    fhir_database = _mock_fhir_database(update_rowcounts=[1])
    added = _persist_cpr_matched_identifiers(
        cpr_matched={"3282347821": "fhir-uuid-A"},
        fhir_database=fhir_database,
        context=_mock_context(),
    )
    assert added == 1
    assert fhir_database._cursor.execute.call_count == 1
    fhir_database._conn.commit.assert_called_once()

    # Validate the SQL references the identifier JSON shape
    sql, params = fhir_database._cursor.execute.call_args.args
    assert "identifier" in sql
    assert "jsonb_build_array" in sql
    assert "@>" in sql  # containment check = idempotent
    assert params == (
        SYSTEM_IFDB_PATIENT_ID,
        "3282347821",
        "fhir-uuid-A",
        SYSTEM_IFDB_PATIENT_ID,
        "3282347821",
    )


def test_idempotent_when_identifier_already_present():
    """If the UPDATE affects 0 rows (identifier already present), count as
    no-op — added = 0, but no error raised."""
    fhir_database = _mock_fhir_database(update_rowcounts=[0])
    added = _persist_cpr_matched_identifiers(
        cpr_matched={"3282347821": "fhir-uuid-A"},
        fhir_database=fhir_database,
        context=_mock_context(),
    )
    assert added == 0
    fhir_database._conn.commit.assert_called_once()


def test_bulk_partial_already_present():
    """Mix of real appends and idempotent no-ops."""
    fhir_database = _mock_fhir_database(update_rowcounts=[1, 0, 1, 0, 1])
    cpr_matched = {
        "id-1": "uuid-1",
        "id-2": "uuid-2",  # already has identifier
        "id-3": "uuid-3",
        "id-4": "uuid-4",  # already has identifier
        "id-5": "uuid-5",
    }
    added = _persist_cpr_matched_identifiers(
        cpr_matched=cpr_matched,
        fhir_database=fhir_database,
        context=_mock_context(),
    )
    assert added == 3
    assert fhir_database._cursor.execute.call_count == 5


def test_commit_called_even_when_all_noops():
    """The transaction must still be committed cleanly when every row
    was an idempotent no-op — otherwise the transaction would linger."""
    fhir_database = _mock_fhir_database(update_rowcounts=[0, 0])
    added = _persist_cpr_matched_identifiers(
        cpr_matched={"id-1": "uuid-1", "id-2": "uuid-2"},
        fhir_database=fhir_database,
        context=_mock_context(),
    )
    assert added == 0
    fhir_database._conn.commit.assert_called_once()


def test_uses_system_ifdb_patient_id_constant():
    """The SQL parameter must use SYSTEM_IFDB_PATIENT_ID — not any other
    identifier system — so downstream lookup by IFDB PATIENT_ID hits."""
    fhir_database = _mock_fhir_database(update_rowcounts=[1])
    _persist_cpr_matched_identifiers(
        cpr_matched={"ifdb-id": "fhir-uuid"},
        fhir_database=fhir_database,
        context=_mock_context(),
    )
    _, params = fhir_database._cursor.execute.call_args.args
    assert params[0] == SYSTEM_IFDB_PATIENT_ID
    assert params[3] == SYSTEM_IFDB_PATIENT_ID
