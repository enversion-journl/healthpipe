# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The GIN alias index: configured for Patient, created by the index asset, survives phase 0."""

from unittest.mock import MagicMock

import dagster as dg
import pytest
from utils.fhir.index_config import IDENTIFIER_ALIAS_GIN_RESOURCES, alias_gin_index_name

pytestmark = pytest.mark.unit


def test_patient_has_an_alias_index_and_the_name_does_not_hit_phase_0():
    assert "Patient" in IDENTIFIER_ALIAS_GIN_RESOURCES
    navn = alias_gin_index_name("patient")
    assert navn == "idx_patient_identifier_alias" and not navn.endswith("_opt") and len(navn) <= 63


def test_the_index_asset_creates_the_gin_index_concurrently_and_analyzes_the_table(monkeypatch):
    from infrastructure.assets import fhir_ifdb_infrastructure as modul

    cur = MagicMock()
    cur.fetchone.return_value = (None, None)  # to_regclass empty; the alias index does not exist
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    db = MagicMock()
    db.get_connection.return_value.__enter__.return_value = conn
    monkeypatch.setattr(modul, "check_sof_required_indexes", lambda *a, **k: [])

    result = modul.fhir_ifdb_database_indexes(context=dg.build_asset_context(), fhir_database=db)
    statements = [str(c.args[0]) for c in cur.execute.call_args_list]
    gin = [s for s in statements if "USING gin" in s and " ON patient " in s]
    # Only the values are indexed; jsonb_path_ops makes the lookup one probe, and
    # fastupdate=off removes the pending list.
    assert gin == [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_patient_identifier_alias "
        "ON patient USING gin ((jsonb_path_query_array(resource->'identifier', "
        "'$[*].value')) jsonb_path_ops) WITH (fastupdate = off)"
    ]
    assert "ANALYZE patient" in statements
    assert "Patient:identifier_alias" in result.metadata["index_specs"].value


def _koer_med_indeks(monkeypatch, eksisterende):
    """Run the asset against a catalog where the reference index has the configured facts."""
    from infrastructure.assets import fhir_ifdb_infrastructure as modul

    reference = ("expr", ["jsonb_path_ops"], ["fastupdate=off"])
    cur = MagicMock()
    sidste: dict = {}

    def execute(sql, params=None):
        sidste["params"] = params

    def fetchone():
        params = sidste.get("params") or ()
        if params == (f"pg_temp.{modul.ALIAS_REFERENCE_INDEX}",):
            return reference
        if params == ("idx_patient_identifier_alias",):
            return eksisterende
        return (None, None)

    cur.execute.side_effect = execute
    cur.fetchone.side_effect = fetchone
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    db = MagicMock()
    db.get_connection.return_value.__enter__.return_value = conn
    monkeypatch.setattr(modul, "check_sof_required_indexes", lambda *a, **k: [])
    modul.fhir_ifdb_database_indexes(context=dg.build_asset_context(), fhir_database=db)
    return [str(c.args[0]) for c in cur.execute.call_args_list]


def _droppet(statements):
    return any(
        s.startswith("DROP INDEX CONCURRENTLY IF EXISTS idx_patient_identifier_alias")
        for s in statements
    )


@pytest.mark.parametrize(
    "eksisterende",
    [
        ("expr", ["jsonb_path_ops"], []),
        ("expr", ["jsonb_ops"], ["fastupdate=off"]),
        ("andet udtryk", ["jsonb_path_ops"], ["fastupdate=off"]),
    ],
    ids=["without-fastupdate", "other-class", "other-expression"],
)
def test_an_outdated_alias_index_is_rebuilt(monkeypatch, eksisterende):
    assert _droppet(_koer_med_indeks(monkeypatch, eksisterende))


def test_a_current_alias_index_is_left_in_place(monkeypatch):
    """The rebuild takes minutes, and meanwhile the lookups scan the whole table."""
    statements = _koer_med_indeks(monkeypatch, ("expr", ["jsonb_path_ops"], ["fastupdate=off"]))
    assert not _droppet(statements)
