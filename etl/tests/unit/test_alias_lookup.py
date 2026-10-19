# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The alias lookup: containment for every value, index-friendly shapes, no scan per batch."""

import pytest
from core.fhir import alias_lookup as modul
from core.fhir.alias_lookup import alias_matches, alias_resources, lookup_values, needle
from utils.fhir.identifier_systems import SYSTEM_CPR

pytestmark = pytest.mark.unit


class _Cur:
    """Answers each query with what the test has prepared, and remembers what it received."""

    def __init__(self, svar):
        self.svar = list(svar)
        self.alle_kald = []

    def execute(self, sql, params=None):
        self.alle_kald.append((repr(sql), params))

    def fetchall(self):
        return self.svar.pop(0) if self.svar else []

    @property
    def kald(self):
        """The queries without the planner setting that every lookup sets first."""
        return [k for k in self.alle_kald if "enable_seqscan" not in k[0]]

    @property
    def indekstvang(self):
        return [k for k in self.alle_kald if "enable_seqscan" not in k[0]] != self.alle_kald


def test_needle_is_a_one_element_identifier_list():
    assert needle("s", "v") == '[{"system": "s", "value": "v"}]'


def test_every_value_is_probed_by_containment_through_unnest():
    cur = _Cur([[("f-1", "A"), ("f-2", "B")]])
    found = lookup_values(cur, resource_type="Patient", system=SYSTEM_CPR, values=["A", "B", " A "])
    assert found == {"A": {"f-1"}, "B": {"f-2"}}
    (kald,) = cur.kald
    assert (
        "unnest" in kald[0]
        and "@> ANY" not in kald[0]
        and "extract_identifier_value" not in kald[0]
    )
    # The index is asked only for the value; the system is rechecked on the candidate rows.
    assert "jsonb_path_query_array" in kald[0]
    assert kald[1] == ([SYSTEM_CPR, SYSTEM_CPR], ["A", "B"])
    # The planner must not pick a scan of the whole table per needle.
    assert cur.alle_kald[0][0].count("enable_seqscan = off") == 1
    assert cur.indekstvang


def test_primary_on_one_resource_and_alias_on_another_returns_both():
    """A duplicate must not be hidden by a fast lookup on the primary identifier."""
    cur = _Cur([[("primary", "same"), ("alias-holder", "same")]])
    found = lookup_values(cur, resource_type="Patient", system=SYSTEM_CPR, values=["same"])
    assert found == {"same": {"primary", "alias-holder"}}


def test_empty_values_make_no_call():
    cur = _Cur([])
    assert lookup_values(cur, resource_type="Patient", system=SYSTEM_CPR, values=["", None]) == {}
    assert cur.alle_kald == []


def test_alias_resources_fetches_ids_through_the_index_and_the_list_by_key():
    """Two calls: the ids through the index, then the stored lists by primary key."""
    cur = _Cur(
        [
            [("f-1", "v"), ("f-2", "v")],  # the needles hit two resources
            [("f-1", [{"system": "s", "value": "v"}]), ("f-2", None)],  # the lists
        ]
    )
    assert alias_resources(cur, "patient", [("s", "v")]) == {
        "f-1": [{"system": "s", "value": "v"}],
        "f-2": [],
    }
    foerste, andet = cur.kald
    assert "unnest" in foerste[0] and "@>" in foerste[0]
    assert "DISTINCT" not in andet[0] and "t.id = ANY" in andet[0]
    assert andet[1] == (["f-1", "f-2"],)


def test_alias_resources_does_not_query_when_nothing_matches():
    cur = _Cur([[]])
    assert alias_resources(cur, "patient", [("s", "v")]) == {}
    assert len(cur.kald) == 1


def test_needles_are_split_into_chunks(monkeypatch):
    monkeypatch.setattr(modul, "CHUNK", 2)
    cur = _Cur([[("f-1", "a")], [("f-2", "c")]])
    found = alias_matches(cur, "patient", [("s", v) for v in "abc"])
    assert found == [("f-1", "a"), ("f-2", "c")] and len(cur.kald) == 2
    # The setting is set once, not per chunk: it holds until the end of the transaction.
    assert sum("enable_seqscan" in k[0] for k in cur.alle_kald) == 1


def test_the_setting_is_set_for_the_connection_when_there_is_no_transaction():
    """Without a transaction there is no LOCAL to bind the setting to."""

    class _Conn:
        autocommit = True

    cur = _Cur([[]])
    cur.connection = _Conn()
    alias_matches(cur, "patient", [("s", "v")])
    knap = cur.alle_kald[0][0]
    assert "enable_seqscan = off" in knap and "LOCAL" not in knap
