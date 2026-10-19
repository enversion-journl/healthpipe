# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Concurrency tests against a real PostgreSQL with synthetic data.

Enabled with PATIENT_TEST_PG=1. The connection always goes to the local test container.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
from core.fhir.alias_lookup import alias_matches, lookup_values, needle
from core.fhir.direct_upsert import batch_upsert_resources
from fhir.resources.R4B.patient import Patient
from infrastructure.assets.fhir_ifdb_infrastructure import _EXTRACT_IDENTIFIER_VALUE_DDL
from utils.fhir.index_config import (
    ALIAS_GIN_EXPRESSION,
    ALIAS_GIN_OPCLASS,
    alias_gin_index_name,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PATIENT_TEST_PG") != "1", reason="local PostgreSQL fixture not enabled"
    ),
]
DSN = "host=127.0.0.1 port=15438 dbname=patient_identity_tests user=postgres"
SYSTEM = "urn:test:patient-identity"


class Database:
    def __init__(self, schema):
        self.schema = schema

    @contextmanager
    def get_connection(self):
        with psycopg.connect(DSN, options=f"-c search_path={self.schema}") as conn:
            yield conn

    def clear_identifier_caches_for_resources(self, _):
        pass


@pytest.fixture
def db():
    schema = "test_" + uuid4().hex
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
        conn.execute(f'SET search_path TO "{schema}"')
        conn.execute("CREATE TYPE resource_status AS ENUM ('created', 'updated')")
        conn.execute(
            "CREATE TABLE patient (id text PRIMARY KEY, txid bigint NOT NULL, "
            "status resource_status NOT NULL, resource jsonb NOT NULL, "
            "ts timestamptz DEFAULT now())"
        )
        # Same index families as fhir_ifdb_database_indexes creates on dev:
        # the expression index (first identifier per system) and the GIN alias index.
        conn.execute(_EXTRACT_IDENTIFIER_VALUE_DDL)
        conn.execute(
            f"CREATE UNIQUE INDEX ON patient (extract_identifier_value(resource, '{SYSTEM}')) "
            f"WHERE resource->'identifier' @> '{json.dumps([{'system': SYSTEM}])}'::jsonb "
            f"AND extract_identifier_value(resource, '{SYSTEM}') IS NOT NULL"
        )
        conn.execute(
            f"CREATE INDEX {alias_gin_index_name('patient')} ON patient "
            f"USING gin (({ALIAS_GIN_EXPRESSION}) {ALIAS_GIN_OPCLASS})"
        )
    try:
        yield Database(schema)
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


def patient(*values, name=None, fid=None):
    return Patient(
        id=fid,
        identifier=[{"system": SYSTEM, "value": v} for v in values],
        name=[{"family": name}] if name else None,
    )


def write(db, *patients, only_new=False):
    return batch_upsert_resources(
        db,
        patients,
        "Patient",
        identity_safe=True,
        create_only=[only_new] * len(patients),
    )


def rows(db):
    with db.get_connection() as conn:
        return conn.execute("SELECT id, resource FROM patient ORDER BY id").fetchall()


def test_stub_cannot_overwrite_full_patient_created_after_preflight(db):
    full = patient("synthetic-a", name="Full")
    assert write(db, full)[0] == 1
    stub = patient("synthetic-a")  # stale preflight: still has no id
    result = write(db, stub, only_new=True)
    assert result[0:2] == (0, 0) and result.skipped == 1
    assert stub.id == full.id
    assert rows(db)[0][1]["name"][0]["family"] == "Full"


def test_concurrent_new_stubs_share_one_id(db):
    candidates = [patient("synthetic-a") for _ in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda p: write(db, p, only_new=True), candidates))
    assert len({p.id for p in candidates}) == 1
    assert len(rows(db)) == 1
    assert sum(r[0] for r in results) == 1
    assert sum(r.skipped for r in results) == 7
    assert sum(r[1] for r in results) == 0


def test_full_and_stub_race_always_leaves_full_resource(db):
    candidates = [patient("synthetic-a"), patient("synthetic-a", name="Full")]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(write, db, candidates[0], only_new=True),
            pool.submit(write, db, candidates[1]),
        ]
        assert all(f.result()[1] == 0 for f in futures)
    assert candidates[0].id == candidates[1].id
    assert len(rows(db)) == 1
    assert rows(db)[0][1]["name"][0]["family"] == "Full"


def test_waits_for_uncommitted_writer_that_does_not_use_our_helper(db):
    full = patient("synthetic-a", name="Full", fid="already-being-created")
    stub = patient("synthetic-a")
    with ThreadPoolExecutor(max_workers=1) as pool:
        with db.get_connection() as conn:
            conn.execute(
                "INSERT INTO patient VALUES (%s, 1, 'created', %s, now())",
                (full.id, full.json(exclude_none=True)),
            )
            future = pool.submit(write, db, stub, only_new=True)
            with pytest.raises(TimeoutError):
                future.result(timeout=0.1)
            conn.commit()
        assert future.result(timeout=5).skipped == 1
    assert stub.id == full.id and len(rows(db)) == 1
    assert rows(db)[0][1]["name"][0]["family"] == "Full"


def test_matches_secondary_identifier_and_retains_aliases(db):
    full = patient("synthetic-current", "synthetic-old", name="Before")
    write(db, full)
    update = patient("synthetic-old", name="After")
    assert write(db, update)[0:2] == (1, 0)
    assert update.id == full.id
    stored = rows(db)[0][1]
    assert {i["value"] for i in stored["identifier"]} == {"synthetic-current", "synthetic-old"}


def test_conflicting_aliases_roll_back_whole_batch_and_never_choose_first(db):
    write(db, patient("synthetic-a"), patient("synthetic-b"))
    new = patient("synthetic-new")
    result = write(db, new, patient("synthetic-a", "synthetic-b"))
    assert result[0:2] == (0, 2)
    assert result[2] == ["IdentityWriteConflict"]
    assert new.id is None and len(rows(db)) == 2


def test_resolved_id_wins_over_an_alias_it_already_carries(db):
    """A known resource already carrying the value is written, even if another carries it too."""
    gammel = patient("synthetic-old")
    ny = patient("synthetic-current")
    write(db, gammel, ny)
    with db.get_connection() as conn:
        conn.execute(
            "UPDATE patient SET resource = jsonb_set(resource, '{identifier}', "
            "resource->'identifier' || %s::jsonb) WHERE id = %s",
            (json.dumps([{"system": SYSTEM, "value": "synthetic-old"}]), ny.id),
        )
        conn.commit()
    opdatering = patient("synthetic-current", "synthetic-old", name="Opdateret", fid=ny.id)
    result = write(db, opdatering)
    assert result[0:2] == (1, 0) and result.withheld == 0
    assert len(rows(db)) == 2


def test_a_new_value_already_on_another_resource_is_not_added(db):
    """A new number already on another resource is not added; the rest is written."""
    e = patient("synthetic-o", name="E")
    s = patient("synthetic-n")
    andre = patient("synthetic-x")
    write(db, e, s, andre)
    rettet = patient("synthetic-n", name="E rettet", fid=e.id)
    anden = patient("synthetic-x", name="X opdateret", fid=andre.id)
    result = write(db, rettet, anden)
    assert result[0:2] == (2, 0)
    assert result.withheld == 1 and result.skipped == 0
    gemt = {fid: r for fid, r in rows(db)}
    assert [i["value"] for i in gemt[e.id]["identifier"]] == ["synthetic-o"]
    assert gemt[e.id]["name"][0]["family"] == "E rettet"
    assert gemt[andre.id]["name"][0]["family"] == "X opdateret"
    baerere = [
        fid for fid, r in gemt.items() if any(i["value"] == "synthetic-n" for i in r["identifier"])
    ]
    assert baerere == [s.id]


def test_known_ids_cost_no_alias_lookup(db, monkeypatch):
    """A known payload id is written without an alias lookup; only new identities are looked up."""
    from core.fhir import identity_upsert

    first, second = patient("synthetic-a"), patient("synthetic-b")
    write(db, first, second)

    naale: list[list] = []
    aegte = identity_upsert.alias_resources

    def spion(cur, table, pairs):
        naale.append(sorted(pairs))
        return aegte(cur, table, pairs)

    monkeypatch.setattr(identity_upsert, "alias_resources", spion)

    genopbygget = patient("synthetic-a", fid=first.id)
    ny_person = patient("synthetic-c")
    assert write(db, genopbygget, ny_person)[0:2] == (2, 0)
    assert naale == [[(SYSTEM, "synthetic-c")]]
    assert genopbygget.id == first.id and len(rows(db)) == 3
    # And the new person still found another resource through its alias.
    tredje = patient("synthetic-b")
    assert write(db, tredje)[0:2] == (1, 0)
    assert tredje.id == second.id and len(rows(db)) == 3


def test_invalid_payload_batch_rolls_back_and_error_contains_no_values(db):
    with db.get_connection() as conn:
        conn.execute("ALTER TABLE patient ADD CHECK (resource->>'gender' <> 'unknown')")
    invalid = patient("synthetic-private-value")
    invalid.gender = "unknown"
    result = write(db, patient("synthetic-new"), invalid)
    assert result[0:2] == (0, 2) and not rows(db)
    assert "synthetic" not in json.dumps(result[2])


def test_known_id_cannot_hide_a_different_identifier_owner(db):
    """The value is withheld and counted; the known resource keeps its own."""
    first, second = patient("synthetic-a"), patient("synthetic-b")
    write(db, first, second)
    result = write(db, patient("synthetic-b", fid=first.id))
    assert result[0:2] == (1, 0) and result.withheld == 1 and len(rows(db)) == 2
    gemt = {fid: r for fid, r in rows(db)}
    assert [i["value"] for i in gemt[first.id]["identifier"]] == ["synthetic-a"]


def test_two_known_resources_wanting_the_same_new_value_are_both_written(db):
    """The first gets the value; the second is written without it in the same batch."""
    foerste = patient("synthetic-a", name="A")
    anden = patient("synthetic-b", name="B")
    write(db, foerste, anden)
    result = write(
        db,
        patient("synthetic-a", "synthetic-faelles", name="A ny", fid=foerste.id),
        patient("synthetic-b", "synthetic-faelles", name="B ny", fid=anden.id),
    )
    assert result[0:2] == (2, 0) and result.withheld == 1
    gemt = {fid: r for fid, r in rows(db)}
    assert [i["value"] for i in gemt[foerste.id]["identifier"]] == [
        "synthetic-a",
        "synthetic-faelles",
    ]
    assert [i["value"] for i in gemt[anden.id]["identifier"]] == ["synthetic-b"]
    assert gemt[anden.id]["name"][0]["family"] == "B ny"


def test_same_batch_create_only_duplicates_are_counted_as_existing(db):
    first, second = patient("synthetic-a"), patient("synthetic-a")
    result = write(db, first, second, only_new=True)
    assert result[0:2] == (1, 0) and result.skipped == 1
    assert first.id == second.id and len(rows(db)) == 1


def test_deletion_requires_all_source_aliases_to_be_missing(db):
    from core.fhir.deletion import mark_resources_deleted
    from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

    p = patient("synthetic-old", "synthetic-live", name="Retained")
    write(db, p)
    assert mark_resources_deleted(
        db, "Patient", [p.id], missing_identifiers={(SYSTEM, "synthetic-old")}
    ) == (0, 1)
    assert "extension" not in rows(db)[0][1]
    missing = {(SYSTEM, "synthetic-old"), (SYSTEM, "synthetic-live")}
    assert mark_resources_deleted(db, "Patient", [p.id], missing_identifiers=missing) == (1, 0)
    stored = rows(db)[0][1]
    assert stored["name"][0]["family"] == "Retained"
    assert stored["extension"] == [{"url": EXTENSION_FHIR_IS_DELETED, "valueInteger": 1}]
    assert mark_resources_deleted(db, "Patient", [p.id], missing_identifiers=missing) == (0, 1)


def test_deletion_protects_only_the_configured_source_system_and_code(db):
    from core.fhir.deletion import mark_resources_deleted

    first, second = patient("synthetic-a"), patient("synthetic-b")
    first.meta = {"tag": [{"system": "urn:test:source", "code": "CPR"}]}
    second.meta = {"tag": [{"system": "urn:test:unrelated", "code": "CPR"}]}
    write(db, first, second)
    assert mark_resources_deleted(
        db,
        "Patient",
        [first.id, second.id],
        missing_identifiers={(SYSTEM, "synthetic-a"), (SYSTEM, "synthetic-b")},
        protected_sources=(("urn:test:source", "CPR"),),
    ) == (1, 1)
    stored = dict(rows(db))
    assert "extension" not in stored[first.id]
    assert "extension" in stored[second.id]


def test_shared_reference_lookup_finds_secondary_patient_aliases(db):
    from core.fhir.identifier_lookup import resolve_identifier_values
    from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

    p = Patient(
        identifier=[
            {"system": SYSTEM_CPR, "value": "synthetic-current"},
            {"system": SYSTEM_CPR, "value": "synthetic-old", "use": "old"},
            {"system": SYSTEM_IFDB_PATIENT_ID, "value": "synthetic-primary"},
            {"system": SYSTEM_IFDB_PATIENT_ID, "value": "synthetic-secondary"},
        ]
    )
    write(db, p)
    for system, value in (
        (SYSTEM_CPR, "synthetic-old"),
        (SYSTEM_IFDB_PATIENT_ID, "synthetic-secondary"),
    ):
        assert resolve_identifier_values(db, "Patient", system, [value]) == {value: p.id}


def test_shared_reference_lookup_rejects_duplicate_patient_identity(db):
    from core.fhir.identifier_lookup import resolve_identifier_values
    from utils.fhir.identifier_systems import SYSTEM_CPR

    with db.get_connection() as conn:
        for fid in ("first", "second"):
            conn.execute(
                "INSERT INTO patient VALUES (%s,1,'created',%s,now())",
                (
                    fid,
                    json.dumps({"identifier": [{"system": SYSTEM_CPR, "value": "synthetic-cpr"}]}),
                ),
            )
    with pytest.raises(ValueError, match="multiple resources"):
        resolve_identifier_values(db, "Patient", SYSTEM_CPR, ["synthetic-cpr"])


# ---------------------------------------------------------------------------
# The alias lookup against real indexes
# ---------------------------------------------------------------------------


def _lookup(db, values, system=SYSTEM):
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            return lookup_values(cur, resource_type="Patient", system=system, values=values)


def test_every_alias_is_found_by_containment(db):
    full = patient("synthetic-primary", "synthetic-old", name="Full")
    write(db, full)
    found = _lookup(db, ["synthetic-primary", "synthetic-old", "synthetic-none"])
    assert found == {"synthetic-primary": {full.id}, "synthetic-old": {full.id}}


def _insert_raw(db, fid, *values):
    """Insert a resource bypassing the write point's guard — this is how a duplicate arises."""
    resource = {
        "resourceType": "Patient",
        "id": fid,
        "identifier": [{"system": SYSTEM, "value": v} for v in values],
    }
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO patient (id, txid, status, resource) VALUES (%s, 1, 'created', %s::jsonb)",
            (fid, json.dumps(resource)),
        )
        conn.commit()


def test_primary_on_one_resource_and_alias_on_another_is_reported_with_both(db):
    """A value that is primary on one resource and alias on another must not be hidden
    by a fast lookup on the primary identifier: both ids must come back."""
    _insert_raw(db, "primary", "same")
    _insert_raw(db, "alias-holder", "other", "same")
    assert _lookup(db, ["same"]) == {"same": {"primary", "alias-holder"}}
    from DSA.FHIR.Patient.patient_identity import IdentityConflict, fetch_fhir_ids

    with pytest.raises(IdentityConflict):
        fetch_fhir_ids(db, SYSTEM, ["same"])
    # The write point also sees both and refuses to choose one of them.
    result = write(db, patient("same", name="Ny"))
    assert result[0] == 0 and result[1] == 1 and len(rows(db)) == 2


def test_write_matches_an_existing_resource_through_its_alias(db):
    full = patient("synthetic-primary", "synthetic-old", name="Full")
    write(db, full)
    stub = patient("synthetic-old")  # only the old number known
    result = write(db, stub, only_new=True)
    assert result[0:2] == (0, 0) and result.skipped == 1
    assert stub.id == full.id and len(rows(db)) == 1


def _fill(db, count):
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO patient (id, txid, status, resource) "
            "SELECT 'synthetic-' || g, g, 'created', jsonb_build_object('resourceType', 'Patient', "
            "'identifier', jsonb_build_array(jsonb_build_object('system', %s::text, 'value', "
            "'synthetic-' || g), jsonb_build_object('system', %s::text, 'value', 'old-' || g, "
            "'use', 'old'))) FROM generate_series(1, %s::int) g",
            (SYSTEM, SYSTEM, count),
        )
        conn.execute("ANALYZE patient")
        conn.commit()


def _plan(db, query, params):
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("EXPLAIN (FORMAT JSON) " + query.as_string(conn), params)
            return json.dumps(cur.fetchone()[0])


def test_lookup_shapes_use_indexes_not_sequential_scans(db):
    """The expression index and the GIN alias index must both serve the lookups."""
    _fill(db, 20_000)
    values = [f"old-{n}" for n in range(1, 400)]
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "EXPLAIN (FORMAT JSON) SELECT t.id::text, n.value "
                "FROM unnest(%s::text[], %s::text[]) AS n(system, value) JOIN patient t "
                f"ON {ALIAS_GIN_EXPRESSION.replace('resource->', 't.resource->')} "
                "@> to_jsonb(n.value) AND t.resource->'identifier' @> "
                "jsonb_build_array(jsonb_build_object('system', n.system, 'value', n.value))",
                ([SYSTEM] * len(values), values),
            )
            alias_plan = json.dumps(cur.fetchone()[0])
            cur.execute(
                "EXPLAIN (FORMAT JSON) SELECT t.id FROM patient t "
                "WHERE t.resource->'identifier' @> ANY(%s::jsonb[])",
                ([needle(SYSTEM, v) for v in values],),
            )
            any_plan = json.dumps(cur.fetchone()[0])
    assert "Seq Scan" not in alias_plan and "Bitmap Index Scan" in alias_plan
    # The shape this replaces: GIN cannot answer ANY(array), so the planner scans.
    assert "Seq Scan" in any_plan
    # The index is asked only for the value; the system is rechecked on the candidate rows.
    assert alias_gin_index_name("patient") in alias_plan
    assert alias_matches(
        db.get_connection().__enter__().cursor(), "patient", [(SYSTEM, "old-7")]
    ) == [("synthetic-7", "old-7")]
    # Another system with the same value must not be included.
    assert (
        alias_matches(db.get_connection().__enter__().cursor(), "patient", [("urn:andet", "old-7")])
        == []
    )


# ---------------------------------------------------------------------------
# The merge against real PostgreSQL: rewriting of references and physical removal
# ---------------------------------------------------------------------------


def _resource_table(db, name):
    with db.get_connection() as conn:
        conn.execute(
            f"CREATE TABLE {name} (id text PRIMARY KEY, txid bigint NOT NULL, "
            "status resource_status NOT NULL, resource jsonb NOT NULL, ts timestamptz DEFAULT now())"
        )
        conn.commit()


def _insert(db, table, fid, resource):
    with db.get_connection() as conn:
        conn.execute(
            f"INSERT INTO {table} (id, txid, status, resource) VALUES (%s, 1, 'created', %s::jsonb)",
            (fid, json.dumps(resource)),
        )
        conn.commit()


def _fetch(db, table, fid):
    with db.get_connection() as conn:
        return conn.execute(
            f"SELECT resource, txid, status::text FROM {table} WHERE id = %s", (fid,)
        ).fetchone()


def test_alias_resources_uses_the_index_and_the_primary_key(db):
    """Two steps: the ids through the index, then the lists by primary key."""
    from core.fhir.alias_lookup import alias_resources

    full = patient("synthetic-current", "synthetic-old", name="Med to navne")
    write(db, full)
    naale = [(SYSTEM, "synthetic-old"), (SYSTEM, "synthetic-ukendt")]
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            fundet = alias_resources(cur, "patient", naale)
            assert list(fundet) == [full.id]
            assert {i["value"] for i in fundet[full.id]} == {"synthetic-current", "synthetic-old"}
            # No needles, no query.
            assert alias_resources(cur, "patient", []) == {}
            # The second step goes by primary key, not through a DISTINCT over the document.
            cur.execute(
                "EXPLAIN (FORMAT JSON) SELECT t.id::text, t.resource->'identifier' "
                "FROM patient t WHERE t.id = ANY(%s)",
                ([full.id],),
            )
            plan = cur.fetchone()[0]
            if isinstance(plan, str):
                import json as _json

                plan = _json.loads(plan)
            assert "Unique" not in str(plan) and "Sort" not in str(plan)
