# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Seed local Aidbox `observation` table with N synthetic rows so the benchmark
runs against a non-trivial GIN+history state.

The rows use a distinct identifier system (`urn:bench:seed-baseline`) so the
benchmark cleanup (which only deletes `urn:bench:load-vs-executemany`) cannot
accidentally remove them.

Usage:
    python _seed_aidbox_baseline.py [N]

Default N = 100000.
"""

from __future__ import annotations

import os
import random
import sys
import time
import uuid

import orjson
import psycopg


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


def _pg_dsn() -> str:
    override = os.environ.get("BENCH_PG_DSN")
    if override:
        return override
    return (
        f"host={os.environ.get('PG_FHIR_HOST', 'fhir-server-db')} "
        f"port={os.environ.get('PG_FHIR_PORT', '5432')} "
        f"dbname={os.environ.get('PG_FHIR_DATABASE', 'aidbox')} "
        f"user={_required_env('PG_FHIR_USER')} "
        f"password={_required_env('PG_FHIR_PASSWORD')}"
    )


SEED_SYSTEM = "urn:bench:seed-baseline"


def _component(i: int) -> dict:
    return {
        "code": {"text": f"Element-{i:04d}"},
        "valueString": f"Værdi nummer {i}",
        "extension": [
            {
                "url": "http://example.org/fhir/StructureDefinition/aktivitetsresultatElementMeta#elementId",
                "valueString": f"RM_Bench:edElement{i}",
            },
            {
                "url": "http://example.org/fhir/StructureDefinition/aktivitetsresultatHierarki#elementNummer",
                "valueInteger": i,
            },
            {
                "url": "http://example.org/fhir/StructureDefinition/aktivitetsresultatRowType",
                "valueString": "Element",
            },
        ],
    }


def make_observation(idx: int) -> dict:
    n_components = random.randint(8, 18)
    return {
        "resourceType": "Observation",
        "id": str(uuid.uuid4()),
        "status": "final",
        "identifier": [{"system": SEED_SYSTEM, "value": f"seed-{idx:08d}"}],
        "code": {"text": f"Seed Observation {idx}"},
        "subject": {"reference": f"Patient/{uuid.uuid4()}"},
        "encounter": {"reference": f"Encounter/{uuid.uuid4()}"},
        "issued": "2025-01-01T11:00:00+00:00",
        "extension": [
            {
                "url": "http://example.org/fhir/StructureDefinition/aktivitetsresultatRootMeta",
                "extension": [{"url": "type", "valueString": "Seed"}],
            }
        ],
        "component": [_component(i) for i in range(n_components)],
    }


def seed(n: int):
    print(f"Seeding {n} observations into local Aidbox...")
    BATCH = 5000
    start = time.perf_counter()
    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(txid), 0) + 1 FROM observation")
            next_txid = cur.fetchone()[0]

            for chunk_start in range(0, n, BATCH):
                chunk_end = min(chunk_start + BATCH, n)
                params = []
                for i in range(chunk_start, chunk_end):
                    obs = make_observation(i)
                    params.append((obs["id"], next_txid + i, orjson.dumps(obs).decode()))
                cur.executemany(
                    """
                    INSERT INTO observation (id, txid, status, resource)
                    VALUES (%s, %s, 'created'::resource_status, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    params,
                )
                conn.commit()
                elapsed = time.perf_counter() - start
                rps = (chunk_end) / elapsed if elapsed > 0 else 0
                pct = 100 * chunk_end / n
                print(
                    f"  {chunk_end:>7}/{n} ({pct:5.1f}%) — " f"{elapsed:.1f}s, {rps:.0f} rows/sec"
                )

    elapsed = time.perf_counter() - start
    print(f"\nDone: {n} rows in {elapsed:.1f}s ({n/elapsed:.0f} rows/sec)")


def cleanup():
    """Remove seed rows."""
    import json

    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM observation WHERE resource @> %s::jsonb",
                (json.dumps({"identifier": [{"system": SEED_SYSTEM}]}),),
            )
            n = cur.rowcount
        conn.commit()
    print(f"Cleaned {n} seed rows")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--clean":
        cleanup()
    else:
        n = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
        seed(n)
