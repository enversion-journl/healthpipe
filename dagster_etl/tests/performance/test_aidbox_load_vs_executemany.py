# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Benchmark Aidbox $load endpoint vs current executemany INSERT path.

Tests against the REAL `observation` table in the local Aidbox so we measure
realistic throughput including: GIN-indexes on `resource jsonb`, history-table
trigger, JSONB validation, ON CONFLICT DO UPDATE overhead, etc.

Run inside the devcontainer:
    python /workspaces/healthpipe/dagster_etl/tests/performance/test_aidbox_load_vs_executemany.py

The Observation dicts mimic the structure produced by the AKTIVITETSRESULTAT
asset (lossless, ~10-20 components per resource, full FHIR extension stack)
so the benchmark reflects real per-row cost, not a synthetic mini-resource.

Output: side-by-side throughput numbers and a verdict.
"""

from __future__ import annotations

import gzip
import http.server
import json
import os
import random
import socketserver
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager

import orjson
import psycopg
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


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


AIDBOX_URL = os.environ.get("BENCH_AIDBOX_URL", "http://fhir-server:8888")


def _aidbox_auth() -> tuple[str, str]:
    return (
        _required_env("BENCH_AIDBOX_HTTP_USER"),
        _required_env("BENCH_AIDBOX_HTTP_PASSWORD"),
    )


# Hostname Aidbox can use to reach this script's HTTP server. Defaults to the
# devcontainer's compose service name (a stable alias on the compose network);
# override with BENCH_HTTP_HOST if your container is reachable under another name.
HTTP_HOST_FROM_AIDBOX = os.environ.get("BENCH_HTTP_HOST", "devcontainer")
HTTP_PORT = int(os.environ.get("BENCH_HTTP_PORT", "9088"))

# Marker identifier system used so we can clean up cleanly afterwards.
BENCH_SYSTEM = "urn:bench:load-vs-executemany"

# Sample sizes to test
SAMPLE_SIZES = [1_000, 5_000, 25_000]


# ---------------------------------------------------------------------------
# Realistic Observation generator
# Mimics the lossless AKTIVITETSRESULTAT output: heavy components, multiple
# extensions, identifier, references.
# ---------------------------------------------------------------------------
def _component(i: int) -> dict:
    return {
        "code": {"text": f"Element-{i:04d}"},
        "valueString": f"Værdi nummer {i}",
        "extension": [
            {
                "url": "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatElementMeta#elementId",
                "valueString": f"RM_Bench:edElement{i}",
            },
            {
                "url": "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatElementMeta#type",
                "valueString": "string",
            },
            {
                "url": "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatHierarki#elementNummer",
                "valueInteger": i,
            },
            {
                "url": "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatRowType",
                "valueString": "Element",
            },
        ],
    }


def make_observation(idx: int) -> dict:
    """Build one realistic-shaped Observation dict.

    Roughly matches what the AKTIVITETSRESULTAT lossless pipeline produces:
    ~12-18 components, several extensions, identifier, references.
    """
    n_components = random.randint(8, 18)
    obs_id = str(uuid.uuid4())
    return {
        "resourceType": "Observation",
        "id": obs_id,
        "status": "final",
        "identifier": [{"system": BENCH_SYSTEM, "value": f"bench-{idx:08d}"}],
        "code": {
            "text": f"Benchmark Observation {idx}",
            "coding": [
                {
                    "code": f"BENCH-{idx % 1000:04d}",
                    "system": "urn:bench:codesystem",
                    "display": f"Bench code {idx % 1000}",
                }
            ],
        },
        "subject": {"reference": f"Patient/{uuid.uuid4()}"},
        "encounter": {"reference": f"Encounter/{uuid.uuid4()}"},
        "effectivePeriod": {
            "start": "2025-01-01T10:00:00+00:00",
            "end": "2025-01-01T11:00:00+00:00",
        },
        "issued": "2025-01-01T11:00:00+00:00",
        "performer": [
            {
                "display": f"Bench Performer {idx % 50}",
                "identifier": {"system": "urn:bench:practitioner", "value": f"prc-{idx % 50}"},
            }
        ],
        "extension": [
            {
                "url": "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatRootMeta",
                "extension": [
                    {"url": "type", "valueString": "BenchType"},
                    {"url": "kladde", "valueString": "False"},
                ],
            },
            {
                "url": "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatKontekst#oprettet",
                "extension": [
                    {"url": "bruger", "valueString": "Bench User"},
                    {"url": "persId", "valueString": "pr-bench"},
                    {"url": "tidspunkt", "valueString": "2025-01-01T10:00:00"},
                ],
            },
        ],
        "component": [_component(i) for i in range(n_components)],
    }


def make_observations(n: int) -> list[dict]:
    return [make_observation(i) for i in range(n)]


# ---------------------------------------------------------------------------
# Path A — current production path: executemany INSERT … ON CONFLICT
# ---------------------------------------------------------------------------
def path_executemany(obs: list[dict], batch_size: int = 2000) -> tuple[float, int]:
    """Mimic batch_upsert_resource_dicts in core/resources.py."""
    start = time.perf_counter()
    inserted = 0
    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(txid), 0) + 1 FROM observation")
            next_txid = cur.fetchone()[0]

            for chunk_start in range(0, len(obs), batch_size):
                chunk = obs[chunk_start : chunk_start + batch_size]
                params = [
                    (
                        o["id"],
                        next_txid + chunk_start + i,
                        orjson.dumps(o).decode(),
                    )
                    for i, o in enumerate(chunk)
                ]
                cur.executemany(
                    """
                    INSERT INTO observation (id, txid, status, resource)
                    VALUES (%s, %s, 'created'::resource_status, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        txid = EXCLUDED.txid,
                        status = 'updated'::resource_status,
                        ts = CURRENT_TIMESTAMP,
                        resource = EXCLUDED.resource
                    """,
                    params,
                )
                inserted += len(chunk)
        conn.commit()
    elapsed = time.perf_counter() - start
    return elapsed, inserted


# ---------------------------------------------------------------------------
# Path B — Aidbox $load endpoint
# ---------------------------------------------------------------------------
@contextmanager
def serve_directory(root: str, port: int):
    """Start an HTTP server in a thread, yield, then shut down."""
    saved_cwd = os.getcwd()
    os.chdir(root)
    try:

        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *args, **kwargs):
                pass

        class ReusableTCPServer(socketserver.TCPServer):
            allow_reuse_address = True

        srv = ReusableTCPServer(("0.0.0.0", port), Quiet)
        th = threading.Thread(target=srv.serve_forever, daemon=True)
        th.start()
        time.sleep(0.2)
        try:
            yield
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        os.chdir(saved_cwd)


def path_load_endpoint(obs: list[dict]) -> tuple[float, int, dict]:
    """Write ndjson.gz, serve via HTTP, POST source URL to /Observation/$load."""
    tmpdir = tempfile.mkdtemp(prefix="bench_load_")
    fname = f"Observation-{uuid.uuid4().hex}.ndjson.gz"
    fpath = os.path.join(tmpdir, fname)

    # 1. Write ndjson.gz
    write_start = time.perf_counter()
    with gzip.open(fpath, "wt") as f:
        for o in obs:
            f.write(orjson.dumps(o).decode() + "\n")
    write_elapsed = time.perf_counter() - write_start
    fsize = os.path.getsize(fpath)

    # 2. Serve + POST
    load_start = time.perf_counter()
    with serve_directory(tmpdir, HTTP_PORT):
        url = f"http://{HTTP_HOST_FROM_AIDBOX}:{HTTP_PORT}/{fname}"
        r = requests.post(
            f"{AIDBOX_URL}/Observation/$load",
            auth=_aidbox_auth(),
            headers={"Content-Type": "application/json"},
            json={"source": url},
            timeout=600,
        )
    load_elapsed = time.perf_counter() - load_start

    # Cleanup
    try:
        os.remove(fpath)
        os.rmdir(tmpdir)
    except OSError:
        pass

    if r.status_code != 200:
        raise RuntimeError(f"$load failed: HTTP {r.status_code}: {r.text[:500]}")

    body = r.json()
    return (
        load_elapsed,
        body.get("total", len(obs)),
        {
            "ndjson_write_seconds": write_elapsed,
            "ndjson_size_bytes": fsize,
            "aidbox_response": body,
        },
    )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def cleanup_bench_rows() -> int:
    """Delete all rows we created with the BENCH_SYSTEM identifier."""
    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM observation
                WHERE resource @> %s::jsonb
                """,
                (json.dumps({"identifier": [{"system": BENCH_SYSTEM}]}),),
            )
            n = cur.rowcount
        conn.commit()
    return n


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Pre-clean
    pre_cleaned = cleanup_bench_rows()
    if pre_cleaned:
        print(f"[setup] cleaned {pre_cleaned} stale benchmark rows from prior runs")

    # Sanity check: Aidbox reachable?
    r = requests.get(f"{AIDBOX_URL}/__healthcheck", timeout=5)
    print(f"[setup] aidbox health: HTTP {r.status_code} ({r.text.strip()})")

    print("\n" + "=" * 76)
    print(f"{'size':>8} | {'path':<14} | {'seconds':>8} | {'rows/sec':>10} | extra")
    print("-" * 76)

    results = []
    for size in SAMPLE_SIZES:
        obs_a = make_observations(size)
        obs_b = make_observations(size)  # different IDs to avoid conflicts

        # ---- Path A: executemany ----
        cleanup_bench_rows()
        t_a, n_a = path_executemany(obs_a)
        rps_a = n_a / t_a
        print(f"{size:>8} | {'executemany':<14} | {t_a:>8.2f} | {rps_a:>10.0f} | batch=2000")

        # ---- Path B: $load ----
        cleanup_bench_rows()
        t_b, n_b, extra = path_load_endpoint(obs_b)
        rps_b = n_b / t_b
        write_s = extra["ndjson_write_seconds"]
        size_mb = extra["ndjson_size_bytes"] / (1024 * 1024)
        print(
            f"{size:>8} | {'$load':<14} | {t_b:>8.2f} | {rps_b:>10.0f} | "
            f"ndjson={size_mb:.1f}MB write={write_s:.2f}s"
        )

        results.append((size, t_a, rps_a, t_b, rps_b))

    # Final cleanup
    n = cleanup_bench_rows()
    print(f"\n[teardown] cleaned {n} benchmark rows")

    print("\n" + "=" * 76)
    print("VERDICT")
    print("=" * 76)
    for size, t_a, rps_a, t_b, rps_b in results:
        speedup = rps_b / rps_a if rps_a > 0 else 0
        print(
            f"  N={size:>6}: executemany={rps_a:>5.0f}/s  "
            f"$load={rps_b:>5.0f}/s  speedup={speedup:.2f}x  "
            f"({t_a:.1f}s -> {t_b:.1f}s)"
        )

    # Extrapolate to a full prod chunk (75k Observations)
    if results:
        size, t_a, rps_a, t_b, rps_b = results[-1]
        prod_chunk = 75_000
        proj_a = prod_chunk / rps_a / 60
        proj_b = prod_chunk / rps_b / 60
        print(
            f"\n  Projected for 75k-row prod chunk: "
            f"executemany={proj_a:.1f}min  $load={proj_b:.1f}min"
        )
        print(
            f"  Projected for full 280k-row partition (4 chunks): "
            f"executemany={proj_a*4:.1f}min  $load={proj_b*4:.1f}min"
        )


if __name__ == "__main__":
    main()
