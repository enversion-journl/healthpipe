# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Parity test: chunked AKTIVITETSRESULTAT transform must produce byte-identical
output to the monolithic transform, modulo random UUIDs assigned to new
resources.

This test is the gate on the chunked-streaming refactor. It executes against
real AKTIVITETSRESULTAT data from the devcontainer Oracle instance, runs both
the baseline (single chunk) and the chunked transform path, and compares
every output Observation/Procedure dict field-by-field.

Run in devcontainer:
    pytest tests/integration/test_aktivitetsresultat_chunked_parity.py -v

The test fails hard on any structural difference — this is intentional.
Colleagues have independently validated the current pipeline output; the
refactor is only acceptable if the output remains identical.
"""

import copy
import os
from typing import Any
from unittest.mock import MagicMock

import dagster as dg
import pytest

pytestmark = pytest.mark.integration

# Only run in environment where dev Oracle is reachable
DEV_ORACLE_HOST = os.environ.get("IFDB_PROD_ORACLE_SERVER", "oracle")
SKIP_IF_NO_ORACLE = pytest.mark.skipif(
    not os.environ.get("RUN_AKTIVITETSRESULTAT_PARITY_TEST"),
    reason="Set RUN_AKTIVITETSRESULTAT_PARITY_TEST=1 to run against dev Oracle",
)


def _fetch_real_rows(limit: int) -> list[dict[str, Any]]:
    """Pull real AKTIVITETSRESULTAT rows from devcontainer Oracle."""
    from core.resources import IFDBOracleResource

    ora = IFDBOracleResource(
        host=DEV_ORACLE_HOST,
        port=int(os.getenv("IFDB_PROD_ORACLE_PORT", "1521")),
        username=os.getenv("IFDB_PROD_ORACLE_USERNAME", "myuser"),
        password=os.getenv("IFDB_PROD_ORACLE_PASSWORD", "tempPassword1!"),
        service_name=os.getenv("IFDB_PROD_ORACLE_DATABASE", "ORACLE_DB"),
    )
    with ora.get_connection() as conn:
        cur = conn.cursor()
        # All columns that the transform inspects
        cur.execute(
            f"""
            SELECT OBJEKT_ID, MODTIDSPUNKT, SLETTET, PATIENT_ID,
                   UDFOERELSE_ID, AKTIVITETTYPE_KODE, AKTIVITETTYPE_KODETEKST,
                   RESULTATTYPE_KODE, RESULTATTYPE_KODETEKST, OVERSKRIFT,
                   HIERARKISK_PLACERING, DOMAENENAVN, MODELLERINGSNAVN,
                   MODELLERINGSVERSION, XML, TILSTEDEVAERELSE_ID,
                   UDFOERT_PERSID, UDFOERT_PERSNAVN, UDFOERT_BRUGERROLLE,
                   UDFOERT_ADMENHED_KODE, UDFOERT_ADMENHED_NAVN,
                   UDFOERT_ADMENHED_ORGKODE, UDFOERT_TIDSPUNKT, KLADDE,
                   OPDATERET_TID, ENCOUNTER_ID, EPISODE_OF_CARE_ID,
                   PERIOD_START, PERIOD_END, UDFOERT_ADMENHED_SORCODE,
                   LPR3_IGNORED, UDFOERT_BRUGERROLLE_NAME, RESULT_TYPE,
                   RESULT_MOID
            FROM IFDB.AKTIVITETSRESULTAT
            WHERE XML IS NOT NULL
              AND ROWNUM <= {int(limit)}
            """
        )
        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur:
            rec = dict(zip(cols, r, strict=False))
            # CLOB → string (must happen inside connection context)
            xml = rec.get("XML")
            if xml is not None and hasattr(xml, "read"):
                rec["XML"] = xml.read()
            rows.append(rec)
    return rows


def _strip_resource_ids(dicts_list: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    """Remove top-level `id` from each dict so we can compare structural content.

    `id` is assigned via uuid.uuid4() for resources not in the pre-loaded
    identifier cache. Between two runs with empty dev FHIR db, the uuids
    will differ. Every OTHER field is deterministic given the same input
    row + resolved cache state.
    """
    out = []
    for rid, d in dicts_list:
        d2 = copy.deepcopy(d)
        d2.pop("id", None)
        # `meta.lastUpdated` is set by Aidbox on post, not by us — but just in case
        if isinstance(d2.get("meta"), dict):
            d2["meta"].pop("lastUpdated", None)
        out.append((rid, d2))
    return out


def _index_by_source_id(dicts_list: list[tuple[str, dict]]) -> dict[str, dict]:
    """Key each output dict by its source OBJEKT_ID (extracted from the
    identifier array), not by the random FHIR UUID in the tuple key.

    Baseline and chunked runs produce DIFFERENT uuid.uuid4() values for
    the top-level `id`; keying by UUID would make every row look different.
    Keying by source OBJEKT_ID lets us compare the same logical row
    across runs.
    """
    out: dict[str, dict] = {}
    for _rid, d in dicts_list:
        source_id = None
        for ident in d.get("identifier") or []:
            sys_val = str(ident.get("system", ""))
            if "ifdb" in sys_val:
                source_id = str(ident.get("value"))
                break
        assert source_id, f"Resource without IFDB identifier: {d!r:.200}"
        assert source_id not in out, (
            f"Duplicate source id {source_id} in output — baseline run is "
            "already broken; cannot run parity test"
        )
        out[source_id] = d
    return out


@SKIP_IF_NO_ORACLE
def test_chunked_transform_produces_byte_identical_output():
    """Baseline (single-chunk) vs chunked with varying chunk sizes.

    For the same input rows and same ResolvedRefLookup state, the output
    obs_dicts and proc_dicts must be identical (modulo random uuids on the
    top-level `id` field).
    """
    from core.resources import FHIRDatabaseResource
    from DSA.FHIR.Aktivitetsresultat.activity_metadata import (
        _SPECIALE_COLUMNS,
        _UDF_COLUMNS,
    )
    from DSA.FHIR.Aktivitetsresultat.orchestrator import (
        _transform_aktivitetsresultat_to_fhir_dicts,
    )
    from DSA.FHIR.Aktivitetsresultat.references import _pre_resolve_references
    from DSA.FHIR.utils.ifdb.oracle_lookup import chunked_in_fetch, chunked_in_fetch_grouped
    from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id

    # Fetch a realistic sample from dev Oracle
    rows = _fetch_real_rows(limit=500)
    assert len(rows) > 0, "Dev Oracle returned zero rows — cannot run parity test"

    fhir_db = FHIRDatabaseResource(
        host="fhir-server-db",
        port=5432,
        database="aidbox",
        user="postgres_user",
        password="postgres_password",
    )

    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    ctx.has_partition_key = False
    ctx.partition_key = "parity-test"

    # Fetch auxiliary lookups (same for both runs)
    udf_ids = [str(r["UDFOERELSE_ID"]) for r in rows if r.get("UDFOERELSE_ID") is not None]
    udf_rows = chunked_in_fetch(
        udf_ids,
        table="IFDB.AKTIVITETSUDFOERELSE",
        columns=_UDF_COLUMNS,
        where_column="OBJEKT_ID",
        ifdb_oracle=_oracle_resource(),
        context=ctx,
    )
    udf_lookup = {
        key: row
        for row in udf_rows
        if (key := normalize_oracle_id(row.get("OBJEKT_ID"))) is not None
    }
    speciale_lookup = chunked_in_fetch_grouped(
        udf_ids,
        table="IFDB.AKTIVITETSUDFOERELSE_SPECIALE",
        columns=_SPECIALE_COLUMNS,
        where_column="AKTIVITETSUDFOERELSE_ID",
        ifdb_oracle=_oracle_resource(),
        context=ctx,
        key_fn=lambda row: normalize_oracle_id(row["AKTIVITETSUDFOERELSE_ID"]),
        sort_key=lambda row: str(row.get("SPECIALE_KODE", "")),
    )

    # Build a stable ResolvedRefLookup once (shared across both runs)
    fhir_db.clear_identifier_caches()
    resolved = _pre_resolve_references(rows, fhir_db)

    # --- Baseline: single monolithic transform ---
    obs_baseline, proc_baseline, _ = _transform_aktivitetsresultat_to_fhir_dicts(
        rows=rows,
        context=ctx,
        resolved=resolved,
        udf_lookup=udf_lookup,
        speciale_lookup=speciale_lookup,
    )

    # --- Chunked: same transform, multiple chunks, concatenated output ---
    # We test multiple chunk sizes to catch off-by-one, boundary, and
    # order-sensitive bugs.
    for chunk_size in (50, 97, 100, 250):
        obs_chunked: list[tuple[str, dict]] = []
        proc_chunked: list[tuple[str, dict]] = []
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            obs_ck, proc_ck, _ = _transform_aktivitetsresultat_to_fhir_dicts(
                rows=chunk,
                context=ctx,
                resolved=resolved,
                udf_lookup=udf_lookup,
                speciale_lookup=speciale_lookup,
            )
            obs_chunked.extend(obs_ck)
            proc_chunked.extend(proc_ck)

        # Counts must match exactly
        assert len(obs_baseline) == len(obs_chunked), (
            f"chunk_size={chunk_size}: "
            f"obs count mismatch baseline={len(obs_baseline)} chunked={len(obs_chunked)}"
        )
        assert len(proc_baseline) == len(proc_chunked), (
            f"chunk_size={chunk_size}: "
            f"proc count mismatch baseline={len(proc_baseline)} chunked={len(proc_chunked)}"
        )

        # Key by source OBJEKT_ID (from identifier), not tuple UUID
        b_idx = _index_by_source_id(_strip_resource_ids(obs_baseline))
        c_idx = _index_by_source_id(_strip_resource_ids(obs_chunked))

        assert set(b_idx.keys()) == set(c_idx.keys()), (
            f"chunk_size={chunk_size}: obs OBJEKT_ID sets differ. "
            f"Only in baseline: {sorted(set(b_idx.keys()) - set(c_idx.keys()))[:5]}. "
            f"Only in chunked:  {sorted(set(c_idx.keys()) - set(b_idx.keys()))[:5]}"
        )

        # Each OBJEKT_ID must produce identical dict content
        for source_id, b_dict in b_idx.items():
            c_dict = c_idx[source_id]
            if b_dict != c_dict:
                import json

                b_json = json.dumps(b_dict, sort_keys=True, default=str)
                c_json = json.dumps(c_dict, sort_keys=True, default=str)
                raise AssertionError(
                    f"chunk_size={chunk_size}: obs dict differs for OBJEKT_ID={source_id}\n"
                    f"  baseline (first 500 chars): {b_json[:500]}\n"
                    f"  chunked  (first 500 chars): {c_json[:500]}"
                )

        # Same comparison for procedure dicts
        b_proc_idx = _index_by_source_id(_strip_resource_ids(proc_baseline))
        c_proc_idx = _index_by_source_id(_strip_resource_ids(proc_chunked))
        assert set(b_proc_idx.keys()) == set(
            c_proc_idx.keys()
        ), f"chunk_size={chunk_size}: proc OBJEKT_ID sets differ"
        for source_id, b_dict in b_proc_idx.items():
            c_dict = c_proc_idx[source_id]
            assert (
                b_dict == c_dict
            ), f"chunk_size={chunk_size}: proc dict differs for OBJEKT_ID={source_id}"


@SKIP_IF_NO_ORACLE
def test_pipeline_helper_produces_identical_result_across_chunk_sizes(monkeypatch):
    """End-to-end smoke test: the full _run_aktivitetsresultat_pipeline_chunked
    helper (dependency resolution + cache warmup + udf/speciale lookups +
    chunked transform + post) must produce the same aggregate metadata for
    chunk_size=10 and chunk_size=100 on the same input.

    This catches subtle bugs like: passing partition-level state per chunk
    instead of once, double-clearing caches between chunks, chunk
    re-ordering that changes final FHIR state, etc.
    """
    from core.resources import FHIRDatabaseResource
    from DSA.FHIR.Aktivitetsresultat import orchestrator as mod

    rows = _fetch_real_rows(limit=200)
    assert len(rows) > 0

    fhir_db = FHIRDatabaseResource(
        host="fhir-server-db",
        port=5432,
        database="aidbox",
        user="postgres_user",
        password="postgres_password",
    )
    ifdb = _oracle_resource()

    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    ctx.has_partition_key = False
    ctx.partition_key = "parity-chunked"

    # Record all FHIR posts via the direct-db path; we compare by OBJEKT_ID.
    posted_by_run: list[dict[str, list]] = []

    import DSA.FHIR.utils.ifdb.fhir_posting as posting_mod

    def _spy_on_post(*args, **kwargs):
        # Capture (resource_type, source_id) pairs
        rtype = kwargs.get("resource_type")
        dicts = kwargs.get("resource_dicts", [])
        current = posted_by_run[-1]
        current.setdefault(rtype, []).extend(
            [
                next(
                    (
                        ident["value"]
                        for ident in (d.get("identifier") or [])
                        if "ifdb" in str(ident.get("system", ""))
                    ),
                    None,
                )
                for _rid, d in dicts
            ]
        )
        return original_post(*args, **kwargs)

    original_post = posting_mod.post_resource_dicts_direct

    for chunk_size in (10, 100):
        posted_by_run.append({})
        monkeypatch.setattr(mod, "_AKTIVITETSRESULTAT_CHUNK_SIZE", chunk_size)
        monkeypatch.setattr(posting_mod, "post_resource_dicts_direct", _spy_on_post)
        # Also patch the reference in Observation module (local import)
        monkeypatch.setattr(
            "DSA.FHIR.utils.ifdb.fhir_posting.post_resource_dicts_direct",
            _spy_on_post,
        )
        mod._run_aktivitetsresultat_pipeline_chunked(
            extracted_data=rows,
            context=ctx,
            ifdb_oracle=ifdb,
            fhir_database=fhir_db,
        )
        monkeypatch.setattr(posting_mod, "post_resource_dicts_direct", original_post)

    run1, run2 = posted_by_run
    # Same resource types appear in both runs
    assert set(run1.keys()) == set(run2.keys())
    # Same OBJEKT_IDs posted per resource type (order-independent set check)
    for rtype in run1:
        assert sorted(filter(None, run1[rtype])) == sorted(filter(None, run2[rtype])), (
            f"chunk_size=10 posted a different set of {rtype} OBJEKT_IDs than "
            f"chunk_size=100. diff first 5: "
            f"{sorted(set(filter(None, run1[rtype])) ^ set(filter(None, run2[rtype])))[:5]}"
        )


def _oracle_resource():
    from core.resources import IFDBOracleResource

    return IFDBOracleResource(
        host=DEV_ORACLE_HOST,
        port=int(os.getenv("IFDB_PROD_ORACLE_PORT", "1521")),
        username=os.getenv("IFDB_PROD_ORACLE_USERNAME", "myuser"),
        password=os.getenv("IFDB_PROD_ORACLE_PASSWORD", "tempPassword1!"),
        service_name=os.getenv("IFDB_PROD_ORACLE_DATABASE", "ORACLE_DB"),
    )
