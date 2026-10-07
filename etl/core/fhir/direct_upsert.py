# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Direct Aidbox/PostgreSQL upsert helpers for FHIR resources."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from time import perf_counter
from typing import Any

import orjson
from core.fhir.aidbox_json import (
    transform_references_for_aidbox as _transform_references_for_aidbox,
)
from core.parallel import resolve_worker_count
from fhir.resources.R4B.resource import Resource
from utils.fhir.identifier_systems import _SAFE_IDENTIFIER_SYSTEM_RE


def _resources_module():
    # Deliberately lazy: core.resources imports this module while preserving
    # compatibility constants/tests that patch core.resources.* at runtime.
    from core import resources as resources_mod

    return resources_mod


def _logger():
    return _resources_module().dg.get_dagster_logger()


_BATCH_MAX_RETRIES = 2
_BATCH_RETRY_BACKOFFS = (0.5, 1.0)


def _resource_identifier_pairs(resource_json: dict[str, Any]) -> list[tuple[str, str]]:
    """Return non-empty ``(system, value)`` identifier pairs from a FHIR resource dict."""
    pairs: list[tuple[str, str]] = []
    for identifier in resource_json.get("identifier") or []:
        if not isinstance(identifier, dict):
            continue
        system = identifier.get("system")
        value = identifier.get("value")
        if system and value:
            pairs.append((str(system), str(value)))
    return pairs


def _resolve_existing_id_for_identifier_collision(
    *,
    conn: Any,
    cur: Any,
    table_name: str,
    resource_json: dict[str, Any],
) -> str | None:
    """Resolve an existing row after a unique identifier collision.

    This is intentionally conservative: a duplicate-key error is counted as
    idempotent success only when the database can prove that exactly one existing
    row has one of the incoming FHIR identifiers. Otherwise (including when the
    lookup itself errors) the original insert remains a real failure. Runs in its
    own subtransaction so a failing lookup cannot abort the batch's transaction.
    """
    try:
        with conn.transaction():
            for system, value in _resource_identifier_pairs(resource_json):
                if _SAFE_IDENTIFIER_SYSTEM_RE.match(system):
                    # System inlined as a SQL literal: the partial UNIQUE
                    # expression indexes are built per system-literal, and a
                    # bound parameter never matches an expression index.
                    system_filter = json.dumps([{"system": system}])
                    cur.execute(
                        f"""
                        SELECT id
                        FROM {table_name}
                        WHERE resource->'identifier' @> '{system_filter}'::jsonb
                          AND extract_identifier_value(resource, '{system}') = %s
                        LIMIT 2
                        """,
                        (value,),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT id
                        FROM {table_name}
                        WHERE extract_identifier_value(resource, %s) = %s
                        LIMIT 2
                        """,
                        (system, value),
                    )
                rows = cur.fetchall()
                if len(rows) == 1:
                    return str(rows[0][0])
                if len(rows) > 1:
                    # The identifier value can be a person identifier (e.g. CPR)
                    # and must never reach a log — the system + count suffice.
                    _logger().warning(
                        f"Identifier collision for {table_name} system={system!r} "
                        f"matched {len(rows)} rows; treating insert as failure"
                    )
                    return None
    except Exception as lookup_err:  # noqa: BLE001
        _logger().warning(
            f"Identifier-collision re-resolve failed for {table_name}: "
            f"{_error_label(lookup_err, table_name)}; treating original insert as failure"
        )
        return None
    return None


def _is_unique_violation(exc: Exception) -> bool:
    return getattr(exc, "sqlstate", None) == "23505"


def _constraint_name(exc: Exception) -> str | None:
    return getattr(getattr(exc, "diag", None), "constraint_name", None)


def _safe_error_label(exc: BaseException) -> str:
    """Bounded-provenance stand-in for an error message, safe for logs.

    Driver and serializer error text is of unbounded provenance and can embed
    resource content — for Patient resources the CPR, e.g. in a
    unique-violation DETAIL — so only the exception class name and the
    SQLSTATE code may reach logs and failed-id reports, never the message.
    """
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate:
        return f"{type(exc).__name__} [sqlstate={sqlstate}]"
    return type(exc).__name__


# Resource types whose own resource / unique constraint carries a person
# identifier (the CPR), so their driver or serializer error text can embed a
# CPR — e.g. a unique-violation DETAIL on the CPR identifier. Every other FHIR
# type references a patient by logical id, not the raw CPR (nsh-enversion, !51
# review), so its error text and traceback are safe to log in full and stay
# debuggable. "UnknownResource" is included out of caution: its content is by
# definition unknown, so it is treated as if it could carry a CPR. Matched
# case-insensitively, so the same check serves both resource_type ("Patient")
# and the derived table_name ("patient" == resource_type.lower()).
_PERSON_REFERABLE_TYPES = frozenset({"patient", "unknownresource"})


def _carries_person_identifier(resource_type_or_table: str) -> bool:
    return resource_type_or_table.lower() in _PERSON_REFERABLE_TYPES


def _error_label(exc: BaseException, resource_type_or_table: str) -> str:
    """Error label for logs and failed_ids, scoped to CPR risk by resource type.

    Person-referable types (see ``_PERSON_REFERABLE_TYPES``) get the bounded
    class+SQLSTATE stand-in (``_safe_error_label``) with no message, since both
    the message and the traceback can embed a CPR. Every other type carries no
    CPR, so its full driver message is kept (bounded length) — a failed
    Organization/Encounter/… upsert stays debuggable. Call sites pair this with
    ``exc_info`` only for the non-person types, so the traceback is dropped only
    where it must be.
    """
    if _carries_person_identifier(resource_type_or_table):
        return _safe_error_label(exc)
    return str(exc)[:200]


def _reupsert_resolved_row(
    *,
    conn: Any,
    cur: Any,
    upsert_sql: str,
    resource_id: str,
    txid: int,
    payload: str,
) -> None:
    with conn.transaction():
        cur.execute(upsert_sql, (resource_id, txid, payload))


def batch_upsert_resources(
    self,
    resources: Sequence[Resource],
    resource_type: str,
    batch_size: int = 2000,
    invalidate_cache: bool = True,
) -> tuple[int, int, list[str]]:
    """
    Directly insert or update FHIR resources into the database.

    This bypasses the FHIR API validation and inserts directly into PostgreSQL.
    Useful as a workaround when Aidbox API has validation issues.

    Uses executemany() for batched execution which is 4-7x faster than
    individual inserts. Resources are processed in chunks for better
    memory management and error isolation.

    Args:
        resources: List of FHIR resources to insert
        resource_type: Type of resource (e.g., "EpisodeOfCare")
        batch_size: Number of resources to process per batch (default 500)

    Returns:
        Tuple of (successes, failures, failed_ids). Each failed_ids element
        is ``"<id>: <exception class [sqlstate=…]>"`` — the error message
        itself is never carried, since it can embed resource content
        (for Patient resources the CPR).
    """
    import uuid

    if not resources:
        return 0, 0, []

    table_name = resource_type.lower()
    successes = 0
    failures = 0
    failed_ids: list[str] = []

    # Pre-process all resources: assign IDs and serialize to JSON
    # This separates serialization time from DB time and allows batch execution
    prepared_resources: list[tuple[str, dict, Resource]] = []
    for i, resource in enumerate(resources):
        try:
            resource_id = getattr(resource, "id", None)
            if not resource_id:
                resource_id = str(uuid.uuid4())
                resource.id = resource_id

            resource_json = json.loads(resource.json(exclude_none=True))
            _transform_references_for_aidbox(resource_json)
            prepared_resources.append((resource_id, resource_json, resource))
        except Exception as e:
            failures += 1
            resource_id = getattr(resource, "id", f"resource_{i}")
            failed_ids.append(f"{resource_id}: {_error_label(e, resource_type)}")
            _logger().warning(
                f"Failed to serialize {resource_type} resource {resource_id}: "
                f"{_error_label(e, resource_type)}",
                exc_info=not _carries_person_identifier(resource_type),
            )

    if not prepared_resources:
        return successes, failures, failed_ids

    with self.get_connection() as conn:
        with conn.cursor() as cur:
            # Get the next available txid
            cur.execute(f"SELECT COALESCE(MAX(txid), 0) + 1 FROM {table_name};")
            next_txid = cur.fetchone()[0]

            # Process in batches using executemany for better performance
            # executemany is 4-7x faster than individual execute() calls
            for batch_start in range(0, len(prepared_resources), batch_size):
                batch = prepared_resources[batch_start : batch_start + batch_size]

                # Prepare batch parameters
                params = [
                    (
                        resource_id,
                        next_txid + batch_start + i,
                        json.dumps(resource_json),
                    )
                    for i, (resource_id, resource_json, _resource) in enumerate(batch)
                ]

                upsert_sql = f"""
                    INSERT INTO {table_name} (id, txid, status, resource)
                    VALUES (%s, %s, 'created'::resource_status, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        txid = EXCLUDED.txid,
                        status = 'updated'::resource_status,
                        ts = CURRENT_TIMESTAMP,
                        resource = EXCLUDED.resource
                """

                batch_err = None
                for attempt in range(1 + _BATCH_MAX_RETRIES):
                    try:
                        cur.executemany(upsert_sql, params)
                        successes += len(batch)
                        batch_err = None
                        break
                    except _resources_module().BATCH_UPSERT_FALLBACK_ERROR as e:
                        conn.rollback()
                        batch_err = e
                        if attempt < _BATCH_MAX_RETRIES:
                            delay = _BATCH_RETRY_BACKOFFS[attempt]
                            _logger().warning(
                                f"Batch insert attempt {attempt + 1}/{1 + _BATCH_MAX_RETRIES} "
                                f"failed, retrying in {delay}s: {_error_label(e, resource_type)}"
                            )
                            time.sleep(delay)
                    except Exception:
                        raise

                if batch_err is not None:
                    _logger().warning(
                        f"Batch insert failed after {1 + _BATCH_MAX_RETRIES} attempts, "
                        f"falling back to individual inserts: {_error_label(batch_err, resource_type)}"
                    )
                    for i, (resource_id, resource_json, resource) in enumerate(batch):
                        try:
                            # Per-row subtransaction (SAVEPOINT): one failing row
                            # must not poison the connection transaction and
                            # cascade "current transaction is aborted" onto every
                            # subsequent row in the batch.
                            with conn.transaction():
                                cur.execute(
                                    upsert_sql,
                                    (
                                        resource_id,
                                        next_txid + batch_start + i,
                                        json.dumps(resource_json),
                                    ),
                                )
                            successes += 1
                        except Exception as inner_e:
                            if _is_unique_violation(inner_e):
                                constraint = _constraint_name(inner_e)
                                existing_id = _resolve_existing_id_for_identifier_collision(
                                    conn=conn,
                                    cur=cur,
                                    table_name=table_name,
                                    resource_json=resource_json,
                                )
                                if existing_id:
                                    resource.id = existing_id
                                    resource_json["id"] = existing_id
                                    try:
                                        _reupsert_resolved_row(
                                            conn=conn,
                                            cur=cur,
                                            upsert_sql=upsert_sql,
                                            resource_id=existing_id,
                                            txid=next_txid + batch_start + i,
                                            payload=json.dumps(resource_json),
                                        )
                                    except Exception as retry_e:
                                        failures += 1
                                        failed_ids.append(f"{resource_id}: {str(retry_e)[:100]}")
                                        _logger().warning(
                                            f"Unique-violation inserting {resource_type} "
                                            f"{resource_id} (constraint={constraint}): "
                                            f"resolved existing row {existing_id}; "
                                            f"re-upsert failed: {retry_e}"
                                        )
                                        continue
                                    successes += 1
                                    _logger().warning(
                                        f"Unique-violation inserting {resource_type} "
                                        f"{resource_id} (constraint={constraint}): "
                                        f"resolved existing row {existing_id}; "
                                        "re-executing upsert."
                                    )
                                    continue

                            failures += 1
                            failed_ids.append(
                                f"{resource_id}: {_error_label(inner_e, resource_type)}"
                            )
                            if _is_unique_violation(inner_e):
                                _logger().warning(
                                    f"Unique-violation inserting {resource_type} "
                                    f"{resource_id} (constraint={_constraint_name(inner_e)}): "
                                    "could not re-resolve an existing row; treating as failure."
                                )
                            else:
                                _logger().warning(
                                    f"Failed to insert {resource_type} resource "
                                    f"{resource_id}: {_error_label(inner_e, resource_type)}"
                                )

            conn.commit()

    _logger().info(
        f"[DB Direct] Inserted {successes} {resource_type} resources, " f"{failures} failures"
    )

    # The shared identifier cache was (re)built BEFORE this insert —
    # so it reflects Aidbox's pre-insert state and is stale by one
    # generation of rows. Invalidate it here so the next consumer
    # (typically a clinical chain resolving refs against the
    # resources we just posted) rebuilds from current Aidbox state
    # rather than falling through to the stub-creation path.
    # See BACKLOG: without this, a reset + full_load sequence leaves
    # the on-disk SQLite cache at 0 entries, and clinical chains
    # auto-create N stubs for every reference they can't resolve.
    if successes and invalidate_cache:
        try:
            self.clear_identifier_caches_for_resources({resource_type})
        except Exception as exc:  # noqa: BLE001
            _logger().warning(
                f"[SharedCache] post-upsert invalidation failed for "
                f"{resource_type}: {exc} — downstream ref resolution "
                "may need a cold rebuild"
            )

    return successes, failures, failed_ids


def batch_upsert_resource_dicts(
    self,
    resource_dicts: list[tuple[str, dict]],
    resource_type: str,
    batch_size: int = 2000,
    invalidate_cache: bool = True,
    max_workers: int | None = None,
) -> tuple[int, int, list[str]]:
    """Upsert pre-built FHIR resource dicts — zero Pydantic overhead.

    Accepts ``(resource_id, resource_dict)`` tuples that are already in
    FHIR JSON structure.  Uses ``orjson`` for serialisation (3-10x faster
    than stdlib ``json``) and the same ``INSERT ... ON CONFLICT`` upsert
    pattern as :meth:`batch_upsert_resources`.

    Parallel sharded writes
    -----------------------
    For payloads large enough to span multiple batches, prepared rows are
    split into contiguous shards and each shard runs on its own
    connection inside a thread pool. This converts the DB-write phase
    from "one connection serialises N batches against PostgreSQL" to
    "K connections write in parallel", which is typically a 2–4x
    speedup on Aidbox-style workloads dominated by per-row server work
    (constraint checks, search-param triggers, WAL).

    Each shard runs in its own transaction and commits independently —
    there is no global atomicity across shards. The asset is idempotent
    (``ON CONFLICT (id) DO UPDATE``) so a partial failure is safely
    recovered by re-running with the same input.

    Sharding is gated by:

    - ``max_workers`` (caller override) or :data:`_DB_UPSERT_MAX_WORKERS`
      (env: ``FHIR_DB_UPSERT_MAX_WORKERS``, default 4).
    - At most one shard per ``batch_size`` rows (no shard runs <1
      batch).
    - At most one shard per :data:`_DB_UPSERT_MIN_ROWS_PER_WORKER` rows
      (env: ``FHIR_DB_UPSERT_MIN_ROWS_PER_WORKER``, default 5 000) so
      shard fixed costs are amortised.

    When the effective worker count resolves to 1 we fall back to a
    single connection holding one transaction across all batches —
    identical semantics to the pre-sharded implementation.
    """
    if not resource_dicts:
        return 0, 0, []

    total_start = perf_counter()
    prepare_start = perf_counter()
    prepared, failures, failed_ids = self._prepare_resource_dicts_for_upsert(
        resource_dicts, resource_type
    )
    prepare_seconds = perf_counter() - prepare_start
    if not prepared:
        _logger().info(
            "[DB Direct Dict Timing] "
            f"resource_type={resource_type} rows_in={len(resource_dicts)} "
            f"prepared=0 prepare={prepare_seconds:.3f}s "
            f"total={perf_counter() - total_start:.3f}s"
        )
        return 0, failures, failed_ids

    workers = self._resolve_dict_upsert_workers(
        n_rows=len(prepared),
        batch_size=batch_size,
        requested=max_workers,
    )

    upsert_start = perf_counter()
    if workers == 1:
        successes, shard_failures, shard_failed_ids = self._upsert_dicts_single_connection(
            prepared=prepared,
            resource_type=resource_type,
            batch_size=batch_size,
        )
    else:
        successes, shard_failures, shard_failed_ids = self._upsert_dicts_sharded(
            prepared=prepared,
            resource_type=resource_type,
            batch_size=batch_size,
            workers=workers,
        )
    upsert_seconds = perf_counter() - upsert_start

    failures += shard_failures
    failed_ids.extend(shard_failed_ids)

    n_batches = ceil(len(prepared) / batch_size) if batch_size > 0 else 0
    _logger().info(
        f"[DB Direct Dict] Inserted {successes} {resource_type} dicts, "
        f"{failures} failures (workers={workers}, batches={n_batches})"
    )

    invalidate_seconds = 0.0
    # Invalidate the shared identifier cache — see the matching
    # comment in :meth:`batch_upsert_resources` for the rationale.
    if successes and invalidate_cache:
        invalidate_start = perf_counter()
        try:
            self.clear_identifier_caches_for_resources({resource_type})
        except Exception as exc:  # noqa: BLE001
            _logger().warning(
                f"[SharedCache] post-upsert invalidation failed for "
                f"{resource_type}: {exc} — downstream ref resolution "
                "may need a cold rebuild"
            )
        invalidate_seconds = perf_counter() - invalidate_start

    _logger().info(
        "[DB Direct Dict Timing] "
        f"resource_type={resource_type} rows_in={len(resource_dicts)} "
        f"prepared={len(prepared)} successes={successes} failures={failures} "
        f"workers={workers} batches={n_batches} "
        f"prepare={prepare_seconds:.3f}s upsert={upsert_seconds:.3f}s "
        f"invalidate={invalidate_seconds:.3f}s "
        f"total={perf_counter() - total_start:.3f}s"
    )

    return successes, failures, failed_ids


def _prepare_resource_dicts_for_upsert(
    resource_dicts: list[tuple[str, dict]],
    resource_type: str,
) -> tuple[list[tuple[str, dict]], int, list[str]]:
    """Apply the Aidbox reference transform to each dict in-place.

    Splits into the (prepared, failure_count, failed_id_messages) shape
    used by the upsert path. CPU-bound and runs on the calling thread —
    cheap relative to the DB write phase that follows.
    """
    prepared: list[tuple[str, dict]] = []
    failures = 0
    failed_ids: list[str] = []
    for resource_id, resource_dict in resource_dicts:
        try:
            _transform_references_for_aidbox(resource_dict)
            prepared.append((resource_id, resource_dict))
        except Exception as e:  # noqa: BLE001 — preserve historic behaviour
            failures += 1
            failed_ids.append(f"{resource_id}: {_error_label(e, resource_type)}")
            _logger().warning(
                f"Failed to prepare {resource_id} for dict upsert: "
                f"{_error_label(e, resource_type)}",
                exc_info=not _carries_person_identifier(resource_type),
            )
    return prepared, failures, failed_ids


def _resolve_dict_upsert_workers(*, n_rows: int, batch_size: int, requested: int | None) -> int:
    """Pick the effective shard count, bounded by config and workload.

    The ``by_rows`` bound uses floor division so it is a *hard* lower
    bound on rows-per-shard: with ``MIN_ROWS_PER_WORKER=10_000`` we
    only authorise a second shard once there are 20_000 rows (so each
    shard sees a full 10_000), not at 10_001 where each shard would
    see ~5_000. The point of the bound is to amortise the per-shard
    fixed cost (connection setup, txid bookkeeping, thread scheduling)
    — splitting a barely-over-threshold payload defeats it.
    """
    cap = (
        max(1, requested)
        if requested is not None
        else max(1, _resources_module()._DB_UPSERT_MAX_WORKERS)
    )
    if n_rows <= 0 or batch_size <= 0:
        return 1
    n_batches = max(1, ceil(n_rows / batch_size))
    rows_per_worker = max(1, _resources_module()._DB_UPSERT_MIN_ROWS_PER_WORKER)
    by_rows = max(1, n_rows // rows_per_worker)
    base_workers, _ = resolve_worker_count(
        configured_max_workers=cap,
        task_count=n_rows,
        parallel_threshold=1,
    )
    return max(1, min(base_workers, n_batches, by_rows))


def _upsert_dicts_single_connection(
    self,
    *,
    prepared: list[tuple[str, dict]],
    resource_type: str,
    batch_size: int,
) -> tuple[int, int, list[str]]:
    """Single-connection, single-transaction path used when sharding is off.

    Behaviour matches the pre-sharded implementation byte-for-byte: one
    ``SELECT MAX(txid)``, one batched insert loop, one commit. The
    existing fallback unit tests exercise this path exclusively (they
    pass two rows, which never crosses the sharding threshold).
    """
    table_name = resource_type.lower()
    total_start = perf_counter()
    txid_seconds = 0.0
    write_seconds = 0.0
    commit_seconds = 0.0
    with self.get_connection() as conn:
        with conn.cursor() as cur:
            txid_start = perf_counter()
            cur.execute(f"SELECT COALESCE(MAX(txid), 0) + 1 FROM {table_name};")
            txid_base = cur.fetchone()[0]
            txid_seconds = perf_counter() - txid_start
            write_start = perf_counter()
            successes, failures, failed_ids = self._run_dict_upsert_batches(
                cur=cur,
                conn=conn,
                rows=prepared,
                table_name=table_name,
                batch_size=batch_size,
                txid_base=txid_base,
            )
            write_seconds = perf_counter() - write_start
        commit_start = perf_counter()
        conn.commit()
        commit_seconds = perf_counter() - commit_start
    _logger().info(
        "[DB Direct Dict Timing] "
        f"table={table_name} path=single rows={len(prepared)} "
        f"batches={ceil(len(prepared) / batch_size) if batch_size > 0 else 0} "
        f"txid={txid_seconds:.3f}s write={write_seconds:.3f}s "
        f"commit={commit_seconds:.3f}s total={perf_counter() - total_start:.3f}s"
    )
    return successes, failures, failed_ids


def _upsert_dicts_sharded(
    self,
    *,
    prepared: list[tuple[str, dict]],
    resource_type: str,
    batch_size: int,
    workers: int,
) -> tuple[int, int, list[str]]:
    """Reserve a contiguous txid range, split rows into shards, write in parallel.

    Each shard owns ``rows[shard_start : shard_start + shard_size]`` and
    is assigned txids ``[txid_base + shard_start, ...)`` so the global
    per-row txid mapping matches the single-connection path. Shards run
    on separate connections; each commits independently.

    Errors are aggregated: every shard runs to completion (or until its
    own connection raises) before we re-raise the first exception. This
    avoids leaving worker threads running after the pool's ``__exit__``
    and keeps the diagnostics deterministic.
    """
    table_name = resource_type.lower()
    total_start = perf_counter()
    txid_start = perf_counter()
    txid_base = self._reserve_txid_base(table_name)
    txid_seconds = perf_counter() - txid_start

    shard_size = max(1, ceil(len(prepared) / workers))
    shards: list[tuple[int, list[tuple[str, dict]]]] = []
    for shard_index in range(workers):
        start = shard_index * shard_size
        end = min(len(prepared), start + shard_size)
        if start >= end:
            break
        shards.append((start, prepared[start:end]))

    successes = 0
    failures = 0
    failed_ids: list[str] = []
    shard_exceptions: list[BaseException] = []

    write_start = perf_counter()
    with ThreadPoolExecutor(
        max_workers=len(shards),
        thread_name_prefix=f"upsert-{table_name}",
    ) as executor:
        future_to_offset = {
            executor.submit(
                self._upsert_dicts_shard_worker,
                shard_rows=shard_rows,
                table_name=table_name,
                batch_size=batch_size,
                txid_base=txid_base + shard_start,
            ): shard_start
            for shard_start, shard_rows in shards
        }
        for future in as_completed(future_to_offset):
            try:
                s, f, fids = future.result()
            except BaseException as exc:  # noqa: BLE001
                shard_exceptions.append(exc)
                continue
            successes += s
            failures += f
            failed_ids.extend(fids)
    write_seconds = perf_counter() - write_start

    if shard_exceptions:
        # Re-raise the first failure; remaining shards (if any) committed
        # via their own transactions and the asset is idempotent on retry.
        raise shard_exceptions[0]

    _logger().info(
        "[DB Direct Dict Timing] "
        f"table={table_name} path=sharded rows={len(prepared)} "
        f"workers={workers} shards={len(shards)} shard_size={shard_size} "
        f"txid={txid_seconds:.3f}s write={write_seconds:.3f}s "
        f"total={perf_counter() - total_start:.3f}s"
    )

    return successes, failures, failed_ids


def _reserve_txid_base(self, table_name: str) -> int:
    """Read the next available txid for a resource table.

    Concurrent writers between this read and our INSERT can consume
    txids inside our reserved range. ``ON CONFLICT (id)`` is keyed by
    id (not txid) and Aidbox tolerates non-monotonic txids per row, so
    the only impact of a collision is on change-stream consumers that
    use txid as a watermark — a pre-existing risk shared with the
    single-connection path.
    """
    with self.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(txid), 0) + 1 FROM {table_name};")
            return cur.fetchone()[0]


def _upsert_dicts_shard_worker(
    self,
    *,
    shard_rows: list[tuple[str, dict]],
    table_name: str,
    batch_size: int,
    txid_base: int,
) -> tuple[int, int, list[str]]:
    """Run the upsert loop for a single shard on its own connection."""
    total_start = perf_counter()
    write_seconds = 0.0
    commit_seconds = 0.0
    with self.get_connection() as conn:
        with conn.cursor() as cur:
            write_start = perf_counter()
            successes, failures, failed_ids = self._run_dict_upsert_batches(
                cur=cur,
                conn=conn,
                rows=shard_rows,
                table_name=table_name,
                batch_size=batch_size,
                txid_base=txid_base,
            )
            write_seconds = perf_counter() - write_start
        commit_start = perf_counter()
        conn.commit()
        commit_seconds = perf_counter() - commit_start
    _logger().info(
        "[DB Direct Dict Timing] "
        f"table={table_name} path=shard rows={len(shard_rows)} "
        f"batches={ceil(len(shard_rows) / batch_size) if batch_size > 0 else 0} "
        f"write={write_seconds:.3f}s commit={commit_seconds:.3f}s "
        f"total={perf_counter() - total_start:.3f}s"
    )
    return successes, failures, failed_ids


def _run_dict_upsert_batches(
    *,
    cur: Any,
    conn: Any,
    rows: list[tuple[str, dict]],
    table_name: str,
    batch_size: int,
    txid_base: int,
) -> tuple[int, int, list[str]]:
    """Execute the batched ``INSERT ... ON CONFLICT`` loop on an open cursor.

    Each batch's ``executemany`` is wrapped in a psycopg pipeline block
    so the per-row Bind/Execute round-trips coalesce into a single
    network sync — meaningfully faster on JSONB upserts where the
    per-row work is small relative to network latency.

    On a database-level batch error the pipeline auto-syncs at exit and
    the error surfaces; we then fall back to per-row inserts to isolate
    the offending payload, preserving the pre-existing fallback
    contract exercised by ``test_fhir_database_batch_upsert_fallback``.
    Non-database errors propagate immediately.
    """
    upsert_sql = f"""
        INSERT INTO {table_name} (id, txid, status, resource)
        VALUES (%s, %s, 'created'::resource_status, %s)
        ON CONFLICT (id) DO UPDATE SET
            txid = EXCLUDED.txid,
            status = 'updated'::resource_status,
            ts = CURRENT_TIMESTAMP,
            resource = EXCLUDED.resource
    """

    successes = 0
    failures = 0
    failed_ids: list[str] = []
    serialize_seconds = 0.0
    executemany_seconds = 0.0
    fallback_seconds = 0.0
    n_batches = 0

    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start : batch_start + batch_size]
        n_batches += 1
        serialize_start = perf_counter()
        params = [
            (
                rid,
                txid_base + batch_start + i,
                orjson.dumps(rjson).decode("utf-8"),
            )
            for i, (rid, rjson) in enumerate(batch)
        ]
        serialize_seconds += perf_counter() - serialize_start

        batch_err = None
        for attempt in range(1 + _BATCH_MAX_RETRIES):
            try:
                exec_start = perf_counter()
                with conn.pipeline():
                    cur.executemany(upsert_sql, params)
                executemany_seconds += perf_counter() - exec_start
                successes += len(batch)
                batch_err = None
                break
            except _resources_module().BATCH_UPSERT_FALLBACK_ERROR as e:
                fallback_start = perf_counter()
                conn.rollback()
                batch_err = e
                if attempt < _BATCH_MAX_RETRIES:
                    delay = _BATCH_RETRY_BACKOFFS[attempt]
                    _logger().warning(
                        f"Dict batch insert attempt {attempt + 1}/{1 + _BATCH_MAX_RETRIES} "
                        f"failed, retrying in {delay}s: {_error_label(e, table_name)}"
                    )
                    time.sleep(delay)
            except Exception:
                raise

        if batch_err is not None:
            _logger().warning(
                f"Dict batch insert failed after {1 + _BATCH_MAX_RETRIES} attempts, "
                f"falling back to individual: {_error_label(batch_err, table_name)}"
            )
            for i, (rid, rjson) in enumerate(batch):
                try:
                    # Per-row subtransaction (SAVEPOINT): one failing row must
                    # not poison the connection transaction and cascade
                    # "current transaction is aborted" onto every subsequent
                    # row in the batch.
                    with conn.transaction():
                        cur.execute(
                            upsert_sql,
                            (
                                rid,
                                txid_base + batch_start + i,
                                orjson.dumps(rjson).decode("utf-8"),
                            ),
                        )
                    successes += 1
                except Exception as inner_e:  # noqa: BLE001
                    if _is_unique_violation(inner_e):
                        constraint = _constraint_name(inner_e)
                        existing_id = _resolve_existing_id_for_identifier_collision(
                            conn=conn,
                            cur=cur,
                            table_name=table_name,
                            resource_json=rjson,
                        )
                        if existing_id:
                            rjson["id"] = existing_id
                            try:
                                _reupsert_resolved_row(
                                    conn=conn,
                                    cur=cur,
                                    upsert_sql=upsert_sql,
                                    resource_id=existing_id,
                                    txid=txid_base + batch_start + i,
                                    payload=orjson.dumps(rjson).decode("utf-8"),
                                )
                            except Exception as retry_e:  # noqa: BLE001
                                failures += 1
                                failed_ids.append(f"{rid}: {str(retry_e)[:100]}")
                                _logger().warning(
                                    f"Unique-violation inserting into {table_name} "
                                    f"(id={rid}, constraint={constraint}): "
                                    f"resolved existing row {existing_id}; "
                                    f"re-upsert failed: {retry_e}"
                                )
                                continue
                            successes += 1
                            _logger().warning(
                                f"Unique-violation inserting into {table_name} "
                                f"(id={rid}, constraint={constraint}): resolved "
                                f"existing row {existing_id}; re-executing upsert."
                            )
                            continue

                    failures += 1
                    failed_ids.append(f"{rid}: {_error_label(inner_e, table_name)}")
                    if _is_unique_violation(inner_e):
                        _logger().warning(
                            f"Unique-violation inserting into {table_name} "
                            f"(id={rid}, constraint={_constraint_name(inner_e)}): "
                            "could not re-resolve an existing row; treating as failure."
                        )
            fallback_seconds += perf_counter() - fallback_start

    _logger().info(
        "[DB Direct Dict Timing] "
        f"table={table_name} phase=batches rows={len(rows)} "
        f"batch_size={batch_size} batches={n_batches} "
        f"serialize={serialize_seconds:.3f}s "
        f"executemany={executemany_seconds:.3f}s "
        f"fallback={fallback_seconds:.3f}s "
        f"successes={successes} failures={failures}"
    )

    return successes, failures, failed_ids


# ---------------------------------------------------------------------------
# CodeSystem concept denormalisation for the Aidbox ``concept`` table
# ---------------------------------------------------------------------------


def _flatten_concepts(
    concepts: list[dict],
    *,
    codesystem_id: str,
    system_url: str,
    ancestors: list[str] | None = None,
) -> list[tuple[str, dict]]:
    """Flatten a recursive FHIR ``concept[]`` tree into Aidbox ``concept`` rows.

    Each returned tuple is ``(row_id, resource_dict)`` matching the Aidbox
    ``public.concept`` table schema.
    """
    if ancestors is None:
        ancestors = []

    rows: list[tuple[str, dict]] = []
    for node in concepts:
        code = node["code"]
        row_id = f"{codesystem_id}-{code}"

        resource: dict = {
            "id": row_id,
            "code": code,
            "system": system_url,
            "display": node.get("display", code),
        }

        if ancestors:
            resource["hierarchy"] = list(ancestors)

        fhir_props = node.get("property")
        if fhir_props:
            aidbox_props: dict = {}
            for p in fhir_props:
                prop_code = p.get("code", "")
                for fhir_key, aidbox_key in (
                    ("valueString", "string"),
                    ("valueCode", "code"),
                    ("valueCoding", "Coding"),
                    ("valueInteger", "integer"),
                    ("valueBoolean", "boolean"),
                    ("valueDateTime", "dateTime"),
                    ("valueDecimal", "decimal"),
                ):
                    if fhir_key in p:
                        aidbox_props[prop_code] = {aidbox_key: p[fhir_key]}
                        break
            if aidbox_props:
                resource["property"] = {codesystem_id: aidbox_props}

        rows.append((row_id, resource))

        children = node.get("concept")
        if children:
            rows.extend(
                _flatten_concepts(
                    children,
                    codesystem_id=codesystem_id,
                    system_url=system_url,
                    ancestors=[*ancestors, code],
                )
            )

    return rows


def upsert_codesystem_concepts(
    db_resource,
    *,
    codesystem_id: str,
    system_url: str,
    concept_tree: list[dict],
    batch_size: int = 5000,
) -> tuple[int, int]:
    """Denormalise a CodeSystem concept tree into Aidbox's ``concept`` table.

    Returns ``(inserted, deleted)`` counts.
    """
    flat = _flatten_concepts(
        concept_tree,
        codesystem_id=codesystem_id,
        system_url=system_url,
    )
    if not flat:
        return 0, 0

    with db_resource.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(txid), 0) + 1 FROM concept;")
            txid_base = cur.fetchone()[0]

            cur.execute(
                "DELETE FROM concept WHERE id LIKE %s",
                (f"{codesystem_id}-%",),
            )
            deleted = cur.rowcount

            upsert_sql = """
                INSERT INTO concept (id, txid, status, resource)
                VALUES (%s, %s, 'created'::resource_status, %s)
                ON CONFLICT (id) DO UPDATE SET
                    txid = EXCLUDED.txid,
                    status = 'updated'::resource_status,
                    ts = CURRENT_TIMESTAMP,
                    resource = EXCLUDED.resource
            """

            inserted = 0
            for batch_start in range(0, len(flat), batch_size):
                batch = flat[batch_start : batch_start + batch_size]
                params = [
                    (
                        row_id,
                        txid_base + batch_start + i,
                        orjson.dumps(resource).decode("utf-8"),
                    )
                    for i, (row_id, resource) in enumerate(batch)
                ]
                try:
                    with conn.pipeline():
                        cur.executemany(upsert_sql, params)
                    inserted += len(batch)
                except Exception:
                    conn.rollback()
                    for i, (row_id, resource) in enumerate(batch):
                        try:
                            cur.execute(
                                upsert_sql,
                                (
                                    row_id,
                                    txid_base + batch_start + i,
                                    orjson.dumps(resource).decode("utf-8"),
                                ),
                            )
                            inserted += 1
                        except Exception as inner_e:
                            _logger().warning(f"Failed to insert concept {row_id}: {inner_e}")

            conn.commit()

    _logger().info(
        f"[Concept Upsert] Inserted {inserted} concept rows for "
        f"CodeSystem/{codesystem_id} (deleted {deleted} stale)"
    )
    return inserted, deleted
