# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Transactional identity matching for the existing direct-upsert entry point.

The domain service still decides who to build and which sources own fields.
This module only makes match/create/update atomic. A short, per-batch table
lock also excludes writers that do not take advisory locks; ordinary readers
continue.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Sequence
from uuid import uuid4

from core.fhir.aidbox_json import transform_references_for_aidbox
from core.fhir.alias_lookup import alias_resources
from fhir.resources.R4B.resource import Resource


class IdentityWriteConflict(ValueError):
    """Multiple stored resources match one incoming identity; never choose one."""


def _pairs(identifiers: list[dict]) -> set[tuple[str, str]]:
    return {
        (str(i["system"]), str(i["value"]))
        for i in identifiers
        if i.get("system") and i.get("value")
    }


def batch_upsert_by_identity(
    database,
    resources: Sequence[Resource],
    resource_type: str,
    *,
    create_only: Sequence[bool],
    batch_size: int,
    invalidate_cache: bool,
):
    """Return actual write/skip counts; assign resolved ids only after commit.

    Lookup covers every identifier, including secondary and historical values.
    Existing identifiers are retained when a concurrent producer supplies a
    narrower identity set. A resource whose id the domain already resolved is
    written to that id even when an alias it already carries also lives on
    another resource. A value it does not carry yet, but another resource does,
    is never added: the value is withheld, the rest of the resource is written,
    and the resource is counted as ``withheld``. Only an unresolved identity that
    matches several resources is a conflict, and it rolls back the entire batch.
    Failed batches never enter the generic unique-violation retry/upsert path.
    """
    from core.fhir.direct_upsert import UpsertResult, _logger, _safe_error_label

    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", resource_type):
        raise ValueError("Invalid resource type")
    if len(create_only) != len(resources) or batch_size < 1:
        raise ValueError("Invalid identity-upsert batch arguments")
    table = resource_type.lower()
    written = skipped = failed = withheld = 0
    labels: list[str] = []
    # Step timings; a step over one second is logged at once.
    ur: dict[str, float] = {}
    LANGSOMT = 1.0

    def maalt(navn: str, start: float) -> float:
        naa = time.perf_counter()
        sek = naa - start
        ur[navn] = ur.get(navn, 0.0) + sek
        if sek >= LANGSOMT:
            _logger().info("Identity upsert: %s took %.1f s", navn, sek)
        return naa

    for start in range(0, len(resources), batch_size):
        batch = resources[start : start + batch_size]
        flags = create_only[start : start + batch_size]
        try:
            payloads = [json.loads(r.json(exclude_none=True)) for r in batch]
            for payload in payloads:
                transform_references_for_aidbox(payload)
                if not _pairs(payload.get("identifier", [])):
                    raise IdentityWriteConflict("Identity-safe writes require identifiers")
            ids = [str(p["id"]) for p in payloads if p.get("id")]
            assigned: list[str] = []
            params: list[tuple] = []
            _logger().info(
                "Identity upsert: batch %d of %d started (%d resources)",
                start // batch_size + 1,
                (len(resources) + batch_size - 1) // batch_size,
                len(batch),
            )
            t0 = time.perf_counter()
            with database.get_connection() as conn:
                with conn.transaction(), conn.cursor() as cur:
                    cur.execute("SET LOCAL lock_timeout = '30s'")
                    cur.execute(f'LOCK TABLE "{table}" IN SHARE ROW EXCLUSIVE MODE')
                    t0 = maalt("laas", t0)
                    # Known ids through the primary key; every alias through one
                    # GIN probe per needle (alias_lookup) -- never a scan per batch.
                    cur.execute(
                        f"SELECT id::text, resource->'identifier' FROM \"{table}\" "
                        "WHERE id = ANY(%s::text[])",
                        (ids,),
                    )
                    identifiers = {str(fid): values or [] for fid, values in cur.fetchall()}
                    t0 = maalt("id_opslag", t0)
                    # Probe only values the resource does not carry yet.
                    naale = sorted(
                        {
                            pair
                            for p in payloads
                            for pair in _pairs(p.get("identifier", []))
                            - _pairs(identifiers.get(str(p.get("id") or ""), []))
                        }
                    )
                    identifiers.update(alias_resources(cur, table, naale))
                    t0 = maalt("alias_opslag", t0)
                    index: dict[tuple[str, str], set[str]] = {}
                    for fid, values in identifiers.items():
                        for pair in _pairs(values):
                            index.setdefault(pair, set()).add(fid)
                    cur.execute(f'SELECT COALESCE(MAX(txid), 0) + 1 FROM "{table}"')
                    txid = cur.fetchone()[0]
                    t0 = maalt("txid", t0)
                    for payload, only_new in zip(payloads, flags, strict=True):
                        pairs = _pairs(payload.get("identifier", []))
                        known = payload.get("id")
                        if known in identifiers:
                            taken = {
                                pair
                                for pair in pairs - _pairs(identifiers[known])
                                if index.get(pair, set()) - {known}
                            }
                            if taken:
                                # A new value already on another resource is withheld.
                                withheld += 1
                                payload["identifier"] = [
                                    i
                                    for i in payload["identifier"]
                                    if (str(i.get("system")), str(i.get("value"))) not in taken
                                ]
                                pairs -= taken
                        matches = set().union(*(index.get(pair, set()) for pair in pairs))
                        if known in identifiers:
                            matches.add(known)
                        if len(matches) > 1 and known in matches:
                            # The domain chose the id; a person may still have two resources.
                            matches = {known}
                        if len(matches) > 1:
                            raise IdentityWriteConflict("Multiple resources match one identity")
                        fid = next(iter(matches)) if matches else payload.get("id") or str(uuid4())
                        assigned.append(fid)
                        if matches and only_new:
                            continue
                        payload["id"] = fid
                        # Preserve aliases another batch added after our domain lookup.
                        payload.setdefault("identifier", []).extend(
                            i
                            for i in identifiers.get(fid, [])
                            if (str(i.get("system")), str(i.get("value"))) not in pairs
                        )
                        identifiers[fid] = payload["identifier"]
                        for pair in _pairs(payload["identifier"]):
                            index.setdefault(pair, set()).add(fid)
                        params.append((fid, txid + len(params), json.dumps(payload)))
                    t0 = maalt("match", t0)
                    if params:
                        cur.executemany(
                            f'INSERT INTO "{table}" (id, txid, status, resource) '
                            "VALUES (%s, %s, 'created'::resource_status, %s) "
                            "ON CONFLICT (id) DO UPDATE SET txid=EXCLUDED.txid, "
                            "status='updated'::resource_status, ts=CURRENT_TIMESTAMP, "
                            "resource=EXCLUDED.resource",
                            params,
                        )
                    t0 = maalt("skrivning", t0)
            for resource, fid in zip(batch, assigned, strict=True):
                resource.id = fid
            written += len(params)
            skipped += len(batch) - len(params)
            _logger().info(
                "Identity upsert: batch %d, %d written — %s",
                start // batch_size + 1,
                len(params),
                ", ".join(f"{navn} {sek:.1f} s" for navn, sek in sorted(ur.items())),
            )
            ur.clear()
        except Exception as exc:
            # Neither identifiers, JSON nor driver text may escape this boundary.
            failed += len(batch)
            labels.append(_safe_error_label(exc))
            _logger().warning("Identity upsert: %d failed (%s)", len(batch), _safe_error_label(exc))
            break  # Do not continue past a failed checkpoint batch.
    if failed:
        failed = len(resources) - written - skipped
    if written and invalidate_cache:
        try:
            database.clear_identifier_caches_for_resources({resource_type})
        except Exception as exc:
            _logger().warning("Identity-upsert cache invalidation failed (%s)", type(exc).__name__)
    if withheld:
        _logger().warning(
            "Identity upsert: %d resource(s) written without a new value that another "
            "resource already carries",
            withheld,
        )
    return UpsertResult(written, failed, labels, skipped=skipped, withheld=withheld)
