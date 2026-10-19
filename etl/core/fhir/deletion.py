# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Transactional deletion-marker primitive; source policy belongs to services."""

import json
import re

from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED


def mark_resources_deleted(
    database, resource_type, row_ids, *, missing_identifiers=None, protected_sources=()
):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", resource_type):
        raise ValueError("Invalid resource type")
    ids = sorted(set(row_ids))
    table = resource_type.lower()
    marker = [{"url": EXTENSION_FHIR_IS_DELETED, "valueInteger": 1}]
    missing_systems = {system for system, _ in missing_identifiers or ()}
    updated = skipped = 0
    for start in range(0, len(ids), 500):
        chunk = ids[start : start + 500]
        with database.get_connection() as conn:
            with conn.transaction(), conn.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '30s'")
                cursor.execute(f'LOCK TABLE "{table}" IN SHARE ROW EXCLUSIVE MODE')
                cursor.execute(
                    f"SELECT id, resource->'identifier', resource#>'{{meta,tag}}', "
                    f'resource @> %s::jsonb FROM "{table}" WHERE id = ANY(%s)',
                    (json.dumps({"extension": marker}), chunk),
                )
                pending = []
                for fid, identifiers, tags, already in cursor.fetchall():
                    supported = any(
                        (t.get("system"), t.get("code")) in protected_sources for t in tags or []
                    )
                    source_ids = {
                        (i.get("system"), i.get("value"))
                        for i in identifiers or []
                        if missing_identifiers is not None and i.get("system") in missing_systems
                    }
                    safe = missing_identifiers is None or (
                        bool(source_ids) and source_ids <= missing_identifiers
                    )
                    if already or supported or not safe:
                        skipped += 1
                    else:
                        pending.append(fid)
                cursor.execute(f'SELECT COALESCE(MAX(txid),0) FROM "{table}"')
                txid = cursor.fetchone()[0]
                if pending:
                    cursor.executemany(
                        f"UPDATE \"{table}\" SET resource=jsonb_set(resource, '{{extension}}', "
                        "COALESCE((SELECT jsonb_agg(e) FROM jsonb_array_elements("
                        "COALESCE(resource->'extension','[]'::jsonb)) e WHERE e->>'url' <> %s),"
                        "'[]'::jsonb) || %s::jsonb), txid=%s, ts=CURRENT_TIMESTAMP, "
                        "status='updated'::resource_status WHERE id=%s",
                        [
                            (EXTENSION_FHIR_IS_DELETED, json.dumps(marker), txid + i + 1, fid)
                            for i, fid in enumerate(pending)
                        ],
                    )
                updated += len(pending)
    return updated, skipped
