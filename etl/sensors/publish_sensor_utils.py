# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared plumbing for the delivery-driven publish sensors.

A publish sensor reads the archive's delivery signature, compares it with the
signature tag on the last successful publish run, and requests a run when the
two differ. The reads below are the part that carries no domain: whether a
table exists yet, the last successful run for a signature, and the tags of a
run record.

``signature_tag`` is a parameter, not a module constant: each domain tags its
runs under its own namespace ("haiba/delivery_signature",
"udredningsret/delivery_signature"), and a sensor that queried another
domain's tag would find no previous publish and request a run on every tick.
"""

import dagster as dg

_OBJECT_EXISTS_SQL = (
    "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s "
    "ON s.schema_id = t.schema_id WHERE s.name = ? AND t.name = ?"
)


def table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(_OBJECT_EXISTS_SQL, (schema, table))
    return bool(cursor.fetchone()[0])


def last_successful_publish(instance, job_name: str, signature: str, signature_tag: str):
    """The most recent SUCCESS publish run covering this delivery, or None."""
    records = instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=job_name,
            statuses=[dg.DagsterRunStatus.SUCCESS],
            tags={signature_tag: signature},
        ),
        limit=1,
    )
    return records[0] if records else None


def run_tags(record) -> dict:
    return dict(getattr(getattr(record, "dagster_run", None), "tags", None) or {})
