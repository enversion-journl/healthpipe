# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Operator-triggered cleanup: soft-delete arc rows whose key vanished from IFDB.

The delta IFDB extract can only see rows that still exist in the source -- a row
hard-deleted in IFDB lingers in the archive with ``dw_is_deleted = 0`` forever.
``source_archive_reconciliation`` detects that (``vanished_not_marked_deleted``); this
asset is the fix: it stages the source's CURRENT live merge keys (see
``checks/oracle_source.stage_ifdb_keys``) and sets ``dw_is_deleted = 1`` on the
active arc rows whose key is gone. The arc analogue of
``dqa_fhir_server_cleanup`` -- same snapshot-cutoff guard, same dry-run/max-rows
posture, soft-delete only (reversible).

Every ``ARC_SUBJECTS`` entity is wired, composite merge keys included --
Location's grain is (OBJECT_ID, PERIOD_START). Location depends on this asset:
its join-delta extract carries no delete detection, so this operator-triggered
cleanup is the only thing that retires its hard-deleted rows;
``source_archive_reconciliation`` counts the backlog weekly as
``vanished_not_marked_deleted``.

NB: IFDB *retains* logically-deleted rows (its own DELETED/SLETTET flags), so a
key genuinely disappearing from IFDB is rare -- expect small counts; a large one
is systemic (wrong source table / partial IFDB state), which the ``max_rows``
guard and the 0-staged-keys refusal are there to catch.
"""

import dagster as dg
from dqa.checks.helpers import sql_literal
from dqa.checks.oracle_source import stage_ifdb_keys
from dqa.config import ARC_SUBJECTS
from dqa.model import DqaSubject
from pydantic import Field


def _entities() -> dict[str, DqaSubject]:
    """entity name -> arc subject. All of them -- the merge key (possibly
    composite) comes from the subject's check params, not from ReplaySpec."""
    return {s.name: s for s in ARC_SUBJECTS}


class ArcVanishedCleanupConfig(dg.Config):
    entity: str = Field(
        description="Which archive to clean: any ARC_SUBJECTS name, e.g. "
        "arc_ifdb_encounter | arc_ifdb_episode_of_care | arc_ifdb_patient | "
        "arc_ifdb_location.",
    )
    dry_run: bool = Field(
        default=False,
        description="Count what would be soft-deleted, log it, and make no changes.",
    )
    max_rows: int = Field(
        default=10_000,
        description="Abort if more rows would be soft-deleted -- IFDB hard-deletes are "
        "rare, so a spike is systemic (wrong source table / partial IFDB state).",
    )


@dg.asset(
    name="dqa_arc_ifdb_vanished_cleanup",
    group_name="dqa_operations",
    compute_kind="mssql",
    required_resource_keys={"mssql_target_staging", "ifdb_oracle"},
    description=(
        "Operator-triggered: soft-delete (dw_is_deleted=1) arc.* rows whose merge key "
        "no longer exists in the IFDB Oracle source. Run dry_run=true first."
    ),
)
def dqa_arc_ifdb_vanished_cleanup(
    context: dg.AssetExecutionContext,
    config: ArcVanishedCleanupConfig,
) -> dg.MaterializeResult:
    entities = _entities()
    if config.entity not in entities:
        raise dg.Failure(
            description=f"Unknown entity {config.entity!r}; expected one of {sorted(entities)}."
        )
    subject = entities[config.entity]
    schema, table = subject.table.split(".")
    arc = f"[{schema}].[{table}]"
    (spec,) = subject.checks
    source_table = spec.params["source_table"]
    keys = list(spec.params["key_columns"])
    # Single key keeps the historical staged column name ``k``; composite keys
    # stage as ``k0..k{n-1}`` (see stage_ifdb_keys).
    stage_cols = ["k"] if len(keys) == 1 else [f"k{i}" for i in range(len(keys))]
    res = context.resources

    with res.mssql_target_staging.get_connection() as conn:
        cur = conn.cursor()
        # Snapshot cutoff BEFORE staging (same TOCTOU guard as fhir_server_cleanup):
        # a row inserted into IFDB after our snapshot and archived by a concurrent
        # daily run before our UPDATE must not be mistaken for "vanished". Only rows
        # that were already archived before the snapshot are eligible.
        cur.execute("SELECT SYSDATETIME()")
        cutoff = cur.fetchone()[0]
        stage, live = stage_ifdb_keys(conn, cur, res.ifdb_oracle, source_table, keys)
        context.log.info(
            f"dqa_arc_ifdb_vanished_cleanup[{config.entity}]: staged {live} live "
            f"({', '.join(keys)}) keys from {source_table} (cutoff {cutoff})."
        )

        # An empty staged set (wrong source table, empty/partial IFDB, staging
        # failure) would soft-delete the WHOLE archive -- refuse.
        if live == 0:
            raise dg.Failure(
                description=(
                    f"dqa_arc_ifdb_vanished_cleanup[{config.entity}]: staging returned 0 "
                    f"live keys from {source_table}. Refusing to soft-delete (that would "
                    f"retire every active arc row). Investigate before retrying."
                ),
            )

        not_null = " AND ".join(f"f.[{k}] IS NOT NULL" for k in keys)
        key_match = " AND ".join(
            f"t.{sc} = f.[{k}]" for sc, k in zip(stage_cols, keys, strict=True)
        )
        where = (
            f"WHERE f.[dw_is_deleted] = 0 AND {not_null} "
            f"AND NOT EXISTS (SELECT 1 FROM {stage} t WHERE {key_match}) "
            f"AND f.[dw_timestamp_insert] <= {sql_literal(cutoff)}"
        )
        cur.execute(f"SELECT COUNT(*) FROM {arc} f {where}")
        vanished = int(cur.fetchone()[0] or 0)
        context.log.info(
            f"dqa_arc_ifdb_vanished_cleanup[{config.entity}]: {vanished} active arc rows "
            f"have no live source key."
        )

        if vanished > config.max_rows:
            raise dg.Failure(
                description=(
                    f"{vanished} rows exceed max_rows ({config.max_rows}). IFDB "
                    f"hard-deletes are rare -- a spike this size is systemic; "
                    f"investigate before soft-deleting."
                ),
            )
        if config.dry_run or vanished == 0:
            return dg.MaterializeResult(
                metadata={
                    "entity": config.entity,
                    "live_in_source": live,
                    "would_soft_delete" if config.dry_run else "soft_deleted": (
                        vanished if config.dry_run else 0
                    ),
                    "status": dg.MetadataValue.text(
                        "dry_run" if config.dry_run else "nothing_to_do"
                    ),
                }
            )

        cur.execute(
            f"UPDATE f SET f.[dw_is_deleted] = 1, f.[dw_timestamp_update] = SYSDATETIME() "
            f"FROM {arc} f {where}"
        )
        soft_deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else vanished
        conn.commit()
        context.log.info(
            f"dqa_arc_ifdb_vanished_cleanup[{config.entity}]: soft-deleted "
            f"{soft_deleted} rows (committed)."
        )

    return dg.MaterializeResult(
        metadata={
            "entity": config.entity,
            "live_in_source": live,
            "soft_deleted": soft_deleted,
        }
    )


dqa_arc_ifdb_vanished_cleanup_job = dg.define_asset_job(
    name="dqa_arc_ifdb_vanished_cleanup_job",
    selection=dg.AssetSelection.assets("dqa_arc_ifdb_vanished_cleanup"),
    description=("Soft-delete arc.* rows whose merge key vanished from IFDB (operator-triggered)."),
    tags={"domain": "dqa", "dqa_track": "arc"},
)
