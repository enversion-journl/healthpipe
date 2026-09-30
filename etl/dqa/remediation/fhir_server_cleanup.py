# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Operator-triggered cleanup: soft-delete flat ``fhir.*`` rows whose resource was
hard-deleted on the FHIR server (Aidbox) but never propagated.

The companion detector is the ``fhir_server_reconciliation`` check
(``stale_in_flat``). This asset stages the server's live resource ids and sets
``dw_is_deleted = 1`` on the active flat rows whose id is gone from the server -- the
fix for the duplicate-``identifier_value`` problem (an old, deleted resource
version lingering alongside the live one).

Soft-delete only (reversible). Mutates production flat tables, so it is
operator-triggered (never scheduled), with ``dry_run`` to preview and a
``max_rows`` guard (a huge stale count is systemic -- investigate, don't bulk
soft-delete).
"""

import dagster as dg
from dqa.checks.fhir_server import not_deleted_pred, stage_fhir_server_ids
from dqa.checks.helpers import sql_literal
from pydantic import Field

# entity -> the Aidbox resource table + the flat tables projected from it. One
# resource can feed several flat tables at different grains (e.g. Encounter ->
# fhir.Encounter + fhir.Encounter_Location), all keyed by the resource id; a
# deleted resource makes the rows stale in ALL of them.
#
# The (resource_table, id) pairing mirrors SOF_RESOURCE_CONFIGS in
# DSA/FHIR/utils/sql_on_fhir/sof_utils.py -- resource_table is the lowercased
# one-word FHIR resource type (the Aidbox table), and id is the column into
# which the SoF view projects that resource's id. Child-grain tables carry the
# PARENT resource id, which is exactly what this cleanup diffs on.


def _targets(id_column: str, *flat_tables: str) -> list[dict]:
    """Flat tables projected from one resource, all keyed by that resource's id.

    ``deleted`` is uniformly ``dw_is_deleted``: the column is
    INT NOT NULL DEFAULT 0 on all 24 fhir.* tables, so there is no longer a
    per-table variation to encode.
    """
    return [
        {"flat": f"[fhir].[{table}]", "id": id_column, "deleted": "dw_is_deleted"}
        for table in flat_tables
    ]


_ENTITIES: dict[str, dict] = {
    "encounter": {
        "resource_table": "encounter",
        "targets": _targets("encounter_id", "Encounter", "Encounter_Location"),
    },
    "episode_of_care": {
        "resource_table": "episodeofcare",
        "targets": _targets("episode_of_care_id", "EpisodeOfCare"),
    },
    "patient": {
        "resource_table": "patient",
        "targets": _targets("patient_id", "Patient", "Patient_Address"),
    },
    # Organization matters most: the duplicate-Organization cleanup on the server is
    # what creates dangling references, so its stale flat rows accumulate steadily.
    "organization": {
        "resource_table": "organization",
        "targets": _targets("organization_id", "Organization"),
    },
    "location": {
        "resource_table": "location",
        "targets": _targets("location_id", "Location"),
    },
    "appointment": {
        "resource_table": "appointment",
        "targets": _targets("appointment_id", "Appointment"),
    },
    # ServiceRequest has no ids-mode replay (multi-source grain), but the cleanup
    # is DFM-independent -- it only stages Aidbox ids and soft-deletes flat rows --
    # so server-deletion staleness IS fixable.
    "service_request": {
        "resource_table": "servicerequest",
        "targets": _targets(
            "service_request_id",
            "ServiceRequest",
            "ServiceRequest_Recipient",
            "ServiceRequest_tillaegskode",
        ),
    },
    "condition": {
        "resource_table": "condition",
        "targets": _targets("condition_id", "Condition", "Condition_tillaegskode"),
    },
    "document_reference": {
        "resource_table": "documentreference",
        "targets": _targets(
            "documentreference_id", "DocumentReference", "DocumentReference_Recipient"
        ),
    },
    "care_plan": {
        "resource_table": "careplan",
        "targets": _targets("careplan_id", "CarePlan", "CarePlan_Activity"),
    },
    "procedure": {
        "resource_table": "procedure",
        "targets": _targets("procedure_id", "Procedure", "Procedure_tillaegskode"),
    },
    # Staging the live id set is a server-side cursor into a temp table, so it
    # streams rather than buffering. Observation is not necessarily the biggest
    # flat table (on Dev DSA it is ~1.9M against QuestionnaireResponse's ~11.2M),
    # but its Aidbox id set is large enough to dominate the run -- size it before
    # assuming this entity is quick.
    # NB: fhir.SFI_datapoint_store is a materialized consumer store derived from
    # Observation and has no dw_is_deleted -- it picks up soft-deletes only when
    # dso_refresh_sfi_datapoint_* rebuilds it, not from this asset.
    "observation": {
        "resource_table": "observation",
        "targets": _targets("observation_id", "Observation"),
    },
    "questionnaire": {
        "resource_table": "questionnaire",
        "targets": _targets("questionnaire_id", "Questionnaire", "Questionnaire_item"),
    },
    # The QR views project the resource id as qr_id (not questionnaireresponse_id).
    "questionnaire_response": {
        "resource_table": "questionnaireresponse",
        "targets": _targets("qr_id", "QuestionnaireResponse", "QuestionnaireResponse_item"),
    },
}


class FhirServerCleanupConfig(dg.Config):
    entity: str = Field(description=f"Which flat table to clean up: one of {sorted(_ENTITIES)}.")
    dry_run: bool = Field(
        default=False,
        description="Count what would be soft-deleted, log it, and make no changes.",
    )
    max_rows: int = Field(
        default=100_000,
        description="Abort if more than this many rows would be soft-deleted -- a spike "
        "that large is systemic and should be investigated, not bulk-soft-deleted.",
    )


@dg.asset(
    name="dqa_fhir_server_cleanup",
    group_name="dqa_operations",
    compute_kind="mssql",
    required_resource_keys={"mssql_target_staging", "fhir_database"},
    description=(
        "Operator-triggered: soft-delete (dw_is_deleted=1) flat fhir.* rows whose resource "
        "was deleted on the FHIR server (Aidbox) but not propagated. Run dry_run=true first."
    ),
)
def dqa_fhir_server_cleanup(
    context: dg.AssetExecutionContext,
    config: FhirServerCleanupConfig,
) -> dg.MaterializeResult:
    if config.entity not in _ENTITIES:
        raise dg.Failure(
            description=f"Unknown entity {config.entity!r}; expected one of {sorted(_ENTITIES)}."
        )
    e = _ENTITIES[config.entity]
    rtable, targets = e["resource_table"], e["targets"]
    res = context.resources

    with res.mssql_target_staging.get_connection() as conn:
        cur = conn.cursor()
        # Capture a cutoff BEFORE snapshotting the server, so a resource created in
        # Aidbox after our snapshot -- and projected into the flat table by a
        # concurrent SoF run before our UPDATE -- can't be mistaken for "deleted on
        # server" and retired. We only ever soft-delete rows that already existed in
        # the flat table before the snapshot (dw_timestamp_insert <= cutoff, or NULL
        # for legacy rows that predate the column). SYSDATETIME() (local) matches the
        # SoF projection's dw_timestamp_insert clock. (Still: don't run this
        # concurrently with the FHIR pipeline -- see the runbook.)
        cur.execute("SELECT SYSDATETIME()")
        cutoff = cur.fetchone()[0]
        lit_cutoff = sql_literal(cutoff)
        stage, live = stage_fhir_server_ids(conn, cur, res.fhir_database, rtable)
        context.log.info(
            f"dqa_fhir_server_cleanup[{config.entity}]: {live} live server ids (cutoff {cutoff})."
        )

        # Safety: an empty live-id set (wrong/empty Aidbox, wrong resource_table, or a
        # staging failure) would make EVERY active flat row look stale and soft-delete
        # the whole table (up to max_rows). Refuse -- a genuine "server has zero of this
        # resource" is itself something to investigate, never to mass-soft-delete on.
        if live == 0:
            raise dg.Failure(
                description=(
                    f"dqa_fhir_server_cleanup[{config.entity}]: the FHIR server returned 0 live "
                    f"'{rtable}' ids. Refusing to soft-delete (that would retire every active "
                    f"row). Investigate the server/connection/resource_table before retrying."
                ),
            )

        # Pass 1: count + guard every target BEFORE mutating any (so we abort
        # cleanly without a partial update).
        plan = []
        for t in targets:
            flat, idc, deleted = t["flat"], t["id"], t["deleted"]
            where = (
                f"WHERE {not_deleted_pred(deleted, 'f')} "
                f"AND NOT EXISTS (SELECT 1 FROM {stage} t WHERE t.id = f.[{idc}]) "
                f"AND (f.[dw_timestamp_insert] <= {lit_cutoff} "
                f"OR f.[dw_timestamp_insert] IS NULL)"
            )
            cur.execute(f"SELECT COUNT(*) FROM {flat} f {where}")
            stale = int(cur.fetchone()[0] or 0)
            context.log.info(f"  {flat}: {stale} stale rows (deleted on server).")
            if stale > config.max_rows:
                raise dg.Failure(
                    description=(
                        f"{flat}: {stale} stale rows exceed max_rows ({config.max_rows}). "
                        f"A spike that large is systemic -- investigate before soft-deleting."
                    ),
                )
            plan.append((flat, deleted, where, stale))

        by_table: dict[str, int] = {}
        if config.dry_run:
            by_table = {flat: stale for flat, _, _, stale in plan}
            context.log.info(f"dqa_fhir_server_cleanup[{config.entity}]: dry-run, no changes.")
            return dg.MaterializeResult(
                metadata={
                    "entity": config.entity,
                    "live_in_server": live,
                    "status": dg.MetadataValue.text("dry_run"),
                    "would_soft_delete": dg.MetadataValue.json(by_table),
                    "would_soft_delete_total": sum(by_table.values()),
                }
            )

        # Pass 2: soft-delete, then commit all targets together (atomic per run).
        for flat, deleted, where, stale in plan:
            if stale == 0:
                by_table[flat] = 0
                continue
            # Bump dw_timestamp_update (the DSO-sync watermark) so the next delta
            # sync carries the soft-delete to DSO. Use SYSDATETIME() (local) to match
            # the SoF projection's watermark convention -- SYSUTCDATETIME() could be
            # behind the local-time watermark (DK is UTC+1/+2) and never re-sync.
            # Do NOT touch last_updated: it mirrors the FHIR server's meta.lastUpdated
            # (projected as r.ts in SoF), not a DW-managed column.
            cur.execute(
                f"UPDATE f SET f.[{deleted}] = 1, f.[dw_timestamp_update] = SYSDATETIME() "
                f"FROM {flat} f {where}"
            )
            by_table[flat] = cur.rowcount if cur.rowcount and cur.rowcount > 0 else stale
        conn.commit()
        context.log.info(
            f"dqa_fhir_server_cleanup[{config.entity}]: soft-deleted {by_table} (committed)."
        )

    return dg.MaterializeResult(
        metadata={
            "entity": config.entity,
            "live_in_server": live,
            "soft_deleted": dg.MetadataValue.json(by_table),
            "soft_deleted_total": sum(by_table.values()),
        }
    )


dqa_fhir_server_cleanup_job = dg.define_asset_job(
    name="dqa_fhir_server_cleanup_job",
    selection=dg.AssetSelection.assets("dqa_fhir_server_cleanup"),
    description="Soft-delete flat fhir.* rows deleted on the FHIR server (operator-triggered).",
    tags={"domain": "dqa", "dqa_track": "fhir"},
)
