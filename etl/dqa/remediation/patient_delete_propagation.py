# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Operator-triggered cleanup: propagate genuine arc patient deletes to fhir.Patient.

Companion to the ``patient_arc_integrity`` check. It soft-deletes
(``dw_is_deleted = 1``) the active ``fhir.Patient`` rows whose kept ``ifdb_patient_id``
resolves to a **deleted** arc row (``SLETTET = 1``) AND whose CPR has **no active
sibling** -- i.e. the whole person is deleted in IFDB, but the delete never reached
the flat table.

Deliberately excludes the ``kept_deleted_dup`` case (a deleted arc id kept while an
active sibling exists): there the person is still alive under a different
PATIENT_ID, so soft-deleting would wrongly retire a live patient. That case is
routed to the patient *replay* (re-dedup) instead.

Source of truth is **arc** (IFDB archive, MSSQL), not the FHIR server -- so this is
separate from ``dqa_fhir_server_cleanup`` (which handles Aidbox hard-deletes).
Soft-delete only (reversible); ``dry_run`` to preview; ``max_rows`` guard.
"""

import dagster as dg
from pydantic import Field

_ARC = "[arc].[ifdb_patient]"
_FHIR = "[fhir].[Patient]"
_PID, _CPR, _SDEL = "PATIENT_ID", "CPR", "SLETTET"
_TKEY, _TDEL = "ifdb_patient_id", "dw_is_deleted"


class PatientDeletePropagationConfig(dg.Config):
    dry_run: bool = Field(
        default=False,
        description="Count what would be soft-deleted, log it, and make no changes.",
    )
    max_rows: int = Field(
        default=100_000,
        description="Abort if more than this many rows would be soft-deleted -- a spike "
        "that large is systemic and should be investigated.",
    )


@dg.asset(
    name="dqa_patient_delete_propagation",
    group_name="dqa_operations",
    compute_kind="mssql",
    required_resource_keys={"mssql_target_staging"},
    description=(
        "Operator-triggered: soft-delete fhir.Patient rows whose kept ifdb_patient_id is "
        "deleted in arc with no active CPR sibling (genuine person delete). Run dry_run=true first."
    ),
)
def dqa_patient_delete_propagation(
    context: dg.AssetExecutionContext,
    config: PatientDeletePropagationConfig,
) -> dg.MaterializeResult:
    active_sibling = (
        f"EXISTS (SELECT 1 FROM {_ARC} a2 WHERE a2.[{_CPR}] = a.[{_CPR}] "
        f"AND (CAST(a2.[{_SDEL}] AS NVARCHAR(10)) <> '1' OR a2.[{_SDEL}] IS NULL))"
    )
    where = (
        f"WHERE (p.[{_TDEL}] IS NULL OR p.[{_TDEL}] <> 1) "
        f"AND EXISTS (SELECT 1 FROM {_ARC} a WHERE a.[{_PID}] = p.[{_TKEY}] "
        f"AND CAST(a.[{_SDEL}] AS NVARCHAR(10)) = '1' AND NOT {active_sibling})"
    )

    with context.resources.mssql_target_staging.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {_FHIR} p {where}")
        stale = int(cur.fetchone()[0] or 0)
        context.log.info(
            f"dqa_patient_delete_propagation: {stale} active fhir.Patient rows are "
            f"fully-deleted in arc (no active CPR sibling)."
        )

        if stale > config.max_rows:
            raise dg.Failure(
                description=(
                    f"{stale} rows exceed max_rows ({config.max_rows}); a spike that large "
                    f"is systemic -- investigate before soft-deleting."
                ),
            )
        if config.dry_run or stale == 0:
            return dg.MaterializeResult(
                metadata={
                    "would_soft_delete" if config.dry_run else "soft_deleted": (
                        stale if config.dry_run else 0
                    ),
                    "status": dg.MetadataValue.text(
                        "dry_run" if config.dry_run else "nothing_to_do"
                    ),
                }
            )

        # Bump dw_timestamp_update (the DSO-sync watermark) so the next delta sync
        # carries the soft-delete to DSO; SYSDATETIME() (local) matches the SoF
        # projection's convention. last_updated is left alone -- it mirrors the FHIR
        # server's meta.lastUpdated (r.ts in SoF), not a DW-managed column.
        cur.execute(
            f"UPDATE p SET p.[{_TDEL}] = 1, p.[dw_timestamp_update] = SYSDATETIME() "
            f"FROM {_FHIR} p {where}"
        )
        soft_deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else stale
        conn.commit()
        context.log.info(f"dqa_patient_delete_propagation: soft-deleted {soft_deleted} rows.")

    return dg.MaterializeResult(metadata={"soft_deleted": soft_deleted})


dqa_patient_delete_propagation_job = dg.define_asset_job(
    name="dqa_patient_delete_propagation_job",
    selection=dg.AssetSelection.assets("dqa_patient_delete_propagation"),
    description="Propagate genuine arc patient deletes to fhir.Patient (operator-triggered).",
    tags={"domain": "dqa", "dqa_track": "fhir"},
)
