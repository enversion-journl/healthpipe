# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Patient stubs for CPRs that only the DRG deliveries know about.

Same two-pass design as HAIBA (Patient_HAIBA.py): DRG contacts
carry real CPRs, and a person the FHIR server has never seen has no
dw_ek_Borger to resolve — pre_stg_DRGKontakt leaves those at -1. This asset
creates the missing Patient stubs; the existing Patient -> Borger chain
(sof_patient_dfm -> ref_PatientToBorger -> dim_Borger) then mints the key,
and the next staging pass resolves it.

The tier split, the deliberate non-fix of tier B, the no-overwrite guard
against Aidbox and the stub-only payload are all inherited decisions — see
Patient_HAIBA.py's module docstring. Only the source differs:

  * CPRs come from arc.drg_kontakt (CPR) and arc.drgpsyk_kontakt (CPRnummer),
    soft-deleted rows excluded — a retired contact should not create a person.
  * meta.source = "SDSDRG" so downstream can tell these stubs apart.
  * Volume: the somatik archive holds ~20M contact rows across takstsystemer,
    but the tier SQL works on DISTINCT CPR (set-based by design) —
    the distinct person count is orders of magnitude smaller.

GDPR: CPR values never leave the staging/FHIR databases; logs and metadata
carry counts only.
"""

import dagster as dg
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.patient import Patient
from pydantic import Field
from utils.fhir.identifier_systems import SYSTEM_CPR

META_SOURCE = "SDSDRG"


class DrgBorgerConfig(dg.Config):
    """Run config for the stub-patient creation."""

    max_create: int | None = Field(
        default=None,
        description=(
            "Create at most this many patients this run, lowest CPR first. "
            "Writing to Aidbox is not cheaply reversible, so a first run "
            "against a fresh environment can be capped to verify the shape. "
            "Leave unset to create every missing patient; the remainder is "
            "created by running again — already-created CPRs resolve against "
            "Aidbox and are skipped, never posted twice."
        ),
    )


TIER_SQL = """
WITH drg AS (
    -- No length cast: CAST(... AS NVARCHAR(10)) would silently truncate a
    -- malformed overlong value and create a stub under the wrong identity.
    SELECT DISTINCT LTRIM(RTRIM([CPR])) AS cpr
    FROM [arc].[drg_kontakt]
    WHERE [dw_is_deleted] = 0 AND [CPR] IS NOT NULL AND LTRIM(RTRIM([CPR])) <> ''
    UNION
    SELECT DISTINCT LTRIM(RTRIM([CPRnummer]))
    FROM [arc].[drgpsyk_kontakt]
    WHERE [dw_is_deleted] = 0 AND [CPRnummer] IS NOT NULL
      AND LTRIM(RTRIM([CPRnummer])) <> ''
),
ref_cpr AS (
    SELECT DISTINCT cpr FROM [ref].[PatientToBorger]
    WHERE dw_is_deleted = 0 AND cpr IS NOT NULL
),
fhir_cpr AS (
    SELECT DISTINCT patient_ssn AS ssn FROM [fhir].[Patient] WHERE patient_ssn IS NOT NULL
    UNION
    SELECT DISTINCT previous_ssn FROM [fhir].[Patient] WHERE previous_ssn IS NOT NULL
)
SELECT d.cpr,
       CASE WHEN r.cpr IS NOT NULL THEN 'A'
            WHEN f.ssn IS NOT NULL THEN 'B'
            ELSE 'C' END AS tier
FROM drg d
LEFT JOIN ref_cpr  r ON r.cpr = d.cpr
LEFT JOIN fhir_cpr f ON f.ssn = d.cpr
"""


def classify_cprs(mssql: MSSQLResource) -> dict[str, list[str]]:
    """Split the DRG deliveries' distinct CPRs into tiers A, B and C."""
    tiers: dict[str, list[str]] = {"A": [], "B": [], "C": []}
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(TIER_SQL)
            for cpr, tier in cursor.fetchall():
                tiers[tier].append(cpr)
        finally:
            cursor.close()
    return tiers


def build_stub_patients(cprs: list[str]) -> list[Patient]:
    """One minimal Patient per CPR, tagged meta.source='SDSDRG'.

    The delivery carries a CPR and nothing else about the person — asserting
    a birth date or gender we do not have would be worse than an empty record.
    """
    return [
        Patient(
            meta=Meta(source=META_SOURCE),
            identifier=[Identifier(system=SYSTEM_CPR, value=cpr)],
        )
        for cpr in cprs
    ]


@dg.asset(
    name="fhir_patient_drg",
    group_name="drg_borger",
    compute_kind="fhir",
    deps=["arc_drg_kontakt", "arc_drgpsyk_kontakt"],
    tags={"domain": "drg", "layer": "dsa", "source": "drg"},
    description=(
        "Create stub Patient resources for CPRs that appear in the DRG "
        "deliveries but are unknown to FHIR, so their contacts can resolve a "
        "dw_ek_Borger in the next staging pass. Tagged meta.source='SDSDRG'."
    ),
)
def fhir_patient_drg(
    context: dg.AssetExecutionContext,
    config: DrgBorgerConfig,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    tiers = classify_cprs(mssql_target_staging)
    a, b, c = len(tiers["A"]), len(tiers["B"]), len(tiers["C"])
    total = a + b + c

    context.log.info(
        f"DRG CPRs: {total} distinct — tier A (resolves) {a}, "
        f"tier B (needs ref_PatientToBorger) {b}, tier C (create) {c}"
    )
    if b:
        context.log.warning(
            f"{b} CPR(s) exist in fhir.Patient without a ref.PatientToBorger row. "
            f"Run ref_PatientToBorger (mode=full_load if a delta run skipped them) "
            f"— do not insert into that table from here."
        )

    to_create = sorted(tiers["C"])
    if config.max_create is not None and len(to_create) > config.max_create:
        context.log.warning(
            f"CAPPED to {config.max_create} of {c} missing patients this run; "
            f"the remainder is created by running again."
        )
        to_create = to_create[: config.max_create]

    malformed = sum(1 for cpr in to_create if not (len(cpr) == 10 and cpr.isdigit()))
    if malformed:
        context.log.warning(
            f"{malformed} of {len(to_create)} CPR(s) to create are not 10 digits. "
            f"Created anyway by decision; a signal, not a rejection."
        )

    posted = failed = skipped_existing = 0
    if to_create:
        candidates = prepare_resources_for_upsert(
            resources=build_stub_patients(to_create),
            fhir_database=fhir_database,
            resource_type="Patient",
            preferred_id_systems=[SYSTEM_CPR],
            logger=context.log,
        )
        new_patients = [p for p in candidates if not p.id]
        skipped_existing = len(candidates) - len(new_patients)
        if skipped_existing:
            context.log.warning(
                f"{skipped_existing} CPR(s) already exist in Aidbox but not in "
                f"fhir.Patient — left untouched; run sof_patient_dfm to project them."
            )
        if new_patients:
            posted, failed, failed_ids = fhir_database.batch_upsert_resources(
                resources=new_patients, resource_type="Patient"
            )
            if failed:
                # failed_ids carries driver error text of unbounded provenance;
                # the only data in these resources IS the person identifier,
                # so only the count may reach the log.
                context.log.error(f"{failed} Patient(s) failed to upsert")
    else:
        context.log.info("No unknown CPRs — nothing to create.")

    return dg.MaterializeResult(
        metadata={
            "cpr_total": dg.MetadataValue.int(total),
            "tier_a_resolves": dg.MetadataValue.int(a),
            "tier_b_missing_ref_row": dg.MetadataValue.int(b),
            "tier_c_missing": dg.MetadataValue.int(c),
            "attempted_this_run": dg.MetadataValue.int(len(to_create)),
            "already_in_aidbox": dg.MetadataValue.int(skipped_existing),
            "posted": dg.MetadataValue.int(posted),
            "failed": dg.MetadataValue.int(failed),
            "malformed_cpr": dg.MetadataValue.int(malformed),
            "meta_source": dg.MetadataValue.text(META_SOURCE),
        }
    )


__all__ = [
    "META_SOURCE",
    "DrgBorgerConfig",
    "build_stub_patients",
    "classify_cprs",
    "fhir_patient_drg",
]
