# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Patient stubs for CPRs that only HAIBA knows about.

HAIBA's case files carry real CPRs, and 4.1% of them (900 of 22,043 distinct)
belong to people the FHIR server has never seen. Without a Patient resource
there is no dw_ek_Borger to resolve, so those cases could never be attributed
to a person.

This asset creates the missing ones — and only the missing ones. The CPRs are
split three ways before anything is written:

  tier A  already in ref.PatientToBorger        -> resolves, nothing to do
  tier B  in fhir.Patient but no ref row        -> the ref asset owns that
                                                   mapping; running it fills
                                                   the gap. Measured at 0.
  tier C  not in fhir.Patient at all            -> re-checked against Aidbox,
                                                   then created if still absent

Tier B is deliberately not fixed by writing into ref.PatientToBorger:
DSA/SQL/ref/patient_borger.py owns that table, mints dw_ek_Borger via IDENTITY
and soft-deletes rows whose patient_id leaves fhir.Patient. A second writer
would paper over a gap in the Patient chain rather than close it.

The resources are stubs by design: HAIBA delivers a CPR and nothing else — no
name, birth date or gender. meta.source is set to "HAIBA" so downstream can
tell these apart from clinically sourced patients; the priority order between
sources (CPR register > IFDB > HAIBA) is [internal ticket].

CPRs are created regardless of format. A structurally invalid CPR still gets a
resource; the count of malformed values is logged as a signal but never blocks,
and the values themselves are never logged.
"""

import dagster as dg
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.patient import Patient
from pydantic import Field
from utils.fhir.identifier_systems import SYSTEM_CPR

META_SOURCE = "HAIBA"


class HaibaBorgerConfig(dg.Config):
    """Run config for the stub-patient creation."""

    max_create: int | None = Field(
        default=None,
        description=(
            "Create at most this many patients this run, lowest CPR first. "
            "Writing to Aidbox is not cheaply reversible — each resource would "
            "have to be deleted individually — so a first run against a fresh "
            "environment can be capped to verify the shape before the rest "
            "follow. Leave unset to create every missing patient. The remainder "
            "is created by running again: a CPR an earlier run created resolves "
            "against Aidbox and is skipped rather than posted twice."
        ),
    )


# tier A / B / C in one pass. previous_ssn counts as "known" — the person is in
# fhir.Patient under a former CPR, so creating a second resource would duplicate
# them.
TIER_SQL = """
WITH haiba AS (
    SELECT DISTINCT cprnr FROM arc.haiba_cases_infektion WHERE cprnr <> ''
    UNION
    SELECT DISTINCT cprnr FROM arc.haiba_cases_ssi WHERE cprnr <> ''
),
ref_cpr AS (
    SELECT DISTINCT cpr FROM ref.PatientToBorger
    WHERE dw_is_deleted = 0 AND cpr IS NOT NULL
),
fhir_cpr AS (
    SELECT DISTINCT patient_ssn AS ssn FROM fhir.Patient WHERE patient_ssn IS NOT NULL
    UNION
    SELECT DISTINCT previous_ssn FROM fhir.Patient WHERE previous_ssn IS NOT NULL
)
SELECT h.cprnr,
       CASE WHEN r.cpr IS NOT NULL THEN 'A'
            WHEN f.ssn IS NOT NULL THEN 'B'
            ELSE 'C' END AS tier
FROM haiba h
LEFT JOIN ref_cpr  r ON r.cpr = h.cprnr
LEFT JOIN fhir_cpr f ON f.ssn = h.cprnr
"""


def classify_cprs(mssql: MSSQLResource) -> dict[str, list[str]]:
    """Split HAIBA's distinct CPRs into tiers A, B and C."""
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
    """One minimal Patient per CPR, tagged with its origin.

    Nothing but the identifier is known, so nothing else is asserted. Claiming
    a birth date or gender we do not have would be worse than an empty record.
    """
    return [
        Patient(
            meta=Meta(source=META_SOURCE),
            identifier=[Identifier(system=SYSTEM_CPR, value=cpr)],
        )
        for cpr in cprs
    ]


def post_missing_patients(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    cprs: list[str],
) -> tuple[int, int, int]:
    """POST a stub for each CPR Aidbox does not already hold.

    Returns (posted, failed, skipped_existing).
    """
    # Tier C is derived from fhir.Patient, which lags Aidbox by one
    # sof_patient_dfm run, and the upsert replaces the whole resource. A stub
    # posted onto an id that already exists would erase that Patient's payload,
    # so a resolved id means skip — never overwrite.
    candidates = prepare_resources_for_upsert(
        resources=build_stub_patients(cprs),
        fhir_database=fhir_database,
        resource_type="Patient",
        preferred_id_systems=[SYSTEM_CPR],
        logger=context.log,
    )
    new_patients = [p for p in candidates if not p.id]
    skipped = len(candidates) - len(new_patients)

    if skipped:
        context.log.warning(
            f"{skipped} CPR(s) already exist in Aidbox but not in fhir.Patient — "
            f"left untouched. Run sof_patient_dfm to project them; if they are still "
            f"missing after mode=full_load, the gap is in the Patient chain and not "
            f"HAIBA's to close."
        )
    if not new_patients:
        return 0, 0, skipped

    posted, failed, failed_ids = fhir_database.batch_upsert_resources(
        resources=new_patients, resource_type="Patient"
    )
    if failed:
        # failed_ids carries driver error text of unbounded provenance;
        # the only data in these resources IS the person identifier,
        # so only the count may reach the log.
        context.log.error(f"{failed} Patient(s) failed to upsert")
    return posted, failed, skipped


@dg.asset(
    name="fhir_patient_haiba",
    group_name="haiba_borger",
    compute_kind="fhir",
    deps=["arc_haiba_cases_infektion", "arc_haiba_cases_ssi"],
    tags={"domain": "haiba", "layer": "dsa", "source": "haiba"},
    description=(
        "Create stub Patient resources for CPRs that appear in HAIBA's case "
        "files but are unknown to FHIR, so their cases can resolve a "
        "dw_ek_Borger. Tagged meta.source='HAIBA'."
    ),
)
def fhir_patient_haiba(
    context: dg.AssetExecutionContext,
    config: HaibaBorgerConfig,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    tiers = classify_cprs(mssql_target_staging)
    a, b, c = len(tiers["A"]), len(tiers["B"]), len(tiers["C"])
    total = a + b + c

    context.log.info(
        f"HAIBA CPRs: {total} distinct — tier A (resolves) {a}, "
        f"tier B (needs ref_PatientToBorger) {b}, tier C (create) {c}"
    )
    if b:
        # Not this asset's job to fix, but silence would hide it.
        context.log.warning(
            f"{b} CPR(s) exist in fhir.Patient without a ref.PatientToBorger row. "
            f"Run ref_PatientToBorger (mode=full_load if a delta run skipped them) "
            f"— do not insert into that table from here."
        )

    to_create = sorted(tiers["C"])
    if config.max_create is not None and len(to_create) > config.max_create:
        context.log.warning(
            f"CAPPED to {config.max_create} of {c} missing patients this run. "
            f"The remainder is created by running again — the ones created now "
            f"resolve against Aidbox next time and are skipped, not posted twice."
        )
        to_create = to_create[: config.max_create]

    malformed = sum(1 for cpr in to_create if not (len(cpr) == 10 and cpr.isdigit()))
    if malformed:
        context.log.warning(
            f"{malformed} of {len(to_create)} CPR(s) to create are not 10 digits. They are "
            f"created anyway by decision; this is a signal, not a rejection."
        )

    posted = failed = skipped_existing = 0
    if to_create:
        posted, failed, skipped_existing = post_missing_patients(context, fhir_database, to_create)
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
    "HaibaBorgerConfig",
    "build_stub_patients",
    "classify_cprs",
    "fhir_patient_haiba",
    "post_missing_patients",
]
