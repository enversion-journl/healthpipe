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
name, birth date or gender. They are written through the single Patient entry
point (DSA/FHIR/Patient/patient_service.ensure_patients): HAIBA hands over the
CPRs, the builder assembles the resource and sets provenance — meta.source is
the builder's URI and meta.tag carries HAIBA, projected as source_haiba in
fhir.Patient — so downstream can tell these apart from clinically sourced
patients. Should the person turn up in EPJ, the same entry point rebuilds her
from EPJ's row; the stub is never written over a fuller resource.

CPRs are created regardless of format. A structurally invalid CPR still gets a
resource; the count of malformed values is logged as a signal but never blocks,
and the values themselves are never logged.
"""

import dagster as dg
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Patient.cpr_stub_asset import (
    build_cpr_stub_asset,
)
from DSA.FHIR.Patient.cpr_stub_asset import (
    classify_cprs as _classify_cprs,
)
from DSA.FHIR.Patient.cpr_stub_asset import (
    post_missing_patients as _post_missing_patients,
)
from DSA.FHIR.Patient.patient_ownership import SOURCE_HAIBA

CPR_SQL = """
SELECT DISTINCT cprnr FROM arc.haiba_cases_infektion WHERE cprnr <> ''
UNION
SELECT DISTINCT cprnr FROM arc.haiba_cases_ssi WHERE cprnr <> ''
"""


def classify_cprs(mssql: MSSQLResource) -> dict[str, list[str]]:
    """Split HAIBA's distinct CPRs into tiers A, B and C."""
    return _classify_cprs(mssql, CPR_SQL)


def post_missing_patients(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    cprs: list[str],
) -> tuple[int, int, int]:
    """Compatibility wrapper; source orchestration lives in cpr_stub_asset."""
    return _post_missing_patients(
        context,
        mssql,
        fhir_database,
        cprs,
        stub_source=SOURCE_HAIBA,
        source_name="haiba_stubs",
    )


fhir_patient_haiba = build_cpr_stub_asset(
    name="fhir_patient_haiba",
    group_name="haiba_borger",
    deps=["arc_haiba_cases_infektion", "arc_haiba_cases_ssi"],
    tags={"domain": "haiba", "layer": "dsa", "source": "haiba"},
    description=(
        "Create stub Patient resources for CPRs that appear in HAIBA's case "
        "files but are unknown to FHIR, so their cases can resolve a "
        "dw_ek_Borger. Written through the Patient entry point; tagged HAIBA."
    ),
    cpr_sql=CPR_SQL,
    stub_source=SOURCE_HAIBA,
    source_label="HAIBA",
    source_name="haiba_stubs",
)


__all__ = [
    "classify_cprs",
    "fhir_patient_haiba",
    "post_missing_patients",
]
