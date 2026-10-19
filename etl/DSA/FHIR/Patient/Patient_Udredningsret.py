# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Patient stubs for CPRs that only the udredningsret deliveries know about.

Same two-pass design as HAIBA (Patient_HAIBA.py) and DRG (Patient_DRG.py):
the udredningsret markers carry real CPRs, and a person the FHIR server has
never seen has no dw_ek_Borger to resolve — pre_stg leaves those at -1 and the
delta scope re-stages them every run (spec §3.1, U9). This asset creates the
missing Patient stubs on ingest; the existing Patient -> Borger chain
(sof_patient_dfm -> ref_PatientToBorger -> dim_Borger) then mints the key in
the nightly DFM chain, and the next publish pass resolves it. A stub is
therefore never a dw_ek_Borger on the day it is created, which is why staging
and publishing live in a separate job (udredningsret_publish_job) instead of
running in the same job as this asset.

The tier split, the deliberate non-fix of tier B and the
create-regardless-of-format rule are inherited decisions — see Patient_HAIBA.py's module docstring and
Patient_DRG.py. Only the source differs:

  * CPRs come from arc.udredningsret_mont (CPR), soft-deleted rows excluded —
    a marker that has left the snapshot should not create a person.
  * Written through the Patient entry point (patient_service.ensure_patients):
    the builder tags the resource SDSUDRED, projected as source_sdsudred in
    fhir.Patient, so downstream can tell these stubs apart.
  * Volume: the archive holds ~8M marker rows, but the CPR query works on
    DISTINCT CPR (set-based by design). Measured on dev 2026-09-12: 889,770
    distinct CPRs, of which 8,115 (0.91 %) had no dim.Borger row before stubs
    and 0 resolved only through a replacement CPR (tidligere_cpr).

GDPR: CPR values never leave the staging/FHIR databases; logs and metadata
carry counts only.
"""

from core.resources import MSSQLResource
from DSA.FHIR.Patient.cpr_stub_asset import build_cpr_stub_asset
from DSA.FHIR.Patient.cpr_stub_asset import classify_cprs as _classify_cprs
from DSA.FHIR.Patient.field_ownership import SOURCE_UDRED

CPR_SQL = """
-- No length cast: CAST(... AS NVARCHAR(10)) would silently truncate a
-- malformed overlong value and create a stub under the wrong identity.
SELECT DISTINCT LTRIM(RTRIM([CPR])) AS cpr
FROM [arc].[udredningsret_mont]
WHERE [dw_is_deleted] = 0 AND [CPR] IS NOT NULL AND LTRIM(RTRIM([CPR])) <> ''
"""


def classify_cprs(mssql: MSSQLResource) -> dict[str, list[str]]:
    """Split the udredningsret archive's distinct CPRs into tiers A, B and C."""
    return _classify_cprs(mssql, CPR_SQL)


fhir_patient_udredningsret = build_cpr_stub_asset(
    name="fhir_patient_udredningsret",
    group_name="udredningsret_borger",
    deps=["arc_udredningsret_mont"],
    tags={"domain": "udredningsret", "layer": "dsa", "source": "udredningsret"},
    description=(
        "Create stub Patient resources for CPRs that appear in the udredningsret "
        "deliveries but are unknown to FHIR, so their forloeb can resolve a "
        "dw_ek_Borger in the next publish pass. Written through the Patient entry "
        "point; tagged SDSUDRED."
    ),
    cpr_sql=CPR_SQL,
    stub_source=SOURCE_UDRED,
    source_label="Udredningsret",
    source_name="udredningsret_stubs",
)


__all__ = [
    "classify_cprs",
    "fhir_patient_udredningsret",
]
