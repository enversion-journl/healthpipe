# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dagster job definitions for the clinical FHIR CodeSystems (SKS, SOR).

Builds the SKS/SOR codesystems, syncs them to DSO, and posts the
content-complete CodeSystems to the FHIR (Aidbox) terminology server for native
$lookup / $validate-code. Kept separate from the daily organisation_cs_job
because the content-complete PUTs are heavy (the SOR resource is ~70 MB), so this
runs on a lighter (weekly) cadence.
"""

import dagster as dg

fhir_codesystem_job = dg.define_asset_job(
    name="fhir_codesystem_job",
    selection=dg.AssetSelection.assets(
        # Infrastructure: ensure staging + DSO codesystem tables exist
        "staging_infra_codesystem",
        "dso_infra_codesystem",
        # Source extracts (external downloads)
        "extract_sks_register",
        "extract_sor_register",
        # Build codesystems (staging codesystem.*)
        "cs_sks",
        "cs_sor",
        # DSO sync (staging -> DSO)
        "dso_sync_codesystem_sks_concept",
        "dso_sync_codesystem_sks_properties",
        "dso_sync_codesystem_sks_hierarchy",
        "dso_sync_codesystem_sor_concept",
        "dso_sync_codesystem_sor_properties",
        "dso_sync_codesystem_sor_hierarchy",
        # Post content-complete CodeSystems to the FHIR terminology server
        "fhir_codesystem_sks_complete",
        "fhir_codesystem_sor_complete",
    ),
    description=(
        "Refresh the clinical SKS/SOR CodeSystems: download source, build "
        "codesystem.* in staging, sync to DSO, and PUT the content-complete "
        "CodeSystems to the FHIR (Aidbox) terminology server for native "
        "$lookup / $validate-code. The SOR PUT is ~70 MB, so this runs weekly "
        "rather than daily. Requires Aidbox with BOX_FHIR_TERMINOLOGY_ENGINE="
        "hybrid, BOX_WEB_MAX_BODY raised, and adequate JVM heap (see the "
        "fhir-server nomad jobs)."
    ),
)

__all__ = [
    "fhir_codesystem_job",
]
