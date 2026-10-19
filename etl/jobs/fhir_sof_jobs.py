# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR (SOF) job definitions.

The partitioned SOF jobs and the partitioned FHIR DSO sync job are gone with the
last partitioned producer (CarePlan): SOF extraction and DSO sync now run inside
``ifdb_dfm_daily_job``, and ``fhir_sof_dfm_job`` re-runs the SOF layer on its own.

NOTE: Infrastructure is now centralized. Use the Dagster job
`full_infrastructure_setup` (Python symbol full_infrastructure_job) for all
DDL setup (staging + DSO + FHIR SOF ViewDefinitions).
"""

import dagster as dg

# ============================================================================
# Asset Selections
# ============================================================================

fhir_sof_infrastructure_assets = dg.AssetSelection.assets(
    "staging_infra_fhir",
    "fhir_sof_infrastructure",
)
# Byggerne skal staa eksplicit ved siden af synkgruppen. Synken er
# truncate_insert og toemmer maalet ogsaa ved nul kilderaekker, saa en gruppe uden
# sin bygger ville rydde kodesystemet paa DSO i stedet for at opdatere det — og
# teksten i vw_fhir ville blive NULL uden at noget fejlede.
codesystem_sfi_sync_assets = dg.AssetSelection.assets(
    "cs_sfi",
    "cs_ifdb_kodelister",
) | dg.AssetSelection.groups("codesystem_dso_sync")

# ============================================================================
# CodeSystem SFI sync (operator-triggered)
# ============================================================================

codesystem_sfi_sync_job = dg.define_asset_job(
    name="codesystem_sfi_sync_job",
    selection=codesystem_sfi_sync_assets,
    description=(
        "Build cs_sfi in staging and sync codesystem.sfi_* to DSO. "
        "Run this when source_model/version metadata in SFI_datapoint is unexpectedly null."
    ),
)

__all__ = [
    "codesystem_sfi_sync_job",
]
