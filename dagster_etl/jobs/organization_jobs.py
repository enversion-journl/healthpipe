# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dagster job definitions for Organization dimension ETL.
"""

import dagster as dg

organisation_cs_job = dg.define_asset_job(
    name="organisation_cs_job",
    selection=dg.AssetSelection.assets(
        # --- Source extracts for the *-to-SOR codesystems (kept in the job so the
        # daily run builds them from FRESH source data). NOTE: ext_*_ils are also
        # pulled by erp_daily (02:00) — a harmless redundant source copy. ---
        "extract_ean_nemhandel",  # cs_ean source (NemHandel)
        "ext_sgh_ils",
        "ext_lgr_ils",  # cs_lager source (ILS)
        "ext_rekv_ils",
        "ext_rekvgrp_ils",  # cs_rekvirent source (ILS)
        # NOTE: ext.omkostningsstedhierarki_sydax is a *partitioned* SYDAX extract —
        # only the _daily/_monthly/_full_load variants are real assets, so the bare
        # key can't be selected here, and a partitioned variant would drag a
        # partition def into this daily job. Selecting it wouldn't help anyway: each
        # daily/monthly extract DROP/recreates [ext] and writes only that window's
        # DELTA (a complete [ext] snapshot exists only transiently right after the
        # on-demand erp_full_load_etl). cs_okosted reads [ext] and UPSERTs (no
        # delete) into the persistent codesystem.Okosted* tables — THOSE accumulate
        # the full hierarchy across runs, and stg/dim_okosted read them, not [ext].
        # So cs_okosted here just merges the last delta erp_daily (02:00) left in
        # [ext] into the accumulated codesystem — correct in steady state.
        # CAVEAT: a from-scratch rebuild of codesystem.Okosted* must run
        # erp_full_load_etl first, so [ext] is complete when cs_okosted runs.
        # OS2MO CSV extracts (manual ConceptMap overrides)
        "ext_os2mo_ean_to_sor",
        "ext_os2mo_lager_to_sor",
        "ext_os2mo_rekvirent_to_sor",
        "ext_os2mo_okosted_to_sor",
        # --- Codesystems (staging) — the entities resolved to SOR ---
        "cs_ean",
        "cs_lager",
        "cs_rekvirent",
        "cs_okosted",
        # --- ConceptMaps: EAN/Lager/Rekvirent/Okosted -> SOR ---
        "cm_ean_to_sor",
        "cm_lager_to_sor",
        "cm_rekvirent_to_sor",
        "cm_okosted_to_sor",
        # --- Codesystem + ConceptMap DSO sync (staging -> DSO) ---
        "dso_sync_codesystem_ean_concept",
        "dso_sync_codesystem_ean_properties",
        "dso_sync_codesystem_ean_hierarchy",
        "dso_sync_codesystem_lager_concept",
        "dso_sync_codesystem_lager_properties",
        "dso_sync_codesystem_lager_hierarchy",
        "dso_sync_codesystem_rekvirent_concept",
        "dso_sync_codesystem_rekvirent_properties",
        "dso_sync_codesystem_rekvirent_hierarchy",
        "dso_sync_codesystem_okosted_concept",
        "dso_sync_codesystem_okosted_properties",
        "dso_sync_codesystem_okosted_hierarchy",
        "dso_sync_conceptmap_ean_to_sor",
        "dso_sync_conceptmap_lager_to_sor",
        "dso_sync_conceptmap_rekvirent_to_sor",
        "dso_sync_conceptmap_okosted_to_sor",
        # --- Okosted DWH pipeline (codesystem -> staging -> map -> dim -> DSO) ---
        "stg_okosted",
        "staging_infra_erp",
        "map_okosted",
        "dim_okosted",
        "dso_sync_dim_okosted",
        # --- SOR staging (feeds the FHIR Organization build below) ---
        "stg_organisation_sor",
        # --- FHIR Organisation (SOR -> FHIR server -> SoF). Materialised
        #     BEFORE ref/dim: dim.Organisation now sources from fhir.Organization
        #     via ref.Organisation, so the SoF flat view must exist first. ---
        "fhir_organization_sor",
        "fhir_sof_infrastructure",
        "sof_organization",
        "dso_sync_fhir_organization",
        # --- DWH infrastructure + dimension (SOR only) ---
        "staging_infra_organisation",
        # NOTE: map_organisation / dso_sync_map_organisation are intentionally NOT
        # in this job. map.Organisation has no runtime consumers left
        # (dim.Organisation, dim.Lager, dim.Rekvirent and conceptmap resolution
        # all use ref.Organisation). Its only remaining purpose is to be the
        # one-time seed source for ref.Organisation — and migration
        # 20260618_ref_organisation.sql reads the persisted table, not a fresh
        # asset run, so there is no reason to rebuild it daily. The asset still
        # exists for an ad-hoc seed/sync; drop the table + assets in a follow-up
        # once ref.Organisation is seeded in every environment.
        "ref_organisation",
        "dim_organisation",
        # --- DSO sync (staging -> DSO) ---
        "dso_sync_dim_organisation",
    ),
    description=(
        "Daily Organisation + ERP->SOR terminology ETL. "
        "Builds the EAN/Lager/Rekvirent/Okosted codesystems and their *-to-SOR "
        "conceptmaps from fresh source extracts (NemHandel / ILS / SYDAX / OS2MO) "
        "and syncs codesystem.* + conceptmap.* to DSO. "
        "Stages SOR, posts FHIR Organizations from SOR, extracts the SoF flat "
        "view, then builds ref.Organisation + dim.Organisation from "
        "fhir.Organization and syncs dim.Organisation + fhir.Organization to DSO. "
        "Also populates the Okosted map+dim tables. "
        "Note: the clinical SKS/SOR codesystems and FHIR CodeSystem posting are "
        "intentionally NOT included (separate terminology-server cadence)."
    ),
)

__all__ = [
    "organisation_cs_job",
]
