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
        # --- Source extracts + archives for the *-to-SOR codesystems (kept in the
        # job so the daily run builds them from FRESH source data). cs_lager /
        # cs_rekvirent read the ILS DFM archive (arc.ils_*), so both the DFM extract
        # (ext.ils_* — fresh 1:1 pull from ILS) and its archive upsert run here. The
        # same arc.ils_* assets are also maintained by erp_daily_dfm — a harmless
        # redundant refresh. ---
        "extract_ean_nemhandel",  # cs_ean source (NemHandel)
        "ext_epji_location",  # cs_shakalike source (EPJI location)
        "arc_epji_shakalike",  # cs_shakalike source (EPJI archive)
        "ext_ils_sgh",
        "arc_ils_sgh",  # cs_lager + cs_rekvirent source (ILS)
        "ext_ils_lgr",
        "arc_ils_lgr",  # cs_lager source (ILS)
        "ext_ils_rekv",
        "arc_ils_rekv",  # cs_rekvirent source (ILS)
        "ext_ils_rekvgrp",
        "arc_ils_rekvgrp",  # cs_rekvirent source (ILS)
        # NOTE: cs_okosted sources the SYDAX omkostningssted hierarchy from the DFM
        # archive (arc.sydax_*), which the sydax_archive group in erp_daily_dfm_job
        # maintains daily and which always holds the full current-version history.
        # cs_okosted UPSERTs (no delete) into the persistent codesystem.Okosted*
        # tables that stg/dim_Okosted read, so a from-scratch rebuild is safe to run
        # directly after the sydax_archive group — no prerequisite full-load.
        # OS2MO CSV extracts (manual ConceptMap overrides)
        "ext_os2mo_ean_to_sor",
        "ext_os2mo_lager_to_sor",
        "ext_os2mo_rekvirent_to_sor",
        "ext_os2mo_okosted_to_sor",
        "extract_shak_register",
        # --- Codesystems (staging) — the entities resolved to SOR ---
        "cs_ean",
        "cs_lager",
        "cs_rekvirent",
        "cs_okosted",
        "cs_shak",
        "cs_shakalike",
        # --- ConceptMaps: ean/Lager/Rekvirent/Okosted/SHAK/SHAK-a-like -> SOR ---
        "cm_ean_to_sor",
        "cm_lager_to_sor",
        "cm_rekvirent_to_sor",
        "cm_okosted_to_sor",
        "cm_shak_to_sor",
        "cm_shakalike_to_sor",
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
        "dso_sync_codesystem_shak_concept",
        "dso_sync_codesystem_shak_properties",
        "dso_sync_codesystem_shak_hierarchy",
        "dso_sync_codesystem_shakalike_concept",
        "dso_sync_codesystem_shakalike_properties",
        "dso_sync_codesystem_shakalike_hierarchy",
        "dso_sync_conceptmap_ean_to_sor",
        "dso_sync_conceptmap_lager_to_sor",
        "dso_sync_conceptmap_rekvirent_to_sor",
        "dso_sync_conceptmap_okosted_to_sor",
        "dso_sync_conceptmap_shak_to_sor",
        "dso_sync_conceptmap_shakalike_to_sor",
        # --- Okosted DWH pipeline (codesystem -> staging -> map -> dim -> DSO) ---
        "stg_Okosted",
        "staging_infra_erp",
        "map_Okosted",
        "dim_Okosted",
        "dso_sync_dim_okosted",
        "stg_SHAK",
        "map_SHAK",
        "dim_SHAK",
        "dso_sync_dim_shak",
        "stg_SHAKAlike",
        "map_SHAKAlike",
        "dim_SHAKAlike",
        "dso_sync_dim_shakalike",
        # --- SOR: single source extract -> version archive -> codesystem
        #     (feeds the FHIR Organization build below). extract_sor_register
        #     is the ONLY SOR download; default mode "current" pulls the daily
        #     snapshot. Run extract + arc + fhir_organization_sor in
        #     full_load/all_historical mode for a historical backfill. ---
        "extract_sor_register",
        "arc_sor_register",
        "cs_sor",
        "dso_sync_codesystem_sor_concept",
        "dso_sync_codesystem_sor_properties",
        "dso_sync_codesystem_sor_hierarchy",
        # --- FHIR Organisation (codesystem.SOR* -> FHIR server -> SoF).
        #     DFM delta: posts only codes changed since the checkpoint.
        #     Materialised BEFORE ref/dim: dim.Organisation sources from
        #     fhir.Organization via ref.Organisation, so the SoF flat view
        #     must exist first. ---
        "fhir_organization_sor",
        "fhir_sof_infrastructure",
        "sof_organization",
        "dso_sync_fhir_organization",
        # --- DWH infrastructure + dimension (SOR only) ---
        "staging_infra_organisation",
        # NOTE: the legacy map_Organisation / dso_sync_map_organisation assets
        # were removed with [internal ticket] (they read the retired stg_Organisation_sor
        # download). The persisted map.Organisation table remains as the
        # one-time seed source for ref.Organisation (migration
        # 20260618_ref_organisation.sql reads the table, not an asset run);
        # drop the table in a follow-up once ref.Organisation is seeded in
        # every environment.
        "ref_organisation",
        "dim_Organisation",
        # --- DSO sync (staging -> DSO) ---
        "dso_sync_dim_organisation",
    ),
    description=(
        "Daily Organisation + ERP->SOR terminology ETL. "
        "Builds the ean/Lager/Rekvirent/Okosted/SHAK/SHAK-a-like codesystems and "
        "their *-to-SOR conceptmaps from fresh source extracts "
        "(NemHandel / ILS / SYDAX / OS2MO / EPJI) "
        "and syncs codesystem.* + conceptmap.* to DSO. "
        "Downloads SOR once (extract -> arc.sor_register archive -> cs_sor), "
        "posts FHIR Organizations from codesystem.SOR* (DFM delta), extracts "
        "the SoF flat view, then builds ref.Organisation + dim.Organisation "
        "from fhir.Organization and syncs dim.Organisation + fhir.Organization "
        "to DSO. Also populates the Okosted map+dim tables. "
        "Note: the FHIR CodeSystem terminology-server posting is intentionally "
        "NOT included (separate weekly cadence in fhir_codesystem_job)."
    ),
)

__all__ = [
    "organisation_cs_job",
]
