# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Centralized Infrastructure Module

Provides DDL management for both DSI/DSA (staging) and DSO (reporting) databases.

This module creates:
- Schemas: stg (empty), map, dim, fct, bridge, sec, meta, vw_dim, vw_fct, fhir, vw_fhir
- Functions: dbo.fn_DateTime_To_TimeKey, dbo.ConvertSASDateToDateTime, etc.
- Persistent Tables: map.*, dim.*, fct.*, sec.*, meta.* (NOT stg.* tables)
- Views: vw_dim.*, vw_fct.*, vw_fhir.* (DSO only)

NOTE: Jobs have been centralized to etl/jobs/
Import jobs from: from jobs import staging_infrastructure_job, dso_infrastructure_job, full_infrastructure_job
"""

from infrastructure.assets import (  # Linked Server assets; FHIR IFDB (database indexes); FHIR SOF (Aidbox ViewDefinitions); Staging Linked Server User; Staging ref tables; Staging infrastructure assets
    dso_infra_codesystem,
    dso_infra_conceptmap,
    dso_infra_erp,
    dso_infra_drg,
    dso_infra_haiba,
    dso_infra_etl_routing,
    dso_infra_fhir,
    dso_infra_functions,
    dso_infra_linked_server,
    dso_infra_meta,
    dso_infra_organisation,
    dso_infra_ref,
    dso_infra_db_settings,
    dso_infra_schemas,
    dso_infra_security,
    dso_infra_shared_dimensions,
    dso_infra_views_codesystem,
    dso_infra_views_erp,
    dso_infra_views_fhir,
    dso_infra_views_drg,
    dso_infra_views_haiba,
    dso_infra_views_security,
    dso_migrations,
    dso_verify_linked_server,
    dso_watermark_indexes,
    erp_index_maintenance,
    all_db_space_assets,
    fhir_ifdb_database_indexes,
    fhir_sof_infrastructure,
    fhir_structure_definitions,
    staging_infra_codesystem,
    staging_infra_conceptmap,
    staging_infra_dfm,
    migrate_dso_sync_watermarks_to_dfm,
    staging_infra_drg,
    staging_infra_erp,
    staging_infra_etl_routing,
    staging_infra_etl_routing_seed,
    staging_infra_haiba,
    staging_infra_fhir,
    staging_infra_functions,
    staging_infra_ifdb_archive,
    staging_infra_dqa,
    staging_infra_kmd_uddata,
    staging_infra_linked_server_user,
    staging_infra_db_space,
    staging_infra_organisation,
    staging_infra_ref,
    staging_infra_db_settings,
    staging_infra_schemas,
    staging_infra_security,
    staging_infra_shared_dimensions,
    staging_infra_udbudskategori,
    staging_infra_udbudskategori_seed,
    staging_infra_views_conceptmap,
    staging_migrations,
    staging_watermark_indexes,
)
from infrastructure.assets.fhir_codesystem_shak_complete import fhir_codesystem_shak_complete
from infrastructure.assets.fhir_codesystem_sks_complete import fhir_codesystem_sks_complete
from infrastructure.assets.fhir_codesystem_sor_complete import fhir_codesystem_sor_complete
from infrastructure.assets.fhir_conceptmap_shak_to_sor import fhir_conceptmap_shak_to_sor

# All assets for registration in definitions.py
all_staging_assets = [
    staging_infra_db_settings,
    staging_infra_schemas,
    staging_infra_functions,
    staging_infra_ref,
    staging_infra_shared_dimensions,
    staging_infra_organisation,
    staging_infra_erp,
    staging_infra_kmd_uddata,
    staging_infra_udbudskategori,
    staging_infra_udbudskategori_seed,
    staging_infra_security,
    staging_infra_fhir,
    staging_infra_codesystem,
    staging_infra_conceptmap,
    staging_infra_views_conceptmap,
    staging_infra_etl_routing,
    staging_infra_etl_routing_seed,
    staging_infra_drg,
    staging_infra_haiba,
    staging_infra_linked_server_user,
    staging_infra_dfm,
    migrate_dso_sync_watermarks_to_dfm,
    staging_infra_db_space,
    staging_infra_ifdb_archive,
    staging_infra_dqa,
    staging_watermark_indexes,
    # FHIR SOF (Aidbox ViewDefinitions)
    fhir_sof_infrastructure,
    # FHIR IFDB (database indexes)
    fhir_ifdb_database_indexes,
    # FHIR setup (StructureDefinitions, CodeSystems)
    fhir_structure_definitions,
    fhir_codesystem_sks_complete,
    fhir_codesystem_sor_complete,
    fhir_codesystem_shak_complete,
    fhir_conceptmap_shak_to_sor,
    # ERP maintenance
    erp_index_maintenance,
    # Migrations (centralized)
    staging_migrations,
]

all_dso_assets = [
    dso_infra_db_settings,
    dso_infra_schemas,
    dso_infra_meta,
    dso_infra_functions,
    dso_infra_ref,
    dso_infra_shared_dimensions,
    dso_infra_organisation,
    dso_infra_erp,
    dso_infra_drg,
    dso_infra_haiba,
    dso_infra_security,
    dso_infra_codesystem,
    dso_infra_etl_routing,
    dso_infra_views_erp,
    dso_infra_views_fhir,
    dso_infra_views_codesystem,
    dso_infra_views_drg,
    dso_infra_views_haiba,
    dso_infra_views_security,
    dso_infra_fhir,
    # Linked Server
    dso_infra_linked_server,
    dso_verify_linked_server,
    # Migrations (centralized)
    dso_migrations,
    dso_infra_conceptmap,
    dso_watermark_indexes,
]

all_infrastructure_assets = all_staging_assets + all_dso_assets + all_db_space_assets

__all__ = [
    # Staging assets
    "staging_infra_db_settings",
    "staging_infra_schemas",
    "staging_infra_functions",
    "staging_infra_ref",
    "staging_infra_shared_dimensions",
    "staging_infra_organisation",
    "staging_infra_erp",
    "staging_infra_kmd_uddata",
    "staging_infra_udbudskategori",
    "staging_infra_udbudskategori_seed",
    "staging_infra_security",
    "staging_infra_fhir",
    "staging_infra_codesystem",
    "staging_infra_conceptmap",
    "staging_infra_views_conceptmap",
    "staging_infra_drg",
    "staging_infra_etl_routing",
    "staging_infra_etl_routing_seed",
    "staging_infra_haiba",
    "fhir_sof_infrastructure",
    "fhir_ifdb_database_indexes",
    "fhir_structure_definitions",
    "fhir_codesystem_sks_complete",
    "fhir_codesystem_sor_complete",
    "fhir_codesystem_shak_complete",
    "fhir_conceptmap_shak_to_sor",
    "erp_index_maintenance",
    "staging_infra_linked_server_user",
    "staging_infra_dfm",
    "migrate_dso_sync_watermarks_to_dfm",
    "staging_infra_ifdb_archive",
    "staging_infra_db_space",
    "staging_infra_dqa",
    # DSO assets
    "dso_infra_db_settings",
    "dso_infra_schemas",
    "dso_infra_meta",
    "dso_infra_functions",
    "dso_infra_ref",
    "dso_infra_shared_dimensions",
    "dso_infra_organisation",
    "dso_infra_erp",
    "dso_infra_drg",
    "dso_infra_haiba",
    "dso_infra_security",
    "dso_infra_codesystem",
    "dso_infra_etl_routing",
    "dso_infra_views_erp",
    "dso_infra_views_fhir",
    "dso_infra_views_codesystem",
    "dso_infra_views_drg",
    "dso_infra_views_haiba",
    "dso_infra_views_security",
    "dso_infra_fhir",
    "dso_infra_conceptmap",
    # Linked Server assets
    "dso_infra_linked_server",
    "dso_verify_linked_server",
    "dso_watermark_indexes",
    "staging_watermark_indexes",
    # Migrations (centralized)
    "staging_migrations",
    "dso_migrations",
    # Collections
    "all_staging_assets",
    "all_dso_assets",
    "all_infrastructure_assets",
]
