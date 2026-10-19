# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Infrastructure assets for staging and DSO databases."""

from infrastructure.assets.db_space import (
    all_db_space_assets,
    all_db_space_checks,
    infra_db_space_dso,
    infra_db_space_staging,
)
from infrastructure.assets.dso_infrastructure import (
    dso_infra_codesystem,
    dso_infra_conceptmap,
    dso_infra_erp,
    dso_infra_etl_routing,
    dso_infra_fhir,
    dso_infra_functions,
    dso_infra_drg,
    dso_infra_haiba,
    dso_infra_meta,
    dso_infra_organisation,
    dso_infra_ref,
    dso_infra_db_settings,
    dso_infra_schemas,
    dso_infra_security,
    dso_infra_shared_dimensions,
    dso_infra_udredningsret,
    dso_infra_views_codesystem,
    dso_infra_views_erp,
    dso_infra_views_fhir,
    dso_infra_views_drg,
    dso_infra_views_haiba,
    dso_infra_views_security,
    dso_migrations,
)
from infrastructure.assets.fhir_codesystem_shak_complete import (
    fhir_codesystem_shak_complete,
)
from infrastructure.assets.fhir_codesystem_sks_complete import (
    fhir_codesystem_sks_complete,
)
from infrastructure.assets.fhir_codesystem_sor_complete import (
    fhir_codesystem_sor_complete,
)
from infrastructure.assets.fhir_conceptmap_shak_to_sor import (
    fhir_conceptmap_shak_to_sor,
)
from infrastructure.assets.fhir_ifdb_infrastructure import (
    fhir_ifdb_database_indexes,
)
from infrastructure.assets.fhir_sof_infrastructure import (
    fhir_sof_infrastructure,
)
from infrastructure.assets.fhir_structure_definitions import (
    fhir_structure_definitions,
)
from infrastructure.assets.index_maintenance import (
    erp_index_maintenance,
)
from infrastructure.assets.linked_server import (
    dso_infra_linked_server,
    dso_verify_linked_server,
)
from infrastructure.assets.migrate_dso_sync_watermarks import (
    migrate_dso_sync_watermarks_to_dfm,
)
from infrastructure.assets.staging_infrastructure import (
    staging_infra_codesystem,
    staging_infra_conceptmap,
    staging_infra_dfm,
    staging_infra_dqa,
    staging_infra_cpr,
    staging_infra_drg,
    staging_infra_udredningsret,
    staging_infra_lpr3,
    staging_infra_erp,
    staging_infra_etl_routing,
    staging_infra_etl_routing_seed,
    staging_infra_haiba,
    staging_infra_fhir,
    staging_infra_functions,
    staging_infra_epji_archive,
    staging_infra_ifdb_archive,
    staging_infra_kmd_uddata,
    staging_infra_linked_server_user,
    staging_infra_db_space,
    staging_infra_organisation,
    staging_infra_ref,
    staging_infra_db_settings,
    staging_infra_schemas,
    staging_infra_security,
    staging_infra_shared_dimensions,
    staging_infra_silkeborgdata,
    staging_infra_views_conceptmap,
    staging_migrations,
)
from infrastructure.assets.watermark_indexes import (
    dso_watermark_indexes,
    staging_watermark_indexes,
)

__all__ = [
    # Staging
    "staging_infra_db_settings",
    "staging_infra_schemas",
    "staging_infra_functions",
    "staging_infra_ref",
    "staging_infra_shared_dimensions",
    "staging_infra_organisation",
    "staging_infra_erp",
    "staging_infra_kmd_uddata",
    "staging_infra_silkeborgdata",
    "staging_infra_security",
    "staging_infra_fhir",
    "staging_infra_codesystem",
    "staging_infra_conceptmap",
    "staging_infra_views_conceptmap",
    "staging_infra_cpr",
    "staging_infra_drg",
    "staging_infra_udredningsret",
    "staging_infra_lpr3",
    "staging_infra_etl_routing",
    "staging_infra_etl_routing_seed",
    "staging_infra_haiba",
    # FHIR SOF (Aidbox ViewDefinitions)
    "fhir_sof_infrastructure",
    # FHIR IFDB (database indexes)
    "fhir_ifdb_database_indexes",
    # FHIR setup (StructureDefinitions)
    "fhir_structure_definitions",
    # Staging Linked Server User
    "staging_infra_linked_server_user",
    "staging_infra_db_space",
    # DFM
    "staging_infra_dfm",
    "migrate_dso_sync_watermarks_to_dfm",
    # IFDB Archive
    "staging_infra_ifdb_archive",
    # EPJI Archive
    "staging_infra_epji_archive",
    "staging_infra_dqa",
    # DSO
    "dso_infra_db_settings",
    "dso_infra_schemas",
    "dso_infra_meta",
    "dso_infra_functions",
    "dso_infra_ref",
    "dso_infra_shared_dimensions",
    "dso_infra_organisation",
    "dso_infra_erp",
    "dso_infra_drg",
    "dso_infra_udredningsret",
    "dso_infra_haiba",
    "dso_infra_security",
    "dso_infra_views_erp",
    "dso_infra_views_fhir",
    "dso_infra_views_codesystem",
    "dso_infra_views_drg",
    "dso_infra_views_haiba",
    "dso_infra_views_security",
    "dso_infra_fhir",
    "dso_infra_codesystem",
    "dso_infra_etl_routing",
    "dso_infra_conceptmap",
    # Migrations (centralized, replaces per-domain migrations/ and cleanup/)
    "staging_migrations",
    "dso_migrations",
    # Linked Server
    "dso_infra_linked_server",
    "dso_verify_linked_server",
    # Watermark indexes (programmatic from sync config)
    "staging_watermark_indexes",
    "dso_watermark_indexes",
    # ERP index maintenance
    "erp_index_maintenance",
    "infra_db_space_staging",
    "infra_db_space_dso",
    "all_db_space_assets",
    "all_db_space_checks",
    # FHIR CodeSystem SKS (content-complete)
    "fhir_codesystem_sks_complete",
    # FHIR CodeSystem SOR (content-complete)
    "fhir_codesystem_sor_complete",
    # FHIR CodeSystem SHAK (content-complete)
    "fhir_codesystem_shak_complete",
    # FHIR ConceptMap SHAK -> SOR (content-complete)
    "fhir_conceptmap_shak_to_sor",
]
