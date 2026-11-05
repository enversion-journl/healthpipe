# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Setup assets for staging and DSO; see docs/implementation/infrastructure_guide.md."""

from infrastructure.assets.dso_infrastructure import (
    dso_infra_codesystem,
    dso_infra_conceptmap,
    dso_infra_erp,
    dso_infra_drg,
    dso_infra_haiba,
    dso_infra_etl_routing,
    dso_infra_fhir,
    dso_infra_functions,
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
from infrastructure.assets.linked_server import dso_infra_linked_server, dso_verify_linked_server
from infrastructure.assets.watermark_indexes import dso_watermark_indexes, staging_watermark_indexes
from infrastructure.assets.index_maintenance import (
    erp_index_maintenance,
    fhir_dw_keys_index_maintenance,
)
from infrastructure.assets.db_space import all_db_space_assets
from infrastructure.assets.fhir_ifdb_infrastructure import fhir_ifdb_database_indexes
from infrastructure.assets.fhir_sof_infrastructure import fhir_sof_infrastructure
from infrastructure.assets.fhir_structure_definitions import fhir_structure_definitions
from infrastructure.assets.staging_infrastructure import (
    staging_infra_codesystem,
    staging_infra_conceptmap,
    staging_infra_dfm,
    staging_infra_drg,
    staging_infra_udredningsret,
    staging_infra_erp,
    staging_infra_etl_routing,
    staging_infra_etl_routing_seed,
    staging_infra_haiba,
    staging_infra_source_cpr,
    staging_infra_source_drg,
    staging_infra_source_epji,
    staging_infra_source_haiba,
    staging_infra_source_ifdb,
    staging_infra_source_ils,
    staging_infra_source_kmd_uddata,
    staging_infra_source_lpr3,
    staging_infra_source_mysupply,
    staging_infra_source_os2mo,
    staging_infra_source_silkeborgdata,
    staging_infra_source_sor,
    staging_infra_source_sydax,
    staging_infra_source_udredningsret,
    staging_infra_fhir,
    staging_infra_functions,
    staging_infra_export,
    staging_infra_dqa,
    staging_infra_linked_server_user,
    staging_infra_db_space,
    staging_infra_organisation,
    staging_infra_ref,
    staging_infra_db_settings,
    staging_infra_schemas,
    staging_infra_security,
    staging_infra_shared_dimensions,
    staging_infra_views_conceptmap,
    staging_migrations,
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
    staging_infra_security,
    staging_infra_fhir,
    staging_infra_codesystem,
    staging_infra_conceptmap,
    staging_infra_views_conceptmap,
    staging_infra_etl_routing,
    staging_infra_etl_routing_seed,
    staging_infra_drg,
    staging_infra_udredningsret,
    staging_infra_haiba,
    staging_infra_source_cpr,
    staging_infra_source_drg,
    staging_infra_source_epji,
    staging_infra_source_haiba,
    staging_infra_source_ifdb,
    staging_infra_source_ils,
    staging_infra_source_kmd_uddata,
    staging_infra_source_lpr3,
    staging_infra_source_mysupply,
    staging_infra_source_os2mo,
    staging_infra_source_silkeborgdata,
    staging_infra_source_sor,
    staging_infra_source_sydax,
    staging_infra_source_udredningsret,
    staging_infra_linked_server_user,
    staging_infra_dfm,
    staging_infra_db_space,
    staging_infra_export,
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
    # Index maintenance
    erp_index_maintenance,
    fhir_dw_keys_index_maintenance,
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
    dso_infra_udredningsret,
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

__all__ = ["all_staging_assets", "all_dso_assets", "all_infrastructure_assets"]
