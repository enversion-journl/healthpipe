# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Load Module

Provides utilities and assets for syncing data from staging (DSI/DSA) to DSO database.
Uses SQL Server Linked Server for optimal performance on large datasets.
"""

from DSO.load.sync_state import SyncStateManager
from DSO.load.sync_utils import (
    LINKED_SERVER_NAME,
    STAGING_DATABASE,
    SchemaValidationError,
    SyncTableConfig,
    build_delta_merge_statement,
    build_merge_statement,
    build_truncate_insert_statement,
    compare_row_counts,
    get_dso_row_count,
    get_staging_row_count,
    get_table_columns,
    sync_table_via_linked_server,
    validate_linked_server,
    validate_table_schemas_match,
)

from DSO.load.sync_configs import (
    ALL_SYNC_CONFIGS,
    CONCEPTMAP_SYNC_CONFIGS,
    ERP_SYNC_CONFIGS,
    CODESYSTEM_SYNC_CONFIGS,
    SHARED_DIMS_SYNC_CONFIGS,
    SECURITY_SYNC_CONFIGS,
    get_config,
)

from DSO.load.sync_codesystem import (
    all_codesystem_dso_sync_assets,
    dso_refresh_codesystem_sfi_search_document,
    dso_sync_codesystem_sfi_concept,
    dso_sync_codesystem_sfi_properties,
    dso_sync_codesystem_sfi_hierarchy,
)

from DSO.load.sync_dim import (
    all_dso_sync_dim_assets,
    dso_sync_dim_aftale,
    dso_sync_dim_AfdelingLagerbeholdningEgenskaber,
    dso_sync_dim_borger,
    dso_sync_dim_ean,
    dso_sync_dim_faktura,
    dso_sync_dim_fakturalinje,
    dso_sync_dim_finanskladde,
    dso_sync_dim_finanskladdelinje,
    dso_sync_dim_ilsvare,
    dso_sync_dim_kontoplan,
    dso_sync_dim_kreditor,
    dso_sync_dim_lager,
    dso_sync_dim_LagerbeholdningEgenskaber,
    dso_sync_dim_organisation,
    dso_sync_dim_postering,
    dso_sync_dim_rekvirent,
    dso_sync_dim_UNSPSC,
    dso_sync_dim_vareudlevering_egenskaber,
    dso_sync_dim_vare,
    dso_sync_map_organisation,
    dso_sync_dim_okosted,
)

from DSO.load.sync_fct import (
    all_dso_sync_fct_assets,
    dso_sync_fct_AfdelingLagerbeholdning,
    dso_sync_fct_faktura,
    dso_sync_fct_fakturalinje,
    dso_sync_fct_finanskladdelinje,
    dso_sync_fct_Lagerbeholdning,
    dso_sync_fct_postering,
    dso_sync_fct_vareudlevering,
    dso_sync_fct_indkoebsanalyse_linje,
)

from DSO.load.sync_fhir import (
    all_fhir_dso_sync_assets,
    all_fhir_dso_sync_daily_assets,
    all_fhir_dso_sync_dfm_assets,
    all_fhir_dso_sync_monthly_assets,
    dso_sync_fhir_organization,
)

from DSO.load.refresh_sfi_datapoint import (
    all_sfi_datapoint_refresh_assets,
    dso_reconcile_sfi_datapoint_store,
    dso_refresh_sfi_datapoint_daily,
    dso_refresh_sfi_datapoint_monthly,
)

from DSO.load.sync_conceptmap import (
    all_conceptmap_dso_sync_assets,
    dso_sync_conceptmap_ean_to_sor,
    dso_sync_conceptmap_lager_to_sor,
    dso_sync_conceptmap_okosted_to_sor,
    dso_sync_conceptmap_rekvirent_to_sor,
)


all_dso_sync_assets = (
    all_dso_sync_dim_assets
    + all_dso_sync_fct_assets
    + all_codesystem_dso_sync_assets
    + all_conceptmap_dso_sync_assets
)


__all__ = [
    # Constants
    "LINKED_SERVER_NAME",
    "STAGING_DATABASE",
    # Exceptions
    "SchemaValidationError",
    # Config classes
    "SyncTableConfig",
    # State management
    "SyncStateManager",
    # Core functions
    "sync_table_via_linked_server",
    "validate_linked_server",
    "validate_table_schemas_match",
    "build_merge_statement",
    "build_delta_merge_statement",
    "build_truncate_insert_statement",
    "get_table_columns",
    # Row count utilities
    "get_staging_row_count",
    "get_dso_row_count",
    "compare_row_counts",
    # Configurations
    "ALL_SYNC_CONFIGS",
    "CONCEPTMAP_SYNC_CONFIGS",
    "ERP_SYNC_CONFIGS",
    "SHARED_DIMS_SYNC_CONFIGS",
    "SECURITY_SYNC_CONFIGS",
    "CODESYSTEM_SYNC_CONFIGS",
    "get_config",
    # Dim sync assets
    "all_dso_sync_dim_assets",
    "dso_sync_dim_aftale",
    "dso_sync_dim_faktura",
    "dso_sync_dim_fakturalinje",
    "dso_sync_dim_finanskladde",
    "dso_sync_dim_finanskladdelinje",
    "dso_sync_dim_kontoplan",
    "dso_sync_dim_kreditor",
    "dso_sync_dim_ean",
    "dso_sync_dim_lager",
    "dso_sync_dim_postering",
    "dso_sync_dim_rekvirent",
    "dso_sync_dim_vareudlevering_egenskaber",
    "dso_sync_dim_vare",
    "dso_sync_dim_borger",
    "dso_sync_map_organisation",
    "dso_sync_dim_organisation",
    "dso_sync_dim_LagerbeholdningEgenskaber",
    "dso_sync_dim_AfdelingLagerbeholdningEgenskaber",
    "dso_sync_dim_ilsvare",
    "dso_sync_dim_UNSPSC",
    "dso_sync_dim_okosted",
    # Fact sync assets
    "all_dso_sync_fct_assets",
    "dso_sync_fct_faktura",
    "dso_sync_fct_fakturalinje",
    "dso_sync_fct_finanskladdelinje",
    "dso_sync_fct_postering",
    "dso_sync_fct_vareudlevering",
    "dso_sync_fct_Lagerbeholdning",
    "dso_sync_fct_AfdelingLagerbeholdning",
    "dso_sync_fct_indkoebsanalyse_linje",
    # ConceptMap sync assets
    "all_conceptmap_dso_sync_assets",
    "dso_sync_conceptmap_rekvirent_to_sor",
    "dso_sync_conceptmap_lager_to_sor",
    "dso_sync_conceptmap_ean_to_sor",
    "dso_sync_conceptmap_okosted_to_sor",
    # FHIR sync assets
    "all_fhir_dso_sync_assets",
    "all_fhir_dso_sync_daily_assets",
    "all_fhir_dso_sync_dfm_assets",
    "all_fhir_dso_sync_monthly_assets",
    "dso_sync_fhir_organization",
    # SFI_datapoint materialized store refresh
    "all_sfi_datapoint_refresh_assets",
    "dso_reconcile_sfi_datapoint_store",
    "dso_refresh_sfi_datapoint_daily",
    "dso_refresh_sfi_datapoint_monthly",
    # Combined
    "all_dso_sync_assets",
    "all_codesystem_dso_sync_assets",
    "dso_refresh_codesystem_sfi_search_document",
    "dso_sync_codesystem_sfi_concept",
    "dso_sync_codesystem_sfi_properties",
    "dso_sync_codesystem_sfi_hierarchy",
]
