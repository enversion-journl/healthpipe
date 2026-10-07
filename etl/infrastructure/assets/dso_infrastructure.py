# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Infrastructure Assets (Data Staging Out Layer)

Creates all database objects on the DSO (reporting) database:
- Schemas (stg, map, dim, fct, bridge, sec, vw_dim, vw_fct, vw_fhir)
- Functions (same as staging)
- Persistent Tables (complete copy from staging)
- Views (vw_dim.*, vw_fct.*, vw_fhir.*)

The DSO database is a complete copy of the staging database persistent tables,
plus the secure views that users access.
"""

import dagster as dg
from core.resources import MSSQLResource
from infrastructure.utils.ddl_loader import (
    apply_migrations,
    execute_legacy_sql2019_functions_if_needed,
    execute_sql_file,
    execute_sql_files_from_directory,
)

# ============================================================================
# SCHEMAS
# ============================================================================


@dg.asset(
    name="dso_infra_schemas",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    tags={"layer": "dso", "type": "infrastructure", "object": "schema"},
    description="Create all required schemas on DSO database",
)
def dso_infra_schemas(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create schemas: stg, map, dim, fct, bridge, sec, meta, vw_dim, vw_fct, fhir, vw_fhir"""

    execute_sql_file(mssql_target_dso, "common/00_schemas.sql", context)

    context.log.info("✅ All schemas created on DSO")
    return dg.MaterializeResult(
        metadata={
            "schemas": "stg, map, dim, fct, bridge, sec, meta, vw_dim, vw_fct, fhir, vw_fhir",
            "database": "DSO (reporting)",
        }
    )


# ============================================================================
# DATABASE-LEVEL SETTINGS
# ============================================================================

_DB_SETTINGS_QUERY = """
    SELECT is_auto_update_stats_async_on, is_auto_close_on
    FROM sys.databases WHERE name = DB_NAME()
"""


@dg.asset(
    name="dso_infra_db_settings",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    tags={"layer": "dso", "type": "infrastructure", "object": "database"},
    description=(
        "Enforce database-level settings on dso (AUTO_UPDATE_STATISTICS_ASYNC ON, "
        "AUTO_CLOSE OFF). Re-applied on every infrastructure run so the settings "
        "survive restores and fresh databases — uniformly for Nomad-hosted and "
        "client-managed servers."
    ),
)
def dso_infra_db_settings(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    execute_sql_file(mssql_target_dso, "common/01_db_settings.sql", context, autocommit=True)

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(_DB_SETTINGS_QUERY)
            stats_async_on, auto_close_on = cursor.fetchone()
        finally:
            cursor.close()

    if not stats_async_on or auto_close_on:
        context.log.warning(
            "DB settings not fully enforced (likely missing ALTER DATABASE "
            "permission): is_auto_update_stats_async_on=%s, is_auto_close_on=%s",
            stats_async_on,
            auto_close_on,
        )
    else:
        context.log.info("✅ DB settings enforced: async stats ON, AUTO_CLOSE OFF")

    return dg.MaterializeResult(
        metadata={
            "is_auto_update_stats_async_on": dg.MetadataValue.bool(bool(stats_async_on)),
            "is_auto_close_on": dg.MetadataValue.bool(bool(auto_close_on)),
        }
    )


# ============================================================================
# META (Operational metadata - sync watermarks, etc.)
# ============================================================================


@dg.asset(
    name="dso_infra_meta",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "object": "table"},
    description="Create metadata tables on DSO (sync watermarks, DFM state, space snapshots)",
)
def dso_infra_meta(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Run every ddl/tables/meta file on DSO (watermarks, DFM, space snapshots)."""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/meta", context)

    context.log.info("✅ Meta tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "batches_executed": sum(results.values()),
        }
    )


# ============================================================================
# FUNCTIONS
# ============================================================================


@dg.asset(
    name="dso_infra_functions",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_shared_dimensions"],
    tags={"layer": "dso", "type": "infrastructure", "object": "function"},
    description="Create all helper functions on DSO database",
)
def dso_infra_functions(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create functions: fn_DateTime_To_TimeKey, GetBorgerSK_TVF, ConvertSASDateToDateTime"""

    results = execute_sql_files_from_directory(mssql_target_dso, "common/functions", context)
    legacy_results = execute_legacy_sql2019_functions_if_needed(mssql_target_dso, context)
    results.update(legacy_results)

    context.log.info(f"✅ {len(results)} functions created on DSO")
    return dg.MaterializeResult(
        metadata={
            "functions_created": list(results.keys()),
            "count": len(results),
            "legacy_sql2019_functions": list(legacy_results.keys()),
        }
    )


# ============================================================================
# SHARED DIMENSIONS (Complete copy from staging)
# ============================================================================


@dg.asset(
    name="dso_infra_shared_dimensions",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations", "dso_infra_ref"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "shared_dimensions"},
    description="Create shared dimension tables on DSO (complete copy from staging)",
)
def dso_infra_shared_dimensions(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create shared dimension tables on DSO (dim.Borger, dim.Dato).

    Depends on dso_migrations so the 20260515 drop-and-rebuild migration
    runs before the new dim.Borger DDL is applied, and on dso_infra_ref
    so the ref.PatientToBorger / ref.Patient_Address_To_Borger tables exist
    before downstream loaders try to reference them.
    """

    results = execute_sql_files_from_directory(
        mssql_target_dso, "tables/shared_dimensions", context
    )

    context.log.info("✅ Shared dimensions tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "dim.Borger, dim.Dato",
        }
    )


# ============================================================================
# REF (Reference tables — INT-surrogate mapping for FHIR-backed dims)
# ============================================================================


@dg.asset(
    name="dso_infra_ref",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "ref"},
    description="Create ref schema tables on DSO (ref.PatientToBorger, ref.Patient_Address_To_Borger, ref.Organisation)",
)
def dso_infra_ref(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create ref-schema tables on DSO.

    The ref schema bridges FHIR-native string IDs and the INT surrogate
    keys used by dim.Borger and fact tables.
    """

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/ref", context)

    context.log.info("✅ Ref tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": (
                "ref.PatientToBorger, ref.Patient_Address_To_Borger, "
                "ref.Organisation, ref.RekvirentUndtagelse"
            ),
        }
    )


# ============================================================================
# ORGANIZATION (Complete copy from staging)
# ============================================================================


@dg.asset(
    name="dso_infra_organisation",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "organisation"},
    description="Create organisation tables on DSO (complete copy from staging)",
)
def dso_infra_organisation(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create organisation map and dim tables on DSO."""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/organisation", context)

    context.log.info("✅ Organisation tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "map.Organisation, dim.Organisation",
        }
    )


# ============================================================================
# ERP (Complete copy from staging)
# ============================================================================


@dg.asset(
    name="dso_infra_erp",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_infra_functions", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "erp"},
    description="Create ERP tables on DSO (complete copy from staging)",
)
def dso_infra_erp(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create all ERP infrastructure on DSO (maps, dims, facts)."""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/erp", context)
    context.log.info("✅ ERP infrastructure created on DSO")

    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "map.Kontoplan, map.Kreditor, map.Leverandoervare, map.Aftale, map.AftaleVarePris, map.Faktura, map.Fakturalinje, map.Postering, map.Lager, map.Vareudlevering, map.Rekvirent, map.ean, map.LagerbeholdningEgenskaber, map.AfdelingLagerbeholdningEgenskaber, map.Okosted, dim.Kontoplan, dim.Kreditor, dim.Leverandoervare, dim.Aftale, dim.Faktura, dim.Fakturalinje, dim.Postering, dim.Lager, dim.Vareudlevering, dim.Rekvirent, dim.ean, dim.LagerbeholdningEgenskaber, dim.AfdelingLagerbeholdningEgenskaber, dim.Okosted, fct.Faktura, fct.Fakturalinje, fct.Postering, fct.Vareudlevering, fct.Lagerbeholdning, fct.AfdelingLagerbeholdning, fct.AftaleVarePris",
        }
    )


# ============================================================================
# HAIBA (kun dm-laget — map/arc/ext/meta er staging-interne)
# ============================================================================


@dg.asset(
    name="dso_infra_haiba",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "haiba"},
    description=(
        "Create the HAIBA dm tables on DSO (facts + egenskaber). Deliberately "
        "only tables/haiba/02_dm.sql: map.HAIBA_* mints keys on staging and its "
        "case clean_codes carry CPR, and arc/ext/meta are pipeline internals — "
        "none of them may exist on DSO."
    ),
)
def dso_infra_haiba(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create the eight dm.HAIBA_* tables on DSO."""

    execute_sql_file(mssql_target_dso, "tables/haiba/02_dm.sql", context)

    context.log.info("✅ HAIBA dm tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": ["tables/haiba/02_dm.sql"],
            "tables": (
                "dm.HAIBA_AggInfektion(+_Egenskaber), "
                "dm.HAIBA_AggOrtopaedi(+_Egenskaber), "
                "dm.HAIBA_CasesInfektion(+_Egenskaber), "
                "dm.HAIBA_CasesSSI(+_Egenskaber)"
            ),
        }
    )


@dg.asset(
    name="dso_infra_views_haiba",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_haiba"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "haiba", "object": "view"},
    description=(
        "Create the HAIBA consumer views on DSO (vw_dm.HAIBA_*). The dm "
        "tables publish the retraction audit trail, and SDS soft-deletes "
        "roughly a fifth of the archive every January — the views filter "
        "dw_is_deleted = 0 so the safe read is the default one."
    ),
)
def dso_infra_views_haiba(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create vw_dm.HAIBA_* views on DSO."""

    results = execute_sql_files_from_directory(mssql_target_dso, "views/haiba", context)

    context.log.info("✅ HAIBA views created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "views": (
                "vw_dm.HAIBA_AggInfektion(+_Egenskaber), "
                "vw_dm.HAIBA_AggOrtopaedi(+_Egenskaber), "
                "vw_dm.HAIBA_CasesInfektion(+_Egenskaber), "
                "vw_dm.HAIBA_CasesSSI(+_Egenskaber)"
            ),
        }
    )


# ============================================================================
# DRG (kun dim/fct/bridge — map/arc/meta/seeds er staging-interne)
# ============================================================================


@dg.asset(
    name="dso_infra_drg",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "drg"},
    description=(
        "Create the DRG dim/fct/bridge tables on DSO. Deliberately only "
        "03_dims/04_facts/06_bridges: map.DRG* mints IDENTITY keys on staging "
        "and 05_seeds also seeds map (absent on DSO), while arc/meta are "
        "pipeline internals — none of them may exist on DSO. The -1 sentinels "
        "arrive with the sync (dims) or inline in 06_bridges (bridges)."
    ),
)
def dso_infra_drg(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create the DRG dim/fct/bridge tables on DSO."""

    files = ["tables/drg/03_dims.sql", "tables/drg/04_facts.sql", "tables/drg/06_bridges.sql"]
    for f in files:
        execute_sql_file(mssql_target_dso, f, context)

    context.log.info("✅ DRG dim/fct/bridge tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": files,
            "tables": (
                "dim.DRGKontaktEgenskaber, dim.DRGKontaktKlassifikation, "
                "dim.DRGDiagnose, dim.DRGProcedure, fct.DRGKontakt, "
                "bridge.DRGDiagnoseGruppe, bridge.DRGProcedureGruppe"
            ),
        }
    )


@dg.asset(
    name="dso_infra_views_drg",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_drg"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "drg", "object": "view"},
    description="Create the CPR-free DRG presentation views on DSO (vw_dim.*, vw_bridge.*)",
)
def dso_infra_views_drg(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create the CPR-free DRG presentation views on DSO.

    Only the pass-through views over the CPR-free code dims and group bridges
    live here (views/drg). The DPA-secured views for the person-referable
    fct.DRGKontakt and the demografi-carrying dim.DRGKontaktEgenskaber are
    owned by the separate security-views repo, so this asset references only
    dso_infra_drg (the dim/fct/bridge tables) — no security infra."""

    results = execute_sql_files_from_directory(mssql_target_dso, "views/drg", context)

    context.log.info("✅ DRG presentation views created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "views": (
                "vw_dim.DRGKontaktKlassifikation, vw_dim.DRGDiagnose, vw_dim.DRGProcedure, "
                "vw_bridge.DRGDiagnoseGruppe, vw_bridge.DRGProcedureGruppe "
                "(DPA-secured vw_fct.DRGKontakt / vw_dim.DRGKontaktEgenskaber "
                "are owned by the separate security-views repo)"
            ),
        }
    )


# ============================================================================
# SECURITY (Complete copy from staging)
# ============================================================================


@dg.asset(
    name="dso_infra_security",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_infra_organisation", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "security"},
    description="Create security tables and functions on DSO",
)
def dso_infra_security(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create security infrastructure on DSO (tables, functions)"""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/security", context)

    context.log.info("✅ Security infrastructure created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "sec.RolleDomaene, sec.RBOrganisation",
            "functions": (
                "sec.HentBrugerRolleDomaene, sec.BeskytDimLeverandoervare_v1, sec.HentDPASikkerhed, "
                "sec.HentRBOrganisation, sec.BeskytPersonoplysning, sec.BeskytDimBorger"
            ),
        }
    )


# ============================================================================
# VIEWS - ERP (DSO only)
# ============================================================================


@dg.asset(
    name="dso_infra_views_erp",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=[
        "dso_infra_erp",
        "dso_infra_security",
        "dso_infra_ref",
    ],
    tags={"layer": "dso", "type": "infrastructure", "domain": "erp", "object": "view"},
    description="Create ERP secure views on DSO (vw_fct.*, vw_dim.*)",
)
def dso_infra_views_erp(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create ERP secure views on DSO"""

    results = execute_sql_files_from_directory(mssql_target_dso, "views/erp", context)

    context.log.info("✅ ERP views created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "views": (
                "vw_fct.Fakturalinje, vw_fct.Postering, vw_fct.Vareudlevering, "
                "vw_fct.Vareudlevering_ReeltForbrug, vw_dim.Kontoplan, vw_dim.Kreditor "
                "(vw_dim.Leverandoervare / vw_dim.Rekvirent live in views/security)"
            ),
        }
    )


# ============================================================================
# VIEWS - Security (DSO only)
# ============================================================================


@dg.asset(
    name="dso_infra_views_security",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=[
        "dso_infra_security",
        "dso_infra_shared_dimensions",
        "dso_infra_organisation",
    ],
    tags={"layer": "dso", "type": "infrastructure", "domain": "security", "object": "view"},
    description="Create security views on DSO (vw_dim.Borger_*, etc.)",
)
def dso_infra_views_security(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create security dimension views on DSO"""

    results = execute_sql_files_from_directory(mssql_target_dso, "views/security", context)

    context.log.info("✅ Security views created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "views": "sec.* security views (vw_dim.Borger, vw_dim.Leverandoervare, vw_dim.Fakturalinje, vw_dim.Organisation, vw_dim.Okosted)",
        }
    )


# ============================================================================
# VIEWS - CodeSystem (DSO only)
# ============================================================================


@dg.asset(
    name="dso_infra_views_codesystem",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_codesystem"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "codesystem", "object": "view"},
    description="Create CodeSystem lookup views on DSO (codesystem.vw_sfi_alias_member)",
)
def dso_infra_views_codesystem(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create CodeSystem lookup views on DSO."""

    results = execute_sql_files_from_directory(mssql_target_dso, "views/codesystem", context)

    context.log.info("CodeSystem views created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "views": "codesystem.vw_sfi_alias_member",
        }
    )


# ============================================================================
# VIEWS - FHIR (DSO only)
# ============================================================================


@dg.asset(
    name="dso_infra_views_fhir",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_fhir", "dso_infra_codesystem", "dso_infra_etl_routing"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "fhir", "object": "view"},
    description="Create FHIR pass-through views on DSO (vw_fhir.*)",
)
def dso_infra_views_fhir(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create FHIR pass-through views on DSO."""

    results = execute_sql_files_from_directory(mssql_target_dso, "views/fhir", context)

    context.log.info("✅ FHIR views created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "views": (
                "vw_fhir.Patient, vw_fhir.Encounter, vw_fhir.Encounter_Location, "
                "vw_fhir.Observation, vw_fhir.EpisodeOfCare, vw_fhir.Location, "
                "vw_fhir.Organization, vw_fhir.CarePlan, vw_fhir.CarePlan_Activity, "
                "vw_fhir.Questionnaire, vw_fhir.Questionnaire_item, "
                "vw_fhir.QuestionnaireResponse, vw_fhir.QuestionnaireResponse_item, "
                "fhir.SFI_datapoint_build, fhir.SFI_datapoint (occurred_at_dt), "
                "vw_fhir.SFI_datapoint, vw_fhir.Appointment"
            ),
        }
    )


# ============================================================================
# CODESYSTEM / ROUTING (Complete copy from staging)
# ============================================================================


@dg.asset(
    name="dso_infra_codesystem",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "codesystem"},
    description="Create codesystem tables on DSO for SFI and related consumer joins.",
)
def dso_infra_codesystem(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create codesystem schema tables on DSO."""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/codesystem", context)
    context.log.info("Codesystem tables created on DSO")
    return dg.MaterializeResult(metadata={"files_executed": list(results.keys())})


@dg.asset(
    name="dso_infra_etl_routing",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "routing"},
    description="Create IFDB SFI routing tables on DSO for transparent SFI_datapoint views.",
)
def dso_infra_etl_routing(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create IFDB routing metadata tables on DSO."""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/etl", context)
    context.log.info("IFDB routing metadata tables created on DSO")
    return dg.MaterializeResult(metadata={"files_executed": list(results.keys())})


# ============================================================================
# FHIR (SQL on FHIR flat tables - Complete copy from staging)
# ============================================================================


@dg.asset(
    name="dso_infra_fhir",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "fhir"},
    description="Create FHIR flat tables on DSO database (fhir.Patient, fhir.Encounter, etc.)",
)
def dso_infra_fhir(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create FHIR flat tables on DSO (complete copy from staging)."""

    results = execute_sql_files_from_directory(
        mssql_target_dso, "tables/fhir", context, autocommit=True
    )

    context.log.info("FHIR flat tables applied on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": (
                "fhir.Patient, fhir.Patient_Address, fhir.Encounter, fhir.Encounter_Location, fhir.Observation, "
                "fhir.Questionnaire, fhir.Questionnaire_item, fhir.QuestionnaireResponse, "
                "fhir.QuestionnaireResponse_item, fhir.EpisodeOfCare, fhir.Location, "
                "fhir.Organization, fhir.CarePlan, fhir.CarePlan_Activity, fhir.Appointment, "
                "fhir.SFI_datapoint_store"
            ),
        }
    )


# ============================================================================
# CONCEPTMAP (DSO physical tables in conceptmap.*; staging uses conceptmap.vw_* views)
# ============================================================================


@dg.asset(
    name="dso_infra_conceptmap",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_migrations"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "conceptmap"},
    description="Create conceptmap.* tables on DSO for ConceptMap DW_EK sync",
)
def dso_infra_conceptmap(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create conceptmap.* physical tables on DSO."""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/conceptmap/dso", context)
    context.log.info("ConceptMap tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": (
                "conceptmap.rekvirent_to_sor, conceptmap.lager_to_sor, "
                "conceptmap.ean_to_sor, conceptmap.okosted_to_sor"
            ),
        }
    )


# ============================================================================
# MIGRATIONS (Centralized, date-prefixed schema migrations and cleanups)
# ============================================================================


@dg.asset(
    name="dso_migrations",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "migrations"},
    description="Run all centralized migrations on DSO (schema changes, data fixes, legacy cleanup)",
)
def dso_migrations(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Run pending migrations/*.sql scripts on DSO in filename-sorted order.

    Each file is applied at most once per database, tracked in
    [meta].[MigrationHistory]. Idempotency in the SQL is still required:
    defense in depth on pre-tracker servers, and the tracker row is written
    after the file's SQL commits, so a partial failure re-runs the whole
    file. Removing a migration file does not remove its tracker row, and
    that is harmless.
    """

    results = apply_migrations(mssql_target_dso, "migrations", context)

    applied = [name for name, status in results.items() if status == "applied"]
    skipped = [name for name, status in results.items() if status == "skipped"]
    baselined = [name for name, status in results.items() if status == "baselined"]
    context.log.info(
        f"✅ migrations on DSO: {len(applied)} applied, {len(skipped)} skipped, "
        f"{len(baselined)} baselined"
    )
    return dg.MaterializeResult(
        metadata={
            "applied_count": len(applied),
            "skipped_count": len(skipped),
            "baselined_count": len(baselined),
            "applied": applied,
            "skipped": skipped,
        }
    )
