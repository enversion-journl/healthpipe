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
    execute_legacy_sql2019_functions_if_needed,
    execute_migrations,
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
# META (Operational metadata - sync watermarks, etc.)
# ============================================================================


@dg.asset(
    name="dso_infra_meta",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas"],
    tags={"layer": "dso", "type": "infrastructure", "object": "table"},
    description="Create metadata tables on DSO (sync watermarks for delta sync)",
)
def dso_infra_meta(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create meta schema tables: meta.sync_watermarks"""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/meta", context)

    context.log.info("✅ Meta tables created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "meta.sync_watermarks",
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
    """Create functions: fn_DateTime_To_DateKey, fn_DateTime_To_TimeKey, GetBorgerSK_TVF, ConvertSASDateToDateTime"""

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
    so the ref.Patient_Borger / ref.Patient_Address_Borger tables exist
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
    description="Create ref schema tables on DSO (ref.Patient_Borger, ref.Patient_Address_Borger, ref.Organisation)",
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
            "tables": "ref.Patient_Borger, ref.Patient_Address_Borger, ref.Organisation",
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
            "tables": "map.Kontoplan, map.Kreditor, map.Vare, map.Aftale, map.Faktura, map.Fakturalinje, map.Postering, map.Lager, map.Vareudlevering, map.Rekvirent, map.EAN, map.LagerbeholdningEgenskaber, map.AfdelingLagerbeholdningEgenskaber, map.Okosted, dim.Kontoplan, dim.Kreditor, dim.Vare, dim.Aftale, dim.Faktura, dim.Fakturalinje, dim.Postering, dim.Lager, dim.VareudleveringEgenskaber, dim.Rekvirent, dim.EAN, dim.LagerbeholdningEgenskaber, dim.AfdelingLagerbeholdningEgenskaber, dim.Okosted, fct.Faktura, fct.Fakturalinje, fct.Postering, fct.Vareudlevering, fct.Lagerbeholdning, fct.AfdelingLagerbeholdning",
        }
    )


# ============================================================================
# SECURITY (Complete copy from staging)
# ============================================================================


@dg.asset(
    name="dso_infra_security",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "dso_infra_organisation"],
    tags={"layer": "dso", "type": "infrastructure", "domain": "security"},
    description="Create security tables, functions, and procedures on DSO",
)
def dso_infra_security(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create security infrastructure on DSO (tables, functions, procedures)"""

    results = execute_sql_files_from_directory(mssql_target_dso, "tables/security", context)

    context.log.info("✅ Security infrastructure created on DSO")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "sec.BrugerRoller, sec.PersonaleOrg, sec.BrugerOrganisation",
            "functions": "sec.HentBrugerRolle_v2, sec.HentDPASikkerhed_v1, sec.BeskytDimBorger_v1, sec.HentRBSikkerhed_v2, sec.HentFritekstAdgang_v1, sec.BeskytDimVare_v1",
            "procedures": "sec.PopulateBrugerOrganisation",
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
            "views": "vw_fct.Fakturalinje, vw_fct.Postering, vw_dim.Kontoplan, vw_dim.Kreditor (vw_dim.Vare lives in views/security)",
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
            "views": "sec.* security views (vw_dim.Borger, vw_dim.Vare, vw_dim.Fakturalinje, vw_dim.Organisation, vw_dim.Okosted)",
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
    deps=["dso_infra_schemas"],
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

    migration_results = execute_sql_files_from_directory(
        mssql_target_dso, "tables/fhir/migrations", context, autocommit=True
    )
    results.update({f"migrations/{k}": v for k, v in migration_results.items()})

    context.log.info("FHIR flat tables and migrations applied on DSO")
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

    Each file is applied at most once per server, tracked in
    [meta].[migration_history]. Idempotency in the SQL is recommended (defense
    in depth, useful on first rollout to a pre-tracker server) but no longer
    required. Removing a migration file does not remove its tracker row, and
    that is harmless.
    """

    results = execute_migrations(mssql_target_dso, "migrations", context)

    applied = [name for name, status in results.items() if status == "applied"]
    skipped = [name for name, status in results.items() if status == "skipped"]
    context.log.info(f"✅ migrations on DSO: {len(applied)} applied, {len(skipped)} skipped")
    return dg.MaterializeResult(
        metadata={
            "applied_count": len(applied),
            "skipped_count": len(skipped),
            "applied": applied,
            "skipped": skipped,
        }
    )
