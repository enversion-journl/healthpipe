# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Infrastructure Assets (DSI/DSA Layer)

Creates all database objects on the staging database:
- Schemas (ext, stg, map, dim, fct, bridge, sec, meta, vw_dim, vw_fct, fhir, vw_fhir)
- Functions (dbo.fn_DateTime_To_TimeKey, dbo.ConvertSASDateToDateTime, etc.)
- Persistent Tables (map.*, dim.*, fct.*, sec.*)
- Linked-server user (login + database user + schema grants for DSO sync)

NOTE: Does NOT create stg.* tables - these are created/dropped by daily ETL.
"""

import dagster as dg
from core.resources import MSSQLResource, get_linked_server_config
from infrastructure.utils.ddl_loader import (
    apply_migrations,
    execute_legacy_sql2019_functions_if_needed,
    execute_sql_file,
    execute_sql_files_from_directory,
)

# Schemas the linked-server user needs SELECT access to on staging.
_STAGING_GRANT_SCHEMAS = ["map", "dim", "fct", "meta", "bridge", "fhir", "sec", "ref", "conceptmap"]


# ============================================================================
# SCHEMAS
# ============================================================================


@dg.asset(
    name="staging_infra_schemas",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    tags={"layer": "staging", "type": "infrastructure", "object": "schema"},
    description="Create all required schemas on staging database",
)
def staging_infra_schemas(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create schemas: ext, stg, map, dim, fct, bridge, sec, meta, vw_dim, vw_fct, fhir, vw_fhir"""

    execute_sql_file(mssql_target_staging, "common/00_schemas.sql", context)

    context.log.info("✅ All schemas created")
    return dg.MaterializeResult(
        metadata={
            "schemas": "ext, stg, map, dim, fct, bridge, sec, meta, vw_dim, vw_fct, fhir, vw_fhir",
            "database": "staging (DSI/DSA)",
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
    name="staging_infra_db_settings",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    tags={"layer": "staging", "type": "infrastructure", "object": "database"},
    description=(
        "Enforce database-level settings on staging (AUTO_UPDATE_STATISTICS_ASYNC ON, "
        "AUTO_CLOSE OFF). Re-applied on every infrastructure run so the settings "
        "survive restores and fresh databases — uniformly for Nomad-hosted and "
        "client-managed servers."
    ),
)
def staging_infra_db_settings(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    execute_sql_file(mssql_target_staging, "common/01_db_settings.sql", context, autocommit=True)

    with mssql_target_staging.get_connection() as conn:
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
# FUNCTIONS
# ============================================================================


@dg.asset(
    name="staging_infra_functions",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_infra_shared_dimensions"],
    tags={"layer": "staging", "type": "infrastructure", "object": "function"},
    description="Create all helper functions on staging database",
)
def staging_infra_functions(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create functions: fn_DateTime_To_TimeKey, GetBorgerSK_TVF, ConvertSASDateToDateTime"""

    results = execute_sql_files_from_directory(mssql_target_staging, "common/functions", context)
    legacy_results = execute_legacy_sql2019_functions_if_needed(mssql_target_staging, context)
    results.update(legacy_results)

    context.log.info(f"✅ {len(results)} functions created")
    return dg.MaterializeResult(
        metadata={
            "functions_created": list(results.keys()),
            "count": len(results),
            "legacy_sql2019_functions": list(legacy_results.keys()),
        }
    )


# ============================================================================
# SHARED DIMENSIONS
# ============================================================================


@dg.asset(
    name="staging_infra_shared_dimensions",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations", "staging_infra_ref"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "shared_dimensions"},
    description="Create shared dimension tables (dim.Borger, dim.Dato)",
)
def staging_infra_shared_dimensions(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create shared dimension tables (dim.Borger, dim.Dato).

    Depends on staging_migrations so the 20260515 drop-and-rebuild migration
    runs before the new dim.Borger DDL is applied, and on staging_infra_ref
    so ref.PatientToBorger / ref.Patient_Address_To_Borger exist first.
    """

    results = execute_sql_files_from_directory(
        mssql_target_staging, "tables/shared_dimensions", context
    )

    context.log.info("✅ Shared dimensions tables created")
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
    name="staging_infra_ref",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_infra_codesystem", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "ref"},
    description="Create ref schema objects, including SKS codesystem views for DSO sync.",
)
def staging_infra_ref(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create ref-schema tables on staging."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/ref", context)

    context.log.info("✅ Ref tables created")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": (
                "ref.PatientToBorger, ref.Patient_Address_To_Borger, "
                "ref.Organisation, ref.FakturalinjeMatchKeys, "
                "ref.RekvirentUndtagelse, ref.SKS_* views"
            ),
        }
    )


# ============================================================================
# ORGANIZATION
# ============================================================================


@dg.asset(
    name="staging_infra_organisation",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "organisation"},
    description="Create organisation tables (map.Organisation, dim.Organisation)",
)
def staging_infra_organisation(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create organisation map and dim tables."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/organisation", context)

    context.log.info("✅ Organisation tables created")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "map.Organisation, dim.Organisation",
        }
    )


# ============================================================================
# ERP
# ============================================================================


@dg.asset(
    name="staging_infra_erp",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_infra_functions", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "erp"},
    description="Create ERP tables (maps, dims, facts)",
)
def staging_infra_erp(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create all ERP infrastructure (archive, maps, dims, facts)."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/erp", context)
    context.log.info("✅ ERP infrastructure created")

    all_files = list(results.keys())
    return dg.MaterializeResult(
        metadata={
            "files_executed": all_files,
            "tables": "map.Kontoplan, map.Kreditor, map.Leverandoervare, map.Aftale, map.AftaleVarePris, map.Faktura, map.Fakturalinje, map.Postering, map.Lager, map.Vareudlevering, map.Rekvirent, map.ean, map.LagerbeholdningEgenskaber, map.AfdelingLagerbeholdningEgenskaber, map.Okosted, dim.Kontoplan, dim.Kreditor, dim.Leverandoervare, dim.Aftale, dim.Faktura, dim.Fakturalinje, dim.Postering, dim.Lager, dim.Vareudlevering, dim.Rekvirent, dim.ean, dim.LagerbeholdningEgenskaber, dim.AfdelingLagerbeholdningEgenskaber, dim.Okosted, fct.Faktura, fct.Fakturalinje, fct.Postering, fct.Vareudlevering, fct.Lagerbeholdning, fct.AfdelingLagerbeholdning, fct.AftaleVarePris",
        }
    )


# ============================================================================
# KMD UDDATA
# ============================================================================


@dg.asset(
    name="staging_infra_kmd_uddata",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_infra_functions", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "kmd_uddata"},
    description="Create KMD UdData archive tables (UNSPSC, SpendAnalysisLines, Invoice)",
)
def staging_infra_kmd_uddata(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create KMD UdData archive infrastructure (arc.kmd_uddata_* tables)."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/kmd_uddata", context)
    context.log.info("✅ KMD UdData archive infrastructure created")

    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": (
                "arc.kmd_uddata_unspsc, arc.kmd_uddata_spend_analysis_lines, "
                "arc.kmd_uddata_invoice"
            ),
        }
    )


# ============================================================================
# SECURITY
# ============================================================================


@dg.asset(
    name="staging_infra_security",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_infra_organisation", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "security"},
    description="Create security tables and functions",
)
def staging_infra_security(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create security infrastructure (tables, functions)"""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/security", context)

    context.log.info("✅ Security infrastructure created")
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
# FHIR (SQL on FHIR flat tables)
# ============================================================================


@dg.asset(
    name="staging_infra_fhir",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "fhir"},
    description="Create FHIR flat tables on staging database (fhir.Patient, fhir.Encounter, etc.)",
)
def staging_infra_fhir(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create FHIR flat tables from SQL on FHIR ViewDefinition materialization."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/fhir", context)

    context.log.info("FHIR flat tables applied on staging")
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
# MIGRATIONS (Centralized, date-prefixed schema migrations and cleanups)
# ============================================================================


@dg.asset(
    name="staging_migrations",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "migrations"},
    description="Run all centralized migrations on staging (schema changes, data fixes, legacy cleanup)",
)
def staging_migrations(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Run pending migrations/*.sql scripts on staging in filename-sorted order.

    Each file is applied at most once per database, tracked in
    [meta].[MigrationHistory]. Idempotency in the SQL is still required:
    defense in depth on pre-tracker servers, and the tracker row is written
    after the file's SQL commits, so a partial failure re-runs the whole
    file. Removing a migration file does not remove its tracker row, and
    that is harmless.
    """

    results = apply_migrations(mssql_target_staging, "migrations", context)

    applied = [name for name, status in results.items() if status == "applied"]
    skipped = [name for name, status in results.items() if status == "skipped"]
    baselined = [name for name, status in results.items() if status == "baselined"]
    context.log.info(
        f"✅ migrations on staging: {len(applied)} applied, {len(skipped)} skipped, "
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


# ============================================================================
# CODESYSTEM / CONCEPTMAP (FHIR persistent tables)
# ============================================================================


@dg.asset(
    name="staging_infra_codesystem",
    group_name="staging_infrastructure",
    deps=["staging_infra_schemas", "staging_migrations"],
    compute_kind="mssql",
    description="Create codesystem schema tables (concept, properties, hierarchy for each codesystem).",
)
def staging_infra_codesystem(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(mssql_target_staging, "tables/codesystem", context)
    return dg.MaterializeResult(metadata={"files_executed": len(results)})


@dg.asset(
    name="staging_infra_etl_routing",
    group_name="staging_infrastructure",
    deps=["staging_infra_schemas", "staging_migrations"],
    compute_kind="mssql",
    description="Create ETL routing tables (meta.IFDBAktivitetstypeRouting) for IFDB SFI routing.",
)
def staging_infra_etl_routing(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(mssql_target_staging, "tables/etl", context)
    return dg.MaterializeResult(metadata={"files_executed": len(results)})


@dg.asset(
    name="staging_infra_etl_routing_seed",
    group_name="staging_infrastructure",
    deps=["staging_infra_etl_routing"],
    compute_kind="mssql",
    description=(
        "Seed meta.IFDBAktivitetstypeRouting from the repo-controlled YAML "
        "via a deterministic MERGE. This is the operational source of truth for "
        "runtime routing; the FHIR runtime no longer reads the YAML directly."
    ),
)
def staging_infra_etl_routing_seed(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deterministically MERGE the YAML routing seed into the audited table.

    Keyed on the table PK (aktivitetstype_kode, active_from). Existing rows are
    updated in place, new rows inserted. Reporting inserted/updated counts in
    Dagster metadata makes the seed observable without querying the table.
    """
    from DSA.FHIR.utils.ifdb.routing import load_routing_rules_from_yaml

    rules = load_routing_rules_from_yaml()

    merge_sql = """
        MERGE [meta].[IFDBAktivitetstypeRouting] AS tgt
        USING (SELECT ? AS aktivitetstype_kode, ? AS active_from) AS src
            ON tgt.[aktivitetstype_kode] = src.aktivitetstype_kode
           AND tgt.[active_from] = src.active_from
        WHEN MATCHED THEN UPDATE SET
            [target_resource]      = ?,
            [observation_category] = ?,
            [route_status]         = ?,
            [rationale]            = ?,
            [decided_by]           = ?,
            [decided_at]           = ?,
            [active_to]            = ?,
            [dw_timestamp_update]  = SYSDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            [aktivitetstype_kode], [target_resource], [observation_category],
            [route_status], [rationale], [decided_by], [decided_at],
            [active_from], [active_to]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    inserted = 0
    updated = 0
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for rule in rules:
                cursor.execute(
                    "SELECT COUNT(*) FROM [meta].[IFDBAktivitetstypeRouting] "
                    "WHERE [aktivitetstype_kode] = ? AND [active_from] = ?",
                    (rule.aktivitetstype_kode, rule.active_from),
                )
                exists = cursor.fetchone()[0] > 0
                cursor.execute(
                    merge_sql,
                    (
                        rule.aktivitetstype_kode,
                        rule.active_from,
                        rule.target_resource.value,
                        rule.observation_category,
                        rule.route_status.value,
                        rule.rationale,
                        rule.decided_by,
                        rule.decided_at,
                        rule.active_to,
                        rule.aktivitetstype_kode,
                        rule.target_resource.value,
                        rule.observation_category,
                        rule.route_status.value,
                        rule.rationale,
                        rule.decided_by,
                        rule.decided_at,
                        rule.active_from,
                        rule.active_to,
                    ),
                )
                if exists:
                    updated += 1
                else:
                    inserted += 1
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"Seeded routing table: {inserted} inserted, {updated} updated, "
        f"{len(rules)} total rules from YAML seed."
    )
    return dg.MaterializeResult(
        metadata={
            "rules_total": len(rules),
            "rows_inserted": inserted,
            "rows_updated": updated,
        }
    )


@dg.asset(
    name="staging_infra_conceptmap",
    group_name="staging_infrastructure",
    deps=["staging_infra_schemas", "staging_migrations"],
    compute_kind="mssql",
    description="Create conceptmap schema tables (mapping tables for each conceptmap).",
)
def staging_infra_conceptmap(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(mssql_target_staging, "tables/conceptmap", context)
    return dg.MaterializeResult(metadata={"files_executed": len(results)})


@dg.asset(
    name="staging_infra_views_conceptmap",
    group_name="staging_infrastructure",
    deps=["staging_infra_conceptmap"],
    compute_kind="mssql",
    tags={"layer": "staging", "type": "infrastructure", "domain": "conceptmap", "object": "view"},
    description="Create conceptmap dedup views (conceptmap.vw_*_to_sor) on staging — sync source for DSO.",
)
def staging_infra_views_conceptmap(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(mssql_target_staging, "views/conceptmap", context)
    return dg.MaterializeResult(metadata={"files_executed": len(results)})


# ============================================================================
# LINKED SERVER USER
# ============================================================================


@dg.asset(
    name="staging_infra_linked_server_user",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas"],
    tags={"layer": "staging", "type": "infrastructure", "object": "linked_server_user"},
    description="Create login and read-only user on staging for the DSO linked server",
)
def staging_infra_linked_server_user(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """
    Create the SQL Server login and database user that DSO's linked server
    will authenticate with when reading from staging.

    The user is granted SELECT on the schemas required for DSO sync
    (map, dim, fct, meta, bridge, fhir, sec, ref) — least-privilege access.

    Configuration is read from environment variables:
    - MSSQL_LINKED_SERVER_DB: Staging database name (default: DW)
    - MSSQL_LINKED_SERVER_USER: Login/user name (default: sa)
    - MSSQL_LINKED_SERVER_PASSWORD: Password for the login

    Falls back to MSSQL_SOURCE_STAGING_* when linked-server-specific vars are unset.
    """
    linked_server_config = get_linked_server_config()
    staging_db = linked_server_config["database"]
    staging_user = linked_server_config["user"]
    staging_password = linked_server_config["password"]

    is_builtin = staging_user.lower() in ("sa", "dbo")
    if is_builtin:
        context.log.info(
            f"Linked server user is built-in principal '{staging_user}' "
            "(typical in dev). Built-in principals already have full access — "
            "skipping login/user creation and schema grants."
        )
        return dg.MaterializeResult(
            metadata={
                "login": staging_user,
                "database": staging_db,
                "is_builtin_principal": True,
                "login_existed": True,
                "user_existed": True,
                "schemas_granted": list(_STAGING_GRANT_SCHEMAS),
            }
        )

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # -- 1) Create server-level login if it does not exist --------
            cursor.execute(
                "SELECT COUNT(*) FROM sys.server_principals WHERE name = ?",
                (staging_user,),
            )
            login_exists = cursor.fetchone()[0] > 0

            if not login_exists:
                cursor.execute(
                    f"""
                    CREATE LOGIN [{staging_user}]
                        WITH PASSWORD = N'{staging_password}',
                             DEFAULT_DATABASE = [{staging_db}],
                             CHECK_EXPIRATION = OFF,
                             CHECK_POLICY = ON
                    """
                )
                conn.commit()
                context.log.info(f"Created server login '{staging_user}'")
            else:
                context.log.info(f"Server login '{staging_user}' already exists")

            # -- 2) Create database user mapped to the login --------------
            cursor.execute(
                "SELECT COUNT(*) FROM sys.database_principals WHERE name = ?",
                (staging_user,),
            )
            user_exists = cursor.fetchone()[0] > 0

            if not user_exists:
                cursor.execute(f"CREATE USER [{staging_user}] FOR LOGIN [{staging_user}]")
                conn.commit()
                context.log.info(f"Created database user '{staging_user}'")
            else:
                context.log.info(f"Database user '{staging_user}' already exists")

            # -- 3) Grant SELECT on required schemas --------------------------
            granted_schemas = []
            for schema in _STAGING_GRANT_SCHEMAS:
                cursor.execute(
                    "SELECT COUNT(*) FROM sys.schemas WHERE name = ?",
                    (schema,),
                )
                if cursor.fetchone()[0] > 0:
                    cursor.execute(f"GRANT SELECT ON SCHEMA::[{schema}] TO [{staging_user}]")
                    granted_schemas.append(schema)
                else:
                    context.log.warning(f"Schema '{schema}' does not exist yet, skipping grant")
            conn.commit()
            context.log.info(f"Granted SELECT on schemas: {', '.join(granted_schemas)}")

        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "login": staging_user,
            "database": staging_db,
            "is_builtin_principal": False,
            "login_existed": login_exists,
            "user_existed": user_exists,
            "schemas_granted": granted_schemas,
        }
    )


# ============================================================================
# DFM (Data Flow Management)
# ============================================================================


@dg.asset(
    name="staging_infra_dfm",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure"},
    description="Create DFM state, run log, and reset procedure in meta schema",
)
def staging_infra_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deploy meta.DFMState, meta.DFMRunLog, and meta.usp_dfm_reset."""

    execute_sql_file(mssql_target_staging, "tables/meta/02_dfm_state.sql", context)
    execute_sql_file(mssql_target_staging, "tables/meta/03_dfm_run_log.sql", context)
    execute_sql_file(mssql_target_staging, "tables/meta/04_usp_dfm_reset.sql", context)

    context.log.info("DFM tables and stored procedure deployed")
    return dg.MaterializeResult(
        metadata={
            "tables": "meta.DFMState, meta.DFMRunLog",
            "procedures": "meta.usp_dfm_reset",
        }
    )


# ============================================================================
# DB SPACE (snapshot tables for the daily space monitor)
# ============================================================================


@dg.asset(
    name="staging_infra_db_space",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "object": "table"},
    description="Create meta.DbSpaceSnapshot + meta.DbSpaceTopObject for the daily space monitor",
)
def staging_infra_db_space(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create meta.DbSpaceSnapshot + meta.DbSpaceTopObject on staging."""

    execute_sql_file(mssql_target_staging, "tables/meta/05_db_space_snapshot.sql", context)

    context.log.info("Space-snapshot tables deployed on staging")
    return dg.MaterializeResult(
        metadata={
            "tables": "meta.DbSpaceSnapshot, meta.DbSpaceTopObject",
        }
    )


# ============================================================================
# IFDB ARCHIVE
# ============================================================================


@dg.asset(
    name="staging_infra_ifdb_archive",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure"},
    description="Create archive tables for IFDB Patient, EpisodeOfCare, Encounter, and Location",
)
def staging_infra_ifdb_archive(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deploy arc.ifdb_patient, arc.ifdb_episode_of_care, arc.ifdb_encounter, and arc.ifdb_location tables."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/ifdb_archive", context)

    context.log.info("IFDB archive tables deployed")
    return dg.MaterializeResult(
        metadata={
            "files_executed": len(results),
            "tables": "arc.ifdb_patient, arc.ifdb_episode_of_care, arc.ifdb_encounter, arc.ifdb_location",
        }
    )


# ============================================================================
# EPJI ARCHIVE
# ============================================================================


@dg.asset(
    name="staging_infra_epji_archive",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure"},
    description="Create the EPJI location archive table (arc.epji_location)",
)
def staging_infra_epji_archive(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deploy arc.epji_location."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/epji_archive", context)

    context.log.info("EPJI archive table deployed")
    return dg.MaterializeResult(
        metadata={
            "files_executed": len(results),
            "tables": "arc.epji_location",
        }
    )


# ============================================================================
# DQA (Data Quality Assurance results store)
# ============================================================================


@dg.asset(
    name="staging_infra_dqa",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    tags={"layer": "staging", "type": "infrastructure", "domain": "dqa"},
    description="Create the DQA results store (dqa schema, dqa.Results table + dqa.v_latest view)",
)
def staging_infra_dqa(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deploy the dqa schema: the dqa.Results + dqa.RemediationQueue tables (from
    ddl/tables/dqa) and their convenience/feeder views (from ddl/views/dqa) --
    tables first, then views, since the views depend on the tables.

    Persistence (dqa/result.py) and remediation capture (dqa/remediation) only
    INSERT; this asset owns the schema + the remediation queue, matching the rest
    of the platform's infra pattern.
    """

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/dqa", context)
    results.update(execute_sql_files_from_directory(mssql_target_staging, "views/dqa", context))

    context.log.info("DQA results store deployed")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "objects": "dqa.Results + dqa.RemediationQueue (tables); dqa.v_latest + dqa.v_replay_* (views)",
        }
    )


# ============================================================================
# HAIBA (weekly SDS deliveries — persistent archive)
# ============================================================================


@dg.asset(
    name="staging_infra_haiba",
    group_name="staging_infrastructure",
    deps=["staging_infra_schemas", "staging_migrations"],
    compute_kind="mssql",
    description=(
        "Create the HAIBA tables: arc.haiba_* (archive incl. the "
        "arc.haiba_agg_revision log), map.HAIBA_* (key minting), the dm.HAIBA_* "
        "facts with their *_Egenskaber transaction dimensions, and "
        "meta.HAIBALeverance. The ext.haiba_* landing tables are created by "
        "the extract assets themselves, as for SOR."
    ),
)
def staging_infra_haiba(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(mssql_target_staging, "tables/haiba", context)
    return dg.MaterializeResult(metadata={"files_executed": len(results)})


@dg.asset(
    name="staging_infra_drg",
    group_name="staging_infrastructure",
    deps=["staging_infra_schemas", "staging_migrations"],
    compute_kind="mssql",
    description=(
        "Create the DRG-grupperet LPR archive tables (arc.drg_* / arc.drgpsyk_*, "
        "the arc.drg_pris_revision log and meta.DRGLeverance). The ext.drg* "
        "landing tables are created by the extract assets themselves, as for "
        "HAIBA and SOR."
    ),
)
def staging_infra_drg(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(mssql_target_staging, "tables/drg", context)
    return dg.MaterializeResult(metadata={"files_executed": len(results)})


@dg.asset(
    name="staging_infra_udredningsret",
    group_name="staging_infrastructure",
    deps=["staging_infra_schemas", "staging_migrations"],
    compute_kind="mssql",
    description=(
        "Create the udredningsret archive table (arc.udredningsret_mont) and "
        "meta.UdredningsretLeverance. The ext.udredningsret* landing tables and "
        "the ext.udredningsret_mont_noegler key table are created by the extract/archive "
        "assets themselves, as for DRG and HAIBA."
    ),
)
def staging_infra_udredningsret(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(
        mssql_target_staging, "tables/udredningsret", context
    )
    return dg.MaterializeResult(metadata={"files_executed": len(results)})
