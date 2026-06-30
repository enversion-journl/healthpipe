# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Infrastructure Assets (DSI/DSA Layer)

Creates all database objects on the staging database:
- Schemas (ext, stg, map, dim, fct, bridge, sec, meta, vw_dim, vw_fct, fhir, vw_fhir)
- Functions (dbo.fn_DateTime_To_DateKey, dbo.fn_DateTime_To_TimeKey, etc.)
- Persistent Tables (map.*, dim.*, fct.*, sec.*)
- Linked-server user (login + database user + schema grants for DSO sync)

NOTE: Does NOT create stg.* tables - these are created/dropped by daily ETL.
"""

import dagster as dg
from core.resources import MSSQLResource, get_linked_server_config
from infrastructure.utils.ddl_loader import (
    execute_legacy_sql2019_functions_if_needed,
    execute_migrations,
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
    """Create functions: fn_DateTime_To_DateKey, fn_DateTime_To_TimeKey, GetBorgerSK_TVF, ConvertSASDateToDateTime"""

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
    so ref.Patient_Borger / ref.Patient_Address_Borger exist first.
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
            "tables": "ref.Patient_Borger, ref.Patient_Address_Borger, ref.Organisation, ref.SKS_* views",
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
            "tables": "map.Kontoplan, map.Kreditor, map.Vare, map.Aftale, map.Faktura, map.Fakturalinje, map.Postering, map.Lager, map.Vareudlevering, map.Rekvirent, map.EAN, map.LagerbeholdningEgenskaber, map.AfdelingLagerbeholdningEgenskaber, map.Okosted, dim.Kontoplan, dim.Kreditor, dim.Vare, dim.Aftale, dim.Faktura, dim.Fakturalinje, dim.Postering, dim.Lager, dim.VareudleveringEgenskaber, dim.Rekvirent, dim.EAN, dim.LagerbeholdningEgenskaber, dim.AfdelingLagerbeholdningEgenskaber, dim.Okosted, fct.Faktura, fct.Fakturalinje, fct.Postering, fct.Vareudlevering, fct.Lagerbeholdning, fct.AfdelingLagerbeholdning",
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
    description="Create KMD UdData archive tables (UNSPSC + SpendAnalysisLines)",
)
def staging_infra_kmd_uddata(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create KMD UdData archive infrastructure (arc.unspsc, arc.spend_analysis_lines)."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/kmd_uddata", context)
    context.log.info("✅ KMD UdData archive infrastructure created")

    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "arc.unspsc_kmd_uddata, arc.spend_analysis_lines_kmd_uddata",
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
    description="Create security tables, functions, and procedures",
)
def staging_infra_security(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create security infrastructure (tables, functions, procedures)"""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/security", context)

    context.log.info("✅ Security infrastructure created")
    return dg.MaterializeResult(
        metadata={
            "files_executed": list(results.keys()),
            "tables": "sec.BrugerRoller, sec.PersonaleOrg, sec.BrugerOrganisation",
            "functions": "sec.HentBrugerRolle_v2, sec.HentDPASikkerhed_v1, sec.BeskytDimBorger_v1, sec.HentRBSikkerhed_v2",
            "procedures": "sec.PopulateBrugerOrganisation",
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

    migration_results = execute_sql_files_from_directory(
        mssql_target_staging, "tables/fhir/migrations", context
    )
    results.update({f"migrations/{k}": v for k, v in migration_results.items()})

    context.log.info("FHIR flat tables and migrations applied on staging")
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

    Each file is applied at most once per server, tracked in
    [meta].[migration_history]. Idempotency in the SQL is recommended (defense
    in depth, useful on first rollout to a pre-tracker server) but no longer
    required. Removing a migration file does not remove its tracker row, and
    that is harmless.
    """

    results = execute_migrations(mssql_target_staging, "migrations", context)

    applied = [name for name, status in results.items() if status == "applied"]
    skipped = [name for name, status in results.items() if status == "skipped"]
    context.log.info(f"✅ migrations on staging: {len(applied)} applied, {len(skipped)} skipped")
    return dg.MaterializeResult(
        metadata={
            "applied_count": len(applied),
            "skipped_count": len(skipped),
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
    deps=["staging_infra_schemas"],
    compute_kind="mssql",
    description="Create ETL routing tables (meta.ifdb_aktivitetstype_routing) for IFDB SFI routing.",
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
        "Seed meta.ifdb_aktivitetstype_routing from the repo-controlled YAML "
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
        MERGE [meta].[ifdb_aktivitetstype_routing] AS tgt
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
            [DW_Timestamp_Update]  = SYSDATETIME()
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
                    "SELECT COUNT(*) FROM [meta].[ifdb_aktivitetstype_routing] "
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
# ETL WATERMARKS
# ============================================================================


@dg.asset(
    name="staging_infra_etl_watermarks",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas"],
    tags={"layer": "staging", "type": "infrastructure"},
    description="Create ETL watermark state and log tables in meta schema",
)
def staging_infra_etl_watermarks(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deploy meta.etl_watermark_state and meta.etl_watermark_log tables."""

    execute_sql_file(mssql_target_staging, "tables/meta/02_etl_watermarks.sql", context)

    context.log.info("ETL watermark tables deployed")
    return dg.MaterializeResult(
        metadata={
            "tables": "meta.etl_watermark_state, meta.etl_watermark_log",
        }
    )


# ============================================================================
# DFM (Data Flow Management)
# ============================================================================


@dg.asset(
    name="staging_infra_dfm",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas"],
    tags={"layer": "staging", "type": "infrastructure"},
    description="Create DFM state, run log, and reset procedure in meta schema",
)
def staging_infra_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deploy meta.dfm_state, meta.dfm_run_log, and meta.usp_dfm_reset."""

    execute_sql_file(mssql_target_staging, "tables/meta/02_dfm_state.sql", context)
    execute_sql_file(mssql_target_staging, "tables/meta/03_dfm_run_log.sql", context)
    execute_sql_file(mssql_target_staging, "tables/meta/04_usp_dfm_reset.sql", context)

    context.log.info("DFM tables and stored procedure deployed")
    return dg.MaterializeResult(
        metadata={
            "tables": "meta.dfm_state, meta.dfm_run_log",
            "procedures": "meta.usp_dfm_reset",
        }
    )


# ============================================================================
# IFDB ARCHIVE
# ============================================================================


@dg.asset(
    name="staging_infra_ifdb_archive",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas"],
    tags={"layer": "staging", "type": "infrastructure"},
    description="Create archive tables for IFDB Patient, EpisodeOfCare, Encounter, and Location",
)
def staging_infra_ifdb_archive(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Deploy arc.patient_ifdb, arc.episode_of_care_ifdb, arc.encounter_ifdb, and arc.location_ifdb tables."""

    results = execute_sql_files_from_directory(mssql_target_staging, "tables/ifdb_archive", context)

    context.log.info("IFDB archive tables deployed")
    return dg.MaterializeResult(
        metadata={
            "files_executed": len(results),
            "tables": (
                "arc.patient_ifdb, arc.episode_of_care_ifdb, "
                "arc.encounter_ifdb, arc.location_ifdb"
            ),
        }
    )
