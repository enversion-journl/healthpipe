# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Linked Server Infrastructure Asset

Creates and manages the linked server on DSO database that points to the
staging (DSI/DSA) database for efficient cross-database data synchronization.

The linked server enables MERGE operations to execute entirely on the DSO
database server, pulling data from staging without Python intermediary.

NOTE: The staging-side login/user (Part A) lives in staging_infrastructure.py
as `staging_infra_linked_server_user`. This file only handles the DSO side.
"""

import re
from typing import Any

import dagster as dg
from core.resources import MSSQLResource, get_linked_server_config

# ============================================================================
# LINKED SERVER INFRASTRUCTURE
# ============================================================================


def _with_uid(provstr: str, user: str) -> str:
    """Add UID=<user> to a provider string without UID, to avoid error 7416 without sysadmin."""
    if not provstr or re.search(r"(^|;)\s*(UID|User ID)\s*=", provstr, re.IGNORECASE):
        return provstr
    return f"{provstr.rstrip(';')};UID={user}"


def _nvarchar_or_null(value: str) -> str:
    """Return the value as an escaped N'...' SQL literal, or NULL when it is empty."""
    return "N'" + value.replace("'", "''") + "'" if value else "NULL"


@dg.asset(
    name="dso_infra_linked_server",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_schemas", "staging_infra_linked_server_user"],
    tags={"layer": "dso", "type": "infrastructure", "object": "linked_server"},
    description="Create and configure linked server from DSO to Staging database",
)
def dso_infra_linked_server(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """
    Create linked server 'STAGING_LINK' on DSO database pointing to staging.

    The linked server enables efficient cross-database syncs using MERGE statements
    that execute entirely on the DSO server, pulling data via the linked connection.

    Configuration is read from environment variables:
    - MSSQL_LINKED_SERVER_HOST: Staging database server hostname
    - MSSQL_LINKED_SERVER_PORT: Staging database server port (default: 1433)
    - MSSQL_LINKED_SERVER_DB: Staging database name (default: DW)
    - MSSQL_LINKED_SERVER_USER: Username for staging connection
    - MSSQL_LINKED_SERVER_PASSWORD: Password for staging connection
    - MSSQL_LINKED_SERVER_NAME: Name of the linked server (default: STAGING_LINK)
    - MSSQL_LINKED_SERVER_PROVSTR: Provider string (optional; when unset, keep the current one).
      A provider string without UID gets UID=<linked server user>.

    Falls back to MSSQL_SOURCE_STAGING_* when linked-server-specific vars are unset.

    Note: Requires sysadmin privileges on DSO database to create linked servers.
    """
    linked_server_config = get_linked_server_config()
    staging_host = linked_server_config["host"]
    staging_port = linked_server_config["port"]
    staging_db = linked_server_config["database"]
    staging_user = linked_server_config["user"]
    staging_password = linked_server_config["password"]
    linked_server_name = linked_server_config["name"]
    staging_datasrc = linked_server_config["datasrc"]
    staging_provstr = linked_server_config["provstr"]

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Azure SQL Edge does not support linked servers.  Detect the
            # edition early so we can skip gracefully in dev environments.
            # CAST to concrete types because SERVERPROPERTY returns sql_variant
            # which the mssql_python driver does not support.
            cursor.execute(
                "SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT), "
                "CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256))"
            )
            row = cursor.fetchone()
            engine_edition = int(row[0]) if row else 0
            edition_name = str(row[1]) if row else ""
            if "Azure SQL Edge" in edition_name:
                context.log.warning(
                    f"DSO is running on {edition_name} which does not support "
                    "linked servers. Skipping linked server creation (dev only)."
                )
                return dg.MaterializeResult(
                    metadata={
                        "linked_server": linked_server_name,
                        "target_host": staging_host,
                        "target_port": staging_port,
                        "target_database": staging_db,
                        "connectivity_status": "skipped (Azure SQL Edge)",
                        "skipped_reason": "Azure SQL Edge does not support linked servers",
                    }
                )

            # Check that the current user has setupadmin or sysadmin.
            # sp_addlinkedsrvlogin requires one of these roles (error 15247 if missing).
            # CAST because IS_SRVROLEMEMBER returns int but some drivers
            # may struggle with the implicit type.
            cursor.execute(
                """
                SELECT CAST(IS_SRVROLEMEMBER('sysadmin') AS INT),
                       CAST(IS_SRVROLEMEMBER('setupadmin') AS INT)
                """
            )
            role_row = cursor.fetchone()
            has_sysadmin = role_row[0] == 1
            has_setupadmin = role_row[1] == 1
            if not has_sysadmin and not has_setupadmin:
                cursor.execute("SELECT SYSTEM_USER")
                current_user = cursor.fetchone()[0]
                raise PermissionError(
                    f"User '{current_user}' lacks the required server role to manage linked servers. "
                    "Grant setupadmin (minimum) or sysadmin on the DSO SQL Server instance:\n"
                    f"    ALTER SERVER ROLE [setupadmin] ADD MEMBER [{current_user}];"
                )

            linked_server_name_sql = linked_server_name.replace("'", "''")
            staging_user_sql = staging_user.replace("'", "''")
            staging_password_sql = staging_password.replace("'", "''")
            staging_datasrc_sql = staging_datasrc.replace("'", "''")

            # Check if linked server already exists
            cursor.execute(
                f"SELECT COUNT(*) FROM sys.servers WHERE name = N'{linked_server_name_sql}'"
            )
            exists = cursor.fetchone()[0] > 0

            needs_recreate = False
            if exists:
                cursor.execute(
                    f"SELECT data_source, provider_string FROM sys.servers "
                    f"WHERE name = N'{linked_server_name_sql}'"
                )
                current_datasrc, current_provstr = cursor.fetchone()
                current_datasrc = current_datasrc or ""
                current_provstr = current_provstr or ""
                staging_provstr = _with_uid(staging_provstr or current_provstr, staging_user)
                if current_datasrc != staging_datasrc or current_provstr != staging_provstr:
                    context.log.warning(
                        f"Linked server config drift detected: "
                        f"current data_source='{current_datasrc}', "
                        f"expected='{staging_datasrc}', "
                        f"current provider_string='{current_provstr}', "
                        f"expected='{staging_provstr}'. Dropping and recreating."
                    )
                    needs_recreate = True
                else:
                    context.log.info(
                        f"Linked server '{linked_server_name}' already exists, "
                        "verifying configuration..."
                    )

            if exists and needs_recreate:
                if callable(getattr(conn, "autocommit", None)):
                    conn.autocommit(True)
                else:
                    conn.autocommit = True
                try:
                    cursor.execute(
                        f"EXEC sp_dropserver @server = N'{linked_server_name_sql}', "
                        f"@droplogins = 'droplogins'"
                    )
                    context.log.info(f"Dropped stale linked server '{linked_server_name}'")
                    exists = False
                finally:
                    if callable(getattr(conn, "autocommit", None)):
                        conn.autocommit(False)
                    else:
                        conn.autocommit = False

            if exists and not needs_recreate:
                # Verify connectivity
                try:
                    cursor.execute(
                        f"EXEC sp_testlinkedserver @servername = N'{linked_server_name_sql}'"
                    )
                    context.log.info(
                        f"✅ Linked server '{linked_server_name}' connectivity verified"
                    )
                    connectivity_status = "verified"
                except Exception as e:
                    context.log.warning(f"⚠️ Linked server exists but connectivity test failed: {e}")
                    connectivity_status = f"failed: {str(e)}"

            if not exists:
                staging_provstr = _with_uid(staging_provstr, staging_user)
                context.log.info(
                    f"Creating linked server '{linked_server_name}' pointing to "
                    f"{staging_host}:{staging_port}..."
                )

                try:
                    if callable(getattr(conn, "autocommit", None)):
                        conn.autocommit(True)
                    else:
                        conn.autocommit = True

                    provider = "MSOLEDBSQL"

                    cursor.execute(
                        f"""
                        EXEC sp_addlinkedserver
                            @server = N'{linked_server_name_sql}',
                            @srvproduct = N'',
                            @provider = N'{provider}',
                            @datasrc = N'{staging_datasrc_sql}',
                            @provstr = {_nvarchar_or_null(staging_provstr)}
                        """
                    )
                    context.log.info(
                        f"Created linked server: {linked_server_name} (provider={provider})"
                    )

                    try:
                        cursor.execute(
                            f"""
                            EXEC sp_addlinkedsrvlogin
                                @rmtsrvname = N'{linked_server_name_sql}',
                                @useself = N'FALSE',
                                @locallogin = NULL,
                                @rmtuser = N'{staging_user_sql}',
                                @rmtpassword = N'{staging_password_sql}'
                            """
                        )
                        context.log.info("Configured login mapping")
                    except Exception as e:
                        context.log.error(
                            f"sp_addlinkedsrvlogin failed: {e}. "
                            "Cleaning up partially created linked server."
                        )
                        cursor.execute(
                            f"EXEC sp_dropserver @server = N'{linked_server_name_sql}', "
                            f"@droplogins = 'droplogins'"
                        )
                        raise RuntimeError(
                            f"Cannot configure linked server login mapping "
                            f"(requires sysadmin or setupadmin on DSO): {e}"
                        ) from e

                    options = [
                        ("rpc", "true"),
                        ("rpc out", "true"),
                        ("data access", "true"),
                        ("collation compatible", "true"),
                        ("lazy schema validation", "true"),
                        ("connect timeout", "30"),
                        ("query timeout", "0"),
                    ]
                    if engine_edition != 3:
                        options = [o for o in options if o[0] != "lazy schema validation"]
                        context.log.info(f"Skipping 'lazy schema validation' on {edition_name}")
                    try:
                        for opt_name, opt_value in options:
                            cursor.execute(
                                f"EXEC sp_serveroption @server = N'{linked_server_name_sql}', "
                                f"@optname = N'{opt_name}', @optvalue = N'{opt_value}'"
                            )
                        context.log.info("Configured linked server options")
                    except Exception as e:
                        context.log.error(
                            f"sp_serveroption '{opt_name}' failed: {e}. "
                            "Cleaning up partially created linked server."
                        )
                        cursor.execute(
                            f"EXEC sp_dropserver @server = N'{linked_server_name_sql}', "
                            f"@droplogins = 'droplogins'"
                        )
                        raise RuntimeError(
                            f"Cannot set linked server option '{opt_name}': {e}"
                        ) from e

                    try:
                        cursor.execute(
                            f"""
                            DECLARE @plan_handle varbinary(64), @free_stmt nvarchar(300);
                            DECLARE stale_plans CURSOR LOCAL FAST_FORWARD FOR
                                SELECT DISTINCT cp.plan_handle
                                FROM sys.dm_exec_cached_plans cp
                                CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) st
                                WHERE CHARINDEX('{linked_server_name_sql}', st.text) > 0;
                            OPEN stale_plans;
                            FETCH NEXT FROM stale_plans INTO @plan_handle;
                            WHILE @@FETCH_STATUS = 0
                            BEGIN
                                SET @free_stmt = N'DBCC FREEPROCCACHE ('
                                    + CONVERT(nvarchar(200), @plan_handle, 1)
                                    + N') WITH NO_INFOMSGS';
                                EXEC sp_executesql @free_stmt;
                                FETCH NEXT FROM stale_plans INTO @plan_handle;
                            END
                            CLOSE stale_plans;
                            DEALLOCATE stale_plans;
                            """
                        )
                        if hasattr(cursor, "nextset"):
                            while cursor.nextset():
                                pass
                        context.log.info(f"Freed cached plans referencing '{linked_server_name}'")
                    except Exception as e:
                        context.log.warning(
                            f"Could not free cached plans for '{linked_server_name}': {e}. "
                            "Distributed queries may keep using the previous data source."
                        )

                    # Test connectivity
                    cursor.execute(
                        f"EXEC sp_testlinkedserver @servername = N'{linked_server_name_sql}'"
                    )
                    context.log.info(
                        f"✅ Linked server '{linked_server_name}' created and verified"
                    )
                    connectivity_status = "created and verified"

                finally:
                    if callable(getattr(conn, "autocommit", None)):
                        conn.autocommit(False)
                    else:
                        conn.autocommit = False

            # Get linked server info for metadata
            cursor.execute(
                f"""
                SELECT
                    s.name AS server_name,
                    s.provider,
                    s.data_source,
                    s.is_data_access_enabled,
                    s.is_rpc_out_enabled
                FROM sys.servers s
                WHERE s.name = N'{linked_server_name_sql}'
                """
            )
            server_info = cursor.fetchone()

            return dg.MaterializeResult(
                metadata={
                    "linked_server": linked_server_name,
                    "target_host": staging_host,
                    "target_port": staging_port,
                    "target_database": staging_db,
                    "provider": server_info[1] if server_info else "unknown",
                    "data_source": server_info[2] if server_info else "unknown",
                    "data_access_enabled": server_info[3] if server_info else False,
                    "rpc_out_enabled": server_info[4] if server_info else False,
                    "connectivity_status": connectivity_status,
                }
            )
        finally:
            cursor.close()


@dg.asset(
    name="dso_verify_linked_server",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=["dso_infra_linked_server"],
    tags={"layer": "dso", "type": "infrastructure", "object": "verification"},
    description="Verify linked server can read from staging database tables",
)
def dso_verify_linked_server(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """
    Verify the linked server can successfully read from staging database.

    Tests:
    1. Linked server connectivity
    2. Access to staging schemas (map, dim, fct)
    3. Sample data retrieval from key tables
    """
    linked_server_config = get_linked_server_config()
    linked_server_name = linked_server_config["name"]
    staging_db = linked_server_config["database"]

    verification_results: dict[str, Any] = {}

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256))")
            edition_name = str(cursor.fetchone()[0]) if cursor.description else ""
            if "Azure SQL Edge" in edition_name:
                context.log.warning(
                    f"DSO is {edition_name} — linked servers not supported. "
                    "Skipping verification (dev only)."
                )
                return dg.MaterializeResult(
                    metadata={
                        "linked_server": linked_server_name,
                        "staging_database": staging_db,
                        "overall_status": "skipped (Azure SQL Edge)",
                    }
                )

            linked_server_name_sql = linked_server_name.replace("'", "''")

            # Test 1: Basic connectivity
            try:
                cursor.execute(
                    f"EXEC sp_testlinkedserver @servername = N'{linked_server_name_sql}'"
                )
                verification_results["connectivity"] = "passed"
                context.log.info("✅ Connectivity test passed")
            except Exception as e:
                verification_results["connectivity"] = f"failed: {str(e)}"
                context.log.error(f"❌ Connectivity test failed: {e}")

            # Test 2: Schema access - check if schemas exist in staging
            schemas_to_check = ["map", "dim", "fct"]
            schema_access = {}
            for schema in schemas_to_check:
                try:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM [{linked_server_name}].[{staging_db}].INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '{schema}'
                        """
                    )
                    count = cursor.fetchone()[0]
                    schema_access[schema] = f"accessible ({count} tables)"
                    context.log.info(f"✅ Schema '{schema}' accessible with {count} tables")
                except Exception as e:
                    schema_access[schema] = f"failed: {str(e)}"
                    context.log.warning(f"⚠️ Schema '{schema}' access failed: {e}")

            verification_results["schema_access"] = schema_access

            # Test 3: Sample data read from key tables
            tables_to_test = [
                ("dim", "Kreditor"),
                ("dim", "Borger"),
                ("fct", "Fakturalinje"),
            ]
            table_access = {}
            for schema, table in tables_to_test:
                try:
                    cursor.execute(
                        f"""
                        SELECT TOP 1 1
                        FROM [{linked_server_name}].[{staging_db}].[{schema}].[{table}]
                        """
                    )
                    result = cursor.fetchone()
                    if result:
                        table_access[f"{schema}.{table}"] = "has data"
                    else:
                        table_access[f"{schema}.{table}"] = "accessible but empty"
                    context.log.info(f"✅ Table {schema}.{table} accessible")
                except Exception as e:
                    table_access[f"{schema}.{table}"] = f"not found or error: {str(e)[:50]}"
                    context.log.warning(f"⚠️ Table {schema}.{table}: {e}")

            verification_results["table_access"] = table_access

            # Overall status
            all_passed = verification_results["connectivity"] == "passed" and all(
                "accessible" in v for v in schema_access.values()
            )
            verification_results["overall_status"] = "ready" if all_passed else "needs_attention"

            return dg.MaterializeResult(
                metadata={
                    "linked_server": linked_server_name,
                    "staging_database": staging_db,
                    **verification_results,
                }
            )
        finally:
            cursor.close()
