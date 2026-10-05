-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Linked Server Setup: DSO -> Staging Database
-- =============================================================================
--
-- FLOW AND WHY DSO CALLS STAGING
-- ------------------------------
-- Data flows one way: Staging (DSI/DSA) -> DSO (reporting). The linked server
-- is created ON the DSO server and POINTS TO the staging server. When Dagster
-- runs a sync asset, it connects to DSO and executes a MERGE that reads from
-- staging via 4-part naming ([STAGING_LINK].[DB].[schema].[table]). The MERGE
-- runs entirely on the DSO server; SQL Server on DSO uses the linked server
-- to pull data from staging. So DSO "calls" staging (initiates the connection
-- and queries), not the other way around. This keeps reporting logic and
-- credentials on DSO and gives staging only a dedicated read-only user.
--
-- EXECUTION ORDER
-- ---------------
-- 1. Part A: Run on STAGING server -- create login and user so DSO can connect.
-- 2. Part B: Run on DSO server     -- create linked server and login mapping.
--
-- PREREQUISITES:
--   Part A: Run on STAGING with a login that can create logins/users (e.g. sysadmin).
--   Part B: Run on DSO with sysadmin; replace variables or use SQLCMD :setvar.
--   Network: DSO server must reach staging server (e.g. port 1433 or custom).
--
-- USAGE (Part B on DSO):
--   sqlcmd -S <DSO_HOST> -d <DSO_DB> -v STAGING_HOST="staging-server,port"
--          -v STAGING_DB="sydbi_staging" -v STAGING_USER="dagster_dso_qa"
--          -v STAGING_PASSWORD="***" -i 00_create_linked_server.sql
--
-- =============================================================================
-- SQLCMD variables (use in VS Code with SQLCMD mode On, or pass via sqlcmd -v)
-- =============================================================================
-- :setvar STAGING_HOST "staging-server,24571"
-- :setvar STAGING_DB "sydbi_staging"
-- :setvar STAGING_USER "dagster_dso_qa"
-- :setvar STAGING_PASSWORD "***"

-- =============================================================================
-- PART A: Run on STAGING server (create login and user for linked server)
-- =============================================================================
-- This section must be executed on the STAGING SQL Server instance. It creates
-- the login that DSO will use when connecting via STAGING_LINK. The password
-- must match STAGING_PASSWORD used in Part B. Do not commit real passwords.
-- Replace <Same_as_STAGING_PASSWORD> below with the same value as STAGING_PASSWORD.
-- =============================================================================
/*
USE [$(STAGING_DB)];  -- e.g. sydbi_staging or DW
GO

-- 1) Create server login if it does not exist
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'$(STAGING_USER)')
BEGIN
    CREATE LOGIN [$(STAGING_USER)] WITH PASSWORD = N'<Same_as_STAGING_PASSWORD>',
        DEFAULT_DATABASE = [$(STAGING_DB)],
        CHECK_EXPIRATION = OFF,
        CHECK_POLICY = ON;
    PRINT 'Login $(STAGING_USER) created.';
END
ELSE
    PRINT 'Login $(STAGING_USER) already exists.';
GO

-- 2) Create database user and map to login
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(STAGING_USER)')
BEGIN
    CREATE USER [$(STAGING_USER)] FOR LOGIN [$(STAGING_USER)];
    PRINT 'User $(STAGING_USER) created in database.';
END
GO

-- 3) Grant SELECT on map, dim, fct (least privilege for DSO sync)
GRANT SELECT ON SCHEMA::[map] TO [$(STAGING_USER)];
GRANT SELECT ON SCHEMA::[dim] TO [$(STAGING_USER)];
GRANT SELECT ON SCHEMA::[fct] TO [$(STAGING_USER)];
GRANT SELECT ON SCHEMA::[meta] TO [$(STAGING_USER)];
GRANT SELECT ON SCHEMA::[bridge] TO [$(STAGING_USER)];
GRANT SELECT ON SCHEMA::[fhir] TO [$(STAGING_USER)];
GRANT SELECT ON SCHEMA::[sec] TO [$(STAGING_USER)];
GRANT SELECT ON SCHEMA::[ref] TO [$(STAGING_USER)];
PRINT 'SELECT granted on map, dim, fct.';
GO
*/

-- =============================================================================
-- PART B: Run on DSO server (create linked server and options)
-- =============================================================================

-- List linked servers (run on DSO to inspect):
--   SELECT name, data_source, provider, is_data_access_enabled, is_rpc_out_enabled
--   FROM sys.servers WHERE is_linked = 1;
-- List linked server login mapping:
--   SELECT s.name, ll.remote_name FROM sys.servers s
--   JOIN sys.linked_logins ll ON s.server_id = ll.server_id WHERE s.name = 'STAGING_LINK';

-- Define the linked server name constant
DECLARE @LinkedServerName NVARCHAR(128) = 'STAGING_LINK';

-- =============================================================================
-- Step 1: Drop existing linked server if configuration changed
-- =============================================================================
-- Uncomment if you need to recreate:
-- IF EXISTS (SELECT * FROM sys.servers WHERE name = @LinkedServerName)
-- BEGIN
--     EXEC sp_dropserver @server = @LinkedServerName, @droplogins = 'droplogins';
--     PRINT 'Dropped existing linked server: ' + @LinkedServerName;
-- END

-- =============================================================================
-- Step 2: Create Linked Server (if not exists)
-- =============================================================================
-- Provider selection
-- ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑
-- MSOLEDBSQL    Microsoft OLE DB Driver 18+.  Recommended driver.
-- MSOLEDBSQL19  Version‑specific alias; requires the v19 driver on the host.
-- SQLNCLI11     SQL Server Native Client 11.  Deprecated since SQL Server 2022;
--               use only as fallback on hosts without MSOLEDBSQL.
IF NOT EXISTS (SELECT * FROM sys.servers WHERE name = @LinkedServerName)
BEGIN
    EXEC sp_addlinkedserver
        @server = @LinkedServerName,
        @srvproduct = N'',
        @provider = N'MSOLEDBSQL',
        @datasrc = N'$(STAGING_HOST)';

    PRINT 'Created linked server: ' + @LinkedServerName;
END
ELSE
BEGIN
    PRINT 'Linked server already exists: ' + @LinkedServerName;
END

-- =============================================================================
-- Step 3: Configure Security (Remote Login)
-- =============================================================================
-- Remove existing login mapping if present
IF EXISTS (
    SELECT * FROM sys.linked_logins
    WHERE server_id = (SELECT server_id FROM sys.servers WHERE name = @LinkedServerName)
)
BEGIN
    EXEC sp_droplinkedsrvlogin
        @rmtsrvname = @LinkedServerName,
        @locallogin = NULL;
END

-- Add login mapping for all local users
EXEC sp_addlinkedsrvlogin
    @rmtsrvname = @LinkedServerName,
    @useself = N'FALSE',
    @locallogin = NULL,
    @rmtuser = N'$(STAGING_USER)',
    @rmtpassword = N'$(STAGING_PASSWORD)';

PRINT 'Configured login mapping for linked server';

-- =============================================================================
-- Step 4: Configure Linked Server Options
-- =============================================================================
--
-- rpc / rpc out
--   Required for sp_testlinkedserver and any remote procedure calls.
--   Not strictly needed for 4-part-name MERGE, but used by the verification
--   asset and validate_linked_server() at sync time.
EXEC sp_serveroption @server = @LinkedServerName, @optname = 'rpc', @optvalue = 'true';
EXEC sp_serveroption @server = @LinkedServerName, @optname = 'rpc out', @optvalue = 'true';

-- data access
--   Permits 4-part-name queries ([LINK].[DB].[schema].[table]).
--   Without this, all distributed MERGE / SELECT statements fail.
EXEC sp_serveroption @server = @LinkedServerName, @optname = 'data access', @optvalue = 'true';

-- collation compatible
--   Asserts the remote (staging) server uses the same collation as DSO; only
--   valid when they actually match. They don't here (staging
--   SQL_Latin1_General_CP1_CI_AS vs DSO Danish_Norwegian_CI_AS), so it must be
--   'false'. Cross-collation join correctness is handled by COLLATE
--   DATABASE_DEFAULT in sync_utils.py.
EXEC sp_serveroption @server = @LinkedServerName, @optname = 'collation compatible', @optvalue = 'false';

-- lazy schema validation
--   Defers remote table schema validation from compile time to execution time.
--   Eliminates one metadata round-trip per query.  Beneficial for the delta
--   sync pattern where the same tables are merged repeatedly with known schemas.
EXEC sp_serveroption @server = @LinkedServerName, @optname = 'lazy schema validation', @optvalue = 'true';

-- connect timeout (seconds)
--   30s is generous for a same-network connection.  Prevents indefinite hangs
--   during network or infrastructure issues.
EXEC sp_serveroption @server = @LinkedServerName, @optname = 'connect timeout', @optvalue = '30';

-- query timeout (seconds, 0 = unlimited)
--   MERGE on large fact tables (e.g. fct.Fakturalinje) can run for several
--   minutes.  Unlimited prevents premature cancellation; Dagster's own run
--   timeout serves as the outer safety net.
EXEC sp_serveroption @server = @LinkedServerName, @optname = 'query timeout', @optvalue = '0';

PRINT 'Configured linked server options';

-- =============================================================================
-- Step 5: Verify Linked Server
-- =============================================================================
BEGIN TRY
    -- Test connectivity
    EXEC sp_testlinkedserver @LinkedServerName;
    PRINT 'Linked server connectivity test: SUCCESS';

    -- Test reading from staging database
    DECLARE @TestQuery NVARCHAR(MAX) =
        N'SELECT TOP 1 1 AS test FROM ' + @LinkedServerName + N'.$(STAGING_DB).INFORMATION_SCHEMA.TABLES';
    EXEC sp_executesql @TestQuery;
    PRINT 'Linked server data access test: SUCCESS';

END TRY
BEGIN CATCH
    PRINT 'WARNING: Linked server test failed - ' + ERROR_MESSAGE();
    PRINT 'The linked server was created but connectivity could not be verified.';
    PRINT 'Please check network connectivity and credentials.';
END CATCH

-- =============================================================================
-- Usage Examples (for reference)
-- =============================================================================
/*
-- Query staging table via linked server (4-part naming)
SELECT * FROM STAGING_LINK.DW.dim.Kreditor;

-- Query using OPENQUERY (can be faster for complex queries)
SELECT * FROM OPENQUERY(STAGING_LINK, 'SELECT * FROM DW.dim.Kreditor');

-- MERGE pattern for syncing
MERGE dbo.LocalTable AS tgt
USING STAGING_LINK.DW.schema.SourceTable AS src
ON tgt.PK = src.PK
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED BY TARGET THEN INSERT ...;
*/

-- =============================================================================
-- List / inspect linked servers (run on DSO; copy and run as needed)
-- =============================================================================
--
-- All linked servers (name, host, provider, options):
--   SELECT name AS linked_server_name, data_source AS host_server, provider,
--          is_data_access_enabled, is_rpc_out_enabled, connect_timeout, query_timeout
--   FROM sys.servers WHERE is_linked = 1;
--
-- Single linked server (STAGING_LINK) and login mapping:
--   SELECT s.name AS linked_server, s.data_source AS host_server, s.provider,
--          ll.remote_name AS configured_remote_user
--   FROM sys.servers s
--   LEFT JOIN sys.linked_logins ll ON s.server_id = ll.server_id
--   WHERE s.name = 'STAGING_LINK';
--
-- Test connectivity:
--   EXEC sp_testlinkedserver @servername = N'STAGING_LINK';
--
-- Test 4-part SELECT (replace DB/schema/table as needed):
--   SELECT COUNT(*) FROM [STAGING_LINK].[sydbi_staging].[dim].[Fakturalinje];

PRINT '=============================================================================';
PRINT 'Linked server setup complete: ' + @LinkedServerName;
PRINT 'Target: $(STAGING_HOST) / $(STAGING_DB)';
PRINT '=============================================================================';
GO
