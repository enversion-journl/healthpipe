-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Enforce database-level settings on the current database.
--
-- Executed by staging_infra_db_settings / dso_infra_db_settings on EVERY run
-- of the infrastructure job (not once-per-filename like migrations), so the
-- settings self-heal after restores or a fresh CREATE DATABASE — uniformly
-- for Nomad-hosted dev servers and the client-managed QA/prod servers.
--
-- AUTO_UPDATE_STATISTICS_ASYNC ON: perf investigation 2026-07-10
-- (feature/sfi-datapoint-perf-investigation, results.md) — synchronous
-- StatMan scans triggered mid-INSERT by the SFI batch apply accounted for
-- 62% of insert time (1128s -> 434s on an identical replayed workload).
-- AUTO_CLOSE OFF: same investigation — sydbi_dso closed/reopened 61x in 37h,
-- flushing the buffer pool so every nightly run started cold on NFS.
--
-- Each ALTER is wrapped in TRY/CATCH: on client-managed servers the ETL login
-- may lack ALTER DATABASE permission; the asset surfaces the resulting flag
-- state as a WARNING instead of failing the infrastructure job.

DECLARE @db SYSNAME = DB_NAME();

IF EXISTS (
    SELECT 1 FROM sys.databases
    WHERE name = @db AND is_auto_update_stats_async_on = 0
)
BEGIN
    BEGIN TRY
        DECLARE @sql_stats NVARCHAR(400) = N'ALTER DATABASE ' + QUOTENAME(@db)
            + N' SET AUTO_UPDATE_STATISTICS_ASYNC ON';
        EXEC (@sql_stats);
        PRINT 'Enabled AUTO_UPDATE_STATISTICS_ASYNC on ' + @db;
    END TRY
    BEGIN CATCH
        PRINT 'Could not enable AUTO_UPDATE_STATISTICS_ASYNC on ' + @db
              + ': ' + ERROR_MESSAGE();
    END CATCH
END

IF EXISTS (
    SELECT 1 FROM sys.databases
    WHERE name = @db AND is_auto_close_on = 1
)
BEGIN
    BEGIN TRY
        DECLARE @sql_close NVARCHAR(400) = N'ALTER DATABASE ' + QUOTENAME(@db)
            + N' SET AUTO_CLOSE OFF';
        EXEC (@sql_close);
        PRINT 'Disabled AUTO_CLOSE on ' + @db;
    END TRY
    BEGIN CATCH
        PRINT 'Could not disable AUTO_CLOSE on ' + @db
              + ': ' + ERROR_MESSAGE();
    END CATCH
END
