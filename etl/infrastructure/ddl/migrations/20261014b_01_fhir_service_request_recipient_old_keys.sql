-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Delete fhir.ServiceRequest_Recipient rows that carry the old receiver key, and reset the
-- fhir_server / RehabilitationPlan checkpoint so the ordinary run reposts every plan.
--
-- Every row of the table comes from arc.ifdb_rehabilitation_plan_receiver, through the
-- receivers extension of ServiceRequest_IFDB_RehabilitationPlan_dfm. recipient_key changes
-- from "<ydernummer>|<afdelingskode>|<afsendelsestidspunkt>" to arc's modtager_noegle: the
-- receiver's ydernummer, afdelingskode or SOR-kode, none of which contains "|". Child-grain
-- reconcile would keep each old-format row as a soft-deleted row beside its replacement, so
-- they are deleted instead.
--
-- The builder only reposts a plan whose own row in arc.ifdb_rehabilitation_plan changed, so a
-- plan nobody touches would keep the old keys in the FHIR server and have no rows here after
-- the delete. With the checkpoint at 1900-01-01, the next ifdb_dfm_daily_job run reposts every
-- plan on the new key, and sof_service_request_recipient_dfm and
-- dso_sync_fhir_service_request_recipient_dfm write the receivers back.
--
-- Runs against both databases: staging is the SQL-on-FHIR target and DSO the sync target, and
-- the DSO sync never removes a row that staging no longer has. The DFM state for fhir_server
-- lives in staging, so the reset is fenced on the RehabilitationPlan checkpoint existing; it is
-- a no-op on DSO and on a database where the builder has not run, whose first run is a full
-- load. The file fails before it deletes anything if meta.DFMState exists without
-- meta.usp_dfm_reset. A rerun deletes nothing more and resets the checkpoint again.

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL
    THROW 50013,
        N'meta.DFMState exists but meta.usp_dfm_reset does not - materialise staging_infra_dfm before this migration. Without the reset, rehabilitation plans that do not change keep no receiver rows.',
        1;
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]', 'U') IS NOT NULL
    EXEC(N'DELETE FROM [fhir].[ServiceRequest_Recipient] WHERE [recipient_key] LIKE N''%|%'';');
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NULL
    PRINT 'No meta.DFMState on this database (expected on DSO) - no checkpoint to reset.';
ELSE
    EXEC sp_executesql N'
        IF EXISTS (SELECT 1 FROM [meta].[DFMState]
                   WHERE [scope] = ''staging'' AND [target_type] = ''fhir_resource''
                     AND [schema_name] = ''fhir_server'' AND [table_name] = ''RehabilitationPlan'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir_server'', @table_name = ''RehabilitationPlan'',
                 @new_value = ''1900-01-01 00:00:00'',
                 @reason = N''Receiver key changed to modtager_noegle: every rehabilitation plan is reposted by the ordinary delta.'',
                 @scope = ''staging'';
            PRINT ''RehabilitationPlan checkpoint reset to 1900-01-01; the next ordinary run reposts every plan.'';
        END
        ELSE
            PRINT ''RehabilitationPlan has not run on this database - its first run is a full load.'';';
GO
