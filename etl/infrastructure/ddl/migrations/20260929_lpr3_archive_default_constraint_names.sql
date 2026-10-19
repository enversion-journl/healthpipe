-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Rename the LPR3 archive DEFAULT constraints to DF_<table>_<column>, the name the
-- generated tables/lpr3/00_archive.sql now uses. Tables created by the earlier DDL
-- keep DF_arc_<table>_{deleted,insert,update}; a fresh database already has the new
-- names. Only constraint names change; no column, default or data is touched.
-- Staging only: the LPR3 archive DDL is loaded by staging_infra_lpr3. Each rename
-- runs only when the old name exists and the new one does not, so it is safe to
-- rerun and a no-op before the tables exist.

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_adresse_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_adresse_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_adresse_deleted]', N'DF_lpr3_adresse_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_adresse_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_adresse_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_adresse_insert]', N'DF_lpr3_adresse_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_adresse_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_adresse_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_adresse_update]', N'DF_lpr3_adresse_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_alder_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_alder_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_alder_deleted]', N'DF_lpr3_alder_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_alder_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_alder_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_alder_insert]', N'DF_lpr3_alder_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_alder_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_alder_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_alder_update]', N'DF_lpr3_alder_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_betalingegenskaber_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_betalingegenskaber_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_betalingegenskaber_deleted]', N'DF_lpr3_betalingegenskaber_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_betalingegenskaber_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_betalingegenskaber_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_betalingegenskaber_insert]', N'DF_lpr3_betalingegenskaber_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_betalingegenskaber_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_betalingegenskaber_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_betalingegenskaber_update]', N'DF_lpr3_betalingegenskaber_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_betalingsoplysning_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_betalingsoplysning_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_betalingsoplysning_deleted]', N'DF_lpr3_betalingsoplysning_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_betalingsoplysning_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_betalingsoplysning_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_betalingsoplysning_insert]', N'DF_lpr3_betalingsoplysning_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_betalingsoplysning_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_betalingsoplysning_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_betalingsoplysning_update]', N'DF_lpr3_betalingsoplysning_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_borger_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_borger_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_borger_deleted]', N'DF_lpr3_borger_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_borger_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_borger_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_borger_insert]', N'DF_lpr3_borger_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_borger_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_borger_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_borger_update]', N'DF_lpr3_borger_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_borgerdemografi_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_borgerdemografi_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_borgerdemografi_deleted]', N'DF_lpr3_borgerdemografi_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_borgerdemografi_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_borgerdemografi_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_borgerdemografi_insert]', N'DF_lpr3_borgerdemografi_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_borgerdemografi_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_borgerdemografi_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_borgerdemografi_update]', N'DF_lpr3_borgerdemografi_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_adressesundhedsklynge_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_adressesundhedsklynge_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_adressesundhedsklynge_deleted]', N'DF_lpr3_bridge_adressesundhedsklynge_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_adressesundhedsklynge_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_adressesundhedsklynge_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_adressesundhedsklynge_insert]', N'DF_lpr3_bridge_adressesundhedsklynge_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_adressesundhedsklynge_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_adressesundhedsklynge_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_adressesundhedsklynge_update]', N'DF_lpr3_bridge_adressesundhedsklynge_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_borgersundhedsklynge_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_borgersundhedsklynge_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_borgersundhedsklynge_deleted]', N'DF_lpr3_bridge_borgersundhedsklynge_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_borgersundhedsklynge_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_borgersundhedsklynge_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_borgersundhedsklynge_insert]', N'DF_lpr3_bridge_borgersundhedsklynge_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_borgersundhedsklynge_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_borgersundhedsklynge_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_borgersundhedsklynge_update]', N'DF_lpr3_bridge_borgersundhedsklynge_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_diagnose_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_diagnose_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_diagnose_deleted]', N'DF_lpr3_bridge_diagnose_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_diagnose_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_diagnose_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_diagnose_insert]', N'DF_lpr3_bridge_diagnose_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_diagnose_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_diagnose_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_diagnose_update]', N'DF_lpr3_bridge_diagnose_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_forloeb_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_forloeb_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_forloeb_deleted]', N'DF_lpr3_bridge_forloeb_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_forloeb_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_forloeb_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_forloeb_insert]', N'DF_lpr3_bridge_forloeb_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_forloeb_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_forloeb_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_forloeb_update]', N'DF_lpr3_bridge_forloeb_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_forloebsrelation_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_forloebsrelation_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_forloebsrelation_deleted]', N'DF_lpr3_bridge_forloebsrelation_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_forloebsrelation_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_forloebsrelation_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_forloebsrelation_insert]', N'DF_lpr3_bridge_forloebsrelation_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_forloebsrelation_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_forloebsrelation_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_forloebsrelation_update]', N'DF_lpr3_bridge_forloebsrelation_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_procedure_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_procedure_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_procedure_deleted]', N'DF_lpr3_bridge_procedure_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_procedure_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_procedure_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_procedure_insert]', N'DF_lpr3_bridge_procedure_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_procedure_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_procedure_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_procedure_update]', N'DF_lpr3_bridge_procedure_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_sygehusophold_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_sygehusophold_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_sygehusophold_deleted]', N'DF_lpr3_bridge_sygehusophold_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_sygehusophold_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_sygehusophold_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_sygehusophold_insert]', N'DF_lpr3_bridge_sygehusophold_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_bridge_sygehusophold_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_bridge_sygehusophold_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_bridge_sygehusophold_update]', N'DF_lpr3_bridge_sygehusophold_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_datakilde_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_datakilde_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_datakilde_deleted]', N'DF_lpr3_datakilde_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_datakilde_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_datakilde_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_datakilde_insert]', N'DF_lpr3_datakilde_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_datakilde_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_datakilde_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_datakilde_update]', N'DF_lpr3_datakilde_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_dato_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_dato_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_dato_deleted]', N'DF_lpr3_dato_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_dato_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_dato_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_dato_insert]', N'DF_lpr3_dato_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_dato_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_dato_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_dato_update]', N'DF_lpr3_dato_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_diagnose_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_diagnose_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_diagnose_deleted]', N'DF_lpr3_diagnose_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_diagnose_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_diagnose_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_diagnose_insert]', N'DF_lpr3_diagnose_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_diagnose_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_diagnose_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_diagnose_update]', N'DF_lpr3_diagnose_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_diagnosegruppe_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_diagnosegruppe_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_diagnosegruppe_deleted]', N'DF_lpr3_diagnosegruppe_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_diagnosegruppe_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_diagnosegruppe_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_diagnosegruppe_insert]', N'DF_lpr3_diagnosegruppe_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_diagnosegruppe_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_diagnosegruppe_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_diagnosegruppe_update]', N'DF_lpr3_diagnosegruppe_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloeb_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloeb_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloeb_deleted]', N'DF_lpr3_forloeb_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloeb_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloeb_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloeb_insert]', N'DF_lpr3_forloeb_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloeb_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloeb_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloeb_update]', N'DF_lpr3_forloeb_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloebegenskaber_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloebegenskaber_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloebegenskaber_deleted]', N'DF_lpr3_forloebegenskaber_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloebegenskaber_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloebegenskaber_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloebegenskaber_insert]', N'DF_lpr3_forloebegenskaber_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloebegenskaber_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloebegenskaber_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloebegenskaber_update]', N'DF_lpr3_forloebegenskaber_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloeblabel_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloeblabel_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloeblabel_deleted]', N'DF_lpr3_forloeblabel_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloeblabel_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloeblabel_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloeblabel_insert]', N'DF_lpr3_forloeblabel_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_forloeblabel_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_forloeblabel_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_forloeblabel_update]', N'DF_lpr3_forloeblabel_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_fravaer_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_fravaer_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_fravaer_deleted]', N'DF_lpr3_fravaer_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_fravaer_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_fravaer_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_fravaer_insert]', N'DF_lpr3_fravaer_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_fravaer_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_fravaer_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_fravaer_update]', N'DF_lpr3_fravaer_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_helbredsforloeb_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_helbredsforloeb_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_helbredsforloeb_deleted]', N'DF_lpr3_helbredsforloeb_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_helbredsforloeb_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_helbredsforloeb_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_helbredsforloeb_insert]', N'DF_lpr3_helbredsforloeb_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_helbredsforloeb_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_helbredsforloeb_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_helbredsforloeb_update]', N'DF_lpr3_helbredsforloeb_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontakt_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontakt_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontakt_deleted]', N'DF_lpr3_kontakt_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontakt_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontakt_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontakt_insert]', N'DF_lpr3_kontakt_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontakt_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontakt_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontakt_update]', N'DF_lpr3_kontakt_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontaktegenskaber_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontaktegenskaber_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontaktegenskaber_deleted]', N'DF_lpr3_kontaktegenskaber_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontaktegenskaber_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontaktegenskaber_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontaktegenskaber_insert]', N'DF_lpr3_kontaktegenskaber_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontaktegenskaber_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontaktegenskaber_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontaktegenskaber_update]', N'DF_lpr3_kontaktegenskaber_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontaktlokation_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontaktlokation_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontaktlokation_deleted]', N'DF_lpr3_kontaktlokation_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontaktlokation_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontaktlokation_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontaktlokation_insert]', N'DF_lpr3_kontaktlokation_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_kontaktlokation_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_kontaktlokation_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_kontaktlokation_update]', N'DF_lpr3_kontaktlokation_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_markoer_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_markoer_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_markoer_deleted]', N'DF_lpr3_markoer_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_markoer_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_markoer_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_markoer_insert]', N'DF_lpr3_markoer_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_markoer_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_markoer_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_markoer_update]', N'DF_lpr3_markoer_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_markoerregistrering_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_markoerregistrering_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_markoerregistrering_deleted]', N'DF_lpr3_markoerregistrering_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_markoerregistrering_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_markoerregistrering_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_markoerregistrering_insert]', N'DF_lpr3_markoerregistrering_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_markoerregistrering_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_markoerregistrering_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_markoerregistrering_update]', N'DF_lpr3_markoerregistrering_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_organisation_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_organisation_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_organisation_deleted]', N'DF_lpr3_organisation_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_organisation_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_organisation_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_organisation_insert]', N'DF_lpr3_organisation_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_organisation_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_organisation_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_organisation_update]', N'DF_lpr3_organisation_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_procedure_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_procedure_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_procedure_deleted]', N'DF_lpr3_procedure_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_procedure_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_procedure_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_procedure_insert]', N'DF_lpr3_procedure_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_procedure_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_procedure_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_procedure_update]', N'DF_lpr3_procedure_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_proceduregruppe_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_proceduregruppe_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_proceduregruppe_deleted]', N'DF_lpr3_proceduregruppe_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_proceduregruppe_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_proceduregruppe_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_proceduregruppe_insert]', N'DF_lpr3_proceduregruppe_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_proceduregruppe_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_proceduregruppe_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_proceduregruppe_update]', N'DF_lpr3_proceduregruppe_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_procedureregistrering_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_procedureregistrering_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_procedureregistrering_deleted]', N'DF_lpr3_procedureregistrering_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_procedureregistrering_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_procedureregistrering_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_procedureregistrering_insert]', N'DF_lpr3_procedureregistrering_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_procedureregistrering_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_procedureregistrering_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_procedureregistrering_update]', N'DF_lpr3_procedureregistrering_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultat_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultat_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultat_deleted]', N'DF_lpr3_resultat_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultat_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultat_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultat_insert]', N'DF_lpr3_resultat_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultat_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultat_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultat_update]', N'DF_lpr3_resultat_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultategenskaber_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultategenskaber_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultategenskaber_deleted]', N'DF_lpr3_resultategenskaber_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultategenskaber_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultategenskaber_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultategenskaber_insert]', N'DF_lpr3_resultategenskaber_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultategenskaber_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultategenskaber_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultategenskaber_update]', N'DF_lpr3_resultategenskaber_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultatindberetning_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultatindberetning_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultatindberetning_deleted]', N'DF_lpr3_resultatindberetning_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultatindberetning_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultatindberetning_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultatindberetning_insert]', N'DF_lpr3_resultatindberetning_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_resultatindberetning_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_resultatindberetning_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_resultatindberetning_update]', N'DF_lpr3_resultatindberetning_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_sundhedsklynge_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_sundhedsklynge_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_sundhedsklynge_deleted]', N'DF_lpr3_sundhedsklynge_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_sundhedsklynge_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_sundhedsklynge_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_sundhedsklynge_insert]', N'DF_lpr3_sundhedsklynge_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_sundhedsklynge_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_sundhedsklynge_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_sundhedsklynge_update]', N'DF_lpr3_sundhedsklynge_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_sygehusophold_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_sygehusophold_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_sygehusophold_deleted]', N'DF_lpr3_sygehusophold_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_sygehusophold_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_sygehusophold_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_sygehusophold_insert]', N'DF_lpr3_sygehusophold_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_sygehusophold_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_sygehusophold_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_sygehusophold_update]', N'DF_lpr3_sygehusophold_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_tidspunkt_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_tidspunkt_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_tidspunkt_deleted]', N'DF_lpr3_tidspunkt_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_tidspunkt_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_tidspunkt_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_tidspunkt_insert]', N'DF_lpr3_tidspunkt_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_tidspunkt_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_tidspunkt_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_tidspunkt_update]', N'DF_lpr3_tidspunkt_dw_timestamp_update', N'OBJECT';
GO

IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_vaerdisks_deleted]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_vaerdisks_dw_is_deleted]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_vaerdisks_deleted]', N'DF_lpr3_vaerdisks_dw_is_deleted', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_vaerdisks_insert]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_vaerdisks_dw_timestamp_insert]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_vaerdisks_insert]', N'DF_lpr3_vaerdisks_dw_timestamp_insert', N'OBJECT';
IF DB_NAME() LIKE N'%staging%'
    AND OBJECT_ID(N'[arc].[DF_arc_lpr3_vaerdisks_update]', N'D') IS NOT NULL
    AND OBJECT_ID(N'[arc].[DF_lpr3_vaerdisks_dw_timestamp_update]', N'D') IS NULL
    EXEC sp_rename N'[arc].[DF_arc_lpr3_vaerdisks_update]', N'DF_lpr3_vaerdisks_dw_timestamp_update', N'OBJECT';
GO
