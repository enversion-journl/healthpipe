-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: BOM_IFDB (Appointment domain — AFTALE + AFTALERESSOURCEDELTAGER + PATIENTINFO)
--
-- 1:1 reflection of BOM_IFDB Oracle source tables plus DWH audit columns.
--
-- bom_ifdb_aftale:                    delta on LASTMODIFIED; UPSERT on OID.
--                                     Soft-delete via DATOSLETTET (source column).
-- bom_ifdb_aftaleressourcedeltager:   full-load + detect_deletes (decision
--                                     2026-08-11: participant links are
--                                     routinely removed in the source and the
--                                     transform reads the archive per AFTALEOID
--                                     — a stale row would never heal).
-- bom_ifdb_patientinfo:              delta on LASTMODIFIED (decision
--                                     2026-08-11, data minimisation: no nightly
--                                     full copy of the person register).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.bom_ifdb_aftale  (flattened 8-way join — 61 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'bom_ifdb_aftale'
)
BEGIN
    CREATE TABLE [arc].[bom_ifdb_aftale] (
        [OID]                            NVARCHAR(50)    NOT NULL,
        [GUID]                           NVARCHAR(100)   NULL,
        [UPDATEID]                       NVARCHAR(50)    NULL,
        [PATIENTINFOOID]                 NVARCHAR(50)    NULL,
        [LASTMODIFIED]                   NVARCHAR(100)   NULL,
        [OPRETTETDATOTID]                NVARCHAR(100)   NULL,
        [OPRETTET_AF_BRUGEROID]          NVARCHAR(50)    NULL,
        [OPRETTET_AF_AFDELINGOID]        NVARCHAR(50)    NULL,
        [BOOKINGSTATUS]                  NVARCHAR(100)   NULL,
        [AFVIKLINGSSTATUSOID]            NVARCHAR(50)    NULL,
        [PATIENT_MOEDEDATOTID]           NVARCHAR(100)   NULL,
        [PATIENT_SLUT_DATOTID]           NVARCHAR(100)   NULL,
        [RESTRAEK_STARTDATOTID]          NVARCHAR(100)   NULL,
        [RESTRAEKTOTALMINUTTER]          NVARCHAR(100)   NULL,
        [HENVISNINGSDATO]                NVARCHAR(100)   NULL,
        [PATIENTDELTAGER]                NVARCHAR(100)   NULL,
        [KONTAKTTYPE]                    NVARCHAR(100)   NULL,
        [OPGAVEOID]                      NVARCHAR(50)    NULL,
        [OPGAVE_AFDELINGOID]             NVARCHAR(50)    NULL,
        [MOEDESTEDOID]                   NVARCHAR(50)    NULL,
        [YDELSEOID]                      NVARCHAR(50)    NULL,
        [BESTILT_BOOKINGSDATO]           NVARCHAR(100)   NULL,
        [TYPEOPERATION]                  NVARCHAR(100)   NULL,
        [RELEVANSDATO]                   NVARCHAR(100)   NULL,
        [DATOSLETTET]                    NVARCHAR(100)   NULL,
        [AFLYSTPAADAGENDATOTID]          NVARCHAR(100)   NULL,
        [TYPEHOLD]                       NVARCHAR(100)   NULL,
        [COLUMNA_AFTALEIDENTIFIKATION]   NVARCHAR(512)   NULL,
        [SKAERMNING_SKAERMET]            NVARCHAR(100)   NULL,
        [SKAERMNING_BEGRUNDELSE]         NVARCHAR(512)   NULL,
        [REKVISITIONOID]                 NVARCHAR(50)    NULL,
        [SOEGEPERIODE_FRADATO]           NVARCHAR(100)   NULL,
        [SOEGEPERIODE_TILDATO]           NVARCHAR(100)   NULL,
        [SOEGEPERIODE_PRIORITETOID]      NVARCHAR(50)    NULL,
        [SOEGEPERIODE_PRIORITET_NR]      NVARCHAR(100)   NULL,
        [SOEGEPERIODE_PRIORITET_NAVN]    NVARCHAR(512)   NULL,
        [ANGAAENDE]                      NVARCHAR(MAX)   NULL,
        [BORGERTITEL]                    NVARCHAR(MAX)   NULL,
        [BREVTEKST]                      NVARCHAR(MAX)   NULL,
        [UDFOERENDE_ORGENHEDOID]         NVARCHAR(50)    NULL,
        [HIST_STATUS]                    NVARCHAR(100)   NULL,
        [HIST_PATIENT_MOEDEDATOTID]      NVARCHAR(100)   NULL,
        [HIST_AFTALE_START_DATOTID]      NVARCHAR(100)   NULL,
        [HIST_REGISTRERET_DATOTID]       NVARCHAR(100)   NULL,
        [HIST_AARSAG_TYPE]               NVARCHAR(512)   NULL,
        [HIST_AARSAG_KODE_OID]           NVARCHAR(512)   NULL,
        [HIST_AARSAG_KODE_TEKST]         NVARCHAR(512)   NULL,
        [HIST_AARSAG_EGENTEKST]          NVARCHAR(MAX)   NULL,
        [PATIENT_CPR]                    NVARCHAR(20)    NULL,
        [UDFOERENDE_SORKODE]             NVARCHAR(512)   NULL,
        [OPGAVE_NAVN]                    NVARCHAR(512)   NULL,
        [OPGAVE_KORTNAVN]                NVARCHAR(512)   NULL,
        [OPGAVE_TITEL]                   NVARCHAR(512)   NULL,
        [OPGAVE_PRIMAERSKSKODE]          NVARCHAR(512)   NULL,
        [OPGAVETYPE_NAVN]                NVARCHAR(512)   NULL,
        [OPGAVETYPE_KORTNAVN]            NVARCHAR(512)   NULL,
        [OPGAVETYPE_SKSKODE]             NVARCHAR(512)   NULL,
        [OPGAVETYPE_KONTAKTTYPE]         NVARCHAR(100)   NULL,
        [AFVIKLINGSSTATUS_NAVN]          NVARCHAR(512)   NULL,
        [AFVIKLINGSSTATUS_KORTNAVN]      NVARCHAR(512)   NULL,
        [AFVIKLINGSSTATUS_TYPE]          NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_bom_ifdb_aftale_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_bom_ifdb_aftale]
            PRIMARY KEY CLUSTERED ([OID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_aftale_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_aftale]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_aftale_DW_Timestamp_Update]
        ON [arc].[bom_ifdb_aftale] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_aftale_LASTMODIFIED'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_aftale]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_aftale_LASTMODIFIED]
        ON [arc].[bom_ifdb_aftale] ([LASTMODIFIED] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_aftale_PATIENTINFOOID'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_aftale]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_aftale_PATIENTINFOOID]
        ON [arc].[bom_ifdb_aftale] ([PATIENTINFOOID]);
END
GO
