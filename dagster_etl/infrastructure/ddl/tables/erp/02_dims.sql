-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Dimension Tables
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes
-- + UNKNOWN sentinel rows + pre-deploy validation).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/NON_FHIR_ELT_Implementeation_Guide.md § DDL Migration Convention
-- ============================================================================

-- dim.Kontoplan
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Kontoplan](
        [DW_EK_Kontoplan] INT NOT NULL,
        [CleanCode_Kontoplan] NVARCHAR(255) NOT NULL,
        [FinanskontoId] BIGINT NULL,
        [Kontonummer] NVARCHAR(255) NULL,
        [F1] NVARCHAR(50) NULL,
        [F2] NVARCHAR(50) NULL,
        [F3] NVARCHAR(50) NULL,
        [D1] NVARCHAR(50) NULL,
        [S1] NVARCHAR(50) NULL,
        [S2] NVARCHAR(50) NULL,
        [S3] NVARCHAR(50) NULL,
        [S4] NVARCHAR(50) NULL,
        [S5] NVARCHAR(50) NULL,
        [S6] NVARCHAR(50) NULL,
        [S7] NVARCHAR(50) NULL,
        [G1] NVARCHAR(50) NULL,
        [G2] NVARCHAR(50) NULL,
        [A1] NVARCHAR(50) NULL,
        [A2] NVARCHAR(50) NULL,
        [tekst_S1] NVARCHAR(255) NULL,
        [tekst_S2] NVARCHAR(255) NULL,
        [tekst_S3] NVARCHAR(255) NULL,
        [tekst_S4] NVARCHAR(255) NULL,
        [tekst_S5] NVARCHAR(255) NULL,
        [tekst_S6] NVARCHAR(255) NULL,
        [tekst_S7] NVARCHAR(255) NULL,
        [tekst_G1] NVARCHAR(255) NULL,
        [tekst_G2] NVARCHAR(255) NULL,
        [tekst_A1] NVARCHAR(255) NULL,
        [tekst_A2] NVARCHAR(255) NULL,
        [tekst_F1] NVARCHAR(255) NULL,
        [tekst_F2] NVARCHAR(255) NULL,
        [tekst_F3] NVARCHAR(255) NULL,
        [tekst_D1] NVARCHAR(255) NULL,
        [Omkostningsstednummer] NVARCHAR(50) NULL,
        [Omkostningssted_niveau] INT NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Kontoplan] PRIMARY KEY CLUSTERED ([DW_EK_Kontoplan] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row (DW_EK = -1) - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[Kontoplan] WHERE [DW_EK_Kontoplan] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Kontoplan] (
            [DW_EK_Kontoplan], [CleanCode_Kontoplan], [FinanskontoId], [Kontonummer],
            [F1], [F2], [F3], [D1]
        ) VALUES (
            -1, N''UNKNOWN'', -1, N''UNKNOWN'',
            N''UNKNOWN'', N''UNKNOWN'', N''UNKNOWN'', N''UNKNOWN''
        )
    ');
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Kreditor](
        [DW_EK_Kreditor] INT NOT NULL,
        [CleanCode_Kreditor] NVARCHAR(255) NOT NULL,
        [KreditorId] BIGINT NULL,
        [Kreditorkonto] NVARCHAR(50) NULL,
        [Cvrnr] NVARCHAR(20) NULL,
        [Cvrnr_Ils] NVARCHAR(50) NULL,
        [Cvrnr_MySupply] NVARCHAR(50) NULL,
        [Cvrnr_SydAX] NVARCHAR(50) NULL,
        [KreditorNavn] NVARCHAR(255) NULL,
        [ByNavn] NVARCHAR(255) NULL,
        [Valutakode] NVARCHAR(10) NULL,
        [SprogId] NVARCHAR(10) NULL,
        [BrancheId] NVARCHAR(20) NULL,
        [Vejnavn] NVARCHAR(255) NULL,
        [Postnummer] NVARCHAR(20) NULL,
        [ILS_Kreditor_Navn] NVARCHAR(255) NULL,
        [ILS_Adresse1] NVARCHAR(255) NULL,
        [ILS_Adresse2] NVARCHAR(255) NULL,
        [ILS_Postdistrikt] NVARCHAR(255) NULL,
        [ILS_ByNavn] NVARCHAR(255) NULL,
        [ILS_Kontakt] NVARCHAR(255) NULL,
        [Kreditornummer] NVARCHAR(50) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Kreditor] PRIMARY KEY CLUSTERED ([DW_EK_Kreditor] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[Kreditor] WHERE [DW_EK_Kreditor] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Kreditor] (
            [DW_EK_Kreditor], [CleanCode_Kreditor], [KreditorId], [Cvrnr],
            [KreditorNavn], [ByNavn], [Valutakode], [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'', -1, ''UNKNOWN'',
            ''Ukendt kreditor'', ''UNKNOWN'', ''UNK'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Vare](
        [DW_EK_Vare] INT NOT NULL,
        [CVR] NVARCHAR(255) NULL,
        [VarenummerKreditor] NVARCHAR(255) NULL,
        [Varenavn] NVARCHAR(MAX) NULL,
        [VarenavnKort] NVARCHAR(MAX) NULL,
        [Maerke] NVARCHAR(255) NULL,
        [ILS_Varenummer] NVARCHAR(100) NULL,
        [FaellesVarenummer] NVARCHAR(100) NULL,
        [Stregkode] NVARCHAR(50) NULL,
        [StoerrelseTekst] NVARCHAR(255) NULL,
        [SammensatNavn] NVARCHAR(255) NULL,
        [Basisenhed] NVARCHAR(50) NULL,
        [Pakkestoerrelse1] DECIMAL(18,4) NULL,
        [Pakkestoerrelse2] DECIMAL(18,4) NULL,
        [Pakkestoerrelse3] DECIMAL(18,4) NULL,
        [VareartKode] NVARCHAR(50) NULL,
        [VareartNavn] NVARCHAR(255) NULL,
        [VaregruppeKode] NVARCHAR(50) NULL,
        [VaregruppeNavn] NVARCHAR(255) NULL,
        [Varetype] NVARCHAR(50) NULL,
        [Aftale] NVARCHAR(100) NULL,
        [AftaleType] NVARCHAR(50) NULL,
        [Aftaletekst] NVARCHAR(MAX) NULL,
        [Kreditorvarenummer1] NVARCHAR(100) NULL,
        [Kreditorvarenummer2] NVARCHAR(100) NULL,
        [Kreditorvarenummer3] NVARCHAR(100) NULL,
        [Pris1] DECIMAL(18,4) NULL,
        [Pris2] DECIMAL(18,4) NULL,
        [Pris3] DECIMAL(18,4) NULL,
        [ils_dsa_aftale_varenummer] NVARCHAR(100) NULL,
        [Kreditornummer] NVARCHAR(50) NULL,
        [DW_EK_Aftale] INT NOT NULL DEFAULT -1,
        [DW_EK_IlsVare] INT NOT NULL DEFAULT -1,
        [momskd] NVARCHAR(50) NULL,
        [afgiftkd] NVARCHAR(50) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Vare] PRIMARY KEY CLUSTERED ([DW_EK_Vare] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[Vare] WHERE [DW_EK_Vare] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Vare] (
            [DW_EK_Vare], [CVR], [VarenummerKreditor], [Varenavn], [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'', ''UNKNOWN'', ''Ukendt vare'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'IX_Dim_Vare_IlsVare'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_Vare_IlsVare]
    ON [dim].[Vare] ([DW_EK_IlsVare]) ON [PRIMARY];
END
GO

-- dim.Aftale (Type 2 SCD)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Aftale](
        [DW_EK_Aftale] INT NOT NULL,
        [CleanCode_Aftale] NVARCHAR(255) NOT NULL,
        [AftaleNummer] NVARCHAR(100) NULL,
        [AftaleType] NVARCHAR(50) NULL,
        [Aftaletekst] NVARCHAR(MAX) NULL,
        [HovedStart] DATETIME2 NULL,
        [HovedSlut] DATETIME2 NULL,
        [DW_ValidFrom] DATETIME2(0) NOT NULL,
        [DW_ValidTo] DATETIME2(0) NOT NULL DEFAULT '9999-12-31',
        [DW_IsDeleted] BIT NOT NULL DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Aftale] PRIMARY KEY CLUSTERED ([DW_EK_Aftale] ASC, [DW_ValidFrom] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'IX_Dim_Aftale_Current')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_Aftale_Current] ON [dim].[Aftale]
    ([DW_EK_Aftale] ASC)
    INCLUDE ([AftaleNummer], [AftaleType], [Aftaletekst])
    WHERE [DW_IsDeleted] = 0 AND [DW_ValidTo] = '9999-12-31';
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (
    SELECT 1
    FROM [dim].[Aftale]
    WHERE [DW_EK_Aftale] = -1 AND [DW_ValidFrom] = '1900-01-01' AND [DW_ValidTo] = '9999-12-31'
)
BEGIN
    EXEC('
        INSERT INTO [dim].[Aftale] (
            [DW_EK_Aftale], [CleanCode_Aftale], [AftaleNummer],
            [DW_ValidFrom], [DW_ValidTo], [DW_IsDeleted], [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'', N''UNKNOWN'',
            ''1900-01-01'', ''9999-12-31'', 0, CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.Faktura (header grain — re-grained from FakturaEgenskaber)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Faktura](
        [DW_EK_Faktura] INT NOT NULL,
        [FakturaNr] NVARCHAR(100) NULL,
        [FakturaValutaKode] NVARCHAR(10) NULL,
        [FirmaNavn] NVARCHAR(255) NULL,
        [LeverandorCVR] NVARCHAR(50) NULL,
        [Adresse] NVARCHAR(255) NULL,
        [ByNavn] NVARCHAR(255) NULL,
        [Postnummer] NVARCHAR(20) NULL,
        [DokumentType] NVARCHAR(20) NULL,
        [RegningsModtagerID] NVARCHAR(MAX) NULL,
        [RegningsModtagerID_Type] NVARCHAR(MAX) NULL,
        [KøbsOrdreNummer] NVARCHAR(MAX) NULL,
        [SalgsOrdreNummer] NVARCHAR(MAX) NULL,
        [KundeIdentification] NVARCHAR(MAX) NULL,
        [KundeIdentification_Type] NVARCHAR(MAX) NULL,
        [DimensionKontoStreng] NVARCHAR(MAX) NULL,
        [KundeNavn] NVARCHAR(MAX) NULL,
        [KundeVejNavn] NVARCHAR(MAX) NULL,
        [KundeHusNr] NVARCHAR(MAX) NULL,
        [KundeByNavn] NVARCHAR(MAX) NULL,
        [KundePostNr] NVARCHAR(MAX) NULL,
        [KundeLandeKode] NVARCHAR(MAX) NULL,
        [KundeReferenceID] NVARCHAR(MAX) NULL,
        [KundeReferenceNavn] NVARCHAR(MAX) NULL,
        [KundeReferenceTelefonNr] NVARCHAR(MAX) NULL,
        [KundeReferenceFaxNr] NVARCHAR(MAX) NULL,
        [KundeReferenceEMail] NVARCHAR(MAX) NULL,
        [KundeReferenceRolle] NVARCHAR(MAX) NULL,
        [Juridisk_KundeIdentification] NVARCHAR(MAX) NULL,
        [Juridisk_KundeIdentification_Type] NVARCHAR(MAX) NULL,
        [Juridisk_DimensionKontoStreng] NVARCHAR(MAX) NULL,
        [Juridisk_KundeNavn] NVARCHAR(MAX) NULL,
        [Juridisk_KundeVejNavn] NVARCHAR(MAX) NULL,
        [Juridisk_KundeHusNr] NVARCHAR(MAX) NULL,
        [Juridisk_KundeByNavn] NVARCHAR(MAX) NULL,
        [Juridisk_KundePostNr] NVARCHAR(MAX) NULL,
        [Juridisk_KundeLandeKode] NVARCHAR(MAX) NULL,
        [Juridisk_KundeReferenceID] NVARCHAR(MAX) NULL,
        [Juridisk_KundeReferenceNavn] NVARCHAR(MAX) NULL,
        [Juridisk_KundeReferenceTelefonNr] NVARCHAR(MAX) NULL,
        [Juridisk_KundeReferenceFaxNr] NVARCHAR(MAX) NULL,
        [Juridisk_KundeReferenceEMail] NVARCHAR(MAX) NULL,
        [Juridisk_KundeReferenceRolle] NVARCHAR(MAX) NULL,
        [FakturaUdstederIdentifikation] NVARCHAR(MAX) NULL,
        [FakturaUdstederIdentifikation_Type] NVARCHAR(MAX) NULL,
        [FakturaUdstederNavn] NVARCHAR(MAX) NULL,
        [FakturaUdstederAdresseID_Vareafsendelse] NVARCHAR(MAX) NULL,
        [FakturaUdstederVejNavn_Vareafsendelse] NVARCHAR(MAX) NULL,
        [FakturaUdstederHusNr_Vareafsendelse] NVARCHAR(MAX) NULL,
        [FakturaUdstederByNavn_Vareafsendelse] NVARCHAR(MAX) NULL,
        [FakturaUdstederPostNr_Vareafsendelse] NVARCHAR(MAX) NULL,
        [FakturaUdstederLandKode_Vareafsendelse] NVARCHAR(MAX) NULL,
        [FakturaUdstederCVRNummer] NVARCHAR(MAX) NULL,
        [FakturaUdstederCVRNummer_Type] NVARCHAR(MAX) NULL,
        [FakturaUdstederReferenceIdenrifikation] NVARCHAR(MAX) NULL,
        [FakturaUdstederReferenceNavn] NVARCHAR(MAX) NULL,
        [FakturaUdstederReferenceTelefonNr] NVARCHAR(MAX) NULL,
        [FakturaUdstederReferenceFaxNr] NVARCHAR(MAX) NULL,
        [FakturaUdstederReferenceEMail] NVARCHAR(MAX) NULL,
        [KortArtsKode] NVARCHAR(MAX) NULL,
        [BetalingsType] NVARCHAR(MAX) NULL,
        [BetalingsID] NVARCHAR(MAX) NULL,
        [Kreditnummer] NVARCHAR(MAX) NULL,
        [Kontonummer] NVARCHAR(MAX) NULL,
        [Kontotype] NVARCHAR(MAX) NULL,
        [AfdelingsID] NVARCHAR(MAX) NULL,
        [BankID] NVARCHAR(MAX) NULL,
        [BankNavn] NVARCHAR(MAX) NULL,
        [ToldKurs] NVARCHAR(MAX) NULL,
        [ToldKurs_Type] NVARCHAR(MAX) NULL,
        [Kontantrabatsats] NVARCHAR(MAX) NULL,
        [Strafrentesats] NVARCHAR(MAX) NULL,
        [KontantRabatDato] NVARCHAR(MAX) NULL,
        [StartRenteDato] NVARCHAR(MAX) NULL,
        [ZeroRated_AfgiftsKategori] NVARCHAR(MAX) NULL,
        [ZeroRated_AfgiftsKategoriProcent] NVARCHAR(MAX) NULL,
        [GebyrRabatAarsag] NVARCHAR(MAX) NULL,
        [GebyrRabatKategori] NVARCHAR(MAX) NULL,
        [GebyrRabatFinanskonto] NVARCHAR(MAX) NULL,
        [GebyrTillaegAarsag] NVARCHAR(MAX) NULL,
        [GebyrTillaegKategori] NVARCHAR(MAX) NULL,
        [GebyrTillaegFinanskonto] NVARCHAR(MAX) NULL,
        -- Header identity carried from MySupply staging so the faktura<->postering
        -- bridge can rank the "newest faktura per postering" off persistent tables
        -- (Header_DW_LoadTimestamp DESC, statistic_invoice_header_id DESC) without
        -- joining the volatile stg.dim_faktura_mysupply at bridge build time.
        [statistic_invoice_header_id] BIGINT NULL,
        [Header_DW_LoadTimestamp] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Faktura] PRIMARY KEY CLUSTERED ([DW_EK_Faktura] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'IX_Dim_Faktura_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Faktura_DW_EK] ON [dim].[Faktura]
    ([DW_EK_Faktura] ASC) ON [PRIMARY];
END
GO

-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (SELECT 1 FROM [dim].[Faktura] WHERE [DW_EK_Faktura] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Faktura] (
            [DW_EK_Faktura], [FakturaNr], [FakturaValutaKode], [FirmaNavn],
            [LeverandorCVR], [Adresse], [ByNavn], [Postnummer], [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'', N''UNKNOWN'', N''UNKNOWN'',
            N''UNKNOWN'', N''UNKNOWN'', N''UNKNOWN'', N''UNKNOWN'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.Fakturalinje
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Fakturalinje](
        [DW_EK_Fakturalinje] INT NOT NULL,
        [Item_FakturaUdstederVareNr] NVARCHAR(255) NULL,
        [Item_FakturaUdstederVareNavn] NVARCHAR(MAX) NULL,
        [FakturaLinjeNr] NVARCHAR(MAX) NULL,
        [AntalEnhederFaktureret_Type] NVARCHAR(MAX) NULL,
        [Item_FakturaUdstederensVareNr] NVARCHAR(MAX) NULL,
        [Item_FakturaUdstederensVareNr_Type] NVARCHAR(MAX) NULL,
        [BasePrice_EnhedsStørrelse] NVARCHAR(MAX) NULL,
        [BasePrice_EnhedsStørrelse_Type] NVARCHAR(MAX) NULL,
        [Item_FakturaUdstederVareNavnKort] NVARCHAR(MAX) NULL,
        [Line_DimensionsKontoStreng] NVARCHAR(MAX) NULL,
        [LeveranceKrav] NVARCHAR(MAX) NULL,
        [LeveringsDato_2] NVARCHAR(MAX) NULL,
        [Item_KundensVareNr] NVARCHAR(MAX) NULL,
        [Item_KundensVareNr_Type] NVARCHAR(MAX) NULL,
        [Item_KundensVareBeskrivelse] NVARCHAR(MAX) NULL,
        [Item_FakturaUdstederensVareBeskrivelse] NVARCHAR(MAX) NULL,
        [Item_FabrikantensVareNr] NVARCHAR(MAX) NULL,
        [Item_FabrikantensVareNr_Type] NVARCHAR(MAX) NULL,
        [Item_FabrikantensVareBeskrivelse] NVARCHAR(MAX) NULL,
        [Item_StandardiseretVareNr] NVARCHAR(MAX) NULL,
        [Item_StandardiseretVareNr_Type] NVARCHAR(MAX) NULL,
        [Item_StandardiseretVareBeskrivelse] NVARCHAR(MAX) NULL,
        [Item_VareKlassifikation] NVARCHAR(MAX) NULL,
        [Item_VareKlassifikation_Type] NVARCHAR(MAX) NULL,
        [Item_AfgiftsKategori] NVARCHAR(MAX) NULL,
        [Item_AfgiftsKategoriProcent] NVARCHAR(MAX) NULL,
        [FakturaUdstederGebyrRabatAarsag] NVARCHAR(MAX) NULL,
        [FakturaUdstederGebyrRabatKategori] NVARCHAR(MAX) NULL,
        [FakturaUdstederGebyrRabatFinanskonto] NVARCHAR(MAX) NULL,
        [FakturaUdstederGebyrTillaegAarsag] NVARCHAR(MAX) NULL,
        [FakturaUdstederGebyrTillaegKategori] NVARCHAR(MAX) NULL,
        [FakturaUdstederGebyrTillaegFinanskonto] NVARCHAR(MAX) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Fakturalinje] PRIMARY KEY CLUSTERED ([DW_EK_Fakturalinje] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = 'IX_Dim_Fakturalinje_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Fakturalinje_DW_EK] ON [dim].[Fakturalinje]
    ([DW_EK_Fakturalinje] ASC) ON [PRIMARY];
END
GO

-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (SELECT 1 FROM [dim].[Fakturalinje] WHERE [DW_EK_Fakturalinje] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Fakturalinje] (
            [DW_EK_Fakturalinje], [Item_FakturaUdstederVareNr], [Item_FakturaUdstederVareNavn], [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'', N''UNKNOWN'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.Postering
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Postering](
        [DW_EK_Postering] INT NOT NULL,
        [ref_LedgertransId] BIGINT NULL,
        [bk_ref_LedgertransId] BIGINT NULL,
        [ref_VendTransId] BIGINT NULL,
        [DokumentId] BIGINT NULL,
        [Kreditornummer] NVARCHAR(50) NULL,
        [Kreditornavn] NVARCHAR(255) NULL,
        [JuridiskEnhed] NVARCHAR(255) NULL,
        [Kontonummer] NVARCHAR(255) NULL,
        [FakturaNr] NVARCHAR(100) NULL,
        [SidstUdlignetBilagsnummer] NVARCHAR(100) NULL,
        [Bilagsnummer_Trans] NVARCHAR(100) NULL,
        [Bilagsnummer_Postering] NVARCHAR(100) NULL,
        [Valutakode_Trans] NVARCHAR(10) NULL,
        [Valutakode_Postering] NVARCHAR(10) NULL,
        [Transaktionstekst] NVARCHAR(MAX) NULL,
        [Posteringstekst] NVARCHAR(MAX) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Postering] PRIMARY KEY CLUSTERED ([DW_EK_Postering] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];

    -- Insert UNKNOWN row (DW_EK = -1)
    INSERT INTO [dim].[Postering] (
        [DW_EK_Postering], [DW_Timestamp_Insert]
    ) VALUES (
        -1, CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO

-- Ensure UNKNOWN row exists even when table predates sentinel seed logic
IF NOT EXISTS (SELECT 1 FROM [dim].[Postering] WHERE [DW_EK_Postering] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Postering] (
            [DW_EK_Postering], [DW_Timestamp_Insert], [DW_Timestamp_Update]
        ) VALUES (
            -1, CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.VareudleveringEgenskaber (1:1 with fct.Vareudlevering)
-- Source: ILS dbo.rvish -- revision/foelgeseddel header for delivery lines
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[VareudleveringEgenskaber](
        [DW_EK_VareudleveringEgenskaber] INT NOT NULL,
        [CleanCode_VareudleveringEgenskaber] NVARCHAR(500) NULL,
        [Rekvisitionsnummer] INT NULL,
        [Foelgeseddelnummer] NVARCHAR(100) NULL,
        [Brik] NVARCHAR(100) NULL,
        [Leverandoernummer] NVARCHAR(100) NULL,
        [Ordrenummer] NVARCHAR(100) NULL,
        [Ordrelinje] NVARCHAR(100) NULL,
        [sw_direkte_udlevering] NVARCHAR(10) NULL,
        [sw_pakkeseddel] NVARCHAR(10) NULL,
        [sw_foelgeseddel] NVARCHAR(10) NULL,
        [sw_fly] NVARCHAR(10) NULL,
        [sw_haster] NVARCHAR(10) NULL,
        [sw_erstat] NVARCHAR(10) NULL,
        [Projektnummer] NVARCHAR(100) NULL,
        [Kontonummer_kode] NVARCHAR(100) NULL,
        [Bruger] NVARCHAR(100) NULL,
        [bk_prrowid] NVARCHAR(100) NULL,
        [Referencenummer] NVARCHAR(100) NULL,
        [Referencelinje] NVARCHAR(100) NULL,
        [Salgsordre] NVARCHAR(100) NULL,
        [SalgsordreId] NVARCHAR(100) NULL,
        [Salgsfakturanummer] NVARCHAR(100) NULL,
        [RekvisitionsvareId] NVARCHAR(100) NULL,
        [PlukvarerId] NVARCHAR(100) NULL,
        [Afleveringsoperatoer] NVARCHAR(100) NULL,
        [Afleveringstekst] NVARCHAR(100) NULL,
        [Afleveringsstregkode] NVARCHAR(100) NULL,
        [Vognnummer] NVARCHAR(100) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_VareudleveringEgenskaber] PRIMARY KEY CLUSTERED ([DW_EK_VareudleveringEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_VareudleveringEgenskaber_CleanCode] ON [dim].[VareudleveringEgenskaber]
    ([CleanCode_VareudleveringEgenskaber] ASC) WHERE [CleanCode_VareudleveringEgenskaber] IS NOT NULL ON [PRIMARY];
END
GO

-- Ensure UNKNOWN row exists even when table predates sentinel seed logic
IF NOT EXISTS (
    SELECT 1 FROM [dim].[VareudleveringEgenskaber] WHERE [DW_EK_VareudleveringEgenskaber] = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[VareudleveringEgenskaber] (
            [DW_EK_VareudleveringEgenskaber], [CleanCode_VareudleveringEgenskaber], [DW_Timestamp_Insert], [DW_Timestamp_Update]
        ) VALUES (
            -1, N''UNKNOWN'', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.Lager
-- Hierarchy and SOR mapping columns for ILS Lager codesystem (REGION_SYD -> SGH -> LGR)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[Lager](
        [DW_EK_Lager] [int] PRIMARY KEY,
        [Code] [varchar](100) NOT NULL,
        [Display] [nvarchar](500),
        [ParentCode] [varchar](100),
        [Level] [int],
        [EntityType] [nvarchar](200),
        [SGH] [nvarchar](200),
        [LGR] [nvarchar](200),
        [Adr] [nvarchar](200),
        [Land] [nvarchar](200),
        [Postdi] [nvarchar](200),
        [Tlf] [nvarchar](200),
        [CodeSOR] [varchar](50),
        [DW_EK_Organisation] [int] DEFAULT -1,
        [DW_EK_Lager_Parent] [int] DEFAULT -1,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT SYSDATETIME()
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'IX_dim_Lager_Code')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U')
BEGIN
    CREATE INDEX IX_dim_Lager_Code ON [dim].[Lager]([Code]);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'IX_dim_Lager_ParentEK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U')
BEGIN
    CREATE INDEX IX_dim_Lager_ParentEK
        ON [dim].[Lager]([DW_EK_Lager_Parent])
        WHERE [DW_EK_Lager_Parent] IS NOT NULL
          AND [DW_EK_Lager_Parent] <> -1;
END
GO

-- Sentinel row for "Unknown Lager" (DW_EK = -1)
-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (
    SELECT 1 FROM [dim].[Lager] WHERE DW_EK_Lager = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[Lager] (
            DW_EK_Lager, Code, Display, [Level], EntityType,
            DW_EK_Organisation, DW_EK_Lager_Parent,
            DW_Timestamp_Insert, DW_Timestamp_Update
        )
        VALUES (
            -1, ''UNKNOWN'', ''Unknown Lager'', 0, ''unknown'',
            -1, -1,
            SYSDATETIME(), SYSDATETIME()
        )
    ');
END
GO

-- dim.Rekvirent
-- Hierarchy and SOR mapping columns for ILS Rekvirent codesystem (REGION_SYD -> SGH -> RGRP -> RTYP -> RNR)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[Rekvirent](
        [DW_EK_Rekvirent] [int] PRIMARY KEY,
        [Code] [varchar](100) NOT NULL,
        [Display] [nvarchar](500),
        [ParentCode] [varchar](100),
        [Level] [int],
        [EntityType] [nvarchar](200),
        [SGH] [nvarchar](200),
        [RGRP] [nvarchar](200),
        [RTYP] [nvarchar](200),
        [RNR] [nvarchar](200),
        [CodeSOR] [varchar](50),
        [SORRelationship] [varchar](50),
        [DW_EK_Organisation] [int] DEFAULT -1,
        [DW_EK_Rekvirent_Parent] [int] DEFAULT -1,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT SYSDATETIME()
    ) ON [PRIMARY];

    CREATE INDEX IX_dim_Rekvirent_Code
        ON [dim].[Rekvirent]([Code]);

    CREATE INDEX IX_dim_Rekvirent_ParentEK
        ON [dim].[Rekvirent]([DW_EK_Rekvirent_Parent])
        WHERE [DW_EK_Rekvirent_Parent] IS NOT NULL
          AND [DW_EK_Rekvirent_Parent] <> -1;
END
GO

-- Sentinel row for "Unknown Rekvirent" (DW_EK = -1)
-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (
    SELECT 1 FROM [dim].[Rekvirent] WHERE DW_EK_Rekvirent = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[Rekvirent] (
            DW_EK_Rekvirent, Code, Display, [Level], EntityType,
            DW_EK_Organisation, DW_EK_Rekvirent_Parent,
            DW_Timestamp_Insert, DW_Timestamp_Update
        )
        VALUES (
            -1, ''UNKNOWN'', ''Unknown Rekvirent'', 0, ''unknown'',
            -1, -1,
            SYSDATETIME(), SYSDATETIME()
        )
    ');
END
GO

-- dim.LagerbeholdningEgenskaber (Type 1 SCD — warehouse stock attributes)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[LagerbeholdningEgenskaber] (
        [DW_EK_LagerbeholdningEgenskaber] INT NOT NULL,
        [CleanCode_LagerbeholdningEgenskaber] NVARCHAR(500) NULL,
        [Lnr] NVARCHAR(32) NULL,
        [Lgrlok] NVARCHAR(32) NULL,
        [Udlevenh] NVARCHAR(80) NULL,
        [Sw_erstat] BIT NULL,
        [Lgrbeh_prio] INT NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_LagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([DW_EK_LagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_LagerbeholdningEgenskaber_CleanCode]
    ON [dim].[LagerbeholdningEgenskaber] ([CleanCode_LagerbeholdningEgenskaber] ASC)
    WHERE [CleanCode_LagerbeholdningEgenskaber] IS NOT NULL ON [PRIMARY];

    PRINT 'Created dim.LagerbeholdningEgenskaber';
END
GO

-- Sentinel row for "Unknown LagerbeholdningEgenskaber" (DW_EK = -1)
IF NOT EXISTS (
    SELECT 1 FROM [dim].[LagerbeholdningEgenskaber]
    WHERE [DW_EK_LagerbeholdningEgenskaber] = -1
)
BEGIN
    INSERT INTO [dim].[LagerbeholdningEgenskaber] (
        [DW_EK_LagerbeholdningEgenskaber],
        [CleanCode_LagerbeholdningEgenskaber],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    ) VALUES (
        -1, N'UNKNOWN',
        GETDATE(), GETDATE()
    );
    PRINT 'Inserted UNKNOWN sentinel for dim.LagerbeholdningEgenskaber (DW_EK = -1)';
END
GO

-- dim.AfdelingLagerbeholdningEgenskaber (Type 1 SCD — department stock attributes)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[AfdelingLagerbeholdningEgenskaber] (
        [DW_EK_AfdelingLagerbeholdningEgenskaber] INT NOT NULL,
        [CleanCode_AfdelingLagerbeholdningEgenskaber] NVARCHAR(500) NULL,
        [Lnr] NVARCHAR(32) NULL,
        [Brik] INT NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_AfdelingLagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([DW_EK_AfdelingLagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_AfdelingLagerbeholdningEgenskaber_CleanCode]
    ON [dim].[AfdelingLagerbeholdningEgenskaber] ([CleanCode_AfdelingLagerbeholdningEgenskaber] ASC)
    WHERE [CleanCode_AfdelingLagerbeholdningEgenskaber] IS NOT NULL ON [PRIMARY];

    PRINT 'Created dim.AfdelingLagerbeholdningEgenskaber';
END
GO

-- Sentinel row for "Unknown AfdelingLagerbeholdningEgenskaber" (DW_EK = -1)
IF NOT EXISTS (
    SELECT 1 FROM [dim].[AfdelingLagerbeholdningEgenskaber]
    WHERE [DW_EK_AfdelingLagerbeholdningEgenskaber] = -1
)
BEGIN
    INSERT INTO [dim].[AfdelingLagerbeholdningEgenskaber] (
        [DW_EK_AfdelingLagerbeholdningEgenskaber],
        [CleanCode_AfdelingLagerbeholdningEgenskaber],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    ) VALUES (
        -1, N'UNKNOWN',
        GETDATE(), GETDATE()
    );
    PRINT 'Inserted UNKNOWN sentinel for dim.AfdelingLagerbeholdningEgenskaber (DW_EK = -1)';
END
GO

-- dim.EAN
-- EAN dimension with EAN number and name from NemHandelsregisteret
-- Flat structure (no hierarchy, no parent)
-- Type 1 SCD (update in place)
-- Rows driven by MySupply invoice data, enriched with NemHandelsregisteret
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[EAN]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[EAN](
        [DW_EK_EAN] [int] PRIMARY KEY,
        [EAN] [nvarchar](50) NOT NULL,
        [Navn_Nemhandelsregister] [nvarchar](512),
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE()
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[EAN]') AND name = 'IX_dim_EAN_EAN')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[EAN]') AND type = 'U')
BEGIN
    CREATE INDEX IX_dim_EAN_EAN ON [dim].[EAN]([EAN]);
END
GO

-- Sentinel row for "Unknown EAN" (DW_EK = -1)
-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (
    SELECT 1 FROM [dim].[EAN] WHERE DW_EK_EAN = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[EAN] (
            DW_EK_EAN, EAN,
            DW_Timestamp_Insert, DW_Timestamp_Update
        )
        VALUES (
            -1, ''UNKNOWN'',
            GETDATE(), GETDATE()
        )
    ');
END
GO

-- dim.IlsVare
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND type = 'U'
) BEGIN
CREATE TABLE [dim].[IlsVare] (
    [DW_EK_IlsVare]        INT            NOT NULL,
    [CleanCode_IlsVare]    NVARCHAR(100)  NULL,
    [Varenummer]            NVARCHAR(100)  NULL,
    [Varenavn]              NVARCHAR(MAX)  NULL,
    [Varenavn2]             NVARCHAR(MAX)  NULL,
    [Varestr]               NVARCHAR(255)  NULL,
    [Basisenhed]            NVARCHAR(50)   NULL,
    [Gruppering]            NVARCHAR(50)   NULL,
    [Gruppering_Navn]       NVARCHAR(255)  NULL,
    [Art]                   NVARCHAR(50)   NULL,
    [Art_Navn]              NVARCHAR(255)  NULL,
    [Varetype]              NVARCHAR(50)   NULL,
    [Varetype_Navn]         NVARCHAR(255)  NULL,
    [Momskode]              NVARCHAR(50)   NULL,
    [Momskode_Navn]         NVARCHAR(255)  NULL,
    [Momsprocent]           DECIMAL(18,4)  NULL,
    [DW_EK_Dato_Oprettet]  INT            NOT NULL DEFAULT -1,
    [Datotid_Oprettet]      DATETIME2      NULL,
    [DW_EK_UNSPSC]          INT            NOT NULL DEFAULT -1,
    [DW_EK_UNSPSC_Intern]   INT            NOT NULL DEFAULT -1,
    [Aktiv]                 BIT            NULL,
    [FaellesVarenummer]     NVARCHAR(100)  NULL,
    [EAN_Varenummer]        NVARCHAR(100)  NULL,
    [Billede]               NVARCHAR(MAX)  NULL,
    [OprBruger]             NVARCHAR(50)   NULL,
    [OprBruger_Navn]        NVARCHAR(255)  NULL,
    [Weblink]               NVARCHAR(MAX)  NULL,
    [MaaIkkeLukkes]         NVARCHAR(50)   NULL,
    [Hygiejne]              NVARCHAR(50)   NULL,
    [DW_Timestamp_Insert]   DATETIME2      NULL,
    [DW_Timestamp_Update]   DATETIME2      NULL,
    CONSTRAINT [PK_Dim_IlsVare] PRIMARY KEY CLUSTERED ([DW_EK_IlsVare] ASC)
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'IX_Dim_IlsVare_CleanCode'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_IlsVare_CleanCode]
    ON [dim].[IlsVare] ([CleanCode_IlsVare] ASC)
    WHERE [CleanCode_IlsVare] IS NOT NULL
    ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'IX_Dim_IlsVare_UNSPSC'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_IlsVare_UNSPSC]
    ON [dim].[IlsVare] ([DW_EK_UNSPSC] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'IX_Dim_IlsVare_UNSPSC_Intern'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_IlsVare_UNSPSC_Intern]
    ON [dim].[IlsVare] ([DW_EK_UNSPSC_Intern] ASC) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row (DW_EK = -1) - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[IlsVare] WHERE [DW_EK_IlsVare] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[IlsVare] (
            [DW_EK_IlsVare], [CleanCode_IlsVare], [Varenummer], [Varenavn], [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'', ''UNKNOWN'', ''Ukendt ILS-vare'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.Finanskladde
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Finanskladde](
        [DW_EK_Finanskladde] INT NOT NULL,
        [bk_ref_LedgerjournaltableId] NVARCHAR(100) NULL,
        [Kladdenummer] NVARCHAR(20) NULL,
        [Kladdetekst] NVARCHAR(1000) NULL,
        [Kladdenavn] NVARCHAR(100) NULL,
        [ErLukket] BIT NULL,
        [DataAreaId] NVARCHAR(20) NULL,
        [Maskinnummer] NVARCHAR(20) NULL,
        [DW_EK_Dato_Oprettet] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Aendret] INT NOT NULL DEFAULT -1,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Finanskladde] PRIMARY KEY CLUSTERED ([DW_EK_Finanskladde] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM [dim].[Finanskladde] WHERE [DW_EK_Finanskladde] = -1)
    INSERT INTO [dim].[Finanskladde] (
        [DW_EK_Finanskladde],
        [bk_ref_LedgerjournaltableId],
        [Kladdenummer], [Kladdetekst], [Kladdenavn],
        [DataAreaId], [Maskinnummer],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    ) VALUES (
        -1,
        N'UNKNOWN',
        N'UNKNOWN', N'UNKNOWN', N'UNKNOWN',
        N'UNKNOWN', N'UNKNOWN',
        CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
GO

-- dim.Finanskladdelinje
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Finanskladdelinje](
        [DW_EK_Finanskladdelinje] INT NOT NULL,
        [bk_ref_LedgertransId] NVARCHAR(100) NULL,
        [Beskrivelse] NVARCHAR(100) NULL,
        [Linjenummer] DECIMAL(32,16) NULL,
        [Ekspeditionsnummer] NVARCHAR(7) NULL,
        [Transaktionstypenummer] INT NULL,
        [Transaktionstypetekst] NVARCHAR(1000) NULL,
        [Maskinnummer] NVARCHAR(20) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_Finanskladdelinje] PRIMARY KEY CLUSTERED ([DW_EK_Finanskladdelinje] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM [dim].[Finanskladdelinje] WHERE [DW_EK_Finanskladdelinje] = -1)
    INSERT INTO [dim].[Finanskladdelinje] (
        [DW_EK_Finanskladdelinje],
        [bk_ref_LedgertransId],
        [Beskrivelse], [Ekspeditionsnummer],
        [Transaktionstypetekst], [Maskinnummer],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    ) VALUES (
        -1,
        N'UNKNOWN',
        N'UNKNOWN', N'UNKNOWN',
        N'UNKNOWN', N'UNKNOWN',
        CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
GO


-- dim.BudgetEgenskaber
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[BudgetEgenskaber](
        [DW_EK_BudgetEgenskaber] INT NOT NULL,
        [bk_ref_BudgetmodelId] NVARCHAR(100) NULL,
        [Budgetmodelnummer] NVARCHAR(100) NULL,
        [Budgetmodeltekst] NVARCHAR(255) NULL,
        [Budgetmodeltype] NVARCHAR(255) NULL,
        [ErLukket] BIT NULL,
        [Valutakode] NVARCHAR(10) NULL,
        [DebetKredit] NVARCHAR(20) NULL,
        [Budgetkode] NVARCHAR(50) NULL,
        [Aendringstype] NVARCHAR(50) NULL,
        [AendringstypeBeskrivelse] NVARCHAR(255) NULL,
        [BeloebstypeId] NVARCHAR(50) NULL,
        [BudgetbeloebTypetekst] NVARCHAR(1000) NULL,
        [BudgetpostStatusId] NVARCHAR(50) NULL,
        [BudgetpostStatusTekst] NVARCHAR(255) NULL,
        [Omkostningsstednummer] NVARCHAR(50) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_BudgetEgenskaber] PRIMARY KEY CLUSTERED ([DW_EK_BudgetEgenskaber] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[BudgetEgenskaber] WHERE [DW_EK_BudgetEgenskaber] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[BudgetEgenskaber] (
            [DW_EK_BudgetEgenskaber], [bk_ref_BudgetmodelId],
            [Budgetmodelnummer], [Budgetmodeltekst],
            [BudgetbeloebTypetekst], [BudgetpostStatusTekst],
            [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'', N''UNKNOWN'',
            N''Ukendt budgetmodel'',
            N''UNKNOWN'', N''UNKNOWN'',
            CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.UNSPSC
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[UNSPSC] (
        [DW_EK_UNSPSC]                       INT           NOT NULL,
        [UNSPSC_Kode]                        NVARCHAR(50)  NULL,
        [Source_ILS]                         BIT           NOT NULL,
        [Source_KMD]                         BIT           NOT NULL,
        [Beskrivelse_ILS]                    NVARCHAR(255) NULL,
        [Beskrivelse_engelsk_ILS]            NVARCHAR(255) NULL,
        [Beskrivelse_KMD]                    NVARCHAR(400) NULL,
        [UNSPSC_Level_KMD]                   SMALLINT      NULL,
        [UNSPSC_Level_1_Kode_KMD]            NVARCHAR(50)  NULL,
        [UNSPSC_Level_1_Beskrivelse_KMD]     NVARCHAR(400) NULL,
        [UNSPSC_Level_2_Kode_KMD]            NVARCHAR(50)  NULL,
        [UNSPSC_Level_2_Beskrivelse_KMD]     NVARCHAR(400) NULL,
        [UNSPSC_Level_3_Kode_KMD]            NVARCHAR(50)  NULL,
        [UNSPSC_Level_3_Beskrivelse_KMD]     NVARCHAR(400) NULL,
        [UNSPSC_Level_4_Kode_KMD]            NVARCHAR(50)  NULL,
        [UNSPSC_Level_4_Beskrivelse_KMD]     NVARCHAR(400) NULL,
        [DW_Timestamp_Insert]                DATETIME2     NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]                DATETIME2     NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_UNSPSC] PRIMARY KEY CLUSTERED ([DW_EK_UNSPSC] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row (DW_EK = -1) — dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[UNSPSC] WHERE [DW_EK_UNSPSC] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[UNSPSC] (
            [DW_EK_UNSPSC], [UNSPSC_Kode],
            [Source_ILS], [Source_KMD], [DW_Timestamp_Insert]
        ) VALUES (
            -1, N''UNKNOWN'',
            0, 0, SYSDATETIME()
        )
    ');
END
GO

-- dim.Okosted
-- Analytics-ready Omkostningssted (cost centre) dimension
-- Self-referencing parent-child hierarchy from SydAX codesystem
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[Okosted]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[Okosted] (
        [DW_EK_Okosted] [int] PRIMARY KEY,
        [CodeOkosted] [varchar](50) NOT NULL,
        [Name] [nvarchar](500),
        [Level] [int],
        [DW_EK_Okosted_Parent] [int],
        [ParentCode] [varchar](50),
        [Active] [bit] DEFAULT 1,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE()
    ) ON [PRIMARY];

    CREATE INDEX IX_dim_Okosted_CodeOkosted
        ON [dim].[Okosted] ([CodeOkosted]);

    CREATE INDEX IX_dim_Okosted_Parent
        ON [dim].[Okosted] ([DW_EK_Okosted_Parent])
        WHERE [DW_EK_Okosted_Parent] IS NOT NULL;
END
GO

-- Sentinel row for "Unknown" Okosted (DW_EK = -1)
IF NOT EXISTS (SELECT 1 FROM [dim].[Okosted] WHERE DW_EK_Okosted = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Okosted] (
            DW_EK_Okosted, CodeOkosted, Name, [Level],
            DW_EK_Okosted_Parent, ParentCode, Active,
            DW_Timestamp_Insert, DW_Timestamp_Update
        )
        VALUES (
            -1, ''UNKNOWN'', ''Unknown Okosted'', NULL,
            NULL, NULL, 1,
            GETDATE(), GETDATE()
        )
    ');
END
GO

-- ============================================================================
-- Pre-deploy validation: ensure no NULL DW_EK values exist
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Kontoplan] WHERE [DW_EK_Kontoplan] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Kontoplan found in dim.Kontoplan', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Kreditor] WHERE [DW_EK_Kreditor] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Kreditor found in dim.Kreditor', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Vare] WHERE [DW_EK_Vare] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Vare found in dim.Vare', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Faktura] WHERE [DW_EK_Faktura] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Faktura found in dim.Faktura', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Fakturalinje] WHERE [DW_EK_Fakturalinje] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Fakturalinje found in dim.Fakturalinje', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Postering] WHERE [DW_EK_Postering] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Postering found in dim.Postering', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Aftale] WHERE [DW_EK_Aftale] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Aftale found in dim.Aftale', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[VareudleveringEgenskaber] WHERE [DW_EK_VareudleveringEgenskaber] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_VareudleveringEgenskaber found in dim.VareudleveringEgenskaber', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Lager] WHERE [DW_EK_Lager] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Lager found in dim.Lager', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Rekvirent] WHERE [DW_EK_Rekvirent] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Rekvirent found in dim.Rekvirent', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[LagerbeholdningEgenskaber] WHERE [DW_EK_LagerbeholdningEgenskaber] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_LagerbeholdningEgenskaber found in dim.LagerbeholdningEgenskaber', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[AfdelingLagerbeholdningEgenskaber] WHERE [DW_EK_AfdelingLagerbeholdningEgenskaber] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_AfdelingLagerbeholdningEgenskaber found in dim.AfdelingLagerbeholdningEgenskaber', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[EAN]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[EAN] WHERE [DW_EK_EAN] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_EAN found in dim.EAN', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[IlsVare] WHERE [DW_EK_IlsVare] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_IlsVare found in dim.IlsVare', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[BudgetEgenskaber] WHERE [DW_EK_BudgetEgenskaber] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_BudgetEgenskaber found in dim.BudgetEgenskaber', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[UNSPSC] WHERE [DW_EK_UNSPSC] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_UNSPSC found in dim.UNSPSC', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Okosted]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Okosted] WHERE [DW_EK_Okosted] IS NULL)
BEGIN
    THROW 50000, 'Null DW_EK_Okosted found in dim.Okosted', 1;
END
GO
