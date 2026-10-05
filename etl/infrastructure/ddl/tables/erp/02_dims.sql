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
        [dw_ek_Kontoplan] INT NOT NULL,
        [clean_code_Kontoplan] NVARCHAR(255) NOT NULL,
        [finanskonto_id] BIGINT NULL,
        [kontonummer] NVARCHAR(255) NULL,
        [f1] NVARCHAR(50) NULL,
        [f2] NVARCHAR(50) NULL,
        [f3] NVARCHAR(50) NULL,
        [d1] NVARCHAR(50) NULL,
        [s1] NVARCHAR(50) NULL,
        [s2] NVARCHAR(50) NULL,
        [s3] NVARCHAR(50) NULL,
        [s4] NVARCHAR(50) NULL,
        [s5] NVARCHAR(50) NULL,
        [s6] NVARCHAR(50) NULL,
        [s7] NVARCHAR(50) NULL,
        [g1] NVARCHAR(50) NULL,
        [g2] NVARCHAR(50) NULL,
        [a1] NVARCHAR(50) NULL,
        [a2] NVARCHAR(50) NULL,
        [tekst_s1] NVARCHAR(255) NULL,
        [tekst_s2] NVARCHAR(255) NULL,
        [tekst_s3] NVARCHAR(255) NULL,
        [tekst_s4] NVARCHAR(255) NULL,
        [tekst_s5] NVARCHAR(255) NULL,
        [tekst_s6] NVARCHAR(255) NULL,
        [tekst_s7] NVARCHAR(255) NULL,
        [tekst_g1] NVARCHAR(255) NULL,
        [tekst_g2] NVARCHAR(255) NULL,
        [tekst_a1] NVARCHAR(255) NULL,
        [tekst_a2] NVARCHAR(255) NULL,
        [tekst_f1] NVARCHAR(255) NULL,
        [tekst_f2] NVARCHAR(255) NULL,
        [tekst_f3] NVARCHAR(255) NULL,
        [tekst_d1] NVARCHAR(255) NULL,
        [omkostningsstednummer] NVARCHAR(50) NULL,
        [omkostningssted_niveau] INT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Kontoplan_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Kontoplan_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Kontoplan] PRIMARY KEY CLUSTERED ([dw_ek_Kontoplan] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row (DW_EK = -1) - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[Kontoplan] WHERE [dw_ek_Kontoplan] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Kontoplan] (
            [dw_ek_Kontoplan], [clean_code_Kontoplan], [finanskonto_id], [kontonummer],
            [f1], [f2], [f3], [d1]
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
        [dw_ek_Kreditor] INT NOT NULL,
        [clean_code_Kreditor] NVARCHAR(255) NOT NULL,
        [cvrnr] NVARCHAR(50) NULL,
        [kreditornavn] NVARCHAR(255) NULL,
        [kreditornavn_sydax] NVARCHAR(255) NULL,
        [kreditornavn_ils] NVARCHAR(255) NULL,
        [kreditornavn_mysupply] NVARCHAR(255) NULL,
        [vejnavn] NVARCHAR(255) NULL,
        [vejnavn_ils] NVARCHAR(255) NULL,
        [vejnavn_mysupply] NVARCHAR(255) NULL,
        [bynavn] NVARCHAR(255) NULL,
        [bynavn_ils] NVARCHAR(255) NULL,
        [bynavn_mysupply] NVARCHAR(255) NULL,
        [postnummer] NVARCHAR(20) NULL,
        [postnummer_ils] NVARCHAR(255) NULL,
        [postnummer_mysupply] NVARCHAR(50) NULL,
        [kontakt] NVARCHAR(255) NULL,
        [valutakode] NVARCHAR(10) NULL,
        [dw_source_sydax] BIT NOT NULL CONSTRAINT [DF_Kreditor_dw_source_sydax] DEFAULT 0,
        [dw_source_ils] BIT NOT NULL CONSTRAINT [DF_Kreditor_dw_source_ils] DEFAULT 0,
        [dw_source_mysupply] BIT NOT NULL CONSTRAINT [DF_Kreditor_dw_source_mysupply] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Kreditor_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Kreditor_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Kreditor] PRIMARY KEY CLUSTERED ([dw_ek_Kreditor] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[Kreditor] WHERE [dw_ek_Kreditor] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Kreditor] (
            [dw_ek_Kreditor], [clean_code_Kreditor], [cvrnr],
            [kreditornavn], [bynavn], [valutakode], [dw_timestamp_insert]
        ) VALUES (
            -1, N''UNKNOWN'', ''UNKNOWN'',
            ''Ukendt kreditor'', ''UNKNOWN'', ''UNK'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Leverandoervare] (
        [dw_ek_Leverandoervare] INT NOT NULL,
        [cvr] NVARCHAR(255) NULL,
        [varenummer_kreditor] NVARCHAR(255) NULL,
        [varenavn] NVARCHAR(MAX) NULL,
        [varenavn_mysupply] NVARCHAR(MAX) NULL,
        [dw_source_mysupply] BIT NOT NULL CONSTRAINT [DF_Leverandoervare_dw_source_mysupply] DEFAULT 0,
        [dw_source_ils] BIT NOT NULL CONSTRAINT [DF_Leverandoervare_dw_source_ils] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Leverandoervare_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Leverandoervare_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Leverandoervare] PRIMARY KEY CLUSTERED ([dw_ek_Leverandoervare] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[Leverandoervare] WHERE [dw_ek_Leverandoervare] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Leverandoervare] (
            [dw_ek_Leverandoervare], [cvr], [varenummer_kreditor], [varenavn], [dw_timestamp_insert]
        ) VALUES (
            -1, N''UNKNOWN'', ''UNKNOWN'', ''Ukendt vare'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO


-- dim.Aftale (Type 2 SCD)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Aftale](
        [dw_ek_Aftale] INT NOT NULL,
        [clean_code_Aftale] NVARCHAR(255) NOT NULL,
        [aftale_nummer] NVARCHAR(100) NULL,
        [leverandoernummer] NVARCHAR(100) NULL,
        [dw_ek_Kreditor] INT NOT NULL CONSTRAINT [DF_Aftale_dw_ek_Kreditor] DEFAULT -1,
        [aftale_dato] DATETIME2 NULL,
        [aftale_ansvarlig] NVARCHAR(100) NULL,
        [aftaletype] NVARCHAR(50) NULL,
        [aftaletekst] NVARCHAR(MAX) NULL,
        [hoved_start] DATETIME2 NULL,
        [hoved_slut] DATETIME2 NULL,
        [dw_valid_from] DATETIME2(0) NOT NULL,
        [dw_valid_to] DATETIME2(0) NOT NULL CONSTRAINT [DF_Aftale_dw_valid_to] DEFAULT '9999-12-31',
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_Aftale_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Aftale_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Aftale_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Aftale] PRIMARY KEY CLUSTERED ([dw_ek_Aftale] ASC, [dw_valid_from] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'IX_Dim_Aftale_Current')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_Aftale_Current] ON [dim].[Aftale]
    ([dw_ek_Aftale] ASC)
    INCLUDE ([aftale_nummer], [aftaletype], [aftaletekst])
    WHERE [dw_is_deleted] = 0 AND [dw_valid_to] = '9999-12-31';
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (
    SELECT 1
    FROM [dim].[Aftale]
    WHERE [dw_ek_Aftale] = -1 AND [dw_valid_from] = '1900-01-01' AND [dw_valid_to] = '9999-12-31'
)
BEGIN
    EXEC('
        INSERT INTO [dim].[Aftale] (
            [dw_ek_Aftale], [clean_code_Aftale], [aftale_nummer],
            [dw_valid_from], [dw_valid_to], [dw_is_deleted], [dw_timestamp_insert]
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
        [dw_ek_Faktura] INT NOT NULL,
        [faktura_nr] NVARCHAR(100) NULL,
        [faktura_valuta_kode] NVARCHAR(10) NULL,
        [firma_navn] NVARCHAR(255) NULL,
        [leverandoer_cvr] NVARCHAR(50) NULL,
        [adresse] NVARCHAR(255) NULL,
        [bynavn] NVARCHAR(255) NULL,
        [postnummer] NVARCHAR(20) NULL,
        [dokument_type] NVARCHAR(20) NULL,
        [regnings_modtager_id] NVARCHAR(255) NULL,
        [regnings_modtager_id_type] NVARCHAR(255) NULL,
        [koebs_ordrenummer] NVARCHAR(255) NULL,
        [salgs_ordrenummer] NVARCHAR(255) NULL,
        [kunde_identification] NVARCHAR(255) NULL,
        [kunde_identification_type] NVARCHAR(255) NULL,
        [dimension_konto_streng] NVARCHAR(255) NULL,
        [kundenavn] NVARCHAR(255) NULL,
        [kunde_vejnavn] NVARCHAR(255) NULL,
        [kunde_hus_nr] NVARCHAR(255) NULL,
        [kunde_bynavn] NVARCHAR(255) NULL,
        [kunde_post_nr] NVARCHAR(255) NULL,
        [kunde_landekode] NVARCHAR(255) NULL,
        [kunde_reference_id] NVARCHAR(512) NULL,
        [kunde_reference_navn] NVARCHAR(255) NULL,
        [kunde_reference_telefon_nr] NVARCHAR(255) NULL,
        [kunde_reference_fax_nr] NVARCHAR(255) NULL,
        [kunde_reference_e_mail] NVARCHAR(255) NULL,
        [kunde_reference_rolle] NVARCHAR(255) NULL,
        [juridisk_kunde_identification] NVARCHAR(255) NULL,
        [juridisk_kunde_identification_type] NVARCHAR(255) NULL,
        [juridisk_dimension_konto_streng] NVARCHAR(255) NULL,
        [juridisk_kundenavn] NVARCHAR(255) NULL,
        [juridisk_kunde_vejnavn] NVARCHAR(255) NULL,
        [juridisk_kunde_hus_nr] NVARCHAR(255) NULL,
        [juridisk_kunde_bynavn] NVARCHAR(255) NULL,
        [juridisk_kunde_post_nr] NVARCHAR(255) NULL,
        [juridisk_kunde_landekode] NVARCHAR(255) NULL,
        [juridisk_kunde_reference_id] NVARCHAR(MAX) NULL,
        [juridisk_kunde_reference_navn] NVARCHAR(255) NULL,
        [juridisk_kunde_reference_telefon_nr] NVARCHAR(255) NULL,
        [juridisk_kunde_reference_fax_nr] NVARCHAR(255) NULL,
        [juridisk_kunde_reference_e_mail] NVARCHAR(255) NULL,
        [juridisk_kunde_reference_rolle] NVARCHAR(255) NULL,
        [faktura_udsteder_identifikation] NVARCHAR(255) NULL,
        [faktura_udsteder_identifikation_type] NVARCHAR(255) NULL,
        [faktura_udsteder_navn] NVARCHAR(255) NULL,
        [faktura_udsteder_adresse_id_vareafsendelse] NVARCHAR(255) NULL,
        [faktura_udsteder_vejnavn_vareafsendelse] NVARCHAR(255) NULL,
        [faktura_udsteder_hus_nr_vareafsendelse] NVARCHAR(255) NULL,
        [faktura_udsteder_bynavn_vareafsendelse] NVARCHAR(255) NULL,
        [faktura_udsteder_post_nr_vareafsendelse] NVARCHAR(255) NULL,
        [faktura_udsteder_land_kode_vareafsendelse] NVARCHAR(255) NULL,
        [faktura_udsteder_cvr_nummer] NVARCHAR(255) NULL,
        [faktura_udsteder_cvr_nummer_type] NVARCHAR(255) NULL,
        [faktura_udsteder_reference_idenrifikation] NVARCHAR(255) NULL,
        [faktura_udsteder_reference_navn] NVARCHAR(255) NULL,
        [faktura_udsteder_reference_telefon_nr] NVARCHAR(255) NULL,
        [faktura_udsteder_reference_fax_nr] NVARCHAR(255) NULL,
        [faktura_udsteder_reference_e_mail] NVARCHAR(255) NULL,
        [kort_arts_kode] NVARCHAR(255) NULL,
        [betalings_type] NVARCHAR(255) NULL,
        [betalings_id] NVARCHAR(255) NULL,
        [kreditnummer] NVARCHAR(255) NULL,
        [kontonummer] NVARCHAR(255) NULL,
        [kontotype] NVARCHAR(255) NULL,
        [afdelings_id] NVARCHAR(255) NULL,
        [bank_id] NVARCHAR(255) NULL,
        [bank_navn] NVARCHAR(255) NULL,
        [told_kurs] NVARCHAR(255) NULL,
        [told_kurs_type] NVARCHAR(255) NULL,
        [kontantrabatsats] NVARCHAR(255) NULL,
        [strafrentesats] NVARCHAR(255) NULL,
        [kontant_rabat_dato] NVARCHAR(255) NULL,
        [start_rente_dato] NVARCHAR(255) NULL,
        [zero_rated_afgifts_kategori] NVARCHAR(255) NULL,
        [zero_rated_afgifts_kategori_procent] NVARCHAR(255) NULL,
        [gebyr_rabat_aarsag] NVARCHAR(MAX) NULL,
        [gebyr_rabat_kategori] NVARCHAR(MAX) NULL,
        [gebyr_rabat_finanskonto] NVARCHAR(MAX) NULL,
        [gebyr_tillaeg_aarsag] NVARCHAR(MAX) NULL,
        [gebyr_tillaeg_kategori] NVARCHAR(MAX) NULL,
        [gebyr_tillaeg_finanskonto] NVARCHAR(MAX) NULL,
        -- Header identity carried from MySupply staging so the faktura<->postering
        -- bridge can rank the "newest faktura per postering" off persistent tables
        -- (header_dw_timestamp_update DESC, statistic_invoice_header_id DESC) without
        -- joining the volatile stg.dim_faktura_mysupply at bridge build time.
        [statistic_invoice_header_id] BIGINT NULL,
        [header_dw_timestamp_update] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Faktura] PRIMARY KEY CLUSTERED ([dw_ek_Faktura] ASC)
    ) ON [PRIMARY];
END
GO

-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (SELECT 1 FROM [dim].[Faktura] WHERE [dw_ek_Faktura] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Faktura] (
            [dw_ek_Faktura], [faktura_nr], [faktura_valuta_kode], [firma_navn],
            [leverandoer_cvr], [adresse], [bynavn], [postnummer], [dw_timestamp_insert]
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
        [dw_ek_Fakturalinje] INT NOT NULL,
        [item_faktura_udsteder_vare_nr] NVARCHAR(255) NULL,
        [item_faktura_udsteder_vare_navn] NVARCHAR(MAX) NULL,
        [faktura_linje_nr] NVARCHAR(512) NULL,
        [antal_enheder_faktureret_type] NVARCHAR(255) NULL,
        [item_faktura_udstederens_vare_nr] NVARCHAR(512) NULL,
        [item_faktura_udstederens_vare_nr_type] NVARCHAR(255) NULL,
        [base_price_enheds_stoerrelse] NVARCHAR(255) NULL,
        [base_price_enheds_stoerrelse_type] NVARCHAR(255) NULL,
        [item_faktura_udsteder_vare_navn_kort] NVARCHAR(MAX) NULL,
        [line_dimensions_konto_streng] NVARCHAR(255) NULL,
        [leverance_krav] NVARCHAR(255) NULL,
        [leverings_dato_2] NVARCHAR(255) NULL,
        [item_kundens_vare_nr] NVARCHAR(255) NULL,
        [item_kundens_vare_nr_type] NVARCHAR(255) NULL,
        [item_kundens_vare_beskrivelse] NVARCHAR(255) NULL,
        [item_faktura_udstederens_vare_beskrivelse] NVARCHAR(2000) NULL,
        [item_fabrikantens_vare_nr] NVARCHAR(255) NULL,
        [item_fabrikantens_vare_nr_type] NVARCHAR(255) NULL,
        [item_fabrikantens_vare_beskrivelse] NVARCHAR(255) NULL,
        [item_standardiseret_vare_nr] NVARCHAR(255) NULL,
        [item_standardiseret_vare_nr_type] NVARCHAR(255) NULL,
        [item_standardiseret_vare_beskrivelse] NVARCHAR(255) NULL,
        [item_vare_klassifikation] NVARCHAR(255) NULL,
        [item_vare_klassifikation_type] NVARCHAR(255) NULL,
        [item_afgifts_kategori] NVARCHAR(255) NULL,
        [item_afgifts_kategori_procent] NVARCHAR(255) NULL,
        [faktura_udsteder_gebyr_rabat_aarsag] NVARCHAR(MAX) NULL,
        [faktura_udsteder_gebyr_rabat_kategori] NVARCHAR(MAX) NULL,
        [faktura_udsteder_gebyr_rabat_finanskonto] NVARCHAR(MAX) NULL,
        [faktura_udsteder_gebyr_tillaeg_aarsag] NVARCHAR(MAX) NULL,
        [faktura_udsteder_gebyr_tillaeg_kategori] NVARCHAR(MAX) NULL,
        [faktura_udsteder_gebyr_tillaeg_finanskonto] NVARCHAR(MAX) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Fakturalinje] PRIMARY KEY CLUSTERED ([dw_ek_Fakturalinje] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (SELECT 1 FROM [dim].[Fakturalinje] WHERE [dw_ek_Fakturalinje] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Fakturalinje] (
            [dw_ek_Fakturalinje], [item_faktura_udsteder_vare_nr], [item_faktura_udsteder_vare_navn], [dw_timestamp_insert]
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
        [dw_ek_Postering] INT NOT NULL,
        [ref_ledgertrans_id] BIGINT NULL,
        [bk_ref_ledgertrans_id] BIGINT NULL,
        [ref_vend_trans_id] BIGINT NULL,
        [dokument_id] BIGINT NULL,
        [kreditornummer] NVARCHAR(50) NULL,
        [kreditornavn] NVARCHAR(255) NULL,
        [juridisk_enhed] NVARCHAR(255) NULL,
        [kontonummer] NVARCHAR(255) NULL,
        [faktura_nr] NVARCHAR(100) NULL,
        [sidst_udlignet_bilagsnummer] NVARCHAR(100) NULL,
        [bilagsnummer_trans] NVARCHAR(100) NULL,
        [bilagsnummer_postering] NVARCHAR(100) NULL,
        [valutakode_trans] NVARCHAR(10) NULL,
        [valutakode_postering] NVARCHAR(10) NULL,
        [transaktionstekst] NVARCHAR(100) NULL,
        [posteringstekst] NVARCHAR(100) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Postering] PRIMARY KEY CLUSTERED ([dw_ek_Postering] ASC)
    ) ON [PRIMARY];

    -- Insert UNKNOWN row (DW_EK = -1)
    INSERT INTO [dim].[Postering] (
        [dw_ek_Postering], [dw_timestamp_insert]
    ) VALUES (
        -1, CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO

-- Ensure UNKNOWN row exists even when table predates sentinel seed logic
IF NOT EXISTS (SELECT 1 FROM [dim].[Postering] WHERE [dw_ek_Postering] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Postering] (
            [dw_ek_Postering], [dw_timestamp_insert], [dw_timestamp_update]
        ) VALUES (
            -1, CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.Vareudlevering
-- Source: ILS dbo.rvish -- revision/foelgeseddel header for delivery lines
-- Transaction dimension: dw_ek_Vareudlevering is the shared PK with
-- fct.Vareudlevering, resolved from map.Vareudlevering.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Vareudlevering](
        [dw_ek_Vareudlevering] INT NOT NULL,
        [clean_code_Vareudlevering] NVARCHAR(500) NULL,
        [rekvisitionsnummer] INT NULL,
        [foelgeseddelnummer] NVARCHAR(100) NULL,
        [brik] NVARCHAR(100) NULL,
        [leverandoernummer] NVARCHAR(100) NULL,
        [ordrenummer] NVARCHAR(100) NULL,
        [ordrelinje] NVARCHAR(100) NULL,
        [sw_direkte_udlevering] NVARCHAR(10) NULL,
        [sw_pakkeseddel] NVARCHAR(10) NULL,
        [sw_foelgeseddel] NVARCHAR(10) NULL,
        [sw_fly] NVARCHAR(10) NULL,
        [sw_haster] NVARCHAR(10) NULL,
        [sw_erstat] NVARCHAR(10) NULL,
        [projektnummer] NVARCHAR(100) NULL,
        [kontonummer_kode] NVARCHAR(100) NULL,
        [bruger] NVARCHAR(100) NULL,
        [bk_prrowid] NVARCHAR(100) NULL,
        [referencenummer] NVARCHAR(100) NULL,
        [referencelinje] NVARCHAR(100) NULL,
        [salgsordre] NVARCHAR(100) NULL,
        [salgsordre_id] NVARCHAR(100) NULL,
        [salgsfakturanummer] NVARCHAR(100) NULL,
        [rekvisitionsvare_id] NVARCHAR(100) NULL,
        [plukvarer_id] NVARCHAR(100) NULL,
        [afleveringsoperatoer] NVARCHAR(100) NULL,
        [afleveringstekst] NVARCHAR(100) NULL,
        [afleveringsstregkode] NVARCHAR(100) NULL,
        [vognnummer] NVARCHAR(100) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Vareudlevering] PRIMARY KEY CLUSTERED ([dw_ek_Vareudlevering] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Vareudlevering_clean_code] ON [dim].[Vareudlevering]
    ([clean_code_Vareudlevering] ASC) WHERE [clean_code_Vareudlevering] IS NOT NULL ON [PRIMARY];
END
GO

-- Ensure UNKNOWN row exists even when table predates sentinel seed logic
IF NOT EXISTS (
    SELECT 1 FROM [dim].[Vareudlevering] WHERE [dw_ek_Vareudlevering] = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[Vareudlevering] (
            [dw_ek_Vareudlevering], [clean_code_Vareudlevering], [dw_timestamp_insert], [dw_timestamp_update]
        ) VALUES (
            -1, N''UNKNOWN'', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
END
GO

-- dim.Lager
-- Hierarchy and SOR mapping columns for ILS Lager codesystem (REGION_SYD -> sgh -> lgr)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[Lager](
        [dw_ek_Lager] [int] PRIMARY KEY,
        [code] [varchar](100) NOT NULL,
        [display] [nvarchar](500),
        [parent_code] [varchar](100),
        [level] [int],
        [entity_type] [nvarchar](200),
        [sgh] [nvarchar](200),
        [lgr] [nvarchar](200),
        [adr] [nvarchar](200),
        [land] [nvarchar](200),
        [postdi] [nvarchar](200),
        [tlf] [nvarchar](200),
        [code_sor] [varchar](50),
        [dw_ek_Organisation] [int] NOT NULL CONSTRAINT [DF_Lager_dw_ek_Organisation] DEFAULT -1,
        [dw_ek_Lager_parent] [int] CONSTRAINT [DF_Lager_dw_ek_Lager_parent] DEFAULT -1,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Lager_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Lager_dw_timestamp_update] DEFAULT SYSDATETIME()
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'IX_dim_Lager_code')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U')
BEGIN
    CREATE INDEX IX_dim_Lager_code ON [dim].[Lager]([code]);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'IX_dim_Lager_ParentEK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U')
BEGIN
    CREATE INDEX IX_dim_Lager_ParentEK
        ON [dim].[Lager]([dw_ek_Lager_parent])
        WHERE [dw_ek_Lager_parent] IS NOT NULL
          AND [dw_ek_Lager_parent] <> -1;
END
GO

-- Sentinel row for "Unknown Lager" (DW_EK = -1)
-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (
    SELECT 1 FROM [dim].[Lager] WHERE dw_ek_Lager = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[Lager] (
            dw_ek_Lager, Code, display, [level], entity_type,
            dw_ek_Organisation, dw_ek_Lager_parent,
            dw_timestamp_insert, dw_timestamp_update
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
-- Hierarchy and SOR mapping columns for ILS Rekvirent codesystem (REGION_SYD -> sgh -> rgrp -> rtyp -> rnr)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[Rekvirent](
        [dw_ek_Rekvirent] [int] PRIMARY KEY,
        [code] [varchar](100) NOT NULL,
        [display] [nvarchar](500),
        [parent_code] [varchar](100),
        [level] [int],
        [entity_type] [nvarchar](200),
        [sgh] [nvarchar](200),
        [rgrp] [nvarchar](200),
        [rtyp] [nvarchar](200),
        [rnr] [nvarchar](200),
        [code_sor] [varchar](50),
        [sor_relationship] [varchar](50),
        [dw_ek_Organisation] [int] NOT NULL CONSTRAINT [DF_Rekvirent_dw_ek_Organisation] DEFAULT -1,
        [dw_ek_Rekvirent_parent] [int] CONSTRAINT [DF_Rekvirent_dw_ek_Rekvirent_parent] DEFAULT -1,
        [er_undtaget] [bit] NOT NULL CONSTRAINT [DF_Rekvirent_er_undtaget] DEFAULT 0,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Rekvirent_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Rekvirent_dw_timestamp_update] DEFAULT SYSDATETIME()
    ) ON [PRIMARY];

    CREATE INDEX IX_dim_Rekvirent_code
        ON [dim].[Rekvirent]([code]);

    CREATE INDEX IX_dim_Rekvirent_ParentEK
        ON [dim].[Rekvirent]([dw_ek_Rekvirent_parent])
        WHERE [dw_ek_Rekvirent_parent] IS NOT NULL
          AND [dw_ek_Rekvirent_parent] <> -1;

    CREATE INDEX IX_dim_Rekvirent_er_undtaget
        ON [dim].[Rekvirent]([er_undtaget])
        WHERE [er_undtaget] = 1;
END
GO

-- Sentinel row for "Unknown Rekvirent" (DW_EK = -1)
-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (
    SELECT 1 FROM [dim].[Rekvirent] WHERE dw_ek_Rekvirent = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[Rekvirent] (
            dw_ek_Rekvirent, Code, display, [level], entity_type,
            dw_ek_Organisation, dw_ek_Rekvirent_parent, er_undtaget,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (
            -1, ''UNKNOWN'', ''Unknown Rekvirent'', 0, ''unknown'',
            -1, -1, 0,
            SYSDATETIME(), SYSDATETIME()
        )
    ');
END
GO

-- dim.LagerbeholdningEgenskaber (Type 1 SCD — warehouse stock attributes)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[LagerbeholdningEgenskaber] (
        [dw_ek_LagerbeholdningEgenskaber] INT NOT NULL,
        [clean_code_LagerbeholdningEgenskaber] NVARCHAR(500) NULL,
        [lnr] NVARCHAR(32) NULL,
        [lgrlok] NVARCHAR(32) NULL,
        [udlevenh] NVARCHAR(80) NULL,
        [sw_erstat] BIT NULL,
        [lgrbeh_prio] INT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_LagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([dw_ek_LagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_LagerbeholdningEgenskaber_clean_code]
    ON [dim].[LagerbeholdningEgenskaber] ([clean_code_LagerbeholdningEgenskaber] ASC)
    WHERE [clean_code_LagerbeholdningEgenskaber] IS NOT NULL ON [PRIMARY];

    PRINT 'Created dim.LagerbeholdningEgenskaber';
END
GO

-- Sentinel row for "Unknown LagerbeholdningEgenskaber" (DW_EK = -1)
IF NOT EXISTS (
    SELECT 1 FROM [dim].[LagerbeholdningEgenskaber]
    WHERE [dw_ek_LagerbeholdningEgenskaber] = -1
)
BEGIN
    INSERT INTO [dim].[LagerbeholdningEgenskaber] (
        [dw_ek_LagerbeholdningEgenskaber],
        [clean_code_LagerbeholdningEgenskaber],
        [dw_timestamp_insert], [dw_timestamp_update]
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
        [dw_ek_AfdelingLagerbeholdningEgenskaber] INT NOT NULL,
        [clean_code_AfdelingLagerbeholdningEgenskaber] NVARCHAR(500) NULL,
        [lnr] NVARCHAR(32) NULL,
        [brik] INT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_AfdelingLagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([dw_ek_AfdelingLagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_AfdelingLagerbeholdningEgenskaber_clean_code]
    ON [dim].[AfdelingLagerbeholdningEgenskaber] ([clean_code_AfdelingLagerbeholdningEgenskaber] ASC)
    WHERE [clean_code_AfdelingLagerbeholdningEgenskaber] IS NOT NULL ON [PRIMARY];

    PRINT 'Created dim.AfdelingLagerbeholdningEgenskaber';
END
GO

-- Sentinel row for "Unknown AfdelingLagerbeholdningEgenskaber" (DW_EK = -1)
IF NOT EXISTS (
    SELECT 1 FROM [dim].[AfdelingLagerbeholdningEgenskaber]
    WHERE [dw_ek_AfdelingLagerbeholdningEgenskaber] = -1
)
BEGIN
    INSERT INTO [dim].[AfdelingLagerbeholdningEgenskaber] (
        [dw_ek_AfdelingLagerbeholdningEgenskaber],
        [clean_code_AfdelingLagerbeholdningEgenskaber],
        [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, N'UNKNOWN',
        GETDATE(), GETDATE()
    );
    PRINT 'Inserted UNKNOWN sentinel for dim.AfdelingLagerbeholdningEgenskaber (DW_EK = -1)';
END
GO

-- dim.ean
-- ean dimension with ean number and name from NemHandelsregisteret
-- Flat structure (no hierarchy, no parent)
-- Type 1 SCD (update in place)
-- Rows driven by MySupply invoice data, enriched with NemHandelsregisteret
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[ean]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[ean](
        [dw_ek_EAN] [int] PRIMARY KEY,
        [ean] [nvarchar](50) NOT NULL,
        [navn_nemhandelsregister] [nvarchar](512),
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_ean_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_ean_dw_timestamp_update] DEFAULT GETDATE()
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[ean]') AND name = 'IX_dim_ean_ean')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[ean]') AND type = 'U')
BEGIN
    CREATE INDEX IX_dim_ean_ean ON [dim].[ean]([ean]);
END
GO

-- Sentinel row for "Unknown ean" (DW_EK = -1)
-- Dynamic SQL avoids compile-time column resolution when table pre-dates new columns
IF NOT EXISTS (
    SELECT 1 FROM [dim].[ean] WHERE dw_ek_EAN = -1
)
BEGIN
    EXEC('
        INSERT INTO [dim].[ean] (
            dw_ek_EAN, ean,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (
            -1, ''UNKNOWN'',
            GETDATE(), GETDATE()
        )
    ');
END
GO

-- dim.ILSVare
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND type = 'U'
) BEGIN
CREATE TABLE [dim].[ILSVare] (
    [dw_ek_ILSVare]        INT            NOT NULL,
    [clean_code_ILSVare]    NVARCHAR(100)  NULL,
    [varenummer]            NVARCHAR(100)  NULL,
    [varenavn]              NVARCHAR(80)   NULL,
    [varenavn2]             NVARCHAR(80)   NULL,
    [varestr]               NVARCHAR(255)  NULL,
    [basisenhed]            NVARCHAR(50)   NULL,
    [gruppering]            NVARCHAR(50)   NULL,
    [gruppering_navn]       NVARCHAR(255)  NULL,
    [art]                   NVARCHAR(50)   NULL,
    [art_navn]              NVARCHAR(255)  NULL,
    [varetype]              NVARCHAR(50)   NULL,
    [varetype_navn]         NVARCHAR(255)  NULL,
    [momskode]              NVARCHAR(50)   NULL,
    [momskode_navn]         NVARCHAR(255)  NULL,
    [momsprocent]           DECIMAL(18,4)  NULL,
    [dw_ek_Dato_oprettet]  INT            NOT NULL CONSTRAINT [DF_ILSVare_dw_ek_Dato_oprettet] DEFAULT -1,
    [dato_tid_oprettet]     DATETIME2      NULL,
    [dw_ek_UNSPSC]          INT            NOT NULL CONSTRAINT [DF_ILSVare_dw_ek_UNSPSC] DEFAULT -1,
    [dw_ek_UNSPSC_intern]   INT            NOT NULL CONSTRAINT [DF_ILSVare_dw_ek_UNSPSC_intern] DEFAULT -1,
    [aktiv]                 BIT            NULL,
    [faelles_varenummer]     NVARCHAR(100)  NULL,
    [ean_varenummer]        NVARCHAR(100)  NULL,
    [billede]               NVARCHAR(104)  NULL,
    [opr_bruger]             NVARCHAR(50)   NULL,
    [opr_bruger_navn]        NVARCHAR(255)  NULL,
    [weblink]               NVARCHAR(140)  NULL,
    [maa_ikke_lukkes]         NVARCHAR(50)   NULL,
    [hygiejne]              NVARCHAR(50)   NULL,
    [dw_timestamp_insert]   DATETIME2      NOT NULL CONSTRAINT [DF_ILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]   DATETIME2      NOT NULL CONSTRAINT [DF_ILSVare_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Dim_ILSVare] PRIMARY KEY CLUSTERED ([dw_ek_ILSVare] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = 'IX_Dim_ILSVare_clean_code'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_ILSVare_clean_code]
    ON [dim].[ILSVare] ([clean_code_ILSVare] ASC)
    WHERE [clean_code_ILSVare] IS NOT NULL
    ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = 'IX_Dim_ILSVare_UNSPSC'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_ILSVare_UNSPSC]
    ON [dim].[ILSVare] ([dw_ek_UNSPSC] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = 'IX_Dim_ILSVare_UNSPSC_Intern'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_ILSVare_UNSPSC_Intern]
    ON [dim].[ILSVare] ([dw_ek_UNSPSC_intern] ASC) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row (DW_EK = -1) - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[ILSVare] WHERE [dw_ek_ILSVare] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[ILSVare] (
            [dw_ek_ILSVare], [clean_code_ILSVare], [varenummer], [varenavn], [dw_timestamp_insert]
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
        [dw_ek_Finanskladde] INT NOT NULL,
        [bk_ref_ledgerjournaltable_id] NVARCHAR(100) NULL,
        [kladdenummer] NVARCHAR(20) NULL,
        [kladdetekst] NVARCHAR(1000) NULL,
        [kladdenavn] NVARCHAR(100) NULL,
        [er_lukket] BIT NULL,
        [data_area_id] NVARCHAR(20) NULL,
        [maskinnummer] NVARCHAR(20) NULL,
        [dw_ek_Dato_oprettet] INT NOT NULL CONSTRAINT [DF_Finanskladde_dw_ek_Dato_oprettet] DEFAULT -1,
        [dato_tid_oprettet] DATETIME2 NULL,
        [dw_ek_Dato_aendret] INT NOT NULL CONSTRAINT [DF_Finanskladde_dw_ek_Dato_aendret] DEFAULT -1,
        [dato_tid_aendret] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladde_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladde_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Finanskladde] PRIMARY KEY CLUSTERED ([dw_ek_Finanskladde] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM [dim].[Finanskladde] WHERE [dw_ek_Finanskladde] = -1)
    INSERT INTO [dim].[Finanskladde] (
        [dw_ek_Finanskladde],
        [bk_ref_ledgerjournaltable_id],
        [kladdenummer], [kladdetekst], [kladdenavn],
        [data_area_id], [maskinnummer],
        [dw_timestamp_insert], [dw_timestamp_update]
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
        [dw_ek_Finanskladdelinje] INT NOT NULL,
        [bk_ref_ledgertrans_id] NVARCHAR(100) NULL,
        [beskrivelse] NVARCHAR(100) NULL,
        [linjenummer] DECIMAL(32,16) NULL,
        [ekspeditionsnummer] NVARCHAR(7) NULL,
        [transaktionstypenummer] INT NULL,
        [transaktionstypetekst] NVARCHAR(1000) NULL,
        [maskinnummer] NVARCHAR(20) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_Finanskladdelinje] PRIMARY KEY CLUSTERED ([dw_ek_Finanskladdelinje] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM [dim].[Finanskladdelinje] WHERE [dw_ek_Finanskladdelinje] = -1)
    INSERT INTO [dim].[Finanskladdelinje] (
        [dw_ek_Finanskladdelinje],
        [bk_ref_ledgertrans_id],
        [beskrivelse], [ekspeditionsnummer],
        [transaktionstypetekst], [maskinnummer],
        [dw_timestamp_insert], [dw_timestamp_update]
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
        [dw_ek_BudgetEgenskaber] INT NOT NULL,
        [bk_ref_budgetmodel_id] NVARCHAR(100) NULL,
        [budgetmodelnummer] NVARCHAR(100) NULL,
        [budgetmodeltekst] NVARCHAR(255) NULL,
        [budgetmodeltype] NVARCHAR(255) NULL,
        [er_lukket] BIT NULL,
        [valutakode] NVARCHAR(10) NULL,
        [debet_kredit] NVARCHAR(20) NULL,
        [budgetkode] NVARCHAR(50) NULL,
        [aendringstype] NVARCHAR(50) NULL,
        [aendringstype_beskrivelse] NVARCHAR(255) NULL,
        [beloebstype_id] NVARCHAR(50) NULL,
        [budgetbeloeb_typetekst] NVARCHAR(1000) NULL,
        [budgetpost_status_id] NVARCHAR(50) NULL,
        [budgetpost_status_tekst] NVARCHAR(255) NULL,
        [omkostningsstednummer] NVARCHAR(50) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_BudgetEgenskaber] PRIMARY KEY CLUSTERED ([dw_ek_BudgetEgenskaber] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row if not exists - dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[BudgetEgenskaber] WHERE [dw_ek_BudgetEgenskaber] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[BudgetEgenskaber] (
            [dw_ek_BudgetEgenskaber], [bk_ref_budgetmodel_id],
            [budgetmodelnummer], [budgetmodeltekst],
            [budgetbeloeb_typetekst], [budgetpost_status_tekst],
            [dw_timestamp_insert]
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
        [dw_ek_UNSPSC]                       INT           NOT NULL,
        [unspsc_kode]                        NVARCHAR(50)  NULL,
        [dw_source_ils]                      BIT           NOT NULL,
        [dw_source_kmd]                      BIT           NOT NULL,
        [beskrivelse_ils]                    NVARCHAR(255) NULL,
        [beskrivelse_engelsk_ils]            NVARCHAR(255) NULL,
        [beskrivelse_kmd]                    NVARCHAR(400) NULL,
        [unspsc_level_kmd]                   SMALLINT      NULL,
        [unspsc_level_1_kode_kmd]            NVARCHAR(50)  NULL,
        [unspsc_level_1_beskrivelse_kmd]     NVARCHAR(400) NULL,
        [unspsc_level_2_kode_kmd]            NVARCHAR(50)  NULL,
        [unspsc_level_2_beskrivelse_kmd]     NVARCHAR(400) NULL,
        [unspsc_level_3_kode_kmd]            NVARCHAR(50)  NULL,
        [unspsc_level_3_beskrivelse_kmd]     NVARCHAR(400) NULL,
        [unspsc_level_4_kode_kmd]            NVARCHAR(50)  NULL,
        [unspsc_level_4_beskrivelse_kmd]     NVARCHAR(400) NULL,
        [dw_source_udbud]                    BIT           NOT NULL CONSTRAINT [DF_UNSPSC_dw_source_udbud] DEFAULT 0,
        [beskrivelse_udbud]                  NVARCHAR(255) NULL,
        [udbudstype]                         NVARCHAR(255) NULL,
        [udbudskategori_niveau_1]            NVARCHAR(255) NULL,
        [udbudskategori_niveau_2]            NVARCHAR(255) NULL,
        [udbudskategori_niveau_3]            NVARCHAR(255) NULL,
        [udbudskategori_niveau_4]            NVARCHAR(255) NULL,
        [dw_timestamp_insert]                DATETIME2     NOT NULL CONSTRAINT [DF_UNSPSC_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]                DATETIME2     NOT NULL CONSTRAINT [DF_UNSPSC_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_UNSPSC] PRIMARY KEY CLUSTERED ([dw_ek_UNSPSC] ASC)
    ) ON [PRIMARY];
END
GO

-- Insert UNKNOWN row (DW_EK = -1) — dynamic SQL avoids compile-time column resolution
IF NOT EXISTS (SELECT 1 FROM [dim].[UNSPSC] WHERE [dw_ek_UNSPSC] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[UNSPSC] (
            [dw_ek_UNSPSC], [unspsc_kode],
            [dw_source_ils], [dw_source_kmd], [dw_timestamp_insert]
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
        [dw_ek_Okosted] [int] PRIMARY KEY,
        [code_okosted] [varchar](50) NOT NULL,
        [name] [nvarchar](500),
        [level] [int],
        [dw_ek_Okosted_parent] [int],
        [parent_code] [varchar](50),
        [active] [bit] CONSTRAINT [DF_Okosted_active] DEFAULT 1,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Okosted_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Okosted_dw_timestamp_update] DEFAULT GETDATE()
    ) ON [PRIMARY];

    CREATE INDEX IX_dim_Okosted_code_okosted
        ON [dim].[Okosted] ([code_okosted]);

    CREATE INDEX IX_dim_Okosted_Parent
        ON [dim].[Okosted] ([dw_ek_Okosted_parent])
        WHERE [dw_ek_Okosted_parent] IS NOT NULL;
END
GO

-- Sentinel row for "Unknown" Okosted (DW_EK = -1)
IF NOT EXISTS (SELECT 1 FROM [dim].[Okosted] WHERE dw_ek_Okosted = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Okosted] (
            dw_ek_Okosted, code_okosted, name, [level],
            dw_ek_Okosted_parent, parent_code, active,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (
            -1, ''UNKNOWN'', ''Unknown Okosted'', NULL,
            NULL, NULL, 1,
            GETDATE(), GETDATE()
        )
    ');
END
GO

-- dim.SHAK
-- Analytics-ready SHAK (Sygehus-Afdelingsklassifikation) dimension
-- Self-referencing parent-child hierarchy derived from the SHAK codes
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[SHAK]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[SHAK] (
        [dw_ek_SHAK] [int] PRIMARY KEY,
        [code_shak] [varchar](50) NOT NULL,
        [name] [nvarchar](500),
        [level] [int],
        [dw_ek_SHAK_parent] [int],
        [parent_code] [varchar](50),
        [active] [bit] CONSTRAINT [DF_SHAK_active] DEFAULT 1,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAK_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAK_dw_timestamp_update] DEFAULT GETDATE()
    ) ON [PRIMARY];

    CREATE INDEX IX_dim_SHAK_code_shak
        ON [dim].[SHAK] ([code_shak]);

    CREATE INDEX IX_dim_SHAK_Parent
        ON [dim].[SHAK] ([dw_ek_SHAK_parent])
        WHERE [dw_ek_SHAK_parent] IS NOT NULL;
END
GO

-- Sentinel row for "Unknown" SHAK (DW_EK = -1)
IF NOT EXISTS (SELECT 1 FROM [dim].[SHAK] WHERE dw_ek_SHAK = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[SHAK] (
            dw_ek_SHAK, code_shak, name, [level],
            dw_ek_SHAK_parent, parent_code, active,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (
            -1, ''UNKNOWN'', ''Unknown SHAK'', NULL,
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
   AND EXISTS (SELECT 1 FROM [dim].[Kontoplan] WHERE [dw_ek_Kontoplan] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Kontoplan found in dim.Kontoplan', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Kreditor] WHERE [dw_ek_Kreditor] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Kreditor found in dim.Kreditor', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Leverandoervare] WHERE [dw_ek_Leverandoervare] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Leverandoervare found in dim.Leverandoervare', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Faktura] WHERE [dw_ek_Faktura] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Faktura found in dim.Faktura', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Fakturalinje] WHERE [dw_ek_Fakturalinje] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Fakturalinje found in dim.Fakturalinje', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Postering] WHERE [dw_ek_Postering] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Postering found in dim.Postering', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Aftale] WHERE [dw_ek_Aftale] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Aftale found in dim.Aftale', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Vareudlevering] WHERE [dw_ek_Vareudlevering] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Vareudlevering found in dim.Vareudlevering', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Lager] WHERE [dw_ek_Lager] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Lager found in dim.Lager', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Rekvirent] WHERE [dw_ek_Rekvirent] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Rekvirent found in dim.Rekvirent', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[LagerbeholdningEgenskaber] WHERE [dw_ek_LagerbeholdningEgenskaber] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_LagerbeholdningEgenskaber found in dim.LagerbeholdningEgenskaber', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[AfdelingLagerbeholdningEgenskaber] WHERE [dw_ek_AfdelingLagerbeholdningEgenskaber] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_AfdelingLagerbeholdningEgenskaber found in dim.AfdelingLagerbeholdningEgenskaber', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[ean]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[ean] WHERE [dw_ek_EAN] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_EAN found in dim.ean', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[ILSVare] WHERE [dw_ek_ILSVare] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_ILSVare found in dim.ILSVare', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[BudgetEgenskaber] WHERE [dw_ek_BudgetEgenskaber] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_BudgetEgenskaber found in dim.BudgetEgenskaber', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[UNSPSC] WHERE [dw_ek_UNSPSC] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_UNSPSC found in dim.UNSPSC', 1;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Okosted]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM [dim].[Okosted] WHERE [dw_ek_Okosted] IS NULL)
BEGIN
    THROW 50000, 'Null dw_ek_Okosted found in dim.Okosted', 1;
END
GO
