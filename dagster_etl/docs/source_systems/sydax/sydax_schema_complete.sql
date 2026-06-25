-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- DDL Export for RDW
-- Exported: 2026-04-22T11:01:12.563230
-- Database: RDW
-- Schemas: Finans, Kreditor
USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'RDW')
BEGIN
    CREATE DATABASE [RDW];
END
GO

USE [RDW];
GO

-- ============================================
-- Schema: Finans (123 tables)
-- ============================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Finans')
    EXEC('CREATE SCHEMA [Finans]');
GO

-- Drop existing FK constraints in Finans
GO

-- Drop existing tables in Finans
IF OBJECT_ID('Finans.Virksomhedsenhed', 'U') IS NOT NULL DROP TABLE [Finans].[Virksomhedsenhed];
IF OBJECT_ID('Finans.Varemomsgruppe', 'U') IS NOT NULL DROP TABLE [Finans].[Varemomsgruppe];
IF OBJECT_ID('Finans.Temp_FinanskontoKontostreng', 'U') IS NOT NULL DROP TABLE [Finans].[Temp_FinanskontoKontostreng];
IF OBJECT_ID('Finans.Sted', 'U') IS NOT NULL DROP TABLE [Finans].[Sted];
IF OBJECT_ID('Finans.Rapporteringstype', 'U') IS NOT NULL DROP TABLE [Finans].[Rapporteringstype];
IF OBJECT_ID('Finans.PrisRegisterPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[PrisRegisterPrisme];
IF OBJECT_ID('Finans.PrisRegister', 'U') IS NOT NULL DROP TABLE [Finans].[PrisRegister];
IF OBJECT_ID('Finans.PolitiskOrganisation', 'U') IS NOT NULL DROP TABLE [Finans].[PolitiskOrganisation];
IF OBJECT_ID('Finans.Oplysningspligt', 'U') IS NOT NULL DROP TABLE [Finans].[Oplysningspligt];
IF OBJECT_ID('Finans.OmkostningsstedHierarki', 'U') IS NOT NULL DROP TABLE [Finans].[OmkostningsstedHierarki];
IF OBJECT_ID('Finans.OmkostningsstedAutoriseret', 'U') IS NOT NULL DROP TABLE [Finans].[OmkostningsstedAutoriseret];
IF OBJECT_ID('Finans.Omkostningssted', 'U') IS NOT NULL DROP TABLE [Finans].[Omkostningssted];
IF OBJECT_ID('Finans.Momsretning', 'U') IS NOT NULL DROP TABLE [Finans].[Momsretning];
IF OBJECT_ID('Finans.Momsregler', 'U') IS NOT NULL DROP TABLE [Finans].[Momsregler];
IF OBJECT_ID('Finans.Momsposteringer', 'U') IS NOT NULL DROP TABLE [Finans].[Momsposteringer];
IF OBJECT_ID('Finans.MomsOprindelse', 'U') IS NOT NULL DROP TABLE [Finans].[MomsOprindelse];
IF OBJECT_ID('Finans.Momskode', 'U') IS NOT NULL DROP TABLE [Finans].[Momskode];
IF OBJECT_ID('Finans.Momskilde', 'U') IS NOT NULL DROP TABLE [Finans].[Momskilde];
IF OBJECT_ID('Finans.Momsfri', 'U') IS NOT NULL DROP TABLE [Finans].[Momsfri];
IF OBJECT_ID('Finans.LedelstilsynKolonnetype', 'U') IS NOT NULL DROP TABLE [Finans].[LedelstilsynKolonnetype];
IF OBJECT_ID('Finans.LedelsestilsynsskabelonPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[LedelsestilsynsskabelonPrisme];
IF OBJECT_ID('Finans.LedelsestilsynsskabelonGruppe', 'U') IS NOT NULL DROP TABLE [Finans].[LedelsestilsynsskabelonGruppe];
IF OBJECT_ID('Finans.Ledelsestilsynsskabelon', 'U') IS NOT NULL DROP TABLE [Finans].[Ledelsestilsynsskabelon];
IF OBJECT_ID('Finans.LedelsestilsynArkivPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[LedelsestilsynArkivPrisme];
IF OBJECT_ID('Finans.LedelsestilsynArkiv', 'U') IS NOT NULL DROP TABLE [Finans].[LedelsestilsynArkiv];
IF OBJECT_ID('Finans.KontotypeAC', 'U') IS NOT NULL DROP TABLE [Finans].[KontotypeAC];
IF OBJECT_ID('Finans.Kontotype', 'U') IS NOT NULL DROP TABLE [Finans].[Kontotype];
IF OBJECT_ID('Finans.Kontostruktur', 'U') IS NOT NULL DROP TABLE [Finans].[Kontostruktur];
IF OBJECT_ID('Finans.KontoRegler2AvanceretRegelstruktur', 'U') IS NOT NULL DROP TABLE [Finans].[KontoRegler2AvanceretRegelstruktur];
IF OBJECT_ID('Finans.Kontoregel', 'U') IS NOT NULL DROP TABLE [Finans].[Kontoregel];
IF OBJECT_ID('Finans.Kontoplan', 'U') IS NOT NULL DROP TABLE [Finans].[Kontoplan];
IF OBJECT_ID('Finans.JuridiskEnhedMatrix', 'U') IS NOT NULL DROP TABLE [Finans].[JuridiskEnhedMatrix];
IF OBJECT_ID('Finans.JuridiskEnhed', 'U') IS NOT NULL DROP TABLE [Finans].[JuridiskEnhed];
IF OBJECT_ID('Finans.IndenrigsministeriumKategori', 'U') IS NOT NULL DROP TABLE [Finans].[IndenrigsministeriumKategori];
IF OBJECT_ID('Finans.Hovedkontotype', 'U') IS NOT NULL DROP TABLE [Finans].[Hovedkontotype];
IF OBJECT_ID('Finans.HovedkontoKategori', 'U') IS NOT NULL DROP TABLE [Finans].[HovedkontoKategori];
IF OBJECT_ID('Finans.HovedkontoDimensioner', 'U') IS NOT NULL DROP TABLE [Finans].[HovedkontoDimensioner];
IF OBJECT_ID('Finans.Hovedkonto', 'U') IS NOT NULL DROP TABLE [Finans].[Hovedkonto];
IF OBJECT_ID('Finans.HierarkiRelationer', 'U') IS NOT NULL DROP TABLE [Finans].[HierarkiRelationer];
IF OBJECT_ID('Finans.Hierarki', 'U') IS NOT NULL DROP TABLE [Finans].[Hierarki];
IF OBJECT_ID('Finans.GrupperingPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[GrupperingPrisme];
IF OBJECT_ID('Finans.GrupperingAutoriseret', 'U') IS NOT NULL DROP TABLE [Finans].[GrupperingAutoriseret];
IF OBJECT_ID('Finans.Gruppering1', 'U') IS NOT NULL DROP TABLE [Finans].[Gruppering1];
IF OBJECT_ID('Finans.Gruppering', 'U') IS NOT NULL DROP TABLE [Finans].[Gruppering];
IF OBJECT_ID('Finans.FunktionPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[FunktionPrisme];
IF OBJECT_ID('Finans.Funktion', 'U') IS NOT NULL DROP TABLE [Finans].[Funktion];
IF OBJECT_ID('Finans.Formål', 'U') IS NOT NULL DROP TABLE [Finans].[Formål];
IF OBJECT_ID('Finans.Finanstransaktionstype', 'U') IS NOT NULL DROP TABLE [Finans].[Finanstransaktionstype];
IF OBJECT_ID('Finans.Finansposteringstype', 'U') IS NOT NULL DROP TABLE [Finans].[Finansposteringstype];
IF OBJECT_ID('Finans.FinansposteringPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[FinansposteringPrisme];
IF OBJECT_ID('Finans.FinansposteringKreditortransRelationPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[FinansposteringKreditortransRelationPrisme];
IF OBJECT_ID('Finans.FinansposteringKreditortransRelation', 'U') IS NOT NULL DROP TABLE [Finans].[FinansposteringKreditortransRelation];
IF OBJECT_ID('Finans.Finanspostering_NRT', 'U') IS NOT NULL DROP TABLE [Finans].[Finanspostering_NRT];
IF OBJECT_ID('Finans.Finanspostering', 'U') IS NOT NULL DROP TABLE [Finans].[Finanspostering];
IF OBJECT_ID('Finans.Finansposter_Momspostering_Relations', 'U') IS NOT NULL DROP TABLE [Finans].[Finansposter_Momspostering_Relations];
IF OBJECT_ID('Finans.FinanskontoVirksomhedsenhed', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoVirksomhedsenhed];
IF OBJECT_ID('Finans.FinanskontoSted', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoSted];
IF OBJECT_ID('Finans.FinanskontoPrisregister', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoPrisregister];
IF OBJECT_ID('Finans.FinanskontoPrismePrisregisterPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoPrismePrisregisterPrisme];
IF OBJECT_ID('Finans.FinanskontoPrismeFinanskonto', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoPrismeFinanskonto];
IF OBJECT_ID('Finans.FinanskontoPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoPrisme];
IF OBJECT_ID('Finans.FinanskontoPolitiskOrganisation', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoPolitiskOrganisation];
IF OBJECT_ID('Finans.FinanskontoOmkostningsstedhierarki', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoOmkostningsstedhierarki];
IF OBJECT_ID('Finans.FinanskontoOmkostningssted', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoOmkostningssted];
IF OBJECT_ID('Finans.FinanskontoKontostreng', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoKontostreng];
IF OBJECT_ID('Finans.FinanskontoJuridiskEnhed', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoJuridiskEnhed];
IF OBJECT_ID('Finans.FinanskontoIndenrigsministeriumKategori', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoIndenrigsministeriumKategori];
IF OBJECT_ID('Finans.FinanskontoHovedkonto', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoHovedkonto];
IF OBJECT_ID('Finans.FinanskontoGrupperingAutoriseret', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoGrupperingAutoriseret];
IF OBJECT_ID('Finans.FinanskontoGruppering1', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoGruppering1];
IF OBJECT_ID('Finans.FinanskontoGruppering', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoGruppering];
IF OBJECT_ID('Finans.FinanskontoFunktion', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoFunktion];
IF OBJECT_ID('Finans.FinanskontoFormål', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoFormål];
IF OBJECT_ID('Finans.FinanskontoFagkonto', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoFagkonto];
IF OBJECT_ID('Finans.FinanskontoEjerforhold', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoEjerforhold];
IF OBJECT_ID('Finans.FinanskontoDranst', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoDranst];
IF OBJECT_ID('Finans.FinanskontoDimensionX', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoDimensionX];
IF OBJECT_ID('Finans.FinanskontoDimensionV', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoDimensionV];
IF OBJECT_ID('Finans.FinanskontoDimensionH', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoDimensionH];
IF OBJECT_ID('Finans.FinanskontoDimension2', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoDimension2];
IF OBJECT_ID('Finans.FinanskontoDimension1', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoDimension1];
IF OBJECT_ID('Finans.FinanskontoBærer', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoBærer];
IF OBJECT_ID('Finans.FinanskontoArt', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoArt];
IF OBJECT_ID('Finans.FinanskontoAfdeling', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoAfdeling];
IF OBJECT_ID('Finans.FinanskontoAdministrativOrganisation', 'U') IS NOT NULL DROP TABLE [Finans].[FinanskontoAdministrativOrganisation];
IF OBJECT_ID('Finans.Finanskonto_light', 'U') IS NOT NULL DROP TABLE [Finans].[Finanskonto_light];
IF OBJECT_ID('Finans.Finanskonto_kela_20200921', 'U') IS NOT NULL DROP TABLE [Finans].[Finanskonto_kela_20200921];
IF OBJECT_ID('Finans.Finanskonto', 'U') IS NOT NULL DROP TABLE [Finans].[Finanskonto];
IF OBJECT_ID('Finans.Finanskladdelinje', 'U') IS NOT NULL DROP TABLE [Finans].[Finanskladdelinje];
IF OBJECT_ID('Finans.Finanskladde', 'U') IS NOT NULL DROP TABLE [Finans].[Finanskladde];
IF OBJECT_ID('Finans.Fagkonto', 'U') IS NOT NULL DROP TABLE [Finans].[Fagkonto];
IF OBJECT_ID('Finans.EjerforholdPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[EjerforholdPrisme];
IF OBJECT_ID('Finans.Ejerforhold', 'U') IS NOT NULL DROP TABLE [Finans].[Ejerforhold];
IF OBJECT_ID('Finans.DranstPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[DranstPrisme];
IF OBJECT_ID('Finans.Dranst', 'U') IS NOT NULL DROP TABLE [Finans].[Dranst];
IF OBJECT_ID('Finans.DimensionX', 'U') IS NOT NULL DROP TABLE [Finans].[DimensionX];
IF OBJECT_ID('Finans.DimensionV', 'U') IS NOT NULL DROP TABLE [Finans].[DimensionV];
IF OBJECT_ID('Finans.DimensionJuridiskEnhed', 'U') IS NOT NULL DROP TABLE [Finans].[DimensionJuridiskEnhed];
IF OBJECT_ID('Finans.DimensionH', 'U') IS NOT NULL DROP TABLE [Finans].[DimensionH];
IF OBJECT_ID('Finans.Dimension2', 'U') IS NOT NULL DROP TABLE [Finans].[Dimension2];
IF OBJECT_ID('Finans.Dimension1_rettet20210222', 'U') IS NOT NULL DROP TABLE [Finans].[Dimension1_rettet20210222];
IF OBJECT_ID('Finans.Dimension1', 'U') IS NOT NULL DROP TABLE [Finans].[Dimension1];
IF OBJECT_ID('Finans.Bærer', 'U') IS NOT NULL DROP TABLE [Finans].[Bærer];
IF OBJECT_ID('Finans.BudgetAar', 'U') IS NOT NULL DROP TABLE [Finans].[BudgetAar];
IF OBJECT_ID('Finans.BudgetpostStatus', 'U') IS NOT NULL DROP TABLE [Finans].[BudgetpostStatus];
IF OBJECT_ID('Finans.Budgetpost', 'U') IS NOT NULL DROP TABLE [Finans].[Budgetpost];
IF OBJECT_ID('Finans.BudgetmodelType', 'U') IS NOT NULL DROP TABLE [Finans].[BudgetmodelType];
IF OBJECT_ID('Finans.Budgetmodelmatrice', 'U') IS NOT NULL DROP TABLE [Finans].[Budgetmodelmatrice];
IF OBJECT_ID('Finans.Budgetmodelhierarki', 'U') IS NOT NULL DROP TABLE [Finans].[Budgetmodelhierarki];
IF OBJECT_ID('Finans.Budgetmodel', 'U') IS NOT NULL DROP TABLE [Finans].[Budgetmodel];
IF OBJECT_ID('Finans.BudgetbeløbType', 'U') IS NOT NULL DROP TABLE [Finans].[BudgetbeløbType];
IF OBJECT_ID('Finans.Bogføringstype', 'U') IS NOT NULL DROP TABLE [Finans].[Bogføringstype];
IF OBJECT_ID('Finans.BilagsrelationerUdvidet', 'U') IS NOT NULL DROP TABLE [Finans].[BilagsrelationerUdvidet];
IF OBJECT_ID('Finans.Bilagsrelationer', 'U') IS NOT NULL DROP TABLE [Finans].[Bilagsrelationer];
IF OBJECT_ID('Finans.BilagsnummerPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[BilagsnummerPrisme];
IF OBJECT_ID('Finans.Bilagsnummer', 'U') IS NOT NULL DROP TABLE [Finans].[Bilagsnummer];
IF OBJECT_ID('Finans.AvanceretRegelstruktur', 'U') IS NOT NULL DROP TABLE [Finans].[AvanceretRegelstruktur];
IF OBJECT_ID('Finans.ArtPrisme', 'U') IS NOT NULL DROP TABLE [Finans].[ArtPrisme];
IF OBJECT_ID('Finans.ArtAutoriseret', 'U') IS NOT NULL DROP TABLE [Finans].[ArtAutoriseret];
IF OBJECT_ID('Finans.Art', 'U') IS NOT NULL DROP TABLE [Finans].[Art];
IF OBJECT_ID('Finans.Anlægsaktiver', 'U') IS NOT NULL DROP TABLE [Finans].[Anlægsaktiver];
IF OBJECT_ID('Finans.Afdeling', 'U') IS NOT NULL DROP TABLE [Finans].[Afdeling];
IF OBJECT_ID('Finans.AdministrativOrganisation', 'U') IS NOT NULL DROP TABLE [Finans].[AdministrativOrganisation];
GO

-- Create tables in Finans
CREATE TABLE [Finans].[AdministrativOrganisation] (
    [AdministrativOrganisationId] bigint IDENTITY(1,1) NOT NULL,
    [Organisationsnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Organisationstekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentAdministrativOrganisationId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_AdministrativOrganisation] PRIMARY KEY ([AdministrativOrganisationId])
);
GO

CREATE TABLE [Finans].[Afdeling] (
    [AfdelingId] bigint IDENTITY(1,1) NOT NULL,
    [Afdelingsnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Afdelingstekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [E1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [O1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_RECID] bigint NULL,
    [ref_DirPartyTableId] bigint NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [RECID] bigint NULL,
    CONSTRAINT [PK_Afdeling] PRIMARY KEY ([AfdelingId])
);
GO

CREATE TABLE [Finans].[Anlægsaktiver] (
    [AnlægsaktiverId] bigint IDENTITY(1,1) NOT NULL,
    [Kategori1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Kategori2] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AnlægsaktiverTekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] date NOT NULL,
    [GyldigTil] date NOT NULL,
    [Autoriseret] int NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [ref_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Anlægsaktiver] PRIMARY KEY ([AnlægsaktiverId])
);
GO

CREATE TABLE [Finans].[Art] (
    [ArtId] bigint IDENTITY(1,1) NOT NULL,
    [Artsnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Artstekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [A1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [A2] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Momsretning] int NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [ArtDimension] varchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [RECID] bigint NULL,
    CONSTRAINT [PK_Art] PRIMARY KEY ([ArtId])
);
GO

CREATE TABLE [Finans].[ArtAutoriseret] (
    [ArtAutoriseretId] bigint IDENTITY(1,1) NOT NULL,
    [A1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [A2] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ArtAutoriseretTekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] date NOT NULL,
    [GyldigTil] date NOT NULL,
    [Autoriseret] int NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [ref_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_ArtAutoriseret] PRIMARY KEY ([ArtAutoriseretId])
);
GO

CREATE TABLE [Finans].[ArtPrisme] (
    [ArtPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Artsnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Artstekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentArtPrismeId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_DimensionFinancialTagId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_ArtPrisme] PRIMARY KEY ([ArtPrismeId])
);
GO

CREATE TABLE [Finans].[AvanceretRegelstruktur] (
    [AvanceretRegelstruktur] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [AvanceretRegelstrukturRecId] bigint NULL,
    [Beskrivelse] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [Segment1] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [Segment2] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [Segment3] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [Aktiv] varchar(3) COLLATE Danish_Norwegian_CI_AS NULL,
    [StartDateTime] datetime NULL,
    [EndDateTime] datetime NULL,
    [RunId] int NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Bilagsnummer] (
    [BilagsnummerID] bigint IDENTITY(1,1) NOT NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LedgerJournalTransId_drop] bigint NULL,
    [ref_GeneralJournalEntryId] bigint NULL,
    [RowReason] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [StartDateTime] datetime2(0) NULL,
    [EndDateTime] datetime2(7) NULL,
    [RunId] int NULL,
    [RdwTs] datetime2(7) NULL,
    [IsCounterPost] bit NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [DokumentId] bigint NULL,
    [Rel_DokumentId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Bilangsnummer] PRIMARY KEY ([BilagsnummerID], [DataAreaId])
);
GO

CREATE TABLE [Finans].[BilagsnummerPrisme] (
    [BilagsnummerPrismeID] bigint IDENTITY(100000000,1) NOT NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgertransId] bigint NULL,
    [RowReason] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [StartDateTime] datetime2(0) NULL,
    [EndDateTime] datetime2(7) NULL,
    [RunId] int NULL,
    [RdwTs] datetime2(7) NULL,
    [IsCounterPost] bit NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [DokumentId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_BilagsnummerPrisme] PRIMARY KEY ([BilagsnummerPrismeID])
);
GO

CREATE TABLE [Finans].[Bilagsrelationer] (
    [BilagsrelationerId] bigint IDENTITY(1,1) NOT NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [Bogfoeringsdato] date NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Bilagsrelationer] PRIMARY KEY ([BilagsrelationerId])
);
GO

CREATE TABLE [Finans].[BilagsrelationerUdvidet] (
    [BilagsrelationerUdvidetId] bigint IDENTITY(1,1) NOT NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [Bogfoeringsdato] date NOT NULL,
    [FinansposteringstypeId] bigint NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_BilagsrelationerUdvidet] PRIMARY KEY ([BilagsrelationerUdvidetId])
);
GO

CREATE TABLE [Finans].[Bogføringstype] (
    [BogføringstypeId] bigint IDENTITY(1,1) NOT NULL,
    [Bogføringstypenummer] int NOT NULL,
    [Bogføringstypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Bogføringstype] PRIMARY KEY ([BogføringstypeId])
);
GO

CREATE TABLE [Finans].[BudgetbeløbType] (
    [BudgetbeløbTypeId] bigint IDENTITY(1,1) NOT NULL,
    [BudgetbeløbTypenummer] int NOT NULL,
    [BudgetbeløbTypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_BudgetbeløbType] PRIMARY KEY ([BudgetbeløbTypeId])
);
GO

CREATE TABLE [Finans].[Budgetmodel] (
    [BudgetmodelId] bigint IDENTITY(1,1) NOT NULL,
    [Budgetmodelnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Budgetmodeltekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Budgetmodeltype] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ErLukket] bit NOT NULL,
    [ref_BudgetmodelId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Budgetmodel] PRIMARY KEY ([BudgetmodelId])
);
GO

CREATE TABLE [Finans].[Budgetmodelhierarki] (
    [BudgetmodelhierarkiId] bigint IDENTITY(1,1) NOT NULL,
    [Budgetmodelnummer_l1] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Budgetmodelnummer_l2] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer_l3] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer_l4] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer_l5] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer_l6] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer_l7] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer_l8] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer_l9] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetmodelnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BudgetmodelId] bigint NOT NULL,
    [DATAAREAID] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Budgetmodelmatrice] (
    [BudgetmodelmatriceId] bigint IDENTITY(1,1) NOT NULL,
    [BudgetmodelId] bigint NOT NULL,
    [SubBudgetmodelId] bigint NOT NULL,
    [Niveau] smallint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Budgetmodelmatrice] PRIMARY KEY ([BudgetmodelmatriceId])
);
GO

CREATE TABLE [Finans].[BudgetmodelType] (
    [BudgetmodelTypeId] bigint IDENTITY(1,1) NOT NULL,
    [BudgetmodelTypenummer] int NOT NULL,
    [BudgetmodelTypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_BudgetmodelType] PRIMARY KEY ([BudgetmodelTypeId])
);
GO

CREATE TABLE [Finans].[Budgetpost] (
    [BudgetpostId] bigint IDENTITY(1,1) NOT NULL,
    [BudgetmodelId] bigint NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [Budgetdato] date NOT NULL,
    [Budgetbeloeb] numeric(28,12) NOT NULL,
    [Valutakode] char(3) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BudgetbeloebValuta] numeric(28,12) NOT NULL,
    [Kommentar] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [AfdelingPrismeId] bigint NULL,
    [DebetKredit] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oprettelsestidspunkt] datetime2(7) NOT NULL,
    [OprettetAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [SidstAendretTidspunkt] datetime2(7) NOT NULL,
    [SidstAendretAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [IsCounterpost] bit NOT NULL,
    [CounterpostsThisBudgetpostId] bigint NULL,
    [ref_LedgerbudgetId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DokumentId] bigint NULL,
    [Budgetloebenr] varchar(50) COLLATE Danish_Norwegian_CI_AS NULL,
    [Expandid] bigint NULL,
    [Report] int NULL,
    [BudgetAdoptedDate] date NULL,
    [Ændringstype] varchar(5) COLLATE Danish_Norwegian_CI_AS NULL,
    [ÆndringstypeBeskrivelse] varchar(40) COLLATE Danish_Norwegian_CI_AS NULL,
    [Budgetkode] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Årsagskode] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Årsagskommentar] varchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [Indtastningsnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [BudgetpostStatusId] bigint NULL,
    [Udloebsdato] date NULL,
    [BeloebstypeId] int NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Budgetpost] PRIMARY KEY ([BudgetpostId])
);
GO

CREATE TABLE [Finans].[BudgetpostStatus] (
    [BudgetpostStatusId] bigint IDENTITY(1,1) NOT NULL,
    [BudgetpostStatusnummer] int NOT NULL,
    [BudgetpostStatustekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_BudgetpostStatus] PRIMARY KEY ([BudgetpostStatusId])
);
GO

CREATE TABLE [Finans].[BudgetAar] (
    [BudgetAarId] bigint IDENTITY(1,1) NOT NULL,
    [BudgetAar] date NOT NULL,
    [Ref_LedgerparametersId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_BudgetAar] PRIMARY KEY ([BudgetAarId])
);
GO

CREATE TABLE [Finans].[Bærer] (
    [BærerId] bigint IDENTITY(1,1) NOT NULL,
    [Bærernummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Bærertekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Medarbejder] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Koncerndimension] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErLukket] bit NOT NULL,
    [ref_DimensionsId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Bærer] PRIMARY KEY ([BærerId])
);
GO

CREATE TABLE [Finans].[Dimension1] (
    [Dimension1Id] bigint IDENTITY(1,1) NOT NULL,
    [Dimension1nummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dimension1tekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dimension1Dimension] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [RECID] bigint NULL,
    CONSTRAINT [PK_Dimension1] PRIMARY KEY ([Dimension1Id])
);
GO

CREATE TABLE [Finans].[Dimension1_rettet20210222] (
    [Dimension1Id] bigint NOT NULL,
    [Dimension1nummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dimension1tekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dimension1Dimension] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Dimension2] (
    [Dimension2Id] bigint IDENTITY(1,1) NOT NULL,
    [Dimension2nummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dimension2tekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dimension2Dimension] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [RECID] bigint NULL,
    CONSTRAINT [PK_Dimension2] PRIMARY KEY ([Dimension2Id])
);
GO

CREATE TABLE [Finans].[DimensionH] (
    [DimensionHId] bigint IDENTITY(1,1) NOT NULL,
    [DimensionHnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DimensionHtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentDimensionHId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_DimensionH] PRIMARY KEY ([DimensionHId])
);
GO

CREATE TABLE [Finans].[DimensionJuridiskEnhed] (
    [DimensionJuridiskEnhedid] bigint IDENTITY(1,1) NOT NULL,
    [ref_DimensionAttributeValueId] bigint NOT NULL,
    [JuridiskEnhedId] bigint NOT NULL,
    [AktivFra] date NULL,
    [AktivTil] date NULL,
    [Suspenderet] int NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_DimensionJuridiskEnhed] PRIMARY KEY ([DimensionJuridiskEnhedid])
);
GO

CREATE TABLE [Finans].[DimensionV] (
    [DimensionVid] bigint IDENTITY(1,1) NOT NULL,
    [DimensionVnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DimensionVtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentDimensionVid] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_DimensionV] PRIMARY KEY ([DimensionVid])
);
GO

CREATE TABLE [Finans].[DimensionX] (
    [DimensionXId] bigint IDENTITY(1,1) NOT NULL,
    [DimensionXnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DimensionXtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentDimensionXId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_DimensionX] PRIMARY KEY ([DimensionXId])
);
GO

CREATE TABLE [Finans].[Dranst] (
    [DranstId] bigint IDENTITY(1,1) NOT NULL,
    [D1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [D1Tekst] varchar(360) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] date NOT NULL,
    [GyldigTil] date NOT NULL,
    [Autoriseret] int NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Dranst] PRIMARY KEY ([DranstId])
);
GO

CREATE TABLE [Finans].[DranstPrisme] (
    [DranstPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [D1] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dransttekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentDranstPrismeId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_DranstPrisme] PRIMARY KEY ([DranstPrismeId])
);
GO

CREATE TABLE [Finans].[Ejerforhold] (
    [EjerforholdId] bigint IDENTITY(1,1) NOT NULL,
    [E1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [E1Tekst] varchar(360) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] date NOT NULL,
    [GyldigTil] date NOT NULL,
    [Autoriseret] int NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Ejerforhold] PRIMARY KEY ([EjerforholdId])
);
GO

CREATE TABLE [Finans].[EjerforholdPrisme] (
    [EjerforholdPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Ejerforholdsnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Ejerforholdstekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentEjerforholdPrismeId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_EjerforholdPrisme] PRIMARY KEY ([EjerforholdPrismeId])
);
GO

CREATE TABLE [Finans].[Fagkonto] (
    [FagkontoId] bigint IDENTITY(1,1) NOT NULL,
    [Fagkontonummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Fagkontotekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentFagkontoId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Fagkonto] PRIMARY KEY ([FagkontoId])
);
GO

CREATE TABLE [Finans].[Finanskladde] (
    [FinanskladdeId] bigint IDENTITY(1,1) NOT NULL,
    [Kladdenummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Kladdetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Kladdenavn] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErLukket] bit NOT NULL,
    [ref_LedgerjournaltableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DokumentId] bigint NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [Maskinnummer] varchar(5) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Finanskladde] PRIMARY KEY ([FinanskladdeId])
);
GO

CREATE TABLE [Finans].[Finanskladdelinje] (
    [FinanskladdelinjeId] bigint IDENTITY(1,1) NOT NULL,
    [Linjenummer] decimal(32,16) NOT NULL,
    [dato] date NOT NULL,
    [Beskrivelse] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [Debet] numeric(28,12) NOT NULL,
    [Kredit] numeric(28,12) NOT NULL,
    [FinanskladdeId] bigint NULL,
    [FinanstransaktionstypeId] bigint NULL,
    [Ekspeditionsnummer] varchar(7) COLLATE Danish_Norwegian_CI_AS NULL,
    [Maskinnummer] varchar(5) COLLATE Danish_Norwegian_CI_AS NULL,
    [FinanskontoId] bigint NULL,
    [BilagsnummerID] bigint NULL,
    [DokumentId] bigint NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LedgertransId] bigint NULL,
    [ref_RecId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Finanskladdelinje] PRIMARY KEY ([FinanskladdelinjeId])
);
GO

CREATE TABLE [Finans].[Finanskonto] (
    [FinanskontoId] bigint IDENTITY(1,1) NOT NULL,
    [Kontonummer] varchar(500) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErLukket] bit NOT NULL,
    [ve_RECID] bigint NULL,
    [afd_RECID] bigint NULL,
    [os_RECID] bigint NULL,
    [hk_RECID] bigint NULL,
    [ma_RECID] bigint NULL,
    [grp_RECID] bigint NULL,
    [art_RECID] bigint NULL,
    [d1_RECID] bigint NULL,
    [d2_RECID] bigint NULL,
    [l_RECID] bigint NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Finanskonto] PRIMARY KEY ([FinanskontoId])
);
GO

CREATE TABLE [Finans].[Finanskonto_kela_20200921] (
    [FinanskontoId] bigint IDENTITY(1,1) NOT NULL,
    [Kontonummer] varchar(500) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErLukket] bit NOT NULL,
    [ve_RECID] bigint NULL,
    [afd_RECID] bigint NULL,
    [os_RECID] bigint NULL,
    [hk_RECID] bigint NULL,
    [ma_RECID] bigint NULL,
    [grp_RECID] bigint NULL,
    [art_RECID] bigint NULL,
    [d1_RECID] bigint NULL,
    [d2_RECID] bigint NULL,
    [l_RECID] bigint NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Finanskonto_light] (
    [Finanskonto] varchar(500) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Omkostningssted] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Afdeling] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Hovedkonto] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Art] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [D1] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [F1] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [F2] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [F3] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [G1] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [Ejerforholdsnummer] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [FinanskontoId] bigint NOT NULL,
    [StartDateTime] datetime NULL,
    [EndDateTime] datetime NULL,
    [RunId] int NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [Virksomhedsenhed] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Gruppering] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [A1] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [A2] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [O1] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [Dimension1] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Dimension2] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL
);
GO

CREATE TABLE [Finans].[FinanskontoAdministrativOrganisation] (
    [FinanskontoAdministrativOrganisationId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [AdministrativOrganisationId] bigint NOT NULL,
    [ref_LedgerDimensionDerivedId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoAdministrativOrganisation] PRIMARY KEY ([FinanskontoAdministrativOrganisationId])
);
GO

CREATE TABLE [Finans].[FinanskontoAfdeling] (
    [FinanskontoAfdelingid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [Afdelingid] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoAfdeling] PRIMARY KEY ([FinanskontoAfdelingid])
);
GO

CREATE TABLE [Finans].[FinanskontoArt] (
    [FinanskontoArtId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [ArtId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoArt] PRIMARY KEY ([FinanskontoArtId])
);
GO

CREATE TABLE [Finans].[FinanskontoBærer] (
    [FinanskontoBærerId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [BærerId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoBærer] PRIMARY KEY ([FinanskontoBærerId])
);
GO

CREATE TABLE [Finans].[FinanskontoDimension1] (
    [FinanskontoDimension1id] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [Dimension1id] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoDimension1] PRIMARY KEY ([FinanskontoDimension1id])
);
GO

CREATE TABLE [Finans].[FinanskontoDimension2] (
    [FinanskontoDimension2id] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [Dimension2id] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoDimension2] PRIMARY KEY ([FinanskontoDimension2id])
);
GO

CREATE TABLE [Finans].[FinanskontoDimensionH] (
    [FinanskontoDimensionHid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [DimensionHid] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoDimensionH] PRIMARY KEY ([FinanskontoDimensionHid])
);
GO

CREATE TABLE [Finans].[FinanskontoDimensionV] (
    [FinanskontoDimensionVid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [DimensionVid] bigint NOT NULL,
    [ref_LedgerDimensionDerivedId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoDimensionV] PRIMARY KEY ([FinanskontoDimensionVid])
);
GO

CREATE TABLE [Finans].[FinanskontoDimensionX] (
    [FinanskontoDimensionXid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [DimensionXid] bigint NOT NULL,
    [ref_LedgerDimensionDerivedId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoDimensionX] PRIMARY KEY ([FinanskontoDimensionXid])
);
GO

CREATE TABLE [Finans].[FinanskontoDranst] (
    [FinanskontoDranstId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [HovedkontoId] bigint NULL,
    [DranstId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoDranst] PRIMARY KEY ([FinanskontoDranstId])
);
GO

CREATE TABLE [Finans].[FinanskontoEjerforhold] (
    [FinanskontoEjerforholdId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [EjerforholdId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoEjerforhold] PRIMARY KEY ([FinanskontoEjerforholdId])
);
GO

CREATE TABLE [Finans].[FinanskontoFagkonto] (
    [FinanskontoFagkontoid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [Fagkontoid] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoFagkonto] PRIMARY KEY ([FinanskontoFagkontoid])
);
GO

CREATE TABLE [Finans].[FinanskontoFormål] (
    [FinanskontoFormålId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FormålId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoFormål] PRIMARY KEY ([FinanskontoFormålId])
);
GO

CREATE TABLE [Finans].[FinanskontoFunktion] (
    [FinanskontoFunktionId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [HovedkontoId] bigint NULL,
    [FunktionId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoFunktion] PRIMARY KEY ([FinanskontoFunktionId])
);
GO

CREATE TABLE [Finans].[FinanskontoGruppering] (
    [FinanskontoGrupperingId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [GrupperingId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [HovedkontoId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoGruppering] PRIMARY KEY ([FinanskontoGrupperingId])
);
GO

CREATE TABLE [Finans].[FinanskontoGruppering1] (
    [FinanskontoGruppering1Id] bigint IDENTITY(1,1) NOT NULL,
    [HovedkontoId] bigint NOT NULL,
    [Gruppering1Id] bigint NOT NULL,
    [ref_LedgerTextId] bigint NOT NULL,
    [RunId] int NOT NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoGruppering1] PRIMARY KEY ([FinanskontoGruppering1Id])
);
GO

CREATE TABLE [Finans].[FinanskontoGrupperingAutoriseret] (
    [FinanskontoGrupperingAutoriseretId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [G1Id] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [HovedkontoId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoGrupperingAutoriseret] PRIMARY KEY ([FinanskontoGrupperingAutoriseretId])
);
GO

CREATE TABLE [Finans].[FinanskontoHovedkonto] (
    [FinanskontoHovedkontoid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [Hovedkontoid] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoHovedkonto] PRIMARY KEY ([FinanskontoHovedkontoid])
);
GO

CREATE TABLE [Finans].[FinanskontoIndenrigsministeriumKategori] (
    [FinanskontoIndenrigsministeriumKategoriId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [IndenrigsministeriumKategoriId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoIndenrigsministeriumKategori] PRIMARY KEY ([FinanskontoIndenrigsministeriumKategoriId])
);
GO

CREATE TABLE [Finans].[FinanskontoJuridiskEnhed] (
    [FinanskontoJuridiskEnhedid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [JuridiskEnhedid] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [HovedkontoId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoJuridiskEnhed] PRIMARY KEY ([FinanskontoJuridiskEnhedid])
);
GO

CREATE TABLE [Finans].[FinanskontoKontostreng] (
    [FinanskontoKontostrengId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NULL,
    [Kontonummer] varchar(500) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoKontostreng] PRIMARY KEY ([FinanskontoKontostrengId])
);
GO

CREATE TABLE [Finans].[FinanskontoOmkostningssted] (
    [FinanskontoOmkostningsstedid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [Omkostningsstedid] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoOmkostningssted] PRIMARY KEY ([FinanskontoOmkostningsstedid])
);
GO

CREATE TABLE [Finans].[FinanskontoOmkostningsstedhierarki] (
    [FinanskontoOmkostningsstedhierarkiId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [Omkostningsstedhierarkiid] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoOmkostningsstedhierarki] PRIMARY KEY ([FinanskontoOmkostningsstedhierarkiId])
);
GO

CREATE TABLE [Finans].[FinanskontoPolitiskOrganisation] (
    [FinanskontoPolitiskOrganisationid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [PolitiskOrganisationid] bigint NOT NULL,
    [ref_LedgerDimensionDerivedId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoPolitiskOrganisation] PRIMARY KEY ([FinanskontoPolitiskOrganisationid])
);
GO

CREATE TABLE [Finans].[FinanskontoPrisme] (
    [FinanskontoPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Kontonummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Kontonavn] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErLukket] bit NOT NULL,
    [LåstEfter] datetime NULL,
    [FørsteBenyttelsesår] datetime NULL,
    [SidsteBenyttelsesår] datetime NULL,
    [KontotypeId] bigint NULL,
    [MomsretningId] bigint NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DokumentID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoPrisme] PRIMARY KEY ([FinanskontoPrismeId])
);
GO

CREATE TABLE [Finans].[FinanskontoPrismeFinanskonto] (
    [FinanskontoPrismeFinanskontoId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoPrismeFinanskonto] PRIMARY KEY ([FinanskontoPrismeFinanskontoId])
);
GO

CREATE TABLE [Finans].[FinanskontoPrismePrisregisterPrisme] (
    [FinanskontoPrismePrisregisterPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Kontonummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [PrisregisterPrismeId] bigint NULL,
    [Type] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoPrismePrisregisterPrisme] PRIMARY KEY ([FinanskontoPrismePrisregisterPrismeId])
);
GO

CREATE TABLE [Finans].[FinanskontoPrisregister] (
    [FinanskontoPrisregisterId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [PrisregisterId] bigint NULL,
    [Type] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoPrisregister] PRIMARY KEY ([FinanskontoPrisregisterId])
);
GO

CREATE TABLE [Finans].[FinanskontoSted] (
    [FinanskontoStedId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NOT NULL,
    [StedId] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoSted] PRIMARY KEY ([FinanskontoStedId])
);
GO

CREATE TABLE [Finans].[FinanskontoVirksomhedsenhed] (
    [FinanskontoVirksomhedsenhedid] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoId] bigint NOT NULL,
    [Virksomhedsenhedid] bigint NOT NULL,
    [ref_LedgerTableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinanskontoVirksomhedsenhed] PRIMARY KEY ([FinanskontoVirksomhedsenhedid])
);
GO

CREATE TABLE [Finans].[Finansposter_Momspostering_Relations] (
    [Finansposter_Momspostering_RelationsID] bigint IDENTITY(1,1) NOT NULL,
    [MomsposteringerId] bigint NOT NULL,
    [FinansposteringId] bigint NOT NULL,
    [Kildesystem] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Finansposter_Momspostering_Relations] PRIMARY KEY ([Finansposter_Momspostering_RelationsID])
);
GO

CREATE TABLE [Finans].[Finanspostering] (
    [FinansposteringId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [Bogfoeringsdato] date NOT NULL,
    [Posteringsbeloeb] numeric(28,12) NULL,
    [Valutakode] char(3) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PosteringsbeloebValuta] numeric(28,12) NULL,
    [Posteringstekst] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [AfdelingPrismeId] bigint NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ErModpostering] bit NOT NULL,
    [DebetKredit] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [FinanskladdeId] bigint NULL,
    [FinanstransaktionstypeId] bigint NULL,
    [FinansposteringstypeId] bigint NULL,
    [KontrolleretDato] date NULL,
    [KontrolleretAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oprettelsestidspunkt] datetime2(7) NOT NULL,
    [OprettetAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgertransId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BilagsnummerID] bigint NULL,
    [Posteringsmoms] numeric(28,12) NULL,
    [PosteringsbeloebInklMoms] numeric(28,12) NULL,
    [DokumentId] bigint NULL,
    [BærerId] bigint NULL,
    [FormålId] bigint NULL,
    [RefusionModtager] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Lager_Kasse] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Ekspeditionsnummer] int NULL,
    [SDIdentifier] nvarchar(26) COLLATE Danish_Norwegian_CI_AS NULL,
    [Primosaldo] numeric(28,12) NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [FinansrelationId] bigint NULL,
    [Oplysningspligt] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [Varemomsgruppe] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Bogføringstypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    CONSTRAINT [PK_Finanspostering] PRIMARY KEY ([FinansposteringId])
);
GO

CREATE TABLE [Finans].[Finanspostering_NRT] (
    [FinansposteringId] bigint IDENTITY(1,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [Bogfoeringsdato] date NOT NULL,
    [Posteringsbeloeb] numeric(28,12) NOT NULL,
    [Valutakode] char(3) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PosteringsbeloebValuta] numeric(28,12) NOT NULL,
    [Posteringstekst] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [AfdelingPrismeId] bigint NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ErModpostering] bit NOT NULL,
    [DebetKredit] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [FinanskladdeId] bigint NULL,
    [FinanstransaktionstypeId] bigint NULL,
    [FinansposteringstypeId] bigint NULL,
    [KontrolleretDato] date NULL,
    [KontrolleretAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oprettelsestidspunkt] datetime2(7) NOT NULL,
    [OprettetAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgertransId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BilagsnummerID] bigint NULL,
    [Posteringsmoms] numeric(28,12) NULL,
    [PosteringsbeloebInklMoms] numeric(28,12) NULL,
    [DokumentId] bigint NULL,
    [BærerId] bigint NULL,
    [FormålId] bigint NULL,
    [RefusionModtager] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Lager_Kasse] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Ekspeditionsnummer] int NULL,
    [SDIdentifier] nvarchar(26) COLLATE Danish_Norwegian_CI_AS NULL,
    [Primosaldo] numeric(28,12) NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [FinansrelationId] bigint NULL,
    [Oplysningspligt] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Finanspostering_NRT] PRIMARY KEY ([FinansposteringId])
);
GO

CREATE TABLE [Finans].[FinansposteringKreditortransRelation] (
    [FinansposteringKreditortransRelationId] bigint NOT NULL,
    [FinansposteringId] bigint NOT NULL,
    [KreditortransaktionId] bigint NOT NULL,
    [RunId] int NOT NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinansposteringKreditortransRelation] PRIMARY KEY ([FinansposteringKreditortransRelationId])
);
GO

CREATE TABLE [Finans].[FinansposteringKreditortransRelationPrisme] (
    [FinansposteringKreditortransRelationId] bigint IDENTITY(1,1) NOT NULL,
    [FinansposteringId] bigint NOT NULL,
    [KreditortransaktionId] bigint NOT NULL,
    [RunId] int NOT NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinansposteringKreditortransRelationPrisme] PRIMARY KEY ([FinansposteringKreditortransRelationId])
);
GO

CREATE TABLE [Finans].[FinansposteringPrisme] (
    [FinansposteringId] bigint IDENTITY(100000000,1) NOT NULL,
    [FinanskontoPrismeId] bigint NULL,
    [FinanskontoId] bigint NULL,
    [Bogfoeringsdato] date NOT NULL,
    [Posteringsbeloeb] numeric(28,12) NULL,
    [Valutakode] char(3) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PosteringsbeloebValuta] numeric(28,12) NULL,
    [Posteringstekst] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [AfdelingPrismeId] bigint NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ErModpostering] bit NOT NULL,
    [DebetKredit] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [FinanskladdeId] bigint NULL,
    [FinanstransaktionstypeId] bigint NULL,
    [FinansposteringstypeId] bigint NULL,
    [KontrolleretDato] date NULL,
    [KontrolleretAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oprettelsestidspunkt] datetime2(7) NOT NULL,
    [OprettetAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgertransId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BilagsnummerID] bigint NULL,
    [Posteringsmoms] numeric(28,12) NULL,
    [PosteringsbeloebInklMoms] numeric(28,12) NULL,
    [DokumentId] bigint NULL,
    [BærerId] bigint NULL,
    [FormålId] bigint NULL,
    [RefusionModtager] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Lager_Kasse] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Ekspeditionsnummer] int NULL,
    [SDIdentifier] nvarchar(26) COLLATE Danish_Norwegian_CI_AS NULL,
    [PrimoSaldo] numeric(28,12) NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oplysningspligt] char(1) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FinansposteringPrisme] PRIMARY KEY ([FinansposteringId])
);
GO

CREATE TABLE [Finans].[Finansposteringstype] (
    [FinansposteringstypeId] bigint IDENTITY(1,1) NOT NULL,
    [Finansposteringstypenummer] int NOT NULL,
    [Finansposteringstypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Finansposteringstype] PRIMARY KEY ([FinansposteringstypeId])
);
GO

CREATE TABLE [Finans].[Finanstransaktionstype] (
    [FinanstransaktionstypeId] bigint IDENTITY(1,1) NOT NULL,
    [Transaktionstypenummer] int NOT NULL,
    [Transaktionstypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Finanstransaktionstype] PRIMARY KEY ([FinanstransaktionstypeId])
);
GO

CREATE TABLE [Finans].[Formål] (
    [FormålId] bigint IDENTITY(1,1) NOT NULL,
    [Formålsnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Formålstekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Medarbejder] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Koncerndimension] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErLukket] bit NOT NULL,
    [ref_DimensionsId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Formål] PRIMARY KEY ([FormålId])
);
GO

CREATE TABLE [Finans].[Funktion] (
    [FunktionId] bigint IDENTITY(1,1) NOT NULL,
    [F1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F2] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F3] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [FunktionTekst] varchar(360) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] date NOT NULL,
    [GyldigTil] date NOT NULL,
    [Autoriseret] int NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Funktion] PRIMARY KEY ([FunktionId])
);
GO

CREATE TABLE [Finans].[FunktionPrisme] (
    [FunktionPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Funktionsnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Funktionstekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentFunktionPrismeId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_FunktionPrisme] PRIMARY KEY ([FunktionPrismeId])
);
GO

CREATE TABLE [Finans].[Gruppering] (
    [GrupperingId] bigint IDENTITY(1,1) NOT NULL,
    [Grupperingsnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Grupperingstekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GrupperingDimension] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [Ejerforhold] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [RECID] bigint NULL,
    CONSTRAINT [PK_Gruppering] PRIMARY KEY ([GrupperingId])
);
GO

CREATE TABLE [Finans].[Gruppering1] (
    [Gruppering1Id] bigint IDENTITY(1,1) NOT NULL,
    [Gruppering1nummer] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Gruppering1tekst] varchar(360) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DEPENDDIM1LEV1] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DEPENDDIM1LEV2] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DEPENDDIM1LEV3] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DEPENDDIM2LEV1] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Gruppering1] PRIMARY KEY ([Gruppering1Id])
);
GO

CREATE TABLE [Finans].[GrupperingAutoriseret] (
    [G1Id] bigint IDENTITY(1,1) NOT NULL,
    [G1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [G1Tekst] varchar(360) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F2] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F3] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [D1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] date NOT NULL,
    [GyldigTil] date NOT NULL,
    [Autoriseret] int NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [ref_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_GrupperingAutoriseret] PRIMARY KEY ([G1Id])
);
GO

CREATE TABLE [Finans].[GrupperingPrisme] (
    [GrupperingPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Grupperingsnummer] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Grupperingstekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentGrupperingPrismeId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Grupperingsnavn] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_DimensionFinancialTagId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_GrupperingPrisme] PRIMARY KEY ([GrupperingPrismeId])
);
GO

CREATE TABLE [Finans].[Hierarki] (
    [HierarkiId] bigint IDENTITY(1,1) NOT NULL,
    [Hierarkinavn] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ValidFrom] datetime2(0) NULL,
    [ref_RECID] bigint NULL,
    [ref_OMHierarchyTypeId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ValidTo] datetime2(0) NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Hierarki] PRIMARY KEY ([HierarkiId])
);
GO

CREATE TABLE [Finans].[HierarkiRelationer] (
    [HierarkiRelationerId] bigint IDENTITY(1,1) NOT NULL,
    [ParentHierarkiRelationerId] bigint NULL,
    [Niveau] tinyint NULL,
    [ref_RECID] bigint NULL,
    [ref_Parent_RECID] bigint NULL,
    [ref_Parent_ORGANIZATIONTYPE] int NULL,
    [ref_Child_RECID] bigint NULL,
    [ref_Child_ORGANIZATIONTYPE] int NULL,
    [ref_Child_OMOPERATINGUNITTYPE] int NULL,
    [ref_OMHIERARCHYRELATIONSHIPId] bigint NULL,
    [HierarkiId] bigint NOT NULL,
    [ValidFrom] datetime2(0) NULL,
    [ValidTo] datetime2(0) NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Slettet] bit NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_HierarkiRelationer] PRIMARY KEY ([HierarkiRelationerId])
);
GO

CREATE TABLE [Finans].[Hovedkonto] (
    [HovedkontoId] bigint IDENTITY(1,1) NOT NULL,
    [Hovedkontonummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Hovedkontotekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [KontoplanId] bigint NOT NULL,
    [HovedkontotypeId] bigint NOT NULL,
    [RapporteringstypeId] bigint NOT NULL,
    [HovedkontoKategoriId] bigint NOT NULL,
    [DKForslag] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DKKrav] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Saldokontrol] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ModkontoId] bigint NOT NULL,
    [PrimokontoId] bigint NOT NULL,
    [SRUKode] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Afslut] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [TvungenReference] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CloseType] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Autoriseret] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Standardkoncernkonto] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F2] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [F3] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [D1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [G1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valuta] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ValutaValidering] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BrugerId] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BrugerValidering] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [FinansposteringstypeId] bigint NOT NULL,
    [KonteringValidering] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [ref_MainAccountId] bigint NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Hovedkonto] PRIMARY KEY ([HovedkontoId])
);
GO

CREATE TABLE [Finans].[HovedkontoDimensioner] (
    [HovedkontoDimensionerId] bigint IDENTITY(1,1) NOT NULL,
    [HovedkontoId] bigint NOT NULL,
    [JuridiskEnhedId] bigint NOT NULL,
    [ArtId] bigint NULL,
    [ArtFast] int NULL,
    [AfdelingId] bigint NULL,
    [AfdelingFast] int NULL,
    [GrupperingId] bigint NULL,
    [GrupperingFast] int NULL,
    [OmkostningsstedId] bigint NULL,
    [OmkostningsstedFast] int NULL,
    [VirksomhedsenhedId] bigint NULL,
    [VirksomhedsenhedFast] int NULL,
    [Dimension1Id] bigint NULL,
    [Dimension1Fast] int NULL,
    [Dimension2Id] bigint NULL,
    [Dimension2Fast] int NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_HovedkontoDimensioner] PRIMARY KEY ([HovedkontoDimensionerId])
);
GO

CREATE TABLE [Finans].[HovedkontoKategori] (
    [HovedkontoKategoriId] bigint IDENTITY(1,1) NOT NULL,
    [HovedkontoKategoriNummer] int NOT NULL,
    [HovedkontoKategoritekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_MainAccountCategoryId] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_HovedkontoKategori] PRIMARY KEY ([HovedkontoKategoriId])
);
GO

CREATE TABLE [Finans].[Hovedkontotype] (
    [HovedkontotypeId] bigint IDENTITY(1,1) NOT NULL,
    [Hovedkontotypenummer] int NOT NULL,
    [Hovedkontotypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Hovedkontotype] PRIMARY KEY ([HovedkontotypeId])
);
GO

CREATE TABLE [Finans].[IndenrigsministeriumKategori] (
    [IndenrigsministeriumKategoriId] bigint IDENTITY(1,1) NOT NULL,
    [IMKategoriNummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [IMKategoriTekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentIndenrigsministeriumKategoriId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_IndenrigsministeriumKategori] PRIMARY KEY ([IndenrigsministeriumKategoriId])
);
GO

CREATE TABLE [Finans].[JuridiskEnhed] (
    [JuridiskEnhedId] bigint IDENTITY(1,1) NOT NULL,
    [JuridiskEnhedsnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [JuridiskEnhedstekst] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LEDGERId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_RECID] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_JuridiskEnhed] PRIMARY KEY ([JuridiskEnhedId])
);
GO

CREATE TABLE [Finans].[JuridiskEnhedMatrix] (
    [JuridiskEnhedMatrixId] bigint NOT NULL,
    [JuridiskEnhedId] bigint NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Kontoplan] (
    [KontoplanId] bigint IDENTITY(1,1) NOT NULL,
    [KontoplanNavn] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_AddressCountryRegionId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Kontoplan] PRIMARY KEY ([KontoplanId])
);
GO

CREATE TABLE [Finans].[Kontoregel] (
    [Juridiskenhed] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [JuridiskenhedRecId] bigint NULL,
    [Kontostruktur] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [KontostrukturRecId] bigint NULL,
    [Kontoregel] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [KontoregelRecId] bigint NULL,
    [Navn] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [Kladde] varchar(3) COLLATE Danish_Norwegian_CI_AS NULL,
    [RunId] int NULL,
    [StartDateTime] datetime NULL,
    [EndDateTime] datetime NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[KontoRegler2AvanceretRegelstruktur] (
    [KontoRegler2AvanceretRegelstrukturRecId] bigint NULL,
    [AvanceretRegelstrukturRecId] bigint NULL,
    [RegelRecId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime NULL,
    [EndDateTime] datetime NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Kontostruktur] (
    [Juridiskenhed] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [JuridiskenhedRecId] bigint NULL,
    [Kontostruktur] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [KontostrukturRecId] bigint NULL,
    [AktivFra] datetime NULL,
    [AktivTil] datetime NULL,
    [Virksomhedsenhed] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [Afdeling] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [Omkostningssted] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [MainAccount] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [Gruppering] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [Art] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [StartDateTime] datetime NULL,
    [EndDateTime] datetime NULL,
    [RunId] int NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Kontotype] (
    [KontotypeId] bigint IDENTITY(1,1) NOT NULL,
    [Kontotypenummer] int NOT NULL,
    [Kontotypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Kontotype] PRIMARY KEY ([KontotypeId])
);
GO

CREATE TABLE [Finans].[KontotypeAC] (
    [KontotypeACId] bigint IDENTITY(1,1) NOT NULL,
    [KontotypeACnummer] int NOT NULL,
    [KontotypeACtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_KontotypeAC] PRIMARY KEY ([KontotypeACId])
);
GO

CREATE TABLE [Finans].[LedelsestilsynArkiv] (
    [LedelsestilsynArkivId] bigint IDENTITY(1,1) NOT NULL,
    [GodkendtAf] varchar(8) COLLATE Danish_Norwegian_CI_AS NULL,
    [GodkendtDato] datetime NOT NULL,
    [Skabelon] varchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [OprettetDato] datetime NOT NULL,
    [OprettetAf] nvarchar(8) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AnnulleretDato] datetime NOT NULL,
    [Stilling] varchar(25) COLLATE Danish_Norwegian_CI_AS NULL,
    [Afdelingsnummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [Afdelingsnavn] varchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [SkabelonBeskrivelse] varchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [EXECUTE_] int NOT NULL,
    [Stillingsbeskrivelse] varchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_AssignmentarchiveId] bigint NOT NULL,
    [ref_RECID] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [DokumentId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_LedelsestilsynArkiv] PRIMARY KEY ([LedelsestilsynArkivId])
);
GO

CREATE TABLE [Finans].[LedelsestilsynArkivPrisme] (
    [LedelsestilsynArkivPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [JobInformation] varchar(255) COLLATE Danish_Norwegian_CI_AS NULL,
    [Skabelon] varchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [AnvisesAf] varchar(5) COLLATE Danish_Norwegian_CI_AS NULL,
    [AnvistDato] datetime NOT NULL,
    [AnvistAf] varchar(5) COLLATE Danish_Norwegian_CI_AS NULL,
    [OprettelseDato] datetime NOT NULL,
    [OprindeligtOprettetAf] varchar(5) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_AssignmentarchiveId] bigint NOT NULL,
    [ref_RECID] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [DokumentId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_LedelsestilsynArkivPrisme] PRIMARY KEY ([LedelsestilsynArkivPrismeId])
);
GO

CREATE TABLE [Finans].[Ledelsestilsynsskabelon] (
    [LedelsestilsynsskabelonId] bigint IDENTITY(1,1) NOT NULL,
    [MedtagPrimo] int NOT NULL,
    [SkabelonNavn] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Budgetmodel] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Beregningsudtryk] nvarchar(50) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Overskrift1] nvarchar(50) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Overskrift2] nvarchar(50) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Kolonne] int NOT NULL,
    [LedelstilsynKolonnetypenummer] int NOT NULL,
    [Datointervalkode] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [FraDato] datetime NOT NULL,
    [MedtagNormale] int NOT NULL,
    [VærdiVedDivisionMed0] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [TilDato] datetime NOT NULL,
    [ref_BudgetAccountableColumnsId] bigint NOT NULL,
    [ref_RECID] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Ledelsestilsynsskabelon] PRIMARY KEY ([LedelsestilsynsskabelonId])
);
GO

CREATE TABLE [Finans].[LedelsestilsynsskabelonGruppe] (
    [LedelsestilsynsskabelonGruppeId] bigint IDENTITY(1,1) NOT NULL,
    [SkabelonNavn] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ØkonomiskDimensionsopsætning] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Godkendelsesfrekvens] int NOT NULL,
    [Beskrivelse] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Tekstskabelon] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PåmindelseEmailSkabelon] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PåmindelseDage] int NOT NULL,
    [RykkerEmailSkabelon] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RykkerDage] int NOT NULL,
    [UndladArt0] int NULL,
    [ref_BudgetAccountableSetupId] bigint NOT NULL,
    [ref_RECID] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_LedelsestilsynsskabelonGruppe] PRIMARY KEY ([LedelsestilsynsskabelonGruppeId])
);
GO

CREATE TABLE [Finans].[LedelsestilsynsskabelonPrisme] (
    [LedelsestilsynsskabelonPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Tilsynsansvarlig] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Cc] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [FraNiveau] int NOT NULL,
    [VisForbrug] int NOT NULL,
    [ForbrugFraDato] datetime NOT NULL,
    [ForbrugTilDato] datetime NOT NULL,
    [ForbrugDatointervalkode] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [VisBudget] int NOT NULL,
    [Budgetmodel] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BudgetFraDato] datetime NOT NULL,
    [BudgetTilDato] datetime NOT NULL,
    [Datointervalkode] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [VisRestbudget] int NOT NULL,
    [VisPctIÅr] int NOT NULL,
    [VisValgfri1Kolonne] int NOT NULL,
    [Valgfri1KolonneIndholdId] int NOT NULL,
    [Valgfri1Budgetmodel] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri1FraDato] datetime NOT NULL,
    [Valgfri1TilDato] datetime NOT NULL,
    [Valgfri1Datointervalkode] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [VisValgfri2Kolonne] int NOT NULL,
    [Valgfri2KolonneIndholdId] int NOT NULL,
    [Valgfri2Budgetmodel] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri2FraDato] datetime NOT NULL,
    [Valgfri2TilDato] datetime NOT NULL,
    [Valgfri2Datointervalkode] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Tekstdato] datetime NOT NULL,
    [ForbrugMedtagPrimo] int NOT NULL,
    [Valgfri1MedtagPrimo] int NOT NULL,
    [Valgfri2MedtagPrimo] int NOT NULL,
    [VisPctÅrMinus1] int NOT NULL,
    [PctÅrMinus1FraDato] datetime NOT NULL,
    [PctÅrMinus1TilDato] datetime NOT NULL,
    [PctÅrMinus1Datointervalkode] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PctÅrMinus1MedtagPrimo] int NOT NULL,
    [UndladRykning] int NOT NULL,
    [UdeladArt0] int NOT NULL,
    [UdeladRækkerPå0] int NOT NULL,
    [BudgetOverskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RestbudgetOverskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ForbrugOverskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PctIÅrOverskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PctÅrMinus1Overskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri1Overskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri2Overskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BudgetOverskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RestbudgetOverskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ForbrugOverskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PctIÅrOverskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PctÅrMinus1Overskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri1Overskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri2Overskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AfledtDimensionDato] datetime NOT NULL,
    [Økonomikonsulent] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Overordnet] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Navn] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri2MedtagNormale] int NOT NULL,
    [Valgfri1MedtagNormale] int NOT NULL,
    [Valgfri2Formel] varchar(250) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri1Formel] varchar(250) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri1VærdiVedDivisionMed0] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri2VærdiVedDivisionMed0] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PctIÅrVærdiVedDivisionMed0] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [VisValgfri3Kolonne] int NOT NULL,
    [Valgfri3KolonneIndholdId] int NOT NULL,
    [Valgfri3Budgetmodel] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri3FraDato] datetime NOT NULL,
    [Valgfri3TilDato] datetime NOT NULL,
    [Valgfri3Datointervalkode] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri3Overskrift1] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri3Overskrift2] varchar(18) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri3MedtagNormale] int NOT NULL,
    [Valgfri3Formel] varchar(250) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri3VærdiVedDivisionMed0] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Valgfri3MedtagPrimo] int NOT NULL,
    [Valgfri1Beregning] int NOT NULL,
    [Valgfri2Beregning] int NOT NULL,
    [Valgfri3Beregning] int NOT NULL,
    [ref_BudgetAccountableSetupId] bigint NOT NULL,
    [ref_RECID] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DokumentId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_LedelsestilsynsskabelonPrisme] PRIMARY KEY ([LedelsestilsynsskabelonPrismeId])
);
GO

CREATE TABLE [Finans].[LedelstilsynKolonnetype] (
    [LedelstilsynKolonnetypeId] bigint IDENTITY(1,1) NOT NULL,
    [LedelstilsynKolonnetypenummer] int NOT NULL,
    [LedelstilsynKolonnetypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_LedelstilsynKolonnetype] PRIMARY KEY ([LedelstilsynKolonnetypeId])
);
GO

CREATE TABLE [Finans].[Momsfri] (
    [MomsfriId] bigint IDENTITY(1,1) NOT NULL,
    [Momsfrinummer] int NOT NULL,
    [Momsfritekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Momsfri] PRIMARY KEY ([MomsfriId])
);
GO

CREATE TABLE [Finans].[Momskilde] (
    [MomskildeId] bigint IDENTITY(1,1) NOT NULL,
    [Momskildenummer] int NOT NULL,
    [Momskildetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Momskilde] PRIMARY KEY ([MomskildeId])
);
GO

CREATE TABLE [Finans].[Momskode] (
    [MomskodeID] bigint IDENTITY(1,1) NOT NULL,
    [Momskode] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Navn] nvarchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Procent] numeric(32,16) NULL,
    [GyldigFra] datetime NULL,
    [GyldigTil] datetime NULL,
    [DATAAREAID] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [RunId] int NULL,
    [Kildesystem] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [Afregningsgrupper] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Finanskonteringsgruppe] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Momsgruppe] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Momsgruppe Navn] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [Beskrivelse gruppe] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [Autoriseret moms] int NULL,
    [Konto for udgående moms] nvarchar(500) COLLATE Danish_Norwegian_CI_AS NULL,
    [Konto for indgående moms] nvarchar(500) COLLATE Danish_Norwegian_CI_AS NULL,
    [Udgift for importmoms] nvarchar(500) COLLATE Danish_Norwegian_CI_AS NULL,
    [Skyldig importmoms] nvarchar(500) COLLATE Danish_Norwegian_CI_AS NULL,
    [Afregningskonto] nvarchar(500) COLLATE Danish_Norwegian_CI_AS NULL,
    [Beskrivelse periode] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [Betalingsbetingelser] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Momsmyndighed] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Periodeinterval] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Antal Enheder] int NULL,
    [Fra dato] datetime NULL,
    [Til dato] datetime NULL,
    CONSTRAINT [PK_Momskode] PRIMARY KEY ([MomskodeID])
);
GO

CREATE TABLE [Finans].[MomsOprindelse] (
    [MomsOprindelseId] bigint IDENTITY(1,1) NOT NULL,
    [MomsOprindelsenummer] int NOT NULL,
    [MomsOprindelsetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_MomsOprindelse] PRIMARY KEY ([MomsOprindelseId])
);
GO

CREATE TABLE [Finans].[Momsposteringer] (
    [MomsposteringerId] bigint IDENTITY(1,1) NOT NULL,
    [Bilag] varchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [BilagsnummerId] bigint NULL,
    [Dato] date NULL,
    [MomskildeId] bigint NOT NULL,
    [Momskode] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oprindelse] decimal(32,16) NOT NULL,
    [Antal] decimal(32,16) NOT NULL,
    [Faktisk_momsbeløb_firmavaluta] decimal(32,16) NOT NULL,
    [Værdi_fakturering] decimal(32,16) NOT NULL,
    [AutoNummer] int NOT NULL,
    [Version] int NOT NULL,
    [MomsretningId] bigint NOT NULL,
    [MomsOprindelseId] bigint NOT NULL,
    [Beregnet_momsudgift_transaktionsvaluta] decimal(32,16) NOT NULL,
    [MomsfriId] bigint NOT NULL,
    [Periode] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Valuta_momsvaluta] varchar(3) COLLATE Danish_Norwegian_CI_AS NULL,
    [Faktisk_momsbeløb_momsvaluta] decimal(32,16) NOT NULL,
    [Beløbsgrundlag_transaktionsvaluta] decimal(32,16) NOT NULL,
    [Beløbsgrundlag_momsvaluta] decimal(32,16) NOT NULL,
    [Beregnet_momsbeløb_transaktionsvaluta] decimal(32,16) NOT NULL,
    [Transaktionsvaluta] varchar(3) COLLATE Danish_Norwegian_CI_AS NULL,
    [Faktisk_momsbeløb_transaktionsvaluta] decimal(32,16) NOT NULL,
    [Faktisk_momsudgift_transaktionsvaluta] decimal(32,16) NOT NULL,
    [Faktisk_momsudgift_firmavaluta] decimal(32,16) NOT NULL,
    [Faktisk_momsudgift_momsvaluta] decimal(32,16) NOT NULL,
    [Varemomsgruppe] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Momsgruppe] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Kladdebatchnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Fritagelseskode] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Udskriftskode] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [Reguleret_grundlagsbeløb_transaktionsvaluta] decimal(32,16) NOT NULL,
    [ref_TaxTrans_DWId] bigint NOT NULL,
    [ref_RECID] bigint NOT NULL,
    [SourceRecId] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Momsposteringer] PRIMARY KEY ([MomsposteringerId])
);
GO

CREATE TABLE [Finans].[Momsregler] (
    [MomsreglerID] bigint IDENTITY(1,1) NOT NULL,
    [Autoriseret] varchar(3) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] datetime NOT NULL,
    [GyldigTil] datetime NULL,
    [Ejerforhold] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Varemomsgruppe] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Finanskonto] nvarchar(500) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DATAAREAID] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RECID] bigint NOT NULL,
    [RunId] int NULL,
    [Kildesystem] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Momsregler] PRIMARY KEY ([MomsreglerID])
);
GO

CREATE TABLE [Finans].[Momsretning] (
    [MomsretningId] bigint IDENTITY(1,1) NOT NULL,
    [Momsretningnummer] int NOT NULL,
    [Momsretningtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Momsretning] PRIMARY KEY ([MomsretningId])
);
GO

CREATE TABLE [Finans].[Omkostningssted] (
    [OmkostningsstedId] bigint IDENTITY(1,1) NOT NULL,
    [Omkostningsstednummer] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Omkostningsstedtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [E1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [O1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_RECID] bigint NULL,
    [ref_DirPartyTableId] bigint NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [RECID] bigint NULL,
    CONSTRAINT [PK_Omkostningssted] PRIMARY KEY ([OmkostningsstedId])
);
GO

CREATE TABLE [Finans].[OmkostningsstedAutoriseret] (
    [O1Id] bigint IDENTITY(1,1) NOT NULL,
    [O1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [O1Tekst] varchar(360) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [GyldigFra] date NOT NULL,
    [GyldigTil] date NOT NULL,
    [Autoriseret] int NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_LedgerTextId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_OmkostningsstedAutoriseret] PRIMARY KEY ([O1Id])
);
GO

CREATE TABLE [Finans].[OmkostningsstedHierarki] (
    [OmkostningsstedHierarkiId] bigint IDENTITY(1,1) NOT NULL,
    [jeId1] bigint NOT NULL,
    [veId2] bigint NULL,
    [afId2] bigint NULL,
    [veId3] bigint NULL,
    [afId3] bigint NULL,
    [osId3] bigint NULL,
    [veId4] bigint NULL,
    [afId4] bigint NULL,
    [osId4] bigint NULL,
    [veId5] bigint NULL,
    [afId5] bigint NULL,
    [osId5] bigint NULL,
    [veId6] bigint NULL,
    [afId6] bigint NULL,
    [osId6] bigint NULL,
    [veId7] bigint NULL,
    [afId7] bigint NULL,
    [osId7] bigint NULL,
    [veId8] bigint NULL,
    [afId8] bigint NULL,
    [osId8] bigint NULL,
    [veId9] bigint NULL,
    [afId9] bigint NULL,
    [osId9] bigint NULL,
    [veId10] bigint NULL,
    [afId10] bigint NULL,
    [osId10] bigint NULL,
    [veId11] bigint NULL,
    [afId11] bigint NULL,
    [osId11] bigint NULL,
    [veId12] bigint NULL,
    [afId12] bigint NULL,
    [osId12] bigint NULL,
    [veId13] bigint NULL,
    [afId13] bigint NULL,
    [osId13] bigint NULL,
    [veId14] bigint NULL,
    [afId14] bigint NULL,
    [osId14] bigint NULL,
    [veId15] bigint NULL,
    [afId15] bigint NULL,
    [osId15] bigint NULL,
    [veId16] bigint NULL,
    [afId16] bigint NULL,
    [osId16] bigint NULL,
    [veId17] bigint NULL,
    [afId17] bigint NULL,
    [osId17] bigint NULL,
    [veId18] bigint NULL,
    [afId18] bigint NULL,
    [osId18] bigint NULL,
    [veId19] bigint NULL,
    [afId19] bigint NULL,
    [osId19] bigint NULL,
    [veId20] bigint NULL,
    [afId20] bigint NULL,
    [osId20] bigint NULL,
    [max_VirksomhedsenhedId] bigint NULL,
    [max_AfdelingId] bigint NULL,
    [max_OmkostningsstedId] bigint NULL,
    [nummer1] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [nummer2] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer3] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer4] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer5] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer6] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer7] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer8] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer9] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer10] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer11] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer12] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer13] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer14] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer15] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer16] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer17] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer18] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer19] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [nummer20] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst1] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [tekst2] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst3] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst4] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst5] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst6] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst7] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst8] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst9] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst10] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst11] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst12] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst13] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst14] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst15] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst16] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst17] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst18] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst19] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [tekst20] varchar(1000) COLLATE Danish_Norwegian_CI_AS NULL,
    [Aktiv] int NULL,
    [HierarkiId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Slettet] int NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_OmkostningsstedHierarki] PRIMARY KEY ([OmkostningsstedHierarkiId])
);
GO

CREATE TABLE [Finans].[Oplysningspligt] (
    [OplysningspligtId] bigint IDENTITY(1,1) NOT NULL,
    [Oplysningspligtnummer] int NOT NULL,
    [Oplysningspligttekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Oplysningspligt] PRIMARY KEY ([OplysningspligtId])
);
GO

CREATE TABLE [Finans].[PolitiskOrganisation] (
    [PolitiskOrganisationId] bigint IDENTITY(1,1) NOT NULL,
    [PolitiskOrganisationnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PolitiskOrganisationtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentPolitiskOrganisationId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_PolitiskOrganisation] PRIMARY KEY ([PolitiskOrganisationId])
);
GO

CREATE TABLE [Finans].[PrisRegister] (
    [PrisRegisterId] bigint IDENTITY(1,1) NOT NULL,
    [Type] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Version] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ArtId] bigint NULL,
    [HovedkontoId] bigint NULL,
    [OmkostningsstedId] bigint NULL,
    [GrupperingId] bigint NULL,
    [PrisIndeksMinus10] decimal(28,12) NOT NULL,
    [PrisIndeksMinus9] decimal(28,12) NOT NULL,
    [PrisIndeksMinus8] decimal(28,12) NOT NULL,
    [PrisIndeksMinus7] decimal(28,12) NOT NULL,
    [PrisIndeksMinus6] decimal(28,12) NOT NULL,
    [PrisIndeksMinus5] decimal(28,12) NOT NULL,
    [PrisIndeksMinus4] decimal(28,12) NOT NULL,
    [PrisIndeksMinus3] decimal(28,12) NOT NULL,
    [PrisIndeksMinus2] decimal(28,12) NOT NULL,
    [PrisIndeksMinus1] decimal(28,12) NOT NULL,
    [PrisIndeks] decimal(28,12) NOT NULL,
    [PrisIndeksPlus1] decimal(28,12) NOT NULL,
    [Ref_BudgetPriceFluctId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_PrisRegister] PRIMARY KEY ([PrisRegisterId])
);
GO

CREATE TABLE [Finans].[PrisRegisterPrisme] (
    [PrisregisterPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [Type] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Version] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Art1] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Art2] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Art3] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Art4] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Art5] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Dranst] varchar(1) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Funktion1] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Funktion2] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Funktion3] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Funktion4] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Funktion5] varchar(2) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [PrisIndeksMinus10] decimal(28,12) NOT NULL,
    [PrisIndeksMinus9] decimal(28,12) NOT NULL,
    [PrisIndeksMinus8] decimal(28,12) NOT NULL,
    [PrisIndeksMinus7] decimal(28,12) NOT NULL,
    [PrisIndeksMinus6] decimal(28,12) NOT NULL,
    [PrisIndeksMinus5] decimal(28,12) NOT NULL,
    [PrisIndeksMinus4] decimal(28,12) NOT NULL,
    [PrisIndeksMinus3] decimal(28,12) NOT NULL,
    [PrisIndeksMinus2] decimal(28,12) NOT NULL,
    [PrisIndeksMinus1] decimal(28,12) NOT NULL,
    [PrisIndeks] decimal(28,12) NOT NULL,
    [PrisIndeksPlus1] decimal(28,12) NOT NULL,
    [Ref_LedgerpricefluctId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_AnalysePrisregisterId] bigint NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_PrisRegisterPrisme] PRIMARY KEY ([PrisregisterPrismeId])
);
GO

CREATE TABLE [Finans].[Rapporteringstype] (
    [RapporteringstypeId] bigint IDENTITY(1,1) NOT NULL,
    [Rapporteringstypenummer] int NOT NULL,
    [Rapporteringstypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Rapporteringstype] PRIMARY KEY ([RapporteringstypeId])
);
GO

CREATE TABLE [Finans].[Sted] (
    [StedId] bigint IDENTITY(1,1) NOT NULL,
    [Stednummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Stedtekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ParentStedId] bigint NULL,
    [Niveau] tinyint NOT NULL,
    [IMAutoriseret] int NOT NULL,
    [Anvendes] int NOT NULL,
    [FørsteBenyttelsesår] datetime NOT NULL,
    [AnlægsaktiverFunktion] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AnlægsaktiverSted] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [SORKode] nvarchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_OwnPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_ParentPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [int_JoinPath] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_LedgerTextId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Sted] PRIMARY KEY ([StedId])
);
GO

CREATE TABLE [Finans].[Temp_FinanskontoKontostreng] (
    [Kontonummer] varchar(500) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID] bigint NOT NULL,
    [ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId] bigint NOT NULL,
    [runid] int NOT NULL,
    [ve_RECID] bigint NULL,
    [afd_RECID] bigint NULL,
    [os_RECID] bigint NULL,
    [hk_RECID] bigint NULL,
    [ma_RECID] bigint NULL,
    [grp_RECID] bigint NULL,
    [art_RECID] bigint NULL,
    [d1_RECID] bigint NULL,
    [d2_RECID] bigint NULL,
    [l_RECID] bigint NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL
);
GO

CREATE TABLE [Finans].[Varemomsgruppe] (
    [VaremomsgruppeID] bigint IDENTITY(1,1) NOT NULL,
    [Varemomsgruppe] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Beskrivelse] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Momskode] nvarchar(10) COLLATE Danish_Norwegian_CI_AS NULL,
    [DATAAREAID] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [Kildesystem] varchar(5) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Varemomsgruppe] PRIMARY KEY ([VaremomsgruppeID])
);
GO

CREATE TABLE [Finans].[Virksomhedsenhed] (
    [VirksomhedsenhedId] bigint IDENTITY(1,1) NOT NULL,
    [VirksomhedsenhedVærdi] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [VirksomhedsenhedBetegnelse] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [AktivFra] date NOT NULL,
    [AktivTil] date NOT NULL,
    [Suspenderet] int NOT NULL,
    [Ejer_id] bigint NOT NULL,
    [TilladIkkeManuelAngivelse] int NOT NULL,
    [E1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [O1] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_RECID] bigint NULL,
    [ref_DirPartyTableId] bigint NULL,
    [ref_DimensionAttributeValueId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [RECID] bigint NULL,
    CONSTRAINT [PK_Virksomhedsenhed] PRIMARY KEY ([VirksomhedsenhedId])
);
GO

-- Indexes in Finans
CREATE NONCLUSTERED INDEX [IX_Afdeling_ref_RECID] ON [Finans].[Afdeling] ([ref_RECID]);
CREATE NONCLUSTERED INDEX [IX_Bilagsnummer_Bilagsnummer] ON [Finans].[Bilagsnummer] ([Bilagsnummer],[DataAreaId]);
CREATE NONCLUSTERED INDEX [IX_Bilagsnummer_BilagsnummerID] ON [Finans].[Bilagsnummer] ([BilagsnummerID]);
CREATE NONCLUSTERED INDEX [IX_BilagsnummerPrisme_Bilagsnummer] ON [Finans].[BilagsnummerPrisme] ([Bilagsnummer]);
CREATE NONCLUSTERED INDEX [IX_Bilagsrelationer_Bilagsnummer_DataAreaId] ON [Finans].[Bilagsrelationer] ([Bilagsnummer],[DataAreaId]);
CREATE NONCLUSTERED INDEX [IX_BilagsrelationerUdvidet_Bogfoeringsdato] ON [Finans].[BilagsrelationerUdvidet] ([Bogfoeringsdato]);
CREATE NONCLUSTERED INDEX [IX_Budgetpost_Budgetdato] ON [Finans].[Budgetpost] ([Budgetdato]);
CREATE UNIQUE NONCLUSTERED INDEX [UN_BudgetAar_Kildesystem] ON [Finans].[BudgetAar] ([Kildesystem]);
CREATE NONCLUSTERED INDEX [IX_DimensionJuridiskEnhed_ref_DimensionAttributeValueId] ON [Finans].[DimensionJuridiskEnhed] ([ref_DimensionAttributeValueId],[EndDateTime]);
CREATE NONCLUSTERED INDEX [IX_Finanskonto_Kontonummer] ON [Finans].[Finanskonto] ([Kontonummer]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoArt_FinanskontoId] ON [Finans].[FinanskontoArt] ([FinanskontoId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoArt_FinanskontoPrismeId] ON [Finans].[FinanskontoArt] ([FinanskontoPrismeId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoDranst_HovedkontoId_DranstId] ON [Finans].[FinanskontoDranst] ([HovedkontoId],[DranstId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoEjerforhold_FinanskontoId] ON [Finans].[FinanskontoEjerforhold] ([FinanskontoId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoFunktion_HovedkontoId] ON [Finans].[FinanskontoFunktion] ([HovedkontoId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoGruppering_FinanskontoId] ON [Finans].[FinanskontoGruppering] ([FinanskontoId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoGruppering_FinanskontoPrismeId] ON [Finans].[FinanskontoGruppering] ([FinanskontoPrismeId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoGrupperingAutoriseret_FinanskontoId] ON [Finans].[FinanskontoGrupperingAutoriseret] ([FinanskontoId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoGrupperingAutoriseret_FinanskontoPrismeId] ON [Finans].[FinanskontoGrupperingAutoriseret] ([FinanskontoPrismeId]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoHovedkonto_HovedkontoId] ON [Finans].[FinanskontoHovedkonto] ([Hovedkontoid]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoOmkostningssted_FinanskontoId_OmkostningsstedId] ON [Finans].[FinanskontoOmkostningssted] ([FinanskontoId],[Omkostningsstedid]);
CREATE NONCLUSTERED INDEX [IX_FinanskontoOmkostningssted_Omkostningsstedid] ON [Finans].[FinanskontoOmkostningssted] ([Omkostningsstedid]);
CREATE NONCLUSTERED INDEX [IX_Finanspostering_Bilagsnummer_FinanskladdeId] ON [Finans].[Finanspostering] ([Bilagsnummer],[FinanskladdeId]);
CREATE NONCLUSTERED INDEX [IX_Finanspostering_BilagsnummerID] ON [Finans].[Finanspostering] ([BilagsnummerID]);
CREATE UNIQUE CLUSTERED INDEX [IX_Finanspostering_Kildesystem_FinansposteringId] ON [Finans].[Finanspostering] ([Kildesystem],[FinansposteringId]);
CREATE NONCLUSTERED INDEX [IX_Finanspostering_ref_LedgertransId] ON [Finans].[Finanspostering] ([ref_LedgertransId]);
CREATE NONCLUSTERED INDEX [IX_Finanspostering_NRT_Bilagsnummer_FinanskladdeId] ON [Finans].[Finanspostering_NRT] ([Bilagsnummer],[FinanskladdeId]);
CREATE NONCLUSTERED INDEX [IX_Finanspostering_NRT_BilagsnummerID] ON [Finans].[Finanspostering_NRT] ([BilagsnummerID]);
CREATE NONCLUSTERED INDEX [IX_FinansposteringPrisme_Bilagsnummer_FinanskladdeId] ON [Finans].[FinansposteringPrisme] ([Bilagsnummer],[FinanskladdeId]);
CREATE NONCLUSTERED INDEX [IX_FinansposteringPrisme_BilagsnummerID] ON [Finans].[FinansposteringPrisme] ([BilagsnummerID]);
CREATE UNIQUE CLUSTERED INDEX [UN_Finansposteringstype_Finansposteringstypenummer] ON [Finans].[Finansposteringstype] ([Kildesystem],[Finansposteringstypenummer]);
CREATE UNIQUE CLUSTERED INDEX [UN_Finanstransaktionstype_Kildesystem_Finanstransaktionstypenummer] ON [Finans].[Finanstransaktionstype] ([Kildesystem],[Transaktionstypenummer]);
CREATE NONCLUSTERED INDEX [IX_HierarkiRelationer_ParentHierarkiRelationerId_Niveau] ON [Finans].[HierarkiRelationer] ([ParentHierarkiRelationerId],[Niveau]);
CREATE NONCLUSTERED INDEX [IX_HierarkiRelationer_ParentHierarkiRelationerId_ref_Child_RECID_Niveau] ON [Finans].[HierarkiRelationer] ([ParentHierarkiRelationerId],[ref_Child_RECID],[Niveau]);
CREATE UNIQUE NONCLUSTERED INDEX [IX_Omkostningssted_Omkostningsstednummer] ON [Finans].[Omkostningssted] ([Omkostningsstednummer],[EndDateTime] DESC);
CREATE NONCLUSTERED INDEX [IX_Omkostningssted_ref_RECID] ON [Finans].[Omkostningssted] ([ref_RECID]);
CREATE NONCLUSTERED INDEX [IX_Prisregister_ArtId_HoverkontoId_OmkostningsstedId_GrupperingId] ON [Finans].[PrisRegister] ([ArtId],[HovedkontoId],[OmkostningsstedId],[GrupperingId]);
CREATE UNIQUE NONCLUSTERED INDEX [IX_Virksomhedsenhed_VirksomhedsenhedVaerdi] ON [Finans].[Virksomhedsenhed] ([VirksomhedsenhedVærdi],[Kildesystem],[EndDateTime] DESC);
GO

-- Foreign keys in Finans
GO

-- ============================================
-- Schema: Kreditor (8 tables)
-- ============================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Kreditor')
    EXEC('CREATE SCHEMA [Kreditor]');
GO

-- Drop existing FK constraints in Kreditor
GO

-- Drop existing tables in Kreditor
IF OBJECT_ID('Kreditor.Kreditortransaktionstype', 'U') IS NOT NULL DROP TABLE [Kreditor].[Kreditortransaktionstype];
IF OBJECT_ID('Kreditor.KreditortransaktionPrisme', 'U') IS NOT NULL DROP TABLE [Kreditor].[KreditortransaktionPrisme];
IF OBJECT_ID('Kreditor.Kreditortransaktion', 'U') IS NOT NULL DROP TABLE [Kreditor].[Kreditortransaktion];
IF OBJECT_ID('Kreditor.KreditorEAN', 'U') IS NOT NULL DROP TABLE [Kreditor].[KreditorEAN];
IF OBJECT_ID('Kreditor.Kreditor', 'U') IS NOT NULL DROP TABLE [Kreditor].[Kreditor];
IF OBJECT_ID('Kreditor.Betalingsmåde', 'U') IS NOT NULL DROP TABLE [Kreditor].[Betalingsmåde];
IF OBJECT_ID('Kreditor.BetalingsgruppeAfdelingPrisme', 'U') IS NOT NULL DROP TABLE [Kreditor].[BetalingsgruppeAfdelingPrisme];
IF OBJECT_ID('Kreditor.Betalingsgruppe', 'U') IS NOT NULL DROP TABLE [Kreditor].[Betalingsgruppe];
GO

-- Create tables in Kreditor
CREATE TABLE [Kreditor].[Betalingsgruppe] (
    [BetalingsgruppeId] bigint IDENTITY(1,1) NOT NULL,
    [Betalingsgruppe] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Betalingsgruppenavn] varchar(60) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Samlekonto] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_VendPaymGroupTableId] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Betalingsgruppe] PRIMARY KEY ([BetalingsgruppeId])
);
GO

CREATE TABLE [Kreditor].[BetalingsgruppeAfdelingPrisme] (
    [BetalingsgruppeAfdelingPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [BetalingsgruppeId] bigint NOT NULL,
    [AfdelingPrismeId] bigint NOT NULL,
    [ref_VendPaymGroupDimensionId] bigint NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_BetalingsgruppeAfdelingPrisme] PRIMARY KEY ([BetalingsgruppeAfdelingPrismeId])
);
GO

CREATE TABLE [Kreditor].[Betalingsmåde] (
    [BetalingsmådeId] bigint IDENTITY(1,1) NOT NULL,
    [BetalingsgruppeId] bigint NULL,
    [VirksomhedsenhedId] bigint NULL,
    [Betalingsmåde] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [KontotypeACId] bigint NULL,
    [Betalingskonto] varchar(20) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_OMOperationUnitPaymModeId] bigint NULL,
    [ref_VendPaymGroupModeSpecId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Betalingsmåde] PRIMARY KEY ([BetalingsmådeId])
);
GO

CREATE TABLE [Kreditor].[Kreditor] (
    [KreditorId] bigint IDENTITY(1,1) NOT NULL,
    [Kreditornummer] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Kreditornavn] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Soegenavn] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [JuridiskEnhed] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErBlokeret] bit NOT NULL,
    [Oprettelsestidspunkt] datetime2(7) NOT NULL,
    [OprettetAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [SidstAendretTidspunkt] datetime2(7) NOT NULL,
    [SidstAendretAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_VendtableId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DokumentId] bigint NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Kreditor] PRIMARY KEY ([KreditorId])
);
GO

CREATE TABLE [Kreditor].[KreditorEAN] (
    [KreditorEANId] bigint IDENTITY(1,1) NOT NULL,
    [EAN] nvarchar(13) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Stilling] nvarchar(25) COLLATE Danish_Norwegian_CI_AS NULL,
    [BrugerId] nvarchar(8) COLLATE Danish_Norwegian_CI_AS NULL,
    [Beskrivelse] nvarchar(60) COLLATE Danish_Norwegian_CI_AS NULL,
    [Omkostningsstednummer] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [DATAAREAID] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Linjetype] PRIMARY KEY ([KreditorEANId])
);
GO

CREATE TABLE [Kreditor].[Kreditortransaktion] (
    [KreditortransaktionId] bigint NOT NULL,
    [KreditorId] bigint NULL,
    [Transaktionsdato] date NOT NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Fakturanummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [EANnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Fakturadato] date NULL,
    [Transaktionsbeloeb] numeric(28,12) NOT NULL,
    [Valutakode] char(3) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [TransaktionsbeloebValuta] numeric(28,12) NOT NULL,
    [UdlignetBeloeb] numeric(28,12) NOT NULL,
    [UdlignetBeloebValuta] numeric(28,12) NOT NULL,
    [Transaktionstekst] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Forfaldsdato] date NOT NULL,
    [ErLukket] bit NOT NULL,
    [LukketDato] date NULL,
    [AfdelingPrismeId] bigint NULL,
    [JuridiskEnhed] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Bankregistreringsnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Bankkontonummer] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErKorrektion] bit NOT NULL,
    [KreditortransaktionstypeId] bigint NULL,
    [ErGodkendt] bit NOT NULL,
    [GodkendtDato] date NULL,
    [GodkendtAf] varchar(50) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oprettelsestidspunkt] datetime2(7) NOT NULL,
    [OprettetAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [IsCounterpost] bit NOT NULL,
    [CounterpostsThisKreditortransaktionId] bigint NULL,
    [Betalingstekst] nvarchar(255) COLLATE Danish_Norwegian_CI_AS NULL,
    [BetalingReturkode] varchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [BetalingYderligerInfo] varchar(105) COLLATE Danish_Norwegian_CI_AS NULL,
    [Virksomhedsenhed] nvarchar(20) COLLATE Danish_Norwegian_CI_AS NULL,
    [VirksomhedsenhedNavn] nvarchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [ref_VendTransId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [SidstUdlignetDato] date NULL,
    [SidstUdlignetBilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DokumentId] bigint NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Kreditortransaktion] PRIMARY KEY ([KreditortransaktionId])
);
GO

CREATE TABLE [Kreditor].[KreditortransaktionPrisme] (
    [KreditortransaktionPrismeId] bigint IDENTITY(1,1) NOT NULL,
    [KreditorId] bigint NULL,
    [Transaktionsdato] date NOT NULL,
    [Bilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Fakturanummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Fakturadato] date NULL,
    [Transaktionsbeloeb] numeric(28,12) NOT NULL,
    [Valutakode] char(3) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [TransaktionsbeloebValuta] numeric(28,12) NOT NULL,
    [UdlignetBeloeb] numeric(28,12) NOT NULL,
    [UdlignetBeloebValuta] numeric(28,12) NOT NULL,
    [Transaktionstekst] varchar(100) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Forfaldsdato] date NOT NULL,
    [ErLukket] bit NOT NULL,
    [LukketDato] date NULL,
    [AfdelingPrismeId] bigint NULL,
    [JuridiskEnhed] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Bankregistreringsnummer] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [Bankkontonummer] varchar(30) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ErKorrektion] bit NOT NULL,
    [KreditortransaktionstypeId] bigint NULL,
    [ErGodkendt] bit NOT NULL,
    [GodkendtDato] date NULL,
    [GodkendtAf] varchar(50) COLLATE Danish_Norwegian_CI_AS NULL,
    [Oprettelsestidspunkt] datetime2(7) NOT NULL,
    [OprettetAf] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [IsCounterpost] bit NOT NULL,
    [CounterpostsThisKreditortransaktionPrismeId] bigint NULL,
    [Betalingstekst] nvarchar(MAX) COLLATE Danish_Norwegian_CI_AS NULL,
    [BetalingReturkode] varchar(4) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [BetalingYderligerInfo] varchar(105) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [ref_VendTransId] bigint NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [SidstUdlignetDato] date NULL,
    [SidstUdlignetBilagsnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [DokumentId] bigint NULL,
    [DataAreaId] nvarchar(4) COLLATE Danish_Norwegian_CI_AS NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    [EANnummer] varchar(100) COLLATE Danish_Norwegian_CI_AS NULL,
    CONSTRAINT [PK_KreditortransaktionPrisme] PRIMARY KEY ([KreditortransaktionPrismeId])
);
GO

CREATE TABLE [Kreditor].[Kreditortransaktionstype] (
    [KreditortransaktionstypeId] bigint IDENTITY(1,1) NOT NULL,
    [Transaktionstypenummer] int NOT NULL,
    [Transaktionstypetekst] varchar(1000) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [RunId] int NULL,
    [StartDateTime] datetime2(0) NOT NULL,
    [EndDateTime] datetime2(0) NOT NULL,
    [RdwTs] datetime2(7) NOT NULL,
    [Kildesystem] varchar(10) COLLATE Danish_Norwegian_CI_AS NOT NULL,
    [CreatedOn] datetime NOT NULL,
    [Modified] datetime NULL,
    CONSTRAINT [PK_Kreditortransaktionstype] PRIMARY KEY ([KreditortransaktionstypeId])
);
GO

-- Indexes in Kreditor
CREATE UNIQUE NONCLUSTERED INDEX [IX_Betalingsgruppe_Betalingsgruppe_Kildesystem] ON [Kreditor].[Betalingsgruppe] ([Betalingsgruppe],[Kildesystem]);
CREATE UNIQUE NONCLUSTERED INDEX [IX_BetalingsgruppeAfdelingPrisme_AfdelingPrismeId_Kildesystem] ON [Kreditor].[BetalingsgruppeAfdelingPrisme] ([AfdelingPrismeId],[Kildesystem]);
CREATE NONCLUSTERED INDEX [IX_Kreditortransaktion_Bilagsnummer] ON [Kreditor].[Kreditortransaktion] ([Bilagsnummer]);
CREATE NONCLUSTERED INDEX [IX_Kreditortransaktion_IsCounterpostref_VendTransId] ON [Kreditor].[Kreditortransaktion] ([IsCounterpost],[ref_VendTransId]);
CREATE UNIQUE CLUSTERED INDEX [IX_Kreditortransaktion_Kildesystem_Kreditortransaktionid] ON [Kreditor].[Kreditortransaktion] ([Kildesystem],[KreditortransaktionId]);
CREATE NONCLUSTERED INDEX [IX_KreditortransaktionPrisme_Bilagsnummer] ON [Kreditor].[KreditortransaktionPrisme] ([Bilagsnummer]);
CREATE NONCLUSTERED INDEX [IX_KreditortransaktionPrisme_IsCounterpostref_VendTransId] ON [Kreditor].[KreditortransaktionPrisme] ([IsCounterpost],[ref_VendTransId]);
CREATE UNIQUE CLUSTERED INDEX [IX_KreditortransaktionPrisme_Kildesystem_KreditortransaktionPrismeid] ON [Kreditor].[KreditortransaktionPrisme] ([Kildesystem],[KreditortransaktionPrismeId]);
CREATE NONCLUSTERED INDEX [IX_KreditortransaktionPrisme_ref_VendTransId_Kildesystem] ON [Kreditor].[KreditortransaktionPrisme] ([ref_VendTransId],[Kildesystem]);
GO

-- Foreign keys in Kreditor
GO
