-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Dokumenterede henvisningsfelter bevares gennem arkiv og aftagerflade.
-- Kør på staging og DSO, genindlæs henvisninger og opdatér SOF/DSO/views.
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_TIL_YDER_NAVN') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_TIL_YDER_NAVN] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_TIL_YDER_NUMMER') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_TIL_YDER_NUMMER] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_TIL_AFDELING_KODE') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_TIL_AFDELING_KODE] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_TIL_AFDELING_KODETEKST') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_TIL_AFDELING_KODETEKST] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_TIL_MUNI_KODE') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_TIL_MUNI_KODE] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_TIL_MUNI_KODETEKST') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_TIL_MUNI_KODETEKST] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_TIL_MUNI_LOCATION') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_TIL_MUNI_LOCATION] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_YDER_NAVN') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_YDER_NAVN] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_YDER_NUMMER') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_YDER_NUMMER] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_AFDELING_KODE') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_AFDELING_KODE] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_AFDELING_KODETEKST') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_AFDELING_KODETEKST] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_INSTITUTION_NAVN') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_INSTITUTION_NAVN] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_INSTITUTION_POSTNR') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_INSTITUTION_POSTNR] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_INSTITUTION_VEJ') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_INSTITUTION_VEJ] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'HENVIST_FRA_INSTITUTION_TLF_NR') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [HENVIST_FRA_INSTITUTION_TLF_NR] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSLUTNINGSAARSAG_KODE') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSLUTNINGSAARSAG_KODE] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSLUTNINGSAARSAG_KODETEKST') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSLUTNINGSAARSAG_KODETEKST] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_YDER_NAVN') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_YDER_NAVN] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_YDER_NUMMER') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_YDER_NUMMER] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_AFDELING_KODE') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_AFDELING_KODE] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_AFDELING_KODETEKST') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_AFDELING_KODETEKST] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_INSTITUTION_NAVN') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_INSTITUTION_NAVN] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_INSTITUTION_POSTNR') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_INSTITUTION_POSTNR] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_INSTITUTION_VEJ') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_INSTITUTION_VEJ] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'AFSENDER_INSTITUTION_TLF_NR') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [AFSENDER_INSTITUTION_TLF_NR] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'BEHANDLER_YDER_NAVN') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [BEHANDLER_YDER_NAVN] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'BEHANDLER_YDER_NUMMER') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [BEHANDLER_YDER_NUMMER] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'BEHANDLER_AFDELING_KODE') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [BEHANDLER_AFDELING_KODE] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'BEHANDLER_AFDELING_KODETEKST') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [BEHANDLER_AFDELING_KODETEKST] NVARCHAR(512) NULL;
IF OBJECT_ID('arc.ifdb_henvisning', 'U') IS NOT NULL AND COL_LENGTH('arc.ifdb_henvisning', 'SENT_ELECTRONICAL') IS NULL
    ALTER TABLE [arc].[ifdb_henvisning] ADD [SENT_ELECTRONICAL] NVARCHAR(512) NULL;
GO
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_yder_navn') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_yder_navn] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_yder_nummer') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_yder_nummer] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_afdeling_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_afdeling_kode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_afdeling_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_afdeling_kodetekst] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_muni_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_muni_kode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_muni_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_muni_kodetekst] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_muni_location') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_muni_location] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_yder_navn') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_yder_navn] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_yder_nummer') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_yder_nummer] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_afdeling_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_afdeling_kode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_afdeling_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_afdeling_kodetekst] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_institution_navn') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_institution_navn] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_institution_postnr') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_institution_postnr] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_institution_vej') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_institution_vej] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_institution_tlf_nr') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_institution_tlf_nr] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afslutningsaarsag_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afslutningsaarsag_kode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afslutningsaarsag_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afslutningsaarsag_kodetekst] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_yder_navn') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_yder_navn] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_yder_nummer') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_yder_nummer] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_afdeling_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_afdeling_kode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_afdeling_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_afdeling_kodetekst] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_institution_navn') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_institution_navn] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_institution_postnr') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_institution_postnr] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_institution_vej') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_institution_vej] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_institution_tlf_nr') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_institution_tlf_nr] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'behandler_yder_navn') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [behandler_yder_navn] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'behandler_yder_nummer') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [behandler_yder_nummer] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'behandler_afdeling_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [behandler_afdeling_kode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'behandler_afdeling_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [behandler_afdeling_kodetekst] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'sent_electronical') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [sent_electronical] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_til_afdeling_sorkode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_til_afdeling_sorkode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvist_fra_afdeling_sorkode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvist_fra_afdeling_sorkode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'afsender_afdeling_sorkode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsender_afdeling_sorkode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'behandler_afdeling_sorkode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [behandler_afdeling_sorkode] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'prioritet_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [prioritet_kodetekst] NVARCHAR(512) NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'henvisningsdato') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvisningsdato] DATETIME2 NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'slutdato') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [slutdato] DATETIME2 NULL;
IF OBJECT_ID('fhir.ServiceRequest', 'U') IS NOT NULL AND COL_LENGTH('fhir.ServiceRequest', 'faellesvisitation') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [faellesvisitation] NVARCHAR(512) NULL;
GO
IF COL_LENGTH('fhir.ServiceRequest', 'referral_purpose') < 3500
    ALTER TABLE [fhir].[ServiceRequest] ALTER COLUMN [referral_purpose] NVARCHAR(1750) NULL;
GO
