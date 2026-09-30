-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Security: Dimension Views (vw_dim schema)
--
-- Security-focused views for PII masking and scoped organization access.
-- ============================================================================

-- vw_dim.Borger (always masked)
CREATE OR ALTER VIEW [vw_dim].[Borger]
AS
SELECT
    [DW_EK_Borger],
    [DW_SK_Borger],
    -- Masked fields (always hidden)
    [CPR]               = CAST([DW_EK_Borger] AS NVARCHAR(10)),  -- Pseudonymized (INT key)
    [Fornavne]          = N'[MASKERET]',
    [Efternavn]         = N'[MASKERET]',
    [Adresseringsnavn]  = N'[MASKERET]',
    -- Non-sensitive fields
    [koen_navn],
    [active],
    [Deceased],
    [dw_valid_from],
    [dw_valid_to],
    [dw_is_current],
    [dw_is_deleted],
    [dw_timestamp_insert],
    [dw_timestamp_update],
    -- Security metadata
    [DPASikkerhed] = 'KB'  -- Kolonnebegrænset
FROM [dim].[Borger];
GO

-- vw_dim.Organisation
CREATE OR ALTER VIEW [vw_dim].[Organisation]
AS
SELECT
    [dw_ek_Organisation],
    [kode],
    [dw_ek_Organisation_parent],
    [parent_kode],
    [navn],
    [aktiv],
    [hierarki_niveau],
    [kode_niveau1],
    [navn_niveau1],
    [kode_niveau2],
    [navn_niveau2],
    [kode_niveau3],
    [navn_niveau3],
    [kode_niveau4],
    [navn_niveau4],
    [kode_niveau5],
    [navn_niveau5],
    [kode_niveau6],
    [navn_niveau6],
    [kode_niveau7],
    [navn_niveau7],
    [kode_niveau8],
    [navn_niveau8],
    [kode_niveau9],
    [navn_niveau9],
    [dw_timestamp_insert],
    [dw_timestamp_update],
    [dw_is_deleted]
FROM [dim].[Organisation];
GO

-- vw_dim.Okosted
CREATE OR ALTER VIEW [vw_dim].[Okosted]
AS
SELECT
    [dw_ek_Okosted],
    [code_okosted],
    [name],
    [level],
    [dw_ek_Okosted_parent],
    [parent_code],
    [active],
    [dw_timestamp_insert],
    [dw_timestamp_update]
FROM [dim].[Okosted];
GO

-- vw_dim.Leverandoervare (free-text columns column-restricted via DPA_ERP role)
--
-- varenavn/varenavn_mysupply can hold invoice free-text (MySupply) that may contain
-- names/CPR, and varenummer_kreditor can be CPR-formatted. These columns are masked
-- unless the caller has Rolle = 'U' in sec.RolleDomaene for Sikkerhedsdomaene
-- DPA_ERP. Users without a DPA_ERP role see no rows (CROSS JOIN).
-- Clean product names remain available via bridge.LeverandoervareILSVare / dim.ILSVare / vw_dim.ILSVare.
-- Downstream reporting joins use the surrogate key dw_ek_Leverandoervare, so masking is safe.
CREATE OR ALTER VIEW [vw_dim].[Leverandoervare]
AS
SELECT
    v.[dw_ek_Leverandoervare],
    v.[cvr],
    -- PII-risk free-text columns (masked unless caller has U-role in DPA_ERP)
    [varenummer_kreditor] = beskyt.[varenummer_kreditor],
    [varenavn]           = beskyt.[varenavn],
    [varenavn_mysupply]   = beskyt.[varenavn_mysupply],
    -- Non-sensitive (ILS attributes live in dim.ILSVare, via bridge.LeverandoervareILSVare)
    v.[dw_source_mysupply],
    v.[dw_source_ils],
    v.[dw_timestamp_insert],
    v.[dw_timestamp_update],
    -- Security metadata
    [DPASikkerhed] = beskyt.[DPASikkerhed]
FROM [dim].[Leverandoervare] AS v
CROSS JOIN [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), N'DPA_ERP') AS R
CROSS APPLY [sec].[BeskytDimLeverandoervare_v1](
    R.[Rolle],
    v.[varenummer_kreditor],
    v.[varenavn],
    v.[varenavn_mysupply]
) AS beskyt;
GO

-- vw_dim.Fakturalinje (free-text dimension, column-restricted via DPA_ERP role)
--
-- dim.Fakturalinje consists entirely of MySupply invoice free-text (user-entered
-- item numbers, names and descriptions) which may contain names/CPR. All descriptive
-- columns are masked ([MASKERET]) unless the caller has Rolle = 'U' in
-- sec.RolleDomaene for Sikkerhedsdomaene DPA_ERP. Users without a DPA_ERP role
-- see no rows (CROSS JOIN). Only the surrogate key and audit timestamps are always
-- visible; reporting joins use dw_ek_Fakturalinje, a shared key with
-- fct.Fakturalinje (both sourced from map.Fakturalinje), so masking does not
-- break joins.
CREATE OR ALTER VIEW [vw_dim].[Fakturalinje]
AS
SELECT
    f.[dw_ek_Fakturalinje],
    -- Free-text / PII-risk columns (masked unless caller has U-role in DPA_ERP)
    [item_faktura_udsteder_vare_nr]            = CASE WHEN R.[Rolle] = 'U' THEN f.[item_faktura_udsteder_vare_nr]            ELSE N'[MASKERET]' END,
    [item_faktura_udsteder_vare_navn]          = CASE WHEN R.[Rolle] = 'U' THEN f.[item_faktura_udsteder_vare_navn]          ELSE N'[MASKERET]' END,
    [faktura_linje_nr]                        = CASE WHEN R.[Rolle] = 'U' THEN f.[faktura_linje_nr]                        ELSE N'[MASKERET]' END,
    [antal_enheder_faktureret_type]           = CASE WHEN R.[Rolle] = 'U' THEN f.[antal_enheder_faktureret_type]           ELSE N'[MASKERET]' END,
    [item_faktura_udstederens_vare_nr]         = CASE WHEN R.[Rolle] = 'U' THEN f.[item_faktura_udstederens_vare_nr]         ELSE N'[MASKERET]' END,
    [item_faktura_udstederens_vare_nr_type]    = CASE WHEN R.[Rolle] = 'U' THEN f.[item_faktura_udstederens_vare_nr_type]    ELSE N'[MASKERET]' END,
    [base_price_enheds_stoerrelse]             = CASE WHEN R.[Rolle] = 'U' THEN f.[base_price_enheds_stoerrelse]             ELSE N'[MASKERET]' END,
    [base_price_enheds_stoerrelse_type]        = CASE WHEN R.[Rolle] = 'U' THEN f.[base_price_enheds_stoerrelse_type]        ELSE N'[MASKERET]' END,
    [item_faktura_udsteder_vare_navn_kort]      = CASE WHEN R.[Rolle] = 'U' THEN f.[item_faktura_udsteder_vare_navn_kort]      ELSE N'[MASKERET]' END,
    [line_dimensions_konto_streng]            = CASE WHEN R.[Rolle] = 'U' THEN f.[line_dimensions_konto_streng]            ELSE N'[MASKERET]' END,
    [leverance_krav]                         = CASE WHEN R.[Rolle] = 'U' THEN f.[leverance_krav]                         ELSE N'[MASKERET]' END,
    [leverings_dato_2]                       = CASE WHEN R.[Rolle] = 'U' THEN f.[leverings_dato_2]                       ELSE N'[MASKERET]' END,
    [item_kundens_vare_nr]                    = CASE WHEN R.[Rolle] = 'U' THEN f.[item_kundens_vare_nr]                    ELSE N'[MASKERET]' END,
    [item_kundens_vare_nr_type]               = CASE WHEN R.[Rolle] = 'U' THEN f.[item_kundens_vare_nr_type]               ELSE N'[MASKERET]' END,
    [item_kundens_vare_beskrivelse]           = CASE WHEN R.[Rolle] = 'U' THEN f.[item_kundens_vare_beskrivelse]           ELSE N'[MASKERET]' END,
    [item_faktura_udstederens_vare_beskrivelse]= CASE WHEN R.[Rolle] = 'U' THEN f.[item_faktura_udstederens_vare_beskrivelse]ELSE N'[MASKERET]' END,
    [item_fabrikantens_vare_nr]               = CASE WHEN R.[Rolle] = 'U' THEN f.[item_fabrikantens_vare_nr]               ELSE N'[MASKERET]' END,
    [item_fabrikantens_vare_nr_type]          = CASE WHEN R.[Rolle] = 'U' THEN f.[item_fabrikantens_vare_nr_type]          ELSE N'[MASKERET]' END,
    [item_fabrikantens_vare_beskrivelse]      = CASE WHEN R.[Rolle] = 'U' THEN f.[item_fabrikantens_vare_beskrivelse]      ELSE N'[MASKERET]' END,
    [item_standardiseret_vare_nr]             = CASE WHEN R.[Rolle] = 'U' THEN f.[item_standardiseret_vare_nr]             ELSE N'[MASKERET]' END,
    [item_standardiseret_vare_nr_type]        = CASE WHEN R.[Rolle] = 'U' THEN f.[item_standardiseret_vare_nr_type]        ELSE N'[MASKERET]' END,
    [item_standardiseret_vare_beskrivelse]    = CASE WHEN R.[Rolle] = 'U' THEN f.[item_standardiseret_vare_beskrivelse]    ELSE N'[MASKERET]' END,
    [item_vare_klassifikation]               = CASE WHEN R.[Rolle] = 'U' THEN f.[item_vare_klassifikation]               ELSE N'[MASKERET]' END,
    [item_vare_klassifikation_type]          = CASE WHEN R.[Rolle] = 'U' THEN f.[item_vare_klassifikation_type]          ELSE N'[MASKERET]' END,
    [item_afgifts_kategori]                  = CASE WHEN R.[Rolle] = 'U' THEN f.[item_afgifts_kategori]                  ELSE N'[MASKERET]' END,
    [item_afgifts_kategori_procent]           = CASE WHEN R.[Rolle] = 'U' THEN f.[item_afgifts_kategori_procent]           ELSE N'[MASKERET]' END,
    [faktura_udsteder_gebyr_rabat_aarsag]       = CASE WHEN R.[Rolle] = 'U' THEN f.[faktura_udsteder_gebyr_rabat_aarsag]       ELSE N'[MASKERET]' END,
    [faktura_udsteder_gebyr_rabat_kategori]     = CASE WHEN R.[Rolle] = 'U' THEN f.[faktura_udsteder_gebyr_rabat_kategori]     ELSE N'[MASKERET]' END,
    [faktura_udsteder_gebyr_rabat_finanskonto]  = CASE WHEN R.[Rolle] = 'U' THEN f.[faktura_udsteder_gebyr_rabat_finanskonto]  ELSE N'[MASKERET]' END,
    [faktura_udsteder_gebyr_tillaeg_aarsag]     = CASE WHEN R.[Rolle] = 'U' THEN f.[faktura_udsteder_gebyr_tillaeg_aarsag]     ELSE N'[MASKERET]' END,
    [faktura_udsteder_gebyr_tillaeg_kategori]   = CASE WHEN R.[Rolle] = 'U' THEN f.[faktura_udsteder_gebyr_tillaeg_kategori]   ELSE N'[MASKERET]' END,
    [faktura_udsteder_gebyr_tillaeg_finanskonto]= CASE WHEN R.[Rolle] = 'U' THEN f.[faktura_udsteder_gebyr_tillaeg_finanskonto]ELSE N'[MASKERET]' END,
    -- Audit (always visible)
    f.[dw_timestamp_insert],
    f.[dw_timestamp_update],
    -- Security metadata
    [DPASikkerhed] = CASE WHEN R.[Rolle] = 'U' THEN 'UB' ELSE 'KB' END
FROM [dim].[Fakturalinje] AS f
CROSS JOIN [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), N'DPA_ERP') AS R;
GO

-- vw_dim.Postering (free-text posting texts column-restricted via DPA_ERP role)
--
-- posteringstekst / transaktionstekst are user-entered NVARCHAR(MAX) free-text on the
-- SydAX ledger/vendor transactions and may inadvertently contain names/CPR. They are
-- masked ([MASKERET]) unless the caller has Rolle = 'U' in sec.RolleDomaene for
-- Sikkerhedsdomaene DPA_ERP. Unlike vw_dim.Leverandoervare/Fakturalinje this view uses
-- OUTER APPLY (not CROSS JOIN): the posting dimension carries non-sensitive structured
-- data (accounts, amounts, dates, vendor no.) needed broadly, so rows are always returned
-- and only the free-text columns are masked. kreditornavn is left visible for consistency
-- with dim.Kreditor (supplier/company names, unmasked there).
CREATE OR ALTER VIEW [vw_dim].[Postering]
AS
SELECT
    p.[dw_ek_Postering],
    p.[ref_ledgertrans_id],
    p.[bk_ref_ledgertrans_id],
    p.[ref_vend_trans_id],
    p.[dokument_id],
    p.[kreditornummer],
    p.[kreditornavn],
    p.[juridisk_enhed],
    p.[kontonummer],
    p.[faktura_nr],
    p.[sidst_udlignet_bilagsnummer],
    p.[bilagsnummer_trans],
    p.[bilagsnummer_postering],
    p.[valutakode_trans],
    p.[valutakode_postering],
    -- Free-text / PII-risk columns (masked unless caller has U-role in DPA_ERP)
    [transaktionstekst] = CASE WHEN R.[Rolle] = 'U' THEN p.[transaktionstekst] ELSE N'[MASKERET]' END,
    [posteringstekst]   = CASE WHEN R.[Rolle] = 'U' THEN p.[posteringstekst]   ELSE N'[MASKERET]' END,
    -- Audit (always visible)
    p.[dw_timestamp_insert],
    p.[dw_timestamp_update],
    -- Security metadata
    [DPASikkerhed] = CASE WHEN R.[Rolle] = 'U' THEN 'UB' ELSE 'KB' END
FROM [dim].[Postering] AS p
OUTER APPLY [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), N'DPA_ERP') AS R;
GO

-- vw_dim.Finanskladdelinje (free-text description column-restricted via DPA_ERP role)
--
-- beskrivelse is user-entered free-text on the SydAX finance journal line and may
-- inadvertently contain names/CPR. It is masked ([MASKERET]) unless the caller has
-- Rolle = 'U' in sec.RolleDomaene for Sikkerhedsdomaene DPA_ERP. OUTER APPLY keeps the
-- non-sensitive structured columns (transaction type, line no.) available to all callers.
CREATE OR ALTER VIEW [vw_dim].[Finanskladdelinje]
AS
SELECT
    f.[dw_ek_Finanskladdelinje],
    f.[bk_ref_ledgertrans_id],
    -- Free-text / PII-risk column (masked unless caller has U-role in DPA_ERP)
    [beskrivelse] = CASE WHEN R.[Rolle] = 'U' THEN f.[beskrivelse] ELSE N'[MASKERET]' END,
    f.[linjenummer],
    f.[ekspeditionsnummer],
    f.[transaktionstypenummer],
    f.[transaktionstypetekst],
    f.[maskinnummer],
    -- Audit (always visible)
    f.[dw_timestamp_insert],
    f.[dw_timestamp_update],
    -- Security metadata
    [DPASikkerhed] = CASE WHEN R.[Rolle] = 'U' THEN 'UB' ELSE 'KB' END
FROM [dim].[Finanskladdelinje] AS f
OUTER APPLY [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), N'DPA_ERP') AS R;
GO

-- vw_dim.Rekvirent (rekvirent name conditionally masked via DPA_ERP role)
--
-- display holds the raw rekvirent name. For home patients (rtyp = HP/OHP) and relatives
-- this is personal/identifiable data (flagged by Strategisk Indkoeb, Bjarke Lyster), so
-- the name is masked for those rekvirent types unless the caller has Rolle = 'U' in
-- sec.RolleDomaene for Sikkerhedsdomaene DPA_ERP. All OTHER rekvirent types are
-- organisational units and remain visible — the rekvirent dimension is core to
-- consumption reporting (forbrugsopgoerelser), so rows are always returned (OUTER APPLY);
-- only the HP/OHP display name is column-restricted.
CREATE OR ALTER VIEW [vw_dim].[Rekvirent]
AS
SELECT
    rek.[dw_ek_Rekvirent],
    rek.[code],
    -- PII-risk name for home patients (rtyp HP/OHP) — masked unless caller has U-role in DPA_ERP
    [display] = CASE
        WHEN rek.[rtyp] IN (N'HP', N'OHP') AND ISNULL(R.[Rolle], N'') <> 'U'
            THEN N'[MASKERET]'
        ELSE rek.[display]
    END,
    rek.[parent_code],
    rek.[level],
    rek.[entity_type],
    rek.[sgh],
    rek.[rgrp],
    rek.[rtyp],
    rek.[rnr],
    rek.[code_sor],
    rek.[sor_relationship],
    rek.[dw_ek_Organisation],
    rek.[dw_ek_Rekvirent_parent],
    rek.[er_undtaget],
    -- Audit (always visible)
    rek.[dw_timestamp_insert],
    rek.[dw_timestamp_update],
    -- Security metadata
    [DPASikkerhed] = CASE
        WHEN rek.[rtyp] IN (N'HP', N'OHP') AND ISNULL(R.[Rolle], N'') <> 'U'
            THEN 'KB'
        ELSE 'UB'
    END
FROM [dim].[Rekvirent] AS rek
OUTER APPLY [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), N'DPA_ERP') AS R;
GO
