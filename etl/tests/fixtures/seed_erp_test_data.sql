-- ============================================================================
-- ERP Test Data Seeding Script
-- ============================================================================
-- This script creates deterministic test data for ERP integration tests.
-- Uses MERGE statements for idempotency - safe to run multiple times.
--
-- Test Data Relationships:
--   cvr: "12345678" (used across all systems for supplier matching)
--   MySupply Invoice: Header ID=10, Line ID=1, Item="ABC123"
--   Sydax: kreditor_id=6787467 (existing), finanskonto_id=1, FinansposteringId=1
--   ILS: cvr_nr="12345678" (existing), pvnr="ITEM123"
-- ============================================================================

-- ============================================================================
-- MySupply Database (mysupply)
-- ============================================================================
-- Uses actual schema: metadata(id, faktura_total, leverandoer_cvr, ...),
-- statistic_invoice_header(id, metadata_id, faktura_nr, FakturaDato, ..., faktura_udsteder_cvr_nummer),
-- statistic_invoice_line(id, statistic_invoice_header_id, faktura_linje_nr, ..., item_faktura_udsteder_vare_nr)

USE [mysupply];
GO

-- Insert/update metadata for supplier (using actual schema)
-- Use MERGE without explicit ID to avoid IDENTITY issues
MERGE INTO [dbo].[metadata] AS target
USING (
    VALUES ('12345678', 'Test Supplier', 'København Ø', 'Hovedgade 123', '2100', '2024-03-10T10:30:00', 'MySupply', 'Region Hovedstaden')
) AS source (
    [leverandoer_cvr], [firma_navn], [By], [adresse], [postnummer], [ReceiveTimeFormat], [SystemNavn], [OrgUnit]
)
ON target.[leverandoer_cvr] = source.[leverandoer_cvr]
WHEN NOT MATCHED THEN
    INSERT ([faktura_total], [leverandoer_cvr], [firma_navn], [By], [adresse], [postnummer], [ReceiveTimeFormat], [SystemNavn], [OrgUnit])
    VALUES (2550.00, source.[leverandoer_cvr], source.[firma_navn], source.[By], source.[adresse], source.[postnummer], source.[ReceiveTimeFormat], source.[SystemNavn], source.[OrgUnit])
WHEN MATCHED THEN
    UPDATE SET
        [firma_navn] = source.[firma_navn],
        [By] = source.[By],
        [adresse] = source.[adresse],
        [postnummer] = source.[postnummer],
        [ReceiveTimeFormat] = source.[ReceiveTimeFormat],
        [SystemNavn] = source.[SystemNavn],
        [OrgUnit] = source.[OrgUnit];

-- Insert/update invoice header (using actual schema)
-- Use MERGE on faktura_nr to avoid IDENTITY issues
MERGE INTO [dbo].[statistic_invoice_header] AS target
USING (
    VALUES (
        'F2024001', '2024-03-10', '2024-03-08', '2024-04-05',
        'Region Hovedstaden', '12345678', 'DKK', 25.0, 510.0, 510.0, 510.0, 0.0, 0.0,
        2550.0, 2550.0, 3060.0, 3060.0
    )
) AS source (
    [faktura_nr], [FakturaDato], [OrdreDato], [BetalingsDato],
    [kundenavn], [faktura_udsteder_cvr_nummer], [faktura_valuta_kode], [VAT_AfgiftsKategoriProcent],
    [VAT_CategoryTotal_AfgiftsKategor], [VAT_TaxAmounts_AfgiftsKategoriMo], [VAT_TaxAmounts_AfgiftsKategoriM0],
    [VAT_TaxAmounts_AfgiftsKategoriM1], [VAT_TaxAmounts_AfgiftsKategoriM2], [FakturaLinjeTotal],
    [FakturaLinjeTotal_Valuta], [faktura_total], [FakturaTotal_Valuta]
)
ON target.[faktura_nr] = source.[faktura_nr]
WHEN NOT MATCHED THEN
    INSERT ([metadata_id], [faktura_nr], [FakturaDato], [OrdreDato], [BetalingsDato],
            [kundenavn], [faktura_udsteder_cvr_nummer], [faktura_valuta_kode], [VAT_AfgiftsKategoriProcent],
            [VAT_CategoryTotal_AfgiftsKategor], [VAT_TaxAmounts_AfgiftsKategoriMo], [VAT_TaxAmounts_AfgiftsKategoriM0],
            [VAT_TaxAmounts_AfgiftsKategoriM1], [VAT_TaxAmounts_AfgiftsKategoriM2], [FakturaLinjeTotal],
            [FakturaLinjeTotal_Valuta], [faktura_total], [FakturaTotal_Valuta])
    VALUES (100, source.[faktura_nr], source.[FakturaDato], source.[OrdreDato], source.[BetalingsDato],
            source.[kundenavn], source.[faktura_udsteder_cvr_nummer], source.[faktura_valuta_kode], source.[VAT_AfgiftsKategoriProcent],
            source.[VAT_CategoryTotal_AfgiftsKategor], source.[VAT_TaxAmounts_AfgiftsKategoriMo], source.[VAT_TaxAmounts_AfgiftsKategoriM0],
            source.[VAT_TaxAmounts_AfgiftsKategoriM1], source.[VAT_TaxAmounts_AfgiftsKategoriM2], source.[FakturaLinjeTotal],
            source.[FakturaLinjeTotal_Valuta], source.[faktura_total], source.[FakturaTotal_Valuta])
WHEN MATCHED THEN
    UPDATE SET
        [FakturaDato] = source.[FakturaDato],
        [OrdreDato] = source.[OrdreDato],
        [BetalingsDato] = source.[BetalingsDato],
        [kundenavn] = source.[kundenavn],
        [faktura_udsteder_cvr_nummer] = source.[faktura_udsteder_cvr_nummer],
        [faktura_valuta_kode] = source.[faktura_valuta_kode],
        [VAT_AfgiftsKategoriProcent] = source.[VAT_AfgiftsKategoriProcent],
        [VAT_CategoryTotal_AfgiftsKategor] = source.[VAT_CategoryTotal_AfgiftsKategor],
        [VAT_TaxAmounts_AfgiftsKategoriMo] = source.[VAT_TaxAmounts_AfgiftsKategoriMo],
        [VAT_TaxAmounts_AfgiftsKategoriM0] = source.[VAT_TaxAmounts_AfgiftsKategoriM0],
        [VAT_TaxAmounts_AfgiftsKategoriM1] = source.[VAT_TaxAmounts_AfgiftsKategoriM1],
        [VAT_TaxAmounts_AfgiftsKategoriM2] = source.[VAT_TaxAmounts_AfgiftsKategoriM2],
        [FakturaLinjeTotal] = source.[FakturaLinjeTotal],
        [FakturaLinjeTotal_Valuta] = source.[FakturaLinjeTotal_Valuta],
        [faktura_total] = source.[faktura_total],
        [FakturaTotal_Valuta] = source.[FakturaTotal_Valuta];

-- Insert/update invoice line (using actual schema)
-- Use MERGE on item_faktura_udsteder_vare_nr to avoid IDENTITY issues
MERGE INTO [dbo].[statistic_invoice_line] AS target
USING (
    VALUES (
        1, 100.0, 2550.0, 'DKK', 'ABC123', 'Paracetamol 500mg tabletter',
        'Paracetamol 500mg', 25.50, 'DKK', 100.0, 'stk', '12345678'
    )
) AS source (
    [faktura_linje_nr], [antal_enheder_faktureret], [LinjeBeløb], [LinjeBeløb_Valuta],
    [item_faktura_udsteder_vare_nr], [item_faktura_udsteder_vare_navn], [item_faktura_udsteder_vare_navn_kort],
    [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta], [base_price_enheds_stoerrelse], [base_price_enheds_stoerrelse_type],
    [faktura_udsteder_cvr_nummer]
)
ON target.[item_faktura_udsteder_vare_nr] = source.[item_faktura_udsteder_vare_nr]
WHEN NOT MATCHED THEN
    INSERT ([statistic_invoice_header_id], [faktura_linje_nr], [antal_enheder_faktureret],
            [LinjeBeløb], [LinjeBeløb_Valuta], [item_faktura_udsteder_vare_nr], [item_faktura_udsteder_vare_navn],
            [item_faktura_udsteder_vare_navn_kort], [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta],
            [base_price_enheds_stoerrelse], [base_price_enheds_stoerrelse_type], [faktura_udsteder_cvr_nummer])
    VALUES ((SELECT id FROM [dbo].[statistic_invoice_header] WHERE [faktura_nr] = 'F2024001'),
            source.[faktura_linje_nr], source.[antal_enheder_faktureret],
            source.[LinjeBeløb], source.[LinjeBeløb_Valuta], source.[item_faktura_udsteder_vare_nr], source.[item_faktura_udsteder_vare_navn],
            source.[item_faktura_udsteder_vare_navn_kort], source.[BasePrice_EnhedsPris], source.[BasePrice_EnhedsPris_Valuta],
            source.[base_price_enheds_stoerrelse], source.[base_price_enheds_stoerrelse_type], source.[faktura_udsteder_cvr_nummer])
WHEN MATCHED THEN
    UPDATE SET
        [faktura_linje_nr] = source.[faktura_linje_nr],
        [antal_enheder_faktureret] = source.[antal_enheder_faktureret],
        [LinjeBeløb] = source.[LinjeBeløb],
        [LinjeBeløb_Valuta] = source.[LinjeBeløb_Valuta],
        [item_faktura_udsteder_vare_navn] = source.[item_faktura_udsteder_vare_navn],
        [item_faktura_udsteder_vare_navn_kort] = source.[item_faktura_udsteder_vare_navn_kort],
        [BasePrice_EnhedsPris] = source.[BasePrice_EnhedsPris],
        [BasePrice_EnhedsPris_Valuta] = source.[BasePrice_EnhedsPris_Valuta],
        [base_price_enheds_stoerrelse] = source.[base_price_enheds_stoerrelse],
        [base_price_enheds_stoerrelse_type] = source.[base_price_enheds_stoerrelse_type],
        [faktura_udsteder_cvr_nummer] = source.[faktura_udsteder_cvr_nummer];

PRINT 'MySupply test data seeded successfully';
GO

-- ============================================================================
-- Sydax Database (sydax)
-- ============================================================================
-- Uses production schema: Kreditor has kreditor_id/juridisk_enhed, Finans has finanskonto_id/kontonummer
-- Tests expect specific clean codes: KREDID_6787467, KONTOPLAN_1, etc.

USE [sydax];
GO

-- Insert/update Kreditor with juridisk_enhed (cvr) for clean code generation
MERGE INTO [Kreditor].[Kreditor] AS target
USING (VALUES (6787467, '12345678', 'Test Kreditor A/S')) AS source ([kreditor_id], [juridisk_enhed], [navn])
ON target.[kreditor_id] = source.[kreditor_id]
WHEN NOT MATCHED THEN
    INSERT ([kreditor_id], [juridisk_enhed], [navn])
    VALUES (source.[kreditor_id], source.[juridisk_enhed], source.[navn])
WHEN MATCHED THEN
    UPDATE SET [juridisk_enhed] = source.[juridisk_enhed], [navn] = source.[navn];

-- Insert chart of accounts entries (production uses kontonummer, not kontonr)
MERGE INTO [Finans].[Finanskonto] AS target
USING (VALUES (1, '1000', 0), (2, '2210', 0)) AS source ([finanskonto_id], [kontonummer], [er_lukket])
ON target.[finanskonto_id] = source.[finanskonto_id]
WHEN NOT MATCHED THEN
    INSERT ([finanskonto_id], [kontonummer], [er_lukket])
    VALUES (source.[finanskonto_id], source.[kontonummer], source.[er_lukket])
WHEN MATCHED THEN
    UPDATE SET [kontonummer] = source.[kontonummer], [er_lukket] = source.[er_lukket];

-- Insert multiple postings for invoice F2024001 (split into 2 postings with different accounts)
-- KreditorTransaktion entries (simplified schema for test data)
MERGE INTO [Kreditor].[KreditorTransaktion] AS target
USING (VALUES
    (1001, 6787467, 1001, 'F2024001', 2000.00, 'Faktura', 'bilag001'),
    (1002, 6787467, 1002, 'F2024001', 510.00, 'Faktura', 'bilag001')
) AS source (
    [KreditortransaktionId], [kreditor_id], [FinansposteringId], [Fakturanummer], [beloeb], [transaktionstype], [bilagsnr]
)
ON target.[KreditortransaktionId] = source.[KreditortransaktionId]
WHEN NOT MATCHED THEN
    INSERT ([KreditortransaktionId], [kreditor_id], [FinansposteringId], [Fakturanummer], [beloeb], [transaktionstype], [bilagsnr])
    VALUES (source.[KreditortransaktionId], source.[kreditor_id], source.[FinansposteringId], source.[Fakturanummer], source.[beloeb], source.[transaktionstype], source.[bilagsnr])
WHEN MATCHED THEN
    UPDATE SET [kreditor_id] = source.[kreditor_id], [Fakturanummer] = source.[Fakturanummer];

-- Finanspostering entries (production column names: Bilagsnummer, posteringsbeloeb, etc.)
MERGE INTO [Finans].[Finanspostering] AS target
USING (VALUES
    (1001, 1, '2024-03-15', 2000.00, 'DKK', 'Faktura for medicin - hovedkonto', 'bilag001'),
    (1002, 2, '2024-03-15', 510.00, 'DKK', 'Faktura for medicin - moms', 'bilag001')
) AS source (
    [FinansposteringId], [finanskonto_id], [Bogfoeringsdato], [posteringsbeloeb], [valutakode], [posteringstekst], [Bilagsnummer]
)
ON target.[FinansposteringId] = source.[FinansposteringId]
WHEN NOT MATCHED THEN
    INSERT ([FinansposteringId], [finanskonto_id], [Bogfoeringsdato], [posteringsbeloeb], [valutakode], [posteringstekst], [Bilagsnummer])
    VALUES (source.[FinansposteringId], source.[finanskonto_id], source.[Bogfoeringsdato], source.[posteringsbeloeb], source.[valutakode], source.[posteringstekst], source.[Bilagsnummer])
WHEN MATCHED THEN
    UPDATE SET [finanskonto_id] = source.[finanskonto_id], [posteringsbeloeb] = source.[posteringsbeloeb];

-- FinansposteringKreditorTransRelation entries (production schema)
MERGE INTO [Finans].[FinansposteringKreditorTransRelation] AS target
USING (VALUES
    (1, 1001, 1001),
    (2, 1002, 1002)
) AS source (
    [FinansposteringKreditortransRelationId], [FinansposteringId], [KreditortransaktionId]
)
ON target.[FinansposteringKreditortransRelationId] = source.[FinansposteringKreditortransRelationId]
WHEN NOT MATCHED THEN
    INSERT ([FinansposteringKreditortransRelationId], [FinansposteringId], [KreditortransaktionId])
    VALUES (source.[FinansposteringKreditortransRelationId], source.[FinansposteringId], source.[KreditortransaktionId]);

PRINT 'Sydax test data updated successfully';
GO

-- ============================================================================
-- ILS Database (ils)
-- ============================================================================
-- Uses actual schema: lev table has cvr_nr, vare table has pvnr and complex structure
-- Tests expect cvr_nr="12345678" and pvnr="ITEM123" for clean code generation

USE [ils];
GO

-- Update existing lev record to have cvr 12345678
MERGE INTO [dbo].[lev] AS target
USING (VALUES (14057175, '12345678')) AS source ([prrowid], [cvr_nr])
ON target.[prrowid] = source.[prrowid]
WHEN MATCHED THEN
    UPDATE SET [cvr_nr] = source.[cvr_nr];

-- Update existing vare record to have pvnr='ITEM123' and link to cvr 12345678
MERGE INTO [dbo].[vare] AS target
USING (VALUES (1173123, 'ITEM123', 'Test Item ABC123')) AS source ([prrowid], [pvnr], [pvarenavn])
ON target.[prrowid] = source.[prrowid]
WHEN MATCHED THEN
    UPDATE SET [pvnr] = source.[pvnr], [pvarenavn] = source.[pvarenavn];

PRINT 'ILS test data updated successfully';
GO

-- ============================================================================
-- Verification Queries
-- ============================================================================
-- Run these to verify data is present and relationships work

-- MySupply verification
-- USE [mysupply];
-- SELECT * FROM [dbo].[metadata] WHERE [leverandoer_cvr] = '12345678';  -- id=100
-- SELECT * FROM [dbo].[statistic_invoice_header] WHERE [id] = 10;      -- faktura_udsteder_cvr_nummer='12345678'
-- SELECT * FROM [dbo].[statistic_invoice_line] WHERE [id] = 1;          -- statistic_invoice_header_id=10, item_faktura_udsteder_vare_nr='ABC123'

-- Sydax verification
-- USE [sydax];
-- SELECT * FROM [Kreditor].[Kreditor] WHERE [kreditor_id] = 6787467;     -- Should have juridisk_enhed='12345678'
-- SELECT * FROM [Finans].[Finanskonto] WHERE [finanskonto_id] IN (1, 2);   -- kontonummer='1000' and '2210'
-- SELECT * FROM [Finans].[Finanspostering] WHERE [FinansposteringId] IN (1001, 1002); -- Invoice F2024001 postings
-- SELECT * FROM [Kreditor].[KreditorTransaktion] WHERE [Fakturanummer] = 'F2024001'; -- Invoice transactions

-- ILS verification
-- USE [ils];
-- SELECT * FROM [dbo].[lev] WHERE [cvr_nr] = '12345678';                 -- prrowid=14057175
-- SELECT * FROM [dbo].[vare] WHERE [pvnr] = 'ITEM123';                   -- prrowid=1173123, should link to cvr_nr='12345678'

-- Clean Code Verification (after running staging assets)
-- SELECT TOP 5 * FROM [stg].[statistic_invoice_line_mysupply] WHERE [Kreditor_CleanCode] = 'CVR_12345678';
-- SELECT TOP 5 * FROM [stg].[kreditor_sydax] WHERE [Kreditor_CleanCode] = 'CVR_12345678';
-- SELECT TOP 5 * FROM [stg].[finanskonto_sydax] WHERE [Kontoplan_CleanCode] = 'KONTOPLAN_1';
