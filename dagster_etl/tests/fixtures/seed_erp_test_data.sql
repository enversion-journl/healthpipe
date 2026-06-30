-- ============================================================================
-- ERP Test Data Seeding Script
-- ============================================================================
-- This script creates deterministic test data for ERP integration tests.
-- Uses MERGE statements for idempotency - safe to run multiple times.
--
-- Test Data Relationships:
--   CVR: "12345678" (used across all systems for supplier matching)
--   MySupply Invoice: Header ID=10, Line ID=1, Item="ABC123"
--   Sydax: KreditorId=6787467 (existing), FinanskontoId=1, FinansposteringId=1
--   ILS: cvr_nr="12345678" (existing), pvnr="ITEM123"
-- ============================================================================

-- ============================================================================
-- MySupply Database (mysupply)
-- ============================================================================
-- Uses actual schema: metadata(id, FakturaTotal, LeverandorCVR, ...),
-- statistic_invoice_header(id, metadata_id, FakturaNr, FakturaDato, ..., FakturaUdstederCVRNummer),
-- statistic_invoice_line(id, statistic_invoice_header_id, FakturaLinjeNr, ..., Item_FakturaUdstederVareNr)

USE [mysupply];
GO

-- Insert/update metadata for supplier (using actual schema)
-- Use MERGE without explicit ID to avoid IDENTITY issues
MERGE INTO [dbo].[metadata] AS target
USING (
    VALUES ('12345678', 'Test Supplier', 'København Ø', 'Hovedgade 123', '2100', '2024-03-10T10:30:00', 'MySupply', 'Region Hovedstaden')
) AS source (
    [LeverandorCVR], [FirmaNavn], [By], [Adresse], [Postnummer], [ReceiveTimeFormat], [SystemNavn], [OrgUnit]
)
ON target.[LeverandorCVR] = source.[LeverandorCVR]
WHEN NOT MATCHED THEN
    INSERT ([FakturaTotal], [LeverandorCVR], [FirmaNavn], [By], [Adresse], [Postnummer], [ReceiveTimeFormat], [SystemNavn], [OrgUnit])
    VALUES (2550.00, source.[LeverandorCVR], source.[FirmaNavn], source.[By], source.[Adresse], source.[Postnummer], source.[ReceiveTimeFormat], source.[SystemNavn], source.[OrgUnit])
WHEN MATCHED THEN
    UPDATE SET
        [FirmaNavn] = source.[FirmaNavn],
        [By] = source.[By],
        [Adresse] = source.[Adresse],
        [Postnummer] = source.[Postnummer],
        [ReceiveTimeFormat] = source.[ReceiveTimeFormat],
        [SystemNavn] = source.[SystemNavn],
        [OrgUnit] = source.[OrgUnit];

-- Insert/update invoice header (using actual schema)
-- Use MERGE on FakturaNr to avoid IDENTITY issues
MERGE INTO [dbo].[statistic_invoice_header] AS target
USING (
    VALUES (
        'F2024001', '2024-03-10', '2024-03-08', '2024-04-05',
        'Region Hovedstaden', '12345678', 'DKK', 25.0, 510.0, 510.0, 510.0, 0.0, 0.0,
        2550.0, 2550.0, 3060.0, 3060.0
    )
) AS source (
    [FakturaNr], [FakturaDato], [OrdreDato], [BetalingsDato],
    [KundeNavn], [FakturaUdstederCVRNummer], [FakturaValutaKode], [VAT_AfgiftsKategoriProcent],
    [VAT_CategoryTotal_AfgiftsKategor], [VAT_TaxAmounts_AfgiftsKategoriMo], [VAT_TaxAmounts_AfgiftsKategoriM0],
    [VAT_TaxAmounts_AfgiftsKategoriM1], [VAT_TaxAmounts_AfgiftsKategoriM2], [FakturaLinjeTotal],
    [FakturaLinjeTotal_Valuta], [FakturaTotal], [FakturaTotal_Valuta]
)
ON target.[FakturaNr] = source.[FakturaNr]
WHEN NOT MATCHED THEN
    INSERT ([metadata_id], [FakturaNr], [FakturaDato], [OrdreDato], [BetalingsDato],
            [KundeNavn], [FakturaUdstederCVRNummer], [FakturaValutaKode], [VAT_AfgiftsKategoriProcent],
            [VAT_CategoryTotal_AfgiftsKategor], [VAT_TaxAmounts_AfgiftsKategoriMo], [VAT_TaxAmounts_AfgiftsKategoriM0],
            [VAT_TaxAmounts_AfgiftsKategoriM1], [VAT_TaxAmounts_AfgiftsKategoriM2], [FakturaLinjeTotal],
            [FakturaLinjeTotal_Valuta], [FakturaTotal], [FakturaTotal_Valuta])
    VALUES (100, source.[FakturaNr], source.[FakturaDato], source.[OrdreDato], source.[BetalingsDato],
            source.[KundeNavn], source.[FakturaUdstederCVRNummer], source.[FakturaValutaKode], source.[VAT_AfgiftsKategoriProcent],
            source.[VAT_CategoryTotal_AfgiftsKategor], source.[VAT_TaxAmounts_AfgiftsKategoriMo], source.[VAT_TaxAmounts_AfgiftsKategoriM0],
            source.[VAT_TaxAmounts_AfgiftsKategoriM1], source.[VAT_TaxAmounts_AfgiftsKategoriM2], source.[FakturaLinjeTotal],
            source.[FakturaLinjeTotal_Valuta], source.[FakturaTotal], source.[FakturaTotal_Valuta])
WHEN MATCHED THEN
    UPDATE SET
        [FakturaDato] = source.[FakturaDato],
        [OrdreDato] = source.[OrdreDato],
        [BetalingsDato] = source.[BetalingsDato],
        [KundeNavn] = source.[KundeNavn],
        [FakturaUdstederCVRNummer] = source.[FakturaUdstederCVRNummer],
        [FakturaValutaKode] = source.[FakturaValutaKode],
        [VAT_AfgiftsKategoriProcent] = source.[VAT_AfgiftsKategoriProcent],
        [VAT_CategoryTotal_AfgiftsKategor] = source.[VAT_CategoryTotal_AfgiftsKategor],
        [VAT_TaxAmounts_AfgiftsKategoriMo] = source.[VAT_TaxAmounts_AfgiftsKategoriMo],
        [VAT_TaxAmounts_AfgiftsKategoriM0] = source.[VAT_TaxAmounts_AfgiftsKategoriM0],
        [VAT_TaxAmounts_AfgiftsKategoriM1] = source.[VAT_TaxAmounts_AfgiftsKategoriM1],
        [VAT_TaxAmounts_AfgiftsKategoriM2] = source.[VAT_TaxAmounts_AfgiftsKategoriM2],
        [FakturaLinjeTotal] = source.[FakturaLinjeTotal],
        [FakturaLinjeTotal_Valuta] = source.[FakturaLinjeTotal_Valuta],
        [FakturaTotal] = source.[FakturaTotal],
        [FakturaTotal_Valuta] = source.[FakturaTotal_Valuta];

-- Insert/update invoice line (using actual schema)
-- Use MERGE on Item_FakturaUdstederVareNr to avoid IDENTITY issues
MERGE INTO [dbo].[statistic_invoice_line] AS target
USING (
    VALUES (
        1, 100.0, 2550.0, 'DKK', 'ABC123', 'Paracetamol 500mg tabletter',
        'Paracetamol 500mg', 25.50, 'DKK', 100.0, 'stk', '12345678'
    )
) AS source (
    [FakturaLinjeNr], [AntalEnhederFaktureret], [LinjeBeløb], [LinjeBeløb_Valuta],
    [Item_FakturaUdstederVareNr], [Item_FakturaUdstederVareNavn], [Item_FakturaUdstederVareNavnKort],
    [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta], [BasePrice_EnhedsStørrelse], [BasePrice_EnhedsStørrelse_Type],
    [FakturaUdstederCVRNummer]
)
ON target.[Item_FakturaUdstederVareNr] = source.[Item_FakturaUdstederVareNr]
WHEN NOT MATCHED THEN
    INSERT ([statistic_invoice_header_id], [FakturaLinjeNr], [AntalEnhederFaktureret],
            [LinjeBeløb], [LinjeBeløb_Valuta], [Item_FakturaUdstederVareNr], [Item_FakturaUdstederVareNavn],
            [Item_FakturaUdstederVareNavnKort], [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta],
            [BasePrice_EnhedsStørrelse], [BasePrice_EnhedsStørrelse_Type], [FakturaUdstederCVRNummer])
    VALUES ((SELECT id FROM [dbo].[statistic_invoice_header] WHERE [FakturaNr] = 'F2024001'),
            source.[FakturaLinjeNr], source.[AntalEnhederFaktureret],
            source.[LinjeBeløb], source.[LinjeBeløb_Valuta], source.[Item_FakturaUdstederVareNr], source.[Item_FakturaUdstederVareNavn],
            source.[Item_FakturaUdstederVareNavnKort], source.[BasePrice_EnhedsPris], source.[BasePrice_EnhedsPris_Valuta],
            source.[BasePrice_EnhedsStørrelse], source.[BasePrice_EnhedsStørrelse_Type], source.[FakturaUdstederCVRNummer])
WHEN MATCHED THEN
    UPDATE SET
        [FakturaLinjeNr] = source.[FakturaLinjeNr],
        [AntalEnhederFaktureret] = source.[AntalEnhederFaktureret],
        [LinjeBeløb] = source.[LinjeBeløb],
        [LinjeBeløb_Valuta] = source.[LinjeBeløb_Valuta],
        [Item_FakturaUdstederVareNavn] = source.[Item_FakturaUdstederVareNavn],
        [Item_FakturaUdstederVareNavnKort] = source.[Item_FakturaUdstederVareNavnKort],
        [BasePrice_EnhedsPris] = source.[BasePrice_EnhedsPris],
        [BasePrice_EnhedsPris_Valuta] = source.[BasePrice_EnhedsPris_Valuta],
        [BasePrice_EnhedsStørrelse] = source.[BasePrice_EnhedsStørrelse],
        [BasePrice_EnhedsStørrelse_Type] = source.[BasePrice_EnhedsStørrelse_Type],
        [FakturaUdstederCVRNummer] = source.[FakturaUdstederCVRNummer];

PRINT 'MySupply test data seeded successfully';
GO

-- ============================================================================
-- Sydax Database (sydax)
-- ============================================================================
-- Uses production schema: Kreditor has KreditorId/JuridiskEnhed, Finans has FinanskontoId/Kontonummer
-- Tests expect specific clean codes: KREDID_6787467, KONTOPLAN_1, etc.

USE [sydax];
GO

-- Insert/update Kreditor with JuridiskEnhed (CVR) for clean code generation
MERGE INTO [Kreditor].[Kreditor] AS target
USING (VALUES (6787467, '12345678', 'Test Kreditor A/S')) AS source ([KreditorId], [JuridiskEnhed], [navn])
ON target.[KreditorId] = source.[KreditorId]
WHEN NOT MATCHED THEN
    INSERT ([KreditorId], [JuridiskEnhed], [navn])
    VALUES (source.[KreditorId], source.[JuridiskEnhed], source.[navn])
WHEN MATCHED THEN
    UPDATE SET [JuridiskEnhed] = source.[JuridiskEnhed], [navn] = source.[navn];

-- Insert chart of accounts entries (production uses Kontonummer, not kontonr)
MERGE INTO [Finans].[Finanskonto] AS target
USING (VALUES (1, '1000', 0), (2, '2210', 0)) AS source ([FinanskontoId], [Kontonummer], [ErLukket])
ON target.[FinanskontoId] = source.[FinanskontoId]
WHEN NOT MATCHED THEN
    INSERT ([FinanskontoId], [Kontonummer], [ErLukket])
    VALUES (source.[FinanskontoId], source.[Kontonummer], source.[ErLukket])
WHEN MATCHED THEN
    UPDATE SET [Kontonummer] = source.[Kontonummer], [ErLukket] = source.[ErLukket];

-- Insert multiple postings for invoice F2024001 (split into 2 postings with different accounts)
-- KreditorTransaktion entries (simplified schema for test data)
MERGE INTO [Kreditor].[KreditorTransaktion] AS target
USING (VALUES
    (1001, 6787467, 1001, 'F2024001', 2000.00, 'Faktura', 'bilag001'),
    (1002, 6787467, 1002, 'F2024001', 510.00, 'Faktura', 'bilag001')
) AS source (
    [KreditortransaktionId], [KreditorId], [FinansposteringId], [Fakturanummer], [beloeb], [transaktionstype], [bilagsnr]
)
ON target.[KreditortransaktionId] = source.[KreditortransaktionId]
WHEN NOT MATCHED THEN
    INSERT ([KreditortransaktionId], [KreditorId], [FinansposteringId], [Fakturanummer], [beloeb], [transaktionstype], [bilagsnr])
    VALUES (source.[KreditortransaktionId], source.[KreditorId], source.[FinansposteringId], source.[Fakturanummer], source.[beloeb], source.[transaktionstype], source.[bilagsnr])
WHEN MATCHED THEN
    UPDATE SET [KreditorId] = source.[KreditorId], [Fakturanummer] = source.[Fakturanummer];

-- Finanspostering entries (production column names: Bilagsnummer, Posteringsbeloeb, etc.)
MERGE INTO [Finans].[Finanspostering] AS target
USING (VALUES
    (1001, 1, '2024-03-15', 2000.00, 'DKK', 'Faktura for medicin - hovedkonto', 'bilag001'),
    (1002, 2, '2024-03-15', 510.00, 'DKK', 'Faktura for medicin - moms', 'bilag001')
) AS source (
    [FinansposteringId], [FinanskontoId], [Bogfoeringsdato], [Posteringsbeloeb], [Valutakode], [Posteringstekst], [Bilagsnummer]
)
ON target.[FinansposteringId] = source.[FinansposteringId]
WHEN NOT MATCHED THEN
    INSERT ([FinansposteringId], [FinanskontoId], [Bogfoeringsdato], [Posteringsbeloeb], [Valutakode], [Posteringstekst], [Bilagsnummer])
    VALUES (source.[FinansposteringId], source.[FinanskontoId], source.[Bogfoeringsdato], source.[Posteringsbeloeb], source.[Valutakode], source.[Posteringstekst], source.[Bilagsnummer])
WHEN MATCHED THEN
    UPDATE SET [FinanskontoId] = source.[FinanskontoId], [Posteringsbeloeb] = source.[Posteringsbeloeb];

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

-- Update existing lev record to have CVR 12345678
MERGE INTO [dbo].[lev] AS target
USING (VALUES (14057175, '12345678')) AS source ([prrowid], [cvr_nr])
ON target.[prrowid] = source.[prrowid]
WHEN MATCHED THEN
    UPDATE SET [cvr_nr] = source.[cvr_nr];

-- Update existing vare record to have pvnr='ITEM123' and link to CVR 12345678
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
-- SELECT * FROM [dbo].[metadata] WHERE [LeverandorCVR] = '12345678';  -- id=100
-- SELECT * FROM [dbo].[statistic_invoice_header] WHERE [id] = 10;      -- FakturaUdstederCVRNummer='12345678'
-- SELECT * FROM [dbo].[statistic_invoice_line] WHERE [id] = 1;          -- statistic_invoice_header_id=10, Item_FakturaUdstederVareNr='ABC123'

-- Sydax verification
-- USE [sydax];
-- SELECT * FROM [Kreditor].[Kreditor] WHERE [KreditorId] = 6787467;     -- Should have JuridiskEnhed='12345678'
-- SELECT * FROM [Finans].[Finanskonto] WHERE [FinanskontoId] IN (1, 2);   -- Kontonummer='1000' and '2210'
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
