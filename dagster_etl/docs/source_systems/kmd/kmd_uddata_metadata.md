<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# KMD UdData — Schema Catalogue

_Source: Azure Synapse (`KmdUddataMSSQLResource`). Generated from `kmd_uddata_schema_query.sql` and pasted in manually._

## Overview

Star schema with one central fact (`fact.SpendAnalysisLines`) joined to dimension views via `DW_SK_*` keys. SCD Type 2 columns (`DW_ValidFrom`, `DW_ValidTo`, `DW_IsCurrent`) are present on every dimension except `RLS` (row-level security helper).

| Type | View | Columns |
|------|------|---------|
| fact | `fact.SpendAnalysisLines` | 51 |
| dim  | `dimension.AllowanceChargeLine` | 15 |
| dim  | `dimension.AppendixNumber` | 10 |
| dim  | `dimension.Compliance` | 23 |
| dim  | `dimension.Date` | 47 |
| dim  | `dimension.Economy` | 26 |
| dim  | `dimension.EconomyCustom` | 13 |
| dim  | `dimension.EtradeObligation` | 11 |
| dim  | `dimension.ExchangeRates` | 13 |
| dim  | `dimension.ExpenceType` | 11 |
| dim  | `dimension.GeoData` | 16 |
| dim  | `dimension.Invoice` | 51 |
| dim  | `dimension.InvoiceLine` | 41 |
| dim  | `dimension.InvoiceType` | 11 |
| dim  | `dimension.LocalPurchase` | 11 |
| dim  | `dimension.NACE` | 28 |
| dim  | `dimension.Online` | 11 |
| dim  | `dimension.OrderType` | 11 |
| dim  | `dimension.Organic` | 11 |
| dim  | `dimension.Organization` | 47 |
| dim  | `dimension.PostingText` | 10 |
| dim  | `dimension.PriceAgreement` | 23 |
| dim  | `dimension.ProcurementAgreement` | 22 |
| dim  | `dimension.ProcurementType` | 11 |
| dim  | `dimension.RLS` | 5 |
| dim  | `dimension.SpendAnalysisLineItem` | 13 |
| dim  | `dimension.Supplier` | 28 |
| dim  | `dimension.TenderGroup` | 25 |
| dim  | `dimension.UNSPSC` | 25 |

---

## Fact

### `fact.SpendAnalysisLines`

| Column | Type |
|--------|------|
| DW_EK_SpendAnalysisLine | bigint |
| DW_SK_Compliance | int |
| DW_SK_Date_SpendAnalysis | int |
| DW_SK_Date_Issue | int |
| DW_SK_Date_OrderIssue | int |
| DW_SK_Date_ActualDelivery | int |
| DW_SK_Date_PaymentDue | int |
| DW_SK_Date_Posting | int |
| DW_SK_Economy | int |
| DW_SK_PostingText | int |
| DW_SK_EconomyCustom | int |
| DW_SK_EconomyLine | int |
| DW_SK_AppendixNumber | int |
| DW_SK_ExchangeRates | int |
| DW_SK_ExpenceType | int |
| DW_SK_GeoData | int |
| DW_SK_Invoice | int |
| DW_SK_InvoiceLine | int |
| DW_SK_AllowanceChargeLine | int |
| DW_SK_InvoiceType | int |
| DW_SK_LocalPurchase | int |
| DW_SK_NACE | int |
| DW_SK_Online | int |
| DW_SK_EtradeObligation | int |
| DW_SK_OrderType | int |
| DW_SK_Organic | int |
| DW_SK_Organization | int |
| DW_SK_PriceAgreement | int |
| DW_SK_ProcurementAgreement | int |
| DW_SK_ProcurementType | int |
| DW_SK_SpendAnalysisLineItem | int |
| DW_SK_Supplier | int |
| DW_SK_TenderGroup | int |
| DW_SK_UNSPSC | int |
| LineAmount | numeric |
| InvoicedQuantity | numeric |
| AgreementItemPrice | numeric |
| RatePercentNumeric | numeric |
| LineTaxAmount | numeric |
| UnitPrice | numeric |
| UnitPriceExchange | numeric |
| ObservedUnitPrice | numeric |
| PriceAgreementItemInScopeIndicator | int |
| LinePercentOfTotal | numeric |
| IsDiffLine | int |
| CO2Emission | numeric |
| DW_ValidDate | date |
| DW_ArchiveID | bigint |
| DW_Audit | int |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |

---

## Dimensions

### `dimension.AllowanceChargeLine`

| Column | Type |
|--------|------|
| DW_SK_AllowanceChargeLine | int |
| DW_EK_AllowanceChargeLine | int |
| GUID | uniqueidentifier |
| AllowanceChargeID | int |
| AllowanceCharge | nvarchar(50) |
| AllowanceChargeTaxName | nvarchar(50) |
| AllowanceItemDescription | nvarchar(100) |
| LineExpenceType | int |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.AppendixNumber`

| Column | Type |
|--------|------|
| DW_SK_AppendixNumber | int |
| DW_EK_AppendixNumber | int |
| AppendixNumber | nvarchar(50) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Compliance`

| Column | Type |
|--------|------|
| DW_SK_Compliance | int |
| DW_EK_Compliance | int |
| RequestForProposalIndicator | int |
| ItemGroupLoyaltyIndicator | int |
| ItemNumberLoyaltyIndicator | int |
| SupplierLoyaltyIndicator | int |
| CatalogueSupplierLoyaltyIndicator | int |
| SortimentLoyaltyIndicator | int |
| ItemGroupLoyaltySort | int |
| ItemNumberLoyaltySort | int |
| RequestForProposalText | nvarchar(100) |
| ItemGroupLoyaltyText | nvarchar(100) |
| ItemNumberLoyaltyText | nvarchar(100) |
| SupplierLoyaltyText | nvarchar(100) |
| CatalogueSupplierLoyaltyText | nvarchar(100) |
| SortimentLoyaltyText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Date`

| Column | Type |
|--------|------|
| DW_SK_Date | int |
| DW_EK_Date | int |
| Date | date |
| Year | int |
| YearCharacter | nvarchar(20) |
| LogicalYearFirstDay | int |
| LogicalYearLastDay | int |
| HalfYearNumber | int |
| YearHalfYearNumber | nvarchar(20) |
| HalfYearShort | nvarchar(20) |
| HalfYearName | nvarchar(20) |
| QuarterNumber | int |
| QuarterShortYear | nvarchar(20) |
| QuarterShort | nvarchar(20) |
| QuarterName | nvarchar(20) |
| YearQuarter | nvarchar(20) |
| YearQuarterName | nvarchar(20) |
| LogicalFirstDayInQuarter | int |
| LogicalLastDayInQuarter | int |
| MonthNumber | int |
| YearMonth | nvarchar(20) |
| YearMonthNumber | int |
| YearMonthName | nvarchar(20) |
| YearMonthShort | nvarchar(20) |
| MonthName | nvarchar(20) |
| MonthNameShort | nvarchar(10) |
| LogicalFirstDayInMonth | int |
| LogicalLastDayInMonth | int |
| WeekNumber | int |
| YearISO | int |
| YearWeekNumber | int |
| YearWeek | nvarchar(20) |
| WeekShort | nvarchar(20) |
| WeekName | nvarchar(20) |
| DayInWeek | int |
| WeekdayShort | nvarchar(20) |
| WeekdayName | nvarchar(20) |
| WeekdayLetter | nchar(1) |
| LogicalWorkday | int |
| WorkdayID | int |
| LogicalPublicHoliday | int |
| YearFourWeeksInterval | nvarchar(20) |
| FourWeeksIntervalShort | nvarchar(20) |
| FourWeeksIntervalText | nvarchar(20) |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Economy`

| Column | Type |
|--------|------|
| DW_SK_Economy | int |
| DW_EK_Economy | int |
| IMartnumber | nvarchar(50) |
| IMartdescription | nvarchar(100) |
| IMArtLabel | nvarchar(153) |
| PSPnumber | nvarchar(50) |
| PSPdescription | nvarchar(100) |
| PSPLabel | nvarchar(153) |
| AccountingID | nvarchar(50) |
| SumPostingKey | bigint |
| CapitalFondID | nvarchar(50) |
| FunctionsAreaID | nvarchar(50) |
| ArtNumber | nvarchar(50) |
| ArtDescription | nvarchar(100) |
| ArtLabel | nvarchar(153) |
| CostCenterNumber | nvarchar(50) |
| CostCenterDescription | nvarchar(100) |
| CostCenterLabel | nvarchar(153) |
| DranstNumber | nvarchar(50) |
| DranstDescription | nvarchar(60) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.EconomyCustom`

| Column | Type |
|--------|------|
| DW_SK_EconomyCustom | int |
| DW_EK_EconomyCustom | int |
| EconomyVariable1 | nvarchar(100) |
| EconomyVariable2 | nvarchar(100) |
| EconomyVariable3 | nvarchar(100) |
| EconomyVariable4 | nvarchar(100) |
| EconomyVariable5 | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.EtradeObligation`

| Column | Type |
|--------|------|
| DW_SK_EtradeObligation | int |
| DW_EK_EtradeObligation | int |
| CategoryUNSPSC | nvarchar(30) |
| CountryCode | nvarchar(30) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.ExchangeRates`

| Column | Type |
|--------|------|
| DW_SK_ExchangeRates | int |
| DW_EK_ExchangeRates | int |
| CurrencyCode | nvarchar(10) |
| ExchangeRate | numeric |
| ExchangeRateDate | date |
| LoadTime | datetime2 |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.ExpenceType`

| Column | Type |
|--------|------|
| DW_SK_ExpenceType | int |
| DW_EK_ExpenceType | int |
| ExpenceTypeIndicator | int |
| ExpenceTypeText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.GeoData`

| Column | Type |
|--------|------|
| DW_SK_GeoData | int |
| DW_EK_GeoData | int |
| CountryCode | nvarchar(50) |
| PostalCode | nvarchar(20) |
| PostalCityName | nvarchar(180) |
| MunicipalityID | nvarchar(20) |
| MunicipalityName | nvarchar(100) |
| RegionID | nvarchar(20) |
| RegionName | nvarchar(100) |
| DW_ArchiveID | int |
| DW_IsCurrent | bit |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Invoice`

| Column | Type |
|--------|------|
| DW_SK_Invoice | int |
| DW_EK_Invoice | int |
| GUID | uniqueidentifier |
| SupplierCompanyTaxID | nvarchar(50) |
| InvoiceNumber | nvarchar(50) |
| InvoiceUUID | nvarchar(50) |
| IsCreditnote | int |
| Filename | nvarchar(300) |
| FileFormat | nvarchar(20) |
| InvoiceSize | nvarchar(50) |
| HasItemID | int |
| HasUnitCode | int |
| HasItemDescription | int |
| HasOrderID | int |
| HasOrderIssueDate | int |
| HasUnitPrice | int |
| InvoiceNote | nvarchar(500) |
| DeliveryLocationPostalZone | nvarchar(10) |
| DeliveryLocationAddressCityName | nvarchar(100) |
| DeliveryLocationStreetName | nvarchar(100) |
| DeliveryLocationBuildingNumber | nvarchar(10) |
| AccountingLocationCode | nvarchar(20) |
| LineExtensionTotalAmount | numeric |
| PostalZoneSupplierFromInvoice | nvarchar(10) |
| SupplierCityFromInvoice | nvarchar(100) |
| SupplierAddressFromInvoice | nvarchar(100) |
| SupplierBuildingNumberFromInvoice | nvarchar(10) |
| BuyersOrderID | nvarchar(50) |
| BuyerReference | nvarchar(50) |
| BuyersAddress | nvarchar(100) |
| BuyersContact | nvarchar(50) |
| BuyersContactID | nvarchar(50) |
| BuyersEAN | nvarchar(50) |
| ContractNumber | nvarchar(50) |
| SupplierName | nvarchar(100) |
| SupplierContact | nvarchar(50) |
| SupplierContactID | nvarchar(50) |
| SupplierOrderID | nvarchar(50) |
| PaymentChannelCode | nvarchar(50) |
| PenaltySurchargeRateNumeric | numeric |
| PersonalSecure | nvarchar(10) |
| PaymentPeriodDays | int |
| ForeignCurrency | nvarchar(10) |
| InvoiceSizeID | int |
| IsDiscarded | int |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.InvoiceLine`

| Column | Type |
|--------|------|
| DW_SK_InvoiceLine | int |
| DW_EK_InvoiceLine | int |
| GUID | uniqueidentifier |
| InvoiceLineID | int |
| LineItemID | nvarchar(50) |
| LineAmount | numeric |
| VatAmount | numeric |
| QuantityInvoiced | numeric |
| UnitPrice | numeric |
| Unit | nvarchar(20) |
| TaxPercent | numeric |
| Class | nvarchar(10) |
| ItemID | nvarchar(50) |
| ItemName | nvarchar(300) |
| ItemDescription | nvarchar(300) |
| BuyersOrderID | nvarchar(50) |
| Supplier&ItemID | nvarchar(101) |
| InvoiceLineNote | nvarchar(500) |
| BuyersItemIdentificationID | nvarchar(50) |
| PackQuantity | numeric |
| PackSizeNumeric | numeric |
| PriceBaseQuantityPrice | numeric |
| PriceChangeReason | nvarchar(50) |
| PriceType | nvarchar(50) |
| PriceOrderableUnitFactorRate | numeric |
| IntItemID | nvarchar(50) |
| SupplierUNSPSC | nvarchar(10) |
| ItemDataValidity | bit |
| UnitPriceMin | numeric |
| AllowanceChargeReason | nvarchar(100) |
| EmissionFactor | nvarchar(100) |
| EmissionFactorCalculationRate | nvarchar(100) |
| EmissionFactorCalculationUnit | nvarchar(100) |
| EmissionFactorSource | nvarchar(100) |
| NetEmissionQuantity | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.InvoiceType`

| Column | Type |
|--------|------|
| DW_SK_InvoiceType | int |
| DW_EK_InvoiceType | int |
| InvoiceTypeIndicator | int |
| InvoiceTypeText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.LocalPurchase`

| Column | Type |
|--------|------|
| DW_SK_LocalPurchase | int |
| DW_EK_LocalPurchase | int |
| LocalPurchaseIndicator | int |
| LocalPurchaseText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.NACE`

| Column | Type |
|--------|------|
| DW_SK_NACE | int |
| DW_EK_NACE | int |
| NACELevel | int |
| NACEParent | nvarchar(10) |
| NACEChild | nvarchar(10) |
| NACECodeTrim | nvarchar(10) |
| NACELev1Code | nvarchar(10) |
| NACELev1Text | nvarchar(250) |
| NACELev1Label | nvarchar(200) |
| NACELev2Code | nvarchar(10) |
| NACELev2Text | nvarchar(250) |
| NACELev2Label | nvarchar(200) |
| NACELev3Code | nvarchar(10) |
| NACELev3Text | nvarchar(250) |
| NACELev3Label | nvarchar(200) |
| NACELev4Code | nvarchar(10) |
| NACELev4Text | nvarchar(250) |
| NACELev4Label | nvarchar(200) |
| NACELev5Code | nvarchar(10) |
| NACELev5Text | nvarchar(250) |
| NACELev5Label | nvarchar(200) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Online`

| Column | Type |
|--------|------|
| DW_SK_Online | int |
| DW_EK_Online | int |
| OnlineIndicator | int |
| OnlineText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.OrderType`

| Column | Type |
|--------|------|
| DW_SK_OrderType | int |
| DW_EK_OrderType | int |
| OrderTypeIndicator | int |
| OrderTypeText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Organic`

| Column | Type |
|--------|------|
| DW_SK_Organic | int |
| DW_EK_Organic | int |
| OrganicIndicator | int |
| OrganicText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Organization`

| Column | Type |
|--------|------|
| DW_SK_Organization | int |
| DW_EK_Organization | int |
| OrganizationID | nvarchar(60) |
| OrganizationLevel | int |
| OrganizationParent | nvarchar(60) |
| OrganizationChild | nvarchar(60) |
| OrganizationDescription | nvarchar(300) |
| OrganizationDescriptionLabel | nvarchar(363) |
| OrganizationLev1Text | nvarchar(200) |
| OrganizationLev1Code | nvarchar(60) |
| OrganizationLev1Label | nvarchar(263) |
| OrganizationLev2Text | nvarchar(200) |
| OrganizationLev2Code | nvarchar(60) |
| OrganizationLev2Label | nvarchar(263) |
| OrganizationLev3Text | nvarchar(200) |
| OrganizationLev3Code | nvarchar(60) |
| OrganizationLev3Label | nvarchar(263) |
| OrganizationLev4Text | nvarchar(200) |
| OrganizationLev4Code | nvarchar(60) |
| OrganizationLev4Label | nvarchar(263) |
| OrganizationLev5Text | nvarchar(200) |
| OrganizationLev5Code | nvarchar(60) |
| OrganizationLev5Label | nvarchar(263) |
| OrganizationLev6Text | nvarchar(200) |
| OrganizationLev6Code | nvarchar(60) |
| OrganizationLev6Label | nvarchar(263) |
| OrganizationLev7Text | nvarchar(200) |
| OrganizationLev7Code | nvarchar(60) |
| OrganizationLev7Label | nvarchar(263) |
| OrganizationLev8Text | nvarchar(200) |
| OrganizationLev8Code | nvarchar(60) |
| OrganizationLev8Label | nvarchar(263) |
| OrganizationLev9Text | nvarchar(200) |
| OrganizationLev9Code | nvarchar(60) |
| OrganizationLev9Label | nvarchar(263) |
| OrganizationLev10Text | nvarchar(200) |
| OrganizationLev10Code | nvarchar(60) |
| OrganizationLev10Label | nvarchar(263) |
| OrganizationLev11Text | nvarchar(200) |
| OrganizationLev11Code | nvarchar(60) |
| OrganizationLev11Label | nvarchar(263) |
| OrganizationLev12Text | nvarchar(200) |
| OrganizationLev12Code | nvarchar(60) |
| OrganizationLev12Label | nvarchar(263) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.PostingText`

| Column | Type |
|--------|------|
| DW_SK_Postingtext | int |
| DW_EK_Postingtext | int |
| PostingtextReporting | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.PriceAgreement`

| Column | Type |
|--------|------|
| DW_SK_PriceAgreement | int |
| DW_EK_PriceAgreement | int |
| CompanyTaxID | nvarchar(25) |
| CatalogueNumber | nvarchar(50) |
| ItemID | nvarchar(50) |
| ItemPrice | numeric |
| UnitCode | nvarchar(50) |
| BeginDate | date |
| EndDate | date |
| OrgID | nvarchar(25) |
| Assortment | nvarchar(50) |
| PriceTxt1 | nvarchar(30) |
| PriceTxt2 | nvarchar(30) |
| PriceTxt3 | nvarchar(30) |
| PriceTxt4 | nvarchar(30) |
| PriceTxt5 | nvarchar(30) |
| ActiveAgreementItem | nvarchar(28) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.ProcurementAgreement`

| Column | Type |
|--------|------|
| DW_SK_ProcurementAgreement | int |
| DW_EK_ProcurementAgreement | int |
| Grouping | nvarchar(50) |
| Number | nvarchar(50) |
| Description | nvarchar(50) |
| CompanyTaxID | nvarchar(25) |
| UNSPSCClassID | int |
| BeginDate | date |
| EndDate | date |
| OrgID | nvarchar(60) |
| NoUNSPSCInheritance | bit |
| ECommerceRelevant | bit |
| ECommerceActual | int |
| ECommercePotential | int |
| CentralAgreement | bit |
| ActiveAgreementSupplier | nvarchar(50) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.ProcurementType`

| Column | Type |
|--------|------|
| DW_SK_ProcurementType | int |
| DW_EK_ProcurementType | int |
| ProcurementTypeIndicator | int |
| ProcurementTypeText | nvarchar(100) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.RLS`

_Row-level security helper view — no SCD columns, no audit metadata._

| Column | Type |
|--------|------|
| Email | nvarchar(50) |
| DW_SK_Organization | int |
| DW_EK_Organization | int |
| OrganizationID | nvarchar(60) |
| OrganizationParent | nvarchar(60) |

### `dimension.SpendAnalysisLineItem`

| Column | Type |
|--------|------|
| DW_SK_SpendAnalysisLineItem | int |
| DW_EK_SpendAnalysisLineItem | int |
| ItemID | nvarchar(300) |
| Unit | nvarchar(20) |
| ItemName | nvarchar(300) |
| ItemDescription | nvarchar(300) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.Supplier`

| Column | Type |
|--------|------|
| DW_SK_Supplier | int |
| DW_EK_Supplier | int |
| SupplierID | nvarchar(50) |
| SupplierName | nvarchar(200) |
| SupplierNameLabel | nvarchar(297) |
| NACEcode | nvarchar(10) |
| SupplierAddress | nvarchar(500) |
| SupplierPostalcode | nvarchar(100) |
| SupplierCity | nvarchar(300) |
| SupplierCountry | nvarchar(40) |
| NumberOfEmployees | int |
| SupplierMainGroupSize | nvarchar(50) |
| SupplierSubGroupSize | nvarchar(50) |
| SupplierSubGroupID | int |
| AddressOfficialRegionCode | nvarchar(100) |
| BusinessFormatCode | nvarchar(100) |
| CreditorInformationValidFromDate | date |
| CreditorStatusInformationCode | nvarchar(260) |
| EmploymentQuaterEmploymentInterv | nvarchar(200) |
| SupplierCessationDate | date |
| SupplierObligationCode | nvarchar(100) |
| SupplierStartDate | date |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.TenderGroup`

| Column | Type |
|--------|------|
| DW_SK_TenderGroup | int |
| DW_EK_TenderGroup | int |
| TaxonomyUNSPSCClassID | int |
| TaxonomyTenderTypeDescription | nvarchar(100) |
| TaxonomyProcurementCategoryLev1Code | nvarchar(100) |
| TaxonomyProcurementCategoryLev2Code | nvarchar(100) |
| TaxonomyProcurementCategoryLev3Code | nvarchar(100) |
| TaxonomyProcurementCategoryLev4Code | nvarchar(100) |
| TaxonomyProcurementCategoryLev5Code | nvarchar(100) |
| TaxonomyProcurementCategoryLev1Text | nvarchar(100) |
| TaxonomyProcurementCategoryLev2Text | nvarchar(100) |
| TaxonomyProcurementCategoryLev3Text | nvarchar(100) |
| TaxonomyProcurementCategoryLev4Text | nvarchar(100) |
| TaxonomyProcurementCategoryLev5Text | nvarchar(100) |
| TaxonomyProcurementCategoryLev1Label | nvarchar(203) |
| TaxonomyProcurementCategoryLev2Label | nvarchar(203) |
| TaxonomyProcurementCategoryLev3Label | nvarchar(203) |
| TaxonomyProcurementCategoryLev4Label | nvarchar(203) |
| TaxonomyProcurementCategoryLev5Label | nvarchar(203) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |

### `dimension.UNSPSC`

| Column | Type |
|--------|------|
| DW_SK_UNSPSC | int |
| DW_EK_UNSPSC | int |
| UNSPSCClassID | nvarchar(100) |
| UNSPSCClassDescription | nvarchar(400) |
| UNSPSCLevel | smallint |
| TenderCategoryKey | nvarchar(100) |
| UNSPSCClassLev1ID | nvarchar(100) |
| UNSPSCClassLev1Description | nvarchar(400) |
| UNSPSCClassLev1Label | nvarchar(502) |
| UNSPSCClassLev2ID | nvarchar(100) |
| UNSPSCClassLev2Description | nvarchar(400) |
| UNSPSCClassLev2Label | nvarchar(502) |
| UNSPSCClassLev3ID | nvarchar(100) |
| UNSPSCClassLev3Description | nvarchar(400) |
| UNSPSCClassLev3Label | nvarchar(502) |
| UNSPSCClassLev4ID | nvarchar(100) |
| UNSPSCClassLev4Description | nvarchar(400) |
| UNSPSCClassLev4Label | nvarchar(502) |
| DW_ArchiveID | bigint |
| DW_ValidFrom | datetime2 |
| DW_ValidTo | datetime2 |
| DW_IsCurrent | bit |
| DW_Audit_Insert | int |
| DW_Audit_Update | int |
| DW_Audit | int |
