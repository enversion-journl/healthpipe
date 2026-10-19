<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# IFDB Database Schema Documentation

## Implementation design 18-12-2025

flowchart LR
    subgraph sources [Oracle Sources]
        IFDB[(IFDB Oracle)]
    end

    subgraph dsi [DSI Layer - Extraction]
        ExtPatient[extract_patient]
        ExtLocation[extract_location]
        ExtEOC[extract_episode_of_care]
        ExtCondition[extract_healthissue]
        ExtEncounter[extract_encounter]
        ExtObservation[extract_observation]
    end

    subgraph dsa [DSA Layer - FHIR Transform and Load]
        FHIRPatient[fhir_patient]
        FHIRLocation[fhir_location]
        FHIREOC[fhir_episode_of_care]
        FHIRCondition[fhir_condition]
        FHIREncounter[fhir_encounter]
        FHIRObservation[fhir_observation]
    end

    subgraph fhir [FHIR Server]
        FHIRServer[(HAPI FHIR)]
    end

    IFDB --> ExtPatient
    IFDB --> ExtLocation
    IFDB --> ExtEOC
    IFDB --> ExtCondition
    IFDB --> ExtEncounter
    IFDB --> ExtObservation

    ExtPatient --> FHIRPatient
    ExtLocation --> FHIRLocation
    ExtEOC --> FHIREOC
    ExtCondition --> FHIRCondition
    ExtEncounter --> FHIREncounter
    ExtObservation --> FHIRObservation

    FHIRPatient --> FHIRServer
    FHIRLocation --> FHIRServer
    FHIREOC --> FHIRServer
    FHIRCondition --> FHIRServer
    FHIREncounter --> FHIRServer
    FHIRObservation --> FHIRServer

## Overview

IFDB (Integrated Hospital Database) is an operational EPJ (Elektronisk Patientjournal) source system. This document provides a comprehensive description of the database schema, including table structures, relationships, and constraints.

**Database Information:**
- **Database**: oracle:1521/ORACLE_DB
- **Schema**: IFDB
- **Total Tables**: 91
- **Export Date**: 2025-10-27 13:47:27

## Schema Architecture

The IFDB database follows a healthcare data model with the following key architectural patterns:

1. **Patient-Centric Design**: All clinical data is linked to patients through `PATIENT_ID`
2. **Encounter-Based Structure**: Clinical activities are organized around encounters and episodes of care
3. **Audit Trail**: Most tables include modification timestamps and user tracking
4. **Multi-language Support**: Danish text fields alongside coded values
5. **XML Integration**: Complex clinical data stored as XML in CLOB fields

## Complete Table List

The IFDB database contains the following 91 tables:

1. **ABSENCE** - Absence records
2. **ADGANGSLOG** - Access logs
3. **ADGANGS_OBJEKTER** - Access objects
4. **ADMINISTRATIVE_PROCEDURE** - Administrative procedures
5. **ADM_PROCEDURE_SUPPL_CODES** - Administrative procedure supplementary codes
6. **AKTIVITETSRESULTAT** - Activity results
7. **AKTIVITETSUDFOERELSE** - Activity executions
8. **AKTIVITETSUDFOERELSE_SPECIALE** - Activity execution specialties
9. **ANDENCAVE** - Other allergies/warnings
10. **BETALERREGISTER** - Payer registry
11. **BLACKLIST_DATAWAREHOUSE** - Data warehouse blacklist
12. **BOM_AFLYSTOPERATION** - Cancelled operations
13. **BOM_PERIODE** - BOM periods
14. **CARETEAM** - Care teams
15. **CARETEAMPARTICIPANT** - Care team participants
16. **CARE_DISCHARGE_REPORT** - Care discharge reports
17. **CARE_DISCHARGE_REPORT_RECEIVER** - Care discharge report receivers
18. **DIAGNOSE** - Diagnoses
19. **DIAGNOSE_TILLAEGSKODE** - Diagnosis supplementary codes
20. **DISPENSED_MEDICATION** - Dispensed medications
21. **ENCOUNTER** - Healthcare encounters
22. **ENCOUNTER_BED_SPACE_HISTORY** - Encounter bed space history
23. **EPIKRISE** - Discharge summaries
24. **EPIKRISE_DIAGNOSE** - Discharge summary diagnoses
25. **EPIKRISE_DIAGNOSE_TILKODE** - Discharge summary diagnosis supplementary codes
26. **EPIKRISE_MODTAGER** - Discharge summary recipients
27. **EPIKRISE_OPERATION** - Discharge summary operations
28. **EPIKRISE_OPERATION_TILKODE** - Discharge summary operation supplementary codes
29. **EPIKRISE_TEKST** - Discharge summary text
30. **EPISODE_OF_CARE** - Episodes of care
31. **EVENTMARKER** - Event markers
32. **EVENT_FLOW** - Event flows
33. **FAERDIGBEHANDLING** - Completed treatments
34. **FEJLADMINISTRATION** - Medication administration errors
35. **GRAVIDITETSPERIODE** - Pregnancy periods
36. **HEALTHISSUE** - Health issues
37. **HEALTHY_COMPANION_PERIOD** - Healthy companion periods
38. **HENVISNING** - Referrals
39. **HENVISNING_HISTORIK** - Referral history
40. **HOSPITAL** - Hospital information
41. **IMPORTANT_INFORMATION** - Important information
42. **JOB_ERRORS** - Job errors
43. **JOB_LOG** - Job logs
44. **JOB_STATUS** - Job status
45. **KA_PRODUKT_VED_SKADESMEKANISME** - Products by injury mechanism
46. **KOMMUNE_ADVIS** - Municipality advice
47. **KOMMUNE_ADVIS_DETAIL** - Municipality advice details
48. **KONTAKTAARSAG** - Contact reasons
49. **LOCATION** - Healthcare locations
50. **MEDADMSTATUSHISTORY** - Medication administration status history
51. **MEDDELELSESBREV** - Notification letters
52. **MEDICATIONORDERPLAN** - Medication order plans
53. **MEDICATIONORDERPLANDOSE** - Medication order plan doses
54. **MEDICATION_BAG** - Medication bags
55. **MEDICINADMINISTRATION** - Medication administrations
56. **MEDICINADMINISTRATION_MIXTURE_INGREDIENT** - Medication administration mixture ingredients
57. **MEDICINCAVE** - Medication allergies/warnings
58. **MEDICINKORTLOG** - Medication card logs
59. **MEDICINORDINATION** - Medication orders
60. **MEDICINORDINATION_MIXTURE_INGREDIENT** - Medication order mixture ingredients
61. **MIGRATION_HISTORY** - Migration history
62. **NEGATIVE_CONSENT** - Negative consent
63. **ORDINATIONSBEGRUNDELSE** - Order justifications
64. **ORD_BEGRUNDELSE_SPECIALE** - Order justification specialties
65. **PASYDELSE** - PAS services
66. **PAS_DIAGNOSE** - PAS diagnoses
67. **PAS_DIAGNOSE_ENCOUNTER_REF** - PAS diagnosis encounter references
68. **PAS_DIAGNOSE_HENVISNING_REF** - PAS diagnosis referral references
69. **PAS_DIAGNOSE_TILLAEGSKODE** - PAS diagnosis supplementary codes
70. **PATIENT_CONTACT_INFO** - Patient contact information
71. **PATIENT_MIDLERTIDIG_ADRESSE** - Patient temporary addresses
72. **PATIENT_OPLYSNINGER_COLUMNA** - Patient information (Column A)
73. **PATIENT_OPLYSNINGER_OFFICIELLE** - Official patient information
74. **PATIENT_TIDLIGERE_ADRESSE** - Patient previous addresses
75. **PAYMENTINFORMATION** - Payment information
76. **PAYOR** - Payors
77. **PLANLAGT_AKTIVITET** - Planned activities
78. **POSTNUMMER** - Postal codes
79. **REFERRALINFORMATION** - Referral information
80. **REFERRAL_ASSESSMENT_STAMP_FIELD** - Referral assessment stamp fields
81. **REFERRAL_REASON_ADD_CODES** - Referral reason additional codes
82. **REHABILITATION_PLAN** - Rehabilitation plans
83. **REHABILITATION_PLAN_RECEIVER** - Rehabilitation plan receivers
84. **RIS_YDELSE** - RIS services
85. **SENGEENHED** - Bed units
86. **SENGEENHED_DISPONIBLE** - Available bed units
87. **SENGEENHED_NORMEREDE** - Standardized bed units
88. **TECHNICAL_CONTACT** - Technical contacts
89. **TILKNYTTET_PERSON** - Related persons
90. **TILKNYTTET_PERSON_TELEFON_NR** - Related person phone numbers
91. **TILKNYTTET_SYGEHUS_BEHANDLER** - Related hospital practitioners

## Detailed Table Structures and Constraints

### Core Patient Tables

#### PATIENT_OPLYSNINGER_COLUMNA
**Primary Key**: PATIENT_ID (PK_PTCOLUMNA)
**Purpose**: Core patient demographic information

**Columns**:
- `PATIENT_ID` (NUMBER(19)) - Primary key
- `MODTIDSPUNKT` (CLOB) - Receipt timestamp
- `SLETTET` (NUMBER(19)) - Deleted flag
- `CPR` (CLOB) - Danish CPR number
- `FORNAVNE` (CLOB) - First names
- `EFTERNAVN` (CLOB) - Last name
- `POSTNR` (CLOB) - Postal code
- `VEJ` (CLOB) - Street address
- `STEDNAVN` (CLOB) - City name
- `KOMMUNEKODE` (NUMBER(19)) - Municipality code
- `KOEN_KODE` (CLOB) - Gender code
- `KOEN_KODETEKST` (CLOB) - Gender code text
- `FOEDSELSDATO` (CLOB) - Birth date
- `LAEGENAVN` (CLOB) - Doctor name
- `LAEGENUMMER` (CLOB) - Doctor number
- `PERSON_STATUSKODE` (CLOB) - Person status code
- `PERSON_STATUSKODETEKST` (CLOB) - Person status code text
- `HAR_ADRESSEBESKYTTELSE` (NUMBER(19)) - Has address protection
- `OENSKER_DISKRETION` (NUMBER(19)) - Wants discretion
- `BEMAERKNINGER` (CLOB) - Remarks
- `PERSON_DISTRIKTKODE` (CLOB) - Person district code
- `PERSON_DISTRIKTKODETEKST` (CLOB) - Person district code text
- `PERSON_SOCIALDISTRIKTKODE` (CLOB) - Person social district code
- `PERSON_SOCIALDISTRIKTKODETEKST` (CLOB) - Person social district code text
- `OPDATERET_TID` (NUMBER(19)) - Updated timestamp

**Constraints**:
- Primary key: PK_PTCOLUMNA on PATIENT_ID

#### PATIENT_OPLYSNINGER_OFFICIELLE
**Primary Key**: PATIENT_ID (PK_PTOFFICIELLE)
**Purpose**: Official patient information from CPR registry

**Columns**:
- `PATIENT_ID` (NUMBER(19)) - Primary key
- `MODTIDSPUNKT` (CLOB) - Receipt timestamp
- `CPR` (NUMBER(19)) - Official CPR number
- `FORNAVNE` (CLOB) - First names
- `EFTERNAVN` (CLOB) - Last name
- `POSTNR` (CLOB) - Postal code
- `VEJ` (CLOB) - Street address
- `STEDNAVN` (CLOB) - City name
- `KOMMUNEKODE` (NUMBER(19)) - Municipality code
- `KOMMUNE_TILFLYTNINGSDATO` (CLOB) - Municipality immigration date
- `KOEN_KODE` (CLOB) - Gender code
- `KOEN_KODETEKST` (CLOB) - Gender code text
- `LAEGENAVN` (CLOB) - Doctor name
- `LAEGENUMMER` (CLOB) - Doctor number
- `PERSON_STATUSKODE` (CLOB) - Person status code
- `PERSON_STATUSKODETEKST` (CLOB) - Person status code text
- `HAR_ADRESSEBESKYTTELSE` (NUMBER(19)) - Has address protection
- `TILFLYTNINGSDATO` (CLOB) - Immigration date
- `SOGN` (CLOB) - Parish
- `PERSON_CIVILSTANDKODE` (CLOB) - Person marital status code
- `PERSON_CIVILSTANDKODETEKST` (CLOB) - Person marital status code text
- `PERSON_CIVILSTANDDATO` (CLOB) - Person marital status date
- `PERSON_DOEDSFALDDATO` (CLOB) - Person death date

**Constraints**:
- Primary key: PK_PTOFFICIELLE on PATIENT_ID
- Foreign key: FK_PTOFF_PTCOLUM to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)

### Encounter Management

#### ENCOUNTER
**Primary Key**: OBJECT_ID (PK_ENCOUNTER)
**Purpose**: Healthcare encounters and visits

**Columns**:
- `OBJECT_ID` (NUMBER(19)) - Primary key
- `EPISODE_OF_CARE_ID` (NUMBER(19)) - Episode of care reference
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `PAYMENT_INFORMATION_ID` (CLOB) - Payment information reference
- `STATUS_CODE` (CLOB) - Encounter status code
- `STATUS_TEXT` (CLOB) - Encounter status text
- `CLASS_CODE` (CLOB) - Encounter class code
- `CLASS_TEXT` (CLOB) - Encounter class text
- `PRIORITY_CODE` (CLOB) - Priority code
- `PRIORITY_TEXT` (CLOB) - Priority text
- `CLINICAL_SPECIALITY_CODE` (CLOB) - Clinical specialty code
- `CLINICAL_SPECIALITY_TEXT` (CLOB) - Clinical specialty text
- `EPJI_CREATED_DATETIME` (NUMBER(19)) - EPJI created datetime
- `EPJI_MODIFICATION_DATETIME` (NUMBER(19)) - EPJI modification datetime
- `IFDB_MODIFICATION_DATETIME` (NUMBER(19)) - IFDB modification datetime
- `BED` (CLOB) - Bed information
- `TEAM` (CLOB) - Team information
- `BROUGHT_IN_BY` (CLOB) - Brought in by
- `SERVICE_PROVIDER_CODE` (CLOB) - Service provider code
- `SERVICE_PROVIDER_NAME` (CLOB) - Service provider name
- `SERVICE_PROVIDER_ORGCODE` (CLOB) - Service provider organization code
- `SERVICE_PROVIDER_SORCODE` (NUMBER(19)) - Service provider SOR code
- `PERIOD_START` (CLOB) - Encounter start time
- `PERIOD_END` (CLOB) - Encounter end time
- `WARD_CODE` (CLOB) - Ward code
- `WARD_NAME` (CLOB) - Ward name
- `WARD_ORGCODE` (CLOB) - Ward organization code
- `WARD_SORCODE` (CLOB) - Ward SOR code
- `MUNICIPALITY_CARE` (CLOB) - Municipality care
- `EXPECTED_DISCHARGE` (CLOB) - Expected discharge
- `DELETED` (NUMBER(19)) - Deleted flag
- `REFERRAL_ID` (CLOB) - Referral ID
- `ROOM` (CLOB) - Room
- `TREATMENT_START` (CLOB) - Treatment start

**Constraints**:
- Primary key: PK_ENCOUNTER on OBJECT_ID
- Foreign key: FK_ENCOUNTER_EOC to EPISODE_OF_CARE (EPISODE_OF_CARE_ID)
- Foreign key: FK_ENCOUNTER_PT to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)

#### EPISODE_OF_CARE
**Primary Key**: OBJECT_ID (PK_EOC)
**Purpose**: Care episodes spanning multiple encounters

**Columns**:
- `OBJECT_ID` (NUMBER(19)) - Primary key
- `EPJI_MODIFICATION_DATETIME` (NUMBER(19)) - EPJI modification datetime
- `IFDB_MODIFICATION_DATETIME` (NUMBER(19)) - IFDB modification datetime
- `STATUS` (CLOB) - Episode status
- `MANAGING_ORGANIZATION_CODE` (CLOB) - Managing organization code
- `MANAGING_ORGANIZATION_NAME` (CLOB) - Managing organization name
- `MANAGING_ORGANIZATION_ORGCODE` (CLOB) - Managing organization org code
- `PERIOD_START` (CLOB) - Episode start time
- `PERIOD_END` (CLOB) - Episode end time
- `DISCHARGE_DISPOSITION_CODE` (CLOB) - Discharge disposition code
- `DISCHARGE_DISPOSITION_TEXT` (CLOB) - Discharge disposition text
- `HEALTHISSUE_ID` (NUMBER(19)) - Health issue reference
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `CARE_MANAGER_ID` (CLOB) - Care manager ID
- `CARE_MANAGER_NAME` (CLOB) - Care manager name
- `EPJI_CREATED_DATETIME` (NUMBER(19)) - EPJI created datetime
- `DELETED` (NUMBER(19)) - Deleted flag
- `MANAGING_ORGANIZATION_SORCODE` (NUMBER(19)) - Managing organization SOR code
- `DISCHARGE_TO_CODE` (CLOB) - Discharge to code
- `DISCHARGE_TO_TEXT` (CLOB) - Discharge to text
- `PREVIOUS_EOC_ID` (CLOB) - Previous episode of care ID
- `PREVIOUS_EOC_REF_TYPE_CODE` (CLOB) - Previous EOC reference type code
- `PREVIOUS_EOC_REF_TYPE_TEXT` (CLOB) - Previous EOC reference type text
- `GROUP` (CLOB) - Group

**Constraints**:
- Primary key: PK_EOC on OBJECT_ID
- Foreign key: FK_EOC_HI to HEALTHISSUE (HEALTHISSUE_ID)
- Foreign key: FK_EOC_PATIENT to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)

### Clinical Activities

#### AKTIVITETSUDFOERELSE
**Primary Key**: OBJEKT_ID (PK_AKTUDF)
**Purpose**: Clinical activity executions

**Columns**:
- `OBJEKT_ID` (NUMBER(19)) - Primary key
- `MODTIDSPUNKT` (CLOB) - Receipt timestamp
- `SLETTET` (NUMBER(19)) - Deleted flag
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `AKTIVITET_ID` (NUMBER(19)) - Activity type reference
- `OPRET_PERSID` (CLOB) - Created by person ID
- `OPRET_PERSNAVN` (CLOB) - Created by person name
- `OPRET_BRUGERROLLE` (CLOB) - Created by user role
- `OPRET_ADMENHED_KODE` (CLOB) - Created by department code
- `OPRET_ADMENHED_NAVN` (CLOB) - Created by department name
- `OPRET_ADMENHED_ORGKODE` (CLOB) - Created by department org code
- `OPRET_TIDSPUNKT` (CLOB) - Created timestamp
- `AENDRET_PERSID` (CLOB) - Modified by person ID
- `AENDRET_PERSNAVN` (CLOB) - Modified by person name
- `AENDRET_BRUGERROLLE` (CLOB) - Modified by user role
- `AENDRET_ADMENHED_KODE` (CLOB) - Modified by department code
- `AENDRET_ADMENHED_NAVN` (CLOB) - Modified by department name
- `AENDRET_ADMENHED_ORGKODE` (CLOB) - Modified by department org code
- `AENDRET_TIDSPUNKT` (CLOB) - Modified timestamp
- `UDFOERT_PERSID` (CLOB) - Executed by person ID
- `UDFOERT_PERSNAVN` (CLOB) - Executed by person name
- `UDFOERT_BRUGERROLLE` (CLOB) - Executed by user role
- `UDFOERT_ADMENHED_KODE` (CLOB) - Executed by department code
- `UDFOERT_ADMENHED_NAVN` (CLOB) - Executed by department name
- `UDFOERT_ADMENHED_ORGKODE` (CLOB) - Executed by department org code
- `UDFOERT_TIDSPUNKT` (CLOB) - Executed timestamp
- `FORTR_NIVEAU` (CLOB) - Trust level
- `FORTR_GRUPPE_ID` (CLOB) - Trust group ID
- `FORTR_GRUPPE_NAVN` (CLOB) - Trust group name
- `KLADDE` (NUMBER(19)) - Draft flag
- `OPDATERET_TID` (NUMBER(19)) - Updated timestamp

**Constraints**:
- Primary key: PK_AKTUDF on OBJEKT_ID
- Foreign key: FK_AKTUDF_PTOFF to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)

#### AKTIVITETSRESULTAT
**Primary Key**: OBJEKT_ID (PK_AKTRES)
**Purpose**: Results of clinical activities (contains XML data)

**Columns**:
- `OBJEKT_ID` (NUMBER(19)) - Primary key
- `MODTIDSPUNKT` (CLOB) - Receipt timestamp
- `SLETTET` (NUMBER(19)) - Deleted flag
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `UDFOERELSE_ID` (NUMBER(19)) - Activity execution reference
- `AKTIVITETTYPE_KODE` (CLOB) - Activity type code
- `AKTIVITETTYPE_KODETEKST` (CLOB) - Activity type code text
- `RESULTATTYPE_KODE` (CLOB) - Result type code
- `RESULTATTYPE_KODETEKST` (CLOB) - Result type code text
- `OVERSKRIFT` (CLOB) - Result title
- `HIERARKISK_PLACERING` (CLOB) - Hierarchical placement
- `DOMAENENAVN` (CLOB) - Domain name
- `MODELLERINGSNAVN` (CLOB) - Modeling name
- `MODELLERINGSVERSION` (NUMBER(19)) - Modeling version
- `XML` (CLOB) - Complex clinical data in XML format
- `TILSTEDEVAERELSE_ID` (CLOB) - Presence ID
- `UDFOERT_PERSID` (CLOB) - Executed by person ID
- `UDFOERT_PERSNAVN` (CLOB) - Executed by person name
- `UDFOERT_BRUGERROLLE` (CLOB) - Executed by user role
- `UDFOERT_ADMENHED_KODE` (CLOB) - Executed by department code
- `UDFOERT_ADMENHED_NAVN` (CLOB) - Executed by department name
- `UDFOERT_ADMENHED_ORGKODE` (CLOB) - Executed by department org code
- `UDFOERT_TIDSPUNKT` (CLOB) - Executed timestamp
- `KLADDE` (NUMBER(19)) - Draft flag
- `OPDATERET_TID` (NUMBER(19)) - Updated timestamp
- `ENCOUNTER_ID` (CLOB) - Encounter reference
- `EPISODE_OF_CARE_ID` (CLOB) - Episode of care reference
- `PERIOD_START` (CLOB) - Period start
- `PERIOD_END` (CLOB) - Period end
- `UDFOERT_ADMENHED_SORCODE` (NUMBER(19)) - Executed by department SOR code
- `LPR3_IGNORED` (CLOB) - LPR3 ignored flag
- `UDFOERT_BRUGERROLLE_NAME` (CLOB) - Executed by user role name
- `RESULT_TYPE` (CLOB) - Result type
- `RESULT_MOID` (CLOB) - Result MOID

**Constraints**:
- Primary key: PK_AKTRES on OBJEKT_ID
- Foreign key: FK_AKTRES_AKTUDF to AKTIVITETSUDFOERELSE (UDFOERELSE_ID)

### Medication Management

#### MEDICINORDINATION
**Primary Key**: OBJEKT_ID (PK_MEDORD)
**Purpose**: Medication orders and prescriptions

**Columns**:
- `OBJEKT_ID` (NUMBER(19)) - Primary key
- `MODTIDSPUNKT` (CLOB) - Receipt timestamp
- `SLETTET` (NUMBER(19)) - Deleted flag
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `ADMVEJ_KODE` (CLOB) - Administration route code
- `ADMVEJ_KODETEKST` (CLOB) - Administration route code text
- `ORD_TYPE_KODE` (CLOB) - Order type code
- `ORD_TYPE_KODETEKST` (CLOB) - Order type code text
- `DRUGID` (NUMBER(19)) - Drug identifier
- `PRAEP_NAVN` (CLOB) - Drug name
- `FORM` (CLOB) - Drug form
- `ENHED` (CLOB) - Unit
- `STYRKE` (CLOB) - Strength
- `LMB_BESKRIVELSE` (CLOB) - LMB description
- `ORDINATION_INDIKATION` (CLOB) - Order indication
- `START_PERSID` (CLOB) - Started by person ID
- `START_PERSNAVN` (CLOB) - Started by person name
- `START_BRUGERROLLE` (CLOB) - Started by user role
- `START_ADMENHED_KODE` (CLOB) - Started by department code
- `START_ADMENHED_NAVN` (CLOB) - Started by department name
- `START_ADMENHED_ORGKODE` (CLOB) - Started by department org code
- `START_TIDSPUNKT` (CLOB) - Started timestamp
- `SEP_PERSID` (CLOB) - Separated by person ID
- `SEP_PERSNAVN` (CLOB) - Separated by person name
- `SEP_BRUGERROLLE` (CLOB) - Separated by user role
- `SEP_ADMENHED_KODE` (CLOB) - Separated by department code
- `SEP_ADMENHED_NAVN` (CLOB) - Separated by department name
- `SEP_ADMENHED_ORGKODE` (CLOB) - Separated by department org code
- `SEP_TIDSPUNKT` (CLOB) - Separated timestamp
- `SEP_REASON_KODE` (CLOB) - Separation reason code
- `SEP_REASON_KODETEKST` (CLOB) - Separation reason code text
- `GODK_PERSID` (CLOB) - Approved by person ID
- `GODK_PERSNAVN` (CLOB) - Approved by person name
- `GODK_BRUGERROLLE` (CLOB) - Approved by user role
- `GODK_ADMENHED_KODE` (CLOB) - Approved by department code
- `GODK_ADMENHED_NAVN` (CLOB) - Approved by department name
- `GODK_ADMENHED_ORGKODE` (CLOB) - Approved by department org code
- `GODK_TIDSPUNKT` (CLOB) - Approved timestamp
- `HISTORIK` (CLOB) - History
- `SENESTEORDINATIONMODIFIKATION` (CLOB) - Latest order modification
- `ORDER_REMARK` (CLOB) - Order remark
- `CAVE_OVERRULE_REASON` (CLOB) - Cave override reason
- `STANDARD_ORDER_KEY` (CLOB) - Standard order key
- `ATC_CODE` (CLOB) - ATC classification code
- `INDICATION_CODE` (CLOB) - Indication code
- `STANDARD_ORDER_PACKAGE_KEY` (CLOB) - Standard order package key
- `FORM_TEXT` (CLOB) - Form text
- `STRENGTH_UNIT_TEXT` (CLOB) - Strength unit text
- `STANDARD_ORDER_CALCULATION_VERSION` (CLOB) - Standard order calculation version

**Constraints**:
- Primary key: PK_MEDORD on OBJEKT_ID
- Foreign key: FK_MEDORD_PTOFF to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)

#### MEDICINADMINISTRATION
**Primary Key**: OBJEKT_ID (PK_MEDADM)
**Purpose**: Medication administration records

**Columns**:
- `OBJEKT_ID` (NUMBER(19)) - Primary key
- `MODTIDSPUNKT` (CLOB) - Receipt timestamp
- `SLETTET` (NUMBER(19)) - Deleted flag
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `REG_PERSID` (CLOB) - Registered by person ID
- `REG_PERSNAVN` (CLOB) - Registered by person name
- `REG_BRUGERROLLE` (CLOB) - Registered by user role
- `REG_ADMENHED_KODE` (CLOB) - Registered by department code
- `REG_ADMENHED_NAVN` (CLOB) - Registered by department name
- `REG_ADMENHED_ORGKODE` (CLOB) - Registered by department org code
- `REG_TIDSPUNKT` (CLOB) - Registered timestamp
- `ADM_TIDSPUNKT` (CLOB) - Administration timestamp
- `DRUGID` (NUMBER(19)) - Drug identifier
- `PRAEP_NAVN` (CLOB) - Drug name
- `LMB_BESKRIVELSE` (CLOB) - LMB description
- `AFVIGELSE` (NUMBER(19)) - Deviation flag
- `TYPE_KODE` (CLOB) - Type code
- `TYPE_KODETEKST` (CLOB) - Type code text
- `STATUSTYPE_KODE` (CLOB) - Status type code
- `STATUSTYPE_KODETEKST` (CLOB) - Status type code text
- `DOSIS` (CLOB) - Dose
- `DOSIS_ENHED_KODE` (CLOB) - Dose unit code
- `DOSIS_ENHED_KODETEKST` (CLOB) - Dose unit code text
- `INDLOEBET_VOLUMEN` (CLOB) - Infused volume
- `ADMVEJ_KODE` (CLOB) - Administration route code
- `ADMVEJ_KODETEKST` (CLOB) - Administration route code text
- `AFVIGELSE_BEGRUND` (CLOB) - Deviation reason
- `ORDINATION_ID` (NUMBER(19)) - Related order reference
- `ORDINATION_START` (CLOB) - Order start
- `ORDINATION_INDIKATION` (CLOB) - Order indication
- `INDICATION_CODE` (CLOB) - Indication code
- `CONTACT_ID` (CLOB) - Contact ID
- `ENCOUNTER_ID` (CLOB) - Encounter reference
- `EPISODE_OF_CARE_ID` (CLOB) - Episode of care reference
- `BATCHNUMRE` (CLOB) - Batch numbers
- `INDLOEBS_HASTIGHED` (CLOB) - Infusion rate
- `INDLOEBS_HASTIGHED_KODE` (CLOB) - Infusion rate code
- `INDLOEBS_HASTIGHED_KODETEKST` (CLOB) - Infusion rate code text
- `PACKAGE_NUMBER` (CLOB) - Package number
- `IS_PACKAGE_NUMBER_SCANNED` (NUMBER(19)) - Is package number scanned
- `PACKAGE_PRICE` (CLOB) - Package price
- `PACKAGE_SIZE` (CLOB) - Package size
- `PACKAGE_UNIT` (CLOB) - Package unit
- `IFDB_MODIFICATION_DATETIME` (NUMBER(19)) - IFDB modification datetime
- `REG_ADMENHED_SORCODE` (NUMBER(19)) - Registered by department SOR code
- `FORM` (CLOB) - Form
- `STRENGTH` (CLOB) - Strength
- `UNIT` (CLOB) - Unit
- `ATC_CODE` (CLOB) - ATC code
- `PLANNED_ADM_DATETIME` (CLOB) - Planned administration datetime
- `ARE_BAGS_SCANNED` (CLOB) - Are bags scanned
- `SCANNED_BAG_IDS` (CLOB) - Scanned bag IDs
- `OWN_MEDICATION` (CLOB) - Own medication
- `DRUG_PRICE_LIST_NAME` (CLOB) - Drug price list name
- `PACKAGE_PRICE_LIST_UNIT` (CLOB) - Package price list unit
- `PACKAGE_PURCHASE_PRICE` (CLOB) - Package purchase price
- `PACKAGE_PURCHASE_PRICE_DATE` (CLOB) - Package purchase price date
- `FORM_TEXT` (CLOB) - Form text
- `STRENGTH_UNIT_TEXT` (CLOB) - Strength unit text

**Constraints**:
- Primary key: PK_MEDADM on OBJEKT_ID
- Foreign key: FK_MEDADM_PTOFF to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)

### Diagnosis and Conditions

#### DIAGNOSE
**Primary Key**: OBJEKT_ID (PK_KP_DIAGNOSE)
**Purpose**: Patient diagnoses

**Columns**:
- `OBJEKT_ID` (NUMBER(19)) - Primary key
- `MODTIDSPUNKT` (CLOB) - Receipt timestamp
- `SLETTET` (NUMBER(19)) - Deleted flag
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `BESLUT_PERSID` (CLOB) - Decided by person ID
- `BESLUT_PERSNAVN` (CLOB) - Decided by person name
- `BESLUT_BRUGERROLLE` (CLOB) - Decided by user role
- `BESLUT_ADMENHED_KODE` (CLOB) - Decided by department code
- `BESLUT_ADMENHED_NAVN` (CLOB) - Decided by department name
- `BESLUT_ADMENHED_ORGKODE` (CLOB) - Decided by department org code
- `BESLUT_TIDSPUNKT` (CLOB) - Decided timestamp
- `AFSLUT_PERSID` (CLOB) - Concluded by person ID
- `AFSLUT_PERSNAVN` (CLOB) - Concluded by person name
- `AFSLUT_BRUGERROLLE` (CLOB) - Concluded by user role
- `AFSLUT_ADMENHED_KODE` (CLOB) - Concluded by department code
- `AFSLUT_ADMENHED_NAVN` (CLOB) - Concluded by department name
- `AFSLUT_ADMENHED_ORGKODE` (CLOB) - Concluded by department org code
- `AFSLUT_TIDSPUNKT` (CLOB) - Concluded timestamp
- `OPRET_PERSID` (CLOB) - Created by person ID
- `OPRET_PERSNAVN` (CLOB) - Created by person name
- `OPRET_BRUGERROLLE` (CLOB) - Created by user role
- `OPRET_ADMENHED_KODE` (CLOB) - Created by department code
- `OPRET_ADMENHED_NAVN` (CLOB) - Created by department name
- `OPRET_ADMENHED_ORGKODE` (CLOB) - Created by department org code
- `OPRET_TIDSPUNKT` (CLOB) - Created timestamp
- `DIAGNOSE_KODE` (CLOB) - Diagnosis code
- `DIAGNOSE_KODETEKST` (CLOB) - Diagnosis code text

**Constraints**:
- Primary key: PK_KP_DIAGNOSE on OBJEKT_ID
- Foreign key: FK_DIAGNOSE_PT to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)

#### HEALTHISSUE
**Primary Key**: OBJECT_ID (PK_HI)
**Purpose**: Health issues and problems

**Columns**:
- `OBJECT_ID` (NUMBER(19)) - Primary key
- `PATIENT_ID` (NUMBER(19)) - Patient reference
- `HEALTHISSUE_CODE` (CLOB) - Health issue code
- `HEALTHISSUE_TEXT` (CLOB) - Health issue text
- `CODE_ELABORATION` (CLOB) - Code elaboration
- `CLINICAL_STATUS` (CLOB) - Clinical status
- `PERIOD_START` (CLOB) - Period start
- `PERIOD_END` (CLOB) - Period end
- `HEALTHISSUE_MANAGER_PERSID` (CLOB) - Health issue manager person ID
- `HEALTHISSUE_MANAGER_PERSNAME` (CLOB) - Health issue manager person name
- `DELETED` (NUMBER(19)) - Deleted flag
- `EPJI_CREATED_DATETIME` (NUMBER(19)) - EPJI created datetime
- `EPJI_MODIFICATION_DATETIME` (NUMBER(19)) - EPJI modification datetime
- `IFDB_MODIFICATION_DATETIME` (NUMBER(19)) - IFDB modification datetime
- `HEALTHISSUE_EXTERNAL_CODE` (CLOB) - Health issue external code

**Constraints**:
- Primary key: PK_HI on OBJECT_ID
- Foreign key: FK_HI_PATIENT to PATIENT_OPLYSNINGER_COLUMNA (PATIENT_ID)


## Key Relationships Summary

### Patient-Centric Relationships
Most clinical tables reference `PATIENT_OPLYSNINGER_COLUMNA.PATIENT_ID`:
- ENCOUNTER → PATIENT_OPLYSNINGER_COLUMNA
- AKTIVITETSUDFOERELSE → PATIENT_OPLYSNINGER_COLUMNA
- MEDICINORDINATION → PATIENT_OPLYSNINGER_COLUMNA
- DIAGNOSE → PATIENT_OPLYSNINGER_COLUMNA
- HEALTHISSUE → PATIENT_OPLYSNINGER_COLUMNA

### Encounter-Based Relationships
- ENCOUNTER → EPISODE_OF_CARE (EPISODE_OF_CARE_ID)
- AKTIVITETSRESULTAT → AKTIVITETSUDFOERELSE (UDFOERELSE_ID)
- ENCOUNTER_BED_SPACE_HISTORY → ENCOUNTER (OBJECT_ID)

### Medication Relationships
- MEDICATIONORDERPLAN → MEDICINORDINATION (ORDINATION_ID)
- MEDICATIONORDERPLANDOSE → MEDICATIONORDERPLAN (PLAN_ID)
- MEDICINADMINISTRATION_MIXTURE_INGREDIENT → MEDICINADMINISTRATION (MEDICINADMINISTRATION_OBJECT_ID)
- MEDICINORDINATION_MIXTURE_INGREDIENT → MEDICINORDINATION (MEDICINORDINATION_OBJECT_ID)

### Diagnosis Relationships
- DIAGNOSE_TILLAEGSKODE → DIAGNOSE (DIAGNOSE_ID)
- PAS_DIAGNOSE_ENCOUNTER_REF → ENCOUNTER (ENCOUNTER_ID)
- PAS_DIAGNOSE_HENVISNING_REF → HENVISNING (HENVISNING_ID)

## Data Types and Patterns

### Common Data Types
- **NUMBER(19)**: Primary keys and numeric identifiers
- **CLOB**: Text fields, codes, and descriptions
- **NUMBER**: Timestamps (Unix epoch format)

### Naming Conventions
- **Primary Keys**: Usually `OBJECT_ID` or `OBJEKT_ID`
- **Patient References**: `PATIENT_ID`
- **Timestamps**: `*_TIDSPUNKT` or `*_DATETIME`
- **Codes**: `*_KODE` (code) and `*_KODETEKST` (code text)
- **Person References**: `*_PERSID` (person ID) and `*_PERSNAVN` (person name)
- **Department References**: `*_ADMENHED_KODE` (department code) and `*_ADMENHED_NAVN` (department name)

### Audit Fields
Most tables include audit fields:
- `MODTIDSPUNKT`: last modification time in EPJ-I (EPJ clock)
- `SLETTET`: Deleted flag
- `OPRET_TIDSPUNKT`: Creation timestamp
- `OPDATERET_TID`: time IFDB transferred the row (IFDB clock)
- `EPJI_MODIFICATION_DATETIME`: last modification time in EPJ-I (EPJ clock)
- `IFDB_MODIFICATION_DATETIME`: time IFDB transferred the row (IFDB clock)

The two clocks differ. A cycle starts at 05, 09, 13 or 17; `JOB_LOG.SLUTTID` is the cut on the EPJ clock up to which the cycle loads (a few seconds past the hour), and IFDB stamps the transferred rows from the cut until the cycle ends, minutes later. A table with an IFDB-clock column uses it as the DFM delta column, bounded by the transfer ceiling; a table with only an EPJ-clock column (`DIAGNOSE`, `SENGEENHED`) uses the EPJ ceiling. See *Two ceilings* in `docs/implementation/dfm_operations_guide.md`.

## XML Data Integration

Several tables contain XML data in CLOB fields:
- **AKTIVITETSRESULTAT.XML**: Complex clinical activity results
- **ORDINATIONSBEGRUNDELSE.XML**: Order justification details

This XML data requires special processing to extract structured information for FHIR transformation.
