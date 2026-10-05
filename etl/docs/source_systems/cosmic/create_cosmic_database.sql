-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- DDL Export for cosmic
-- Exported: 2026-05-05T13:34:04.822686
-- Database: CA
-- Schema: ca
-- Tables: 26

USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'CA')
BEGIN
    CREATE DATABASE [CA];
END
GO

USE [CA];
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ca')
    EXEC('CREATE SCHEMA [ca]');
GO

-- Drop existing tables (in reverse dependency order)
IF OBJECT_ID('ca.FV_WARNING', 'U') IS NOT NULL DROP TABLE [ca].[FV_WARNING];
IF OBJECT_ID('ca.FV_REFERRAL', 'U') IS NOT NULL DROP TABLE [ca].[FV_REFERRAL];
IF OBJECT_ID('ca.FV_PRESCRIPTION', 'U') IS NOT NULL DROP TABLE [ca].[FV_PRESCRIPTION];
IF OBJECT_ID('ca.FV_MEDICATION', 'U') IS NOT NULL DROP TABLE [ca].[FV_MEDICATION];
IF OBJECT_ID('ca.FV_JOURNALTABLE', 'U') IS NOT NULL DROP TABLE [ca].[FV_JOURNALTABLE];
IF OBJECT_ID('ca.FV_JOURNALNOTE', 'U') IS NOT NULL DROP TABLE [ca].[FV_JOURNALNOTE];
IF OBJECT_ID('ca.FV_CONTACT', 'U') IS NOT NULL DROP TABLE [ca].[FV_CONTACT];
IF OBJECT_ID('ca.FV_COMMITMENT', 'U') IS NOT NULL DROP TABLE [ca].[FV_COMMITMENT];
IF OBJECT_ID('ca.FEV_JOURNALNOTE_ADMINISTRATION', 'U') IS NOT NULL DROP TABLE [ca].[FEV_JOURNALNOTE_ADMINISTRATION];
IF OBJECT_ID('ca.FCV_WARNING_ATTRIBUTE', 'U') IS NOT NULL DROP TABLE [ca].[FCV_WARNING_ATTRIBUTE];
IF OBJECT_ID('ca.FCV_MEDICATION_DOSAGE', 'U') IS NOT NULL DROP TABLE [ca].[FCV_MEDICATION_DOSAGE];
IF OBJECT_ID('ca.FCV_MEDICATION_ADMIN', 'U') IS NOT NULL DROP TABLE [ca].[FCV_MEDICATION_ADMIN];
IF OBJECT_ID('ca.FCV_JOURNALVALUE_CODE', 'U') IS NOT NULL DROP TABLE [ca].[FCV_JOURNALVALUE_CODE];
IF OBJECT_ID('ca.FCV_JOURNALVALUE', 'U') IS NOT NULL DROP TABLE [ca].[FCV_JOURNALVALUE];
IF OBJECT_ID('ca.FCV_JOURNALTABLE_VALUE', 'U') IS NOT NULL DROP TABLE [ca].[FCV_JOURNALTABLE_VALUE];
IF OBJECT_ID('ca.FCV_JOURNALTABLE_COLUMN', 'U') IS NOT NULL DROP TABLE [ca].[FCV_JOURNALTABLE_COLUMN];
IF OBJECT_ID('ca.DV_USER', 'U') IS NOT NULL DROP TABLE [ca].[DV_USER];
IF OBJECT_ID('ca.DV_STAFF', 'U') IS NOT NULL DROP TABLE [ca].[DV_STAFF];
IF OBJECT_ID('ca.DV_PATIENT', 'U') IS NOT NULL DROP TABLE [ca].[DV_PATIENT];
IF OBJECT_ID('ca.DV_JOURNALFORM', 'U') IS NOT NULL DROP TABLE [ca].[DV_JOURNALFORM];
IF OBJECT_ID('ca.DV_GROUP', 'U') IS NOT NULL DROP TABLE [ca].[DV_GROUP];
IF OBJECT_ID('ca.DV_DRUG', 'U') IS NOT NULL DROP TABLE [ca].[DV_DRUG];
IF OBJECT_ID('ca.DV_ATC', 'U') IS NOT NULL DROP TABLE [ca].[DV_ATC];
IF OBJECT_ID('ca.DCV_STAFF_IDENTIFIER', 'U') IS NOT NULL DROP TABLE [ca].[DCV_STAFF_IDENTIFIER];
IF OBJECT_ID('ca.CA_JOB_RESULT', 'U') IS NOT NULL DROP TABLE [ca].[CA_JOB_RESULT];
IF OBJECT_ID('ca.CA_JOB', 'U') IS NOT NULL DROP TABLE [ca].[CA_JOB];
GO

-- Table: CA_JOB
CREATE TABLE [ca].[CA_JOB] (
    [id] BIGINT NOT NULL,
    [input_filename] VARCHAR(50) NULL,
    [Patient_ID] VARCHAR(20) NULL,
    [keep_duplicates] TINYINT NULL,
    [timestmp] BIGINT NULL
);
GO

-- Table: CA_JOB_RESULT
CREATE TABLE [ca].[CA_JOB_RESULT] (
    [id] BIGINT NOT NULL,
    [job_id] BIGINT NOT NULL,
    [table_name] VARCHAR(50) NULL,
    [id_count] INT NULL,
    [error_count] INT NULL,
    [start_time] DATETIME NULL,
    [end_time] DATETIME NULL
);
GO

-- Table: DCV_STAFF_IDENTIFIER
CREATE TABLE [ca].[DCV_STAFF_IDENTIFIER] (
    [primaryKey] BIGINT NOT NULL,
    [StaffIdentifier_ID] INT NULL,
    [Staff_ID] INT NULL,
    [Type] INT NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Value] VARCHAR(200) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: DV_ATC
CREATE TABLE [ca].[DV_ATC] (
    [primaryKey] BIGINT NOT NULL,
    [ATC] VARCHAR(200) NULL,
    [atcKey] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [Created] DATETIME NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Description] VARCHAR(200) NULL,
    [Hier1_ATC] CHAR(1) NULL,
    [Hier1_Description] VARCHAR(200) NULL,
    [Hier2_ATC] CHAR(3) NULL,
    [Hier2_Description] VARCHAR(200) NULL,
    [Hier3_ATC] CHAR(4) NULL,
    [Hier3_Description] VARCHAR(200) NULL,
    [Hier4_ATC] CHAR(5) NULL,
    [Hier4_Description] VARCHAR(200) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: DV_DRUG
CREATE TABLE [ca].[DV_DRUG] (
    [primaryKey] BIGINT NOT NULL,
    [Drug_ID] INT NULL,
    [Drug_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [active] TINYINT NULL,
    [Type] INT NULL,
    [Drug] VARCHAR(50) NULL,
    [Description] VARCHAR(500) NULL,
    [ATC] VARCHAR(10) NULL,
    [Strength] FLOAT NULL,
    [StrengthUnit] VARCHAR(100) NULL,
    [OtherStrengthInfo] VARCHAR(100) NULL,
    [Form] VARCHAR(100) NULL,
    [ExternalProduct_ID] VARCHAR(50) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: DV_GROUP
CREATE TABLE [ca].[DV_GROUP] (
    [primaryKey] BIGINT NOT NULL,
    [Group_ID] INT NULL,
    [Group_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [active] TINYINT NULL,
    [Group] VARCHAR(50) NULL,
    [Description] VARCHAR(200) NULL,
    [GroupSize] SMALLINT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: DV_JOURNALFORM
CREATE TABLE [ca].[DV_JOURNALFORM] (
    [primaryKey] BIGINT NOT NULL,
    [JournalForm_ID] INT NULL,
    [JournalForm_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [active] TINYINT NULL,
    [Type] TINYINT NULL,
    [JournalForm] VARCHAR(100) NULL,
    [Description] VARCHAR(200) NULL,
    [NoteType] VARCHAR(100) NULL,
    [FormTree_ID] INT NULL,
    [HCM_FormSubType] TINYINT NULL,
    [Report_ID] INT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: DV_PATIENT
CREATE TABLE [ca].[DV_PATIENT] (
    [primaryKey] BIGINT NOT NULL,
    [Patient_ID] INT NULL,
    [Patient_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [active] TINYINT NULL,
    [Id] VARCHAR(50) NULL,
    [name] VARCHAR(202) NULL,
    [Names] VARCHAR(200) NULL,
    [GivenName] VARCHAR(100) NULL,
    [MiddleName] VARCHAR(100) NULL,
    [FamilyName] VARCHAR(100) NULL,
    [Sex] CHAR(1) NULL,
    [Birth] DATETIME2 NULL,
    [Deceased] TINYINT NULL,
    [Death] DATETIME NULL,
    [Country] VARCHAR(2) NULL,
    [Region] VARCHAR(50) NULL,
    [City] VARCHAR(50) NULL,
    [District] VARCHAR(50) NULL,
    [Confidential] TINYINT NULL,
    [LLKKFF] CHAR(6) NULL,
    [Adopted] SMALLINT NULL,
    [CountryOfBirth] VARCHAR(200) NULL,
    [DayCare] VARCHAR(200) NULL,
    [DayCarePhone] VARCHAR(200) NULL,
    [Geo_ID] INT NULL,
    [ConnectedPatient_ID] INT NULL,
    [HomeHealthCareUnit_ID] INT NULL,
    [HomeCareUnit_ID] INT NULL,
    [SpecialAccomodationUnit_ID] INT NULL,
    [SocialServicesUnit_ID] INT NULL,
    [AssistanceUnit_ID] INT NULL,
    [OrdinaryAccomodationUnit_ID] INT NULL,
    [DayCareUnit_ID] INT NULL,
    [LSS_Unit_ID] INT NULL,
    [GP_Unit_ID] INT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: DV_STAFF
CREATE TABLE [ca].[DV_STAFF] (
    [primaryKey] BIGINT NOT NULL,
    [Staff_ID] INT NULL,
    [Staff_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [active] TINYINT NULL,
    [Staff] VARCHAR(100) NULL,
    [User_ID] VARCHAR(50) NULL,
    [Profession_ID] INT NULL,
    [SubProfession_ID] INT NULL,
    [Title] VARCHAR(50) NULL,
    [ShortName] VARCHAR(50) NULL,
    [UserActive] SMALLINT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: DV_USER
CREATE TABLE [ca].[DV_USER] (
    [primaryKey] BIGINT NOT NULL,
    [User_ID] VARCHAR(50) NULL,
    [User_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Origin_Created] DATETIME NULL,
    [active] TINYINT NULL,
    [Status] INT NULL,
    [Description] VARCHAR(200) NULL,
    [Staff_ID] INT NULL,
    [Timestamp] DATETIME NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FCV_JOURNALTABLE_COLUMN
CREATE TABLE [ca].[FCV_JOURNALTABLE_COLUMN] (
    [primaryKey] BIGINT NOT NULL,
    [JournalTableColumn_ID] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [Created] DATETIME NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Patient_ID] INT NULL,
    [active] TINYINT NULL,
    [JournalTable_ID] INT NULL,
    [Header_Type] TINYINT NULL,
    [Header_Titel] VARCHAR(100) NULL,
    [Header_Datetime] DATETIME NULL,
    [Contact_Datetime] DATETIME NULL,
    [Contact_ID] BIGINT NULL,
    [Unit_ID] INT NULL,
    [JournalNote_ID] BIGINT NULL,
    [DeleteReason] VARCHAR(200) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FCV_JOURNALTABLE_VALUE
CREATE TABLE [ca].[FCV_JOURNALTABLE_VALUE] (
    [primaryKey] BIGINT NOT NULL,
    [JournalTableValue_ID] INT NULL,
    [JournalTableValue_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [active] TINYINT NULL,
    [JournalTableColumn_ID] INT NULL,
    [JournalTable_ID] INT NULL,
    [Type] TINYINT NULL,
    [Keyword] VARCHAR(100) NULL,
    [Keyword_ID] INT NULL,
    [JournalRecord_ID] BIGINT NULL,
    [Analysis_ID] INT NULL,
    [ChemistryResult_ID] BIGINT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FCV_JOURNALVALUE
CREATE TABLE [ca].[FCV_JOURNALVALUE] (
    [primaryKey] BIGINT NOT NULL,
    [JournalValue_ID] BIGINT NULL,
    [JournalValue_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [active] TINYINT NULL,
    [Type] INT NULL,
    [Status] INT NULL,
    [Parent_ID] BIGINT NULL,
    [JournalRecord_ID] BIGINT NULL,
    [JournalNote_ID] BIGINT NULL,
    [JournalForm_ID] INT NULL,
    [HCM_Relation_ID] BIGINT NULL,
    [HCM_Object_ID] BIGINT NULL,
    [HCM_Type] SMALLINT NULL,
    [Keyword_ID] INT NULL,
    [ValueType] SMALLINT NULL,
    [Value] VARCHAR(MAX) NULL,
    [FixedValue_ID] INT NULL,
    [PDF] INT NULL,
    [RefRangeMin] VARCHAR(20) NULL,
    [RefRangeMax] VARCHAR(20) NULL,
    [Term_Key] INT NULL,
    [Concept_ID] INT NULL,
    [VitalParameter_ID] INT NULL,
    [HierKeywords] VARCHAR(2000) NULL,
    [HierLevel] TINYINT NULL,
    [Parent1_Keyword] VARCHAR(100) NULL,
    [Parent1_Keyword_ID] INT NULL,
    [Parent1_Value] VARCHAR(MAX) NULL,
    [Parent2_JournalValue_ID] BIGINT NULL,
    [Parent2_Keyword] VARCHAR(100) NULL,
    [Parent2_Keyword_ID] INT NULL,
    [Parent2_Value] VARCHAR(MAX) NULL,
    [Parent3_JournalValue_ID] BIGINT NULL,
    [Parent3_Keyword] VARCHAR(100) NULL,
    [Parent3_Keyword_ID] INT NULL,
    [Parent3_Value] VARCHAR(MAX) NULL,
    [Parent4_JournalValue_ID] BIGINT NULL,
    [Parent4_Keyword] VARCHAR(100) NULL,
    [Parent4_Keyword_ID] INT NULL,
    [Parent4_Value] VARCHAR(MAX) NULL,
    [Parent5_JournalValue_ID] BIGINT NULL,
    [Parent5_Keyword] VARCHAR(100) NULL,
    [Parent5_Keyword_ID] INT NULL,
    [Parent5_Value] VARCHAR(MAX) NULL,
    [Parent6_JournalValue_ID] BIGINT NULL,
    [Parent6_Keyword] VARCHAR(100) NULL,
    [Parent6_Keyword_ID] INT NULL,
    [Parent6_Value] VARCHAR(MAX) NULL,
    [Parent7_JournalValue_ID] BIGINT NULL,
    [Parent7_Keyword] VARCHAR(100) NULL,
    [Parent7_Keyword_ID] INT NULL,
    [Parent7_Value] VARCHAR(MAX) NULL,
    [Parent8_JournalValue_ID] BIGINT NULL,
    [Parent8_Keyword] VARCHAR(100) NULL,
    [Parent8_Keyword_ID] INT NULL,
    [Parent8_Value] VARCHAR(MAX) NULL,
    [Parent9_JournalValue_ID] BIGINT NULL,
    [Parent9_Keyword] VARCHAR(100) NULL,
    [Parent9_Keyword_ID] INT NULL,
    [Parent9_Value] VARCHAR(MAX) NULL,
    [JournalNoteForm_ID] INT NULL,
    [IsDynamicForm] INT NULL,
    [IsAddedKeyword] INT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FCV_JOURNALVALUE_CODE
CREATE TABLE [ca].[FCV_JOURNALVALUE_CODE] (
    [primaryKey] BIGINT NOT NULL,
    [JournalValueCode_ID] INT NULL,
    [JournalValue_ID] BIGINT NULL,
    [JournalValue_Key] BIGINT NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Creator] VARCHAR(50) NULL,
    [Created] DATETIME NULL,
    [active] TINYINT NULL,
    [Status] INT NULL,
    [Type] VARCHAR(50) NULL,
    [Subtype] VARCHAR(50) NULL,
    [Patient_ID] INT NULL,
    [JournalNote_ID] BIGINT NULL,
    [Contact_ID] BIGINT NULL,
    [CodeTerminology] VARCHAR(100) NULL,
    [code] VARCHAR(50) NULL,
    [Description] VARCHAR(200) NULL,
    [Concept_ID] INT NULL,
    [SortOrder] SMALLINT NULL,
    [HierLevel] SMALLINT NULL,
    [ParentCode_ID] INT NULL,
    [ParentCodeTerminology] VARCHAR(100) NULL,
    [ActionDate] DATETIME NULL,
    [MainCode] TINYINT NULL,
    [Asterisk] TINYINT NULL,
    [Dagger] TINYINT NULL,
    [OuterReason] TINYINT NULL,
    [CauseOfDeath] TINYINT NULL,
    [Chronic] TINYINT NULL,
    [CodeOrder] INT NULL,
    [IsParent] TINYINT NULL,
    [Identifier] BIGINT NULL,
    [ParentIdentifier] BIGINT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FCV_MEDICATION_ADMIN
CREATE TABLE [ca].[FCV_MEDICATION_ADMIN] (
    [primaryKey] BIGINT NOT NULL,
    [MedicationAdminEvent_ID] BIGINT NULL,
    [MedicationAdminEvent_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Type] INT NULL,
    [active] TINYINT NULL,
    [MedicationAdmin_ID] BIGINT NULL,
    [Timestamp] DATETIME2 NULL,
    [Drug_ID] INT NULL,
    [Unit_ID] INT NULL,
    [Staff_ID] INT NULL,
    [Signer_ID] INT NULL,
    [Signer_Timestamp] DATETIME NULL,
    [Dose] FLOAT NULL,
    [DoseUnit] VARCHAR(200) NULL,
    [MaxDose] FLOAT NULL,
    [Comment] VARCHAR(200) NULL,
    [Reason] VARCHAR(300) NULL,
    [Amount] FLOAT NULL,
    [AmoutUnit] VARCHAR(200) NULL,
    [IsContinious] SMALLINT NULL,
    [Concentration] FLOAT NULL,
    [DurationValue] FLOAT NULL,
    [SpeedValue] FLOAT NULL,
    [SpeedUnit] VARCHAR(100) NULL,
    [Volume] FLOAT NULL,
    [VolumeUnit] VARCHAR(200) NULL,
    [ArchiveDate] DATETIME NOT NULL,
    [latest] TINYINT NULL
);
GO

-- Table: FCV_MEDICATION_DOSAGE
CREATE TABLE [ca].[FCV_MEDICATION_DOSAGE] (
    [id] BIGINT NOT NULL,
    [PrimaryKey] BIGINT NULL,
    [Dosage_ID] BIGINT NULL,
    [Dosage_Key] BIGINT NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Creator] VARCHAR(50) NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Type] SMALLINT NULL,
    [Subtype] SMALLINT NULL,
    [Medication_ID] BIGINT NULL,
    [Dose] FLOAT NULL,
    [Form] VARCHAR(MAX) NULL,
    [Volume] FLOAT NULL,
    [VolumeUnit] VARCHAR(20) NULL,
    [Iteration] INT NULL,
    [Duration] INT NULL,
    [MaxDose] FLOAT NULL,
    [MaxDoseType] VARCHAR(20) NULL,
    [Dosage_Start] DATETIME2 NULL,
    [Dosage_End] DATETIME2 NULL,
    [Interval] INT NULL,
    [IntervalType] VARCHAR(20) NULL,
    [Label] VARCHAR(20) NULL,
    [Time] VARCHAR(5) NULL,
    [DayOfWeek] TINYINT NULL,
    [Speed] FLOAT NULL,
    [SpeedUnit] VARCHAR(20) NULL,
    [MinSpeed] FLOAT NULL,
    [MaxSpeed] FLOAT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FCV_WARNING_ATTRIBUTE
CREATE TABLE [ca].[FCV_WARNING_ATTRIBUTE] (
    [primaryKey] BIGINT NOT NULL,
    [WarningAttribute_ID] BIGINT NULL,
    [WarningAttribute_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Type] INT NULL,
    [Warning_ID] BIGINT NULL,
    [Value] VARCHAR(200) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FEV_JOURNALNOTE_ADMINISTRATION
CREATE TABLE [ca].[FEV_JOURNALNOTE_ADMINISTRATION] (
    [primaryKey] BIGINT NOT NULL,
    [JournalNote_ID] BIGINT NULL,
    [JournalNote_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Origin_Created] DATETIME NULL,
    [Status] INT NULL,
    [Unit_ID] INT NULL,
    [CareProvider_ID] INT NULL,
    [CareProviderProfession_ID] INT NULL,
    [active] TINYINT NULL,
    [Author_ID] INT NULL,
    [Secretary_ID] INT NULL,
    [Signer_ID] INT NULL,
    [Signer_Timestamp] DATETIME NULL,
    [CounterSigner_ID] INT NULL,
    [CounterSigner_Timestamp] DATETIME NULL,
    [Attester_ID] INT NULL,
    [Attester_Timestamp] DATETIME NULL,
    [InvalidateReason] VARCHAR(100) NULL,
    [JournalDictate_ID] BIGINT NULL,
    [Dictate_ID] INT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_COMMITMENT
CREATE TABLE [ca].[FV_COMMITMENT] (
    [primaryKey] BIGINT NOT NULL,
    [Commitment_ID] BIGINT NULL,
    [Commitment_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Group_ID] INT NULL,
    [Type] INT NULL,
    [Status] INT NULL,
    [active] TINYINT NULL,
    [Hidden] TINYINT NULL,
    [ResponsibleUnit_ID] INT NULL,
    [ResponsibleStaff_ID] INT NULL,
    [Commitment_Start] DATETIME NULL,
    [Commitment_End] DATETIME NULL,
    [Classification] INT NULL,
    [Priority] INT NULL,
    [Pregnancy_ID] INT NULL,
    [Age] SMALLINT NULL,
    [MotivationTerm] INT NULL,
    [MotivationDate] DATETIME NULL,
    [ChildHealthcare_ID] INT NULL,
    [PerformingUnit_ID] INT NULL,
    [ExternalCommitment_ID] VARCHAR(50) NULL,
    [PerformingStaff_ID] INT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_CONTACT
CREATE TABLE [ca].[FV_CONTACT] (
    [primaryKey] BIGINT NOT NULL,
    [Contact_ID] BIGINT NULL,
    [Contact_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Group_ID] INT NULL,
    [ContactType] INT NULL,
    [Status] INT NULL,
    [active] TINYINT NULL,
    [GroupContact_ID] BIGINT NULL,
    [Commitment_ID] BIGINT NULL,
    [Referral_ID] BIGINT NULL,
    [Acute] SMALLINT NULL,
    [Planned] SMALLINT NULL,
    [VisitType] INT NULL,
    [ResponsibleUnit_ID] INT NULL,
    [PerformingUnit_ID] INT NULL,
    [TechnicalUnit_ID] INT NULL,
    [ResponsibleStaff_ID] INT NULL,
    [PerformingStaff_ID] INT NULL,
    [Contact_Start] DATETIME NULL,
    [Contact_End] DATETIME NULL,
    [PlannedContact_End] DATETIME NULL,
    [Agreement_ID] INT NULL,
    [CustomerUnit_ID] INT NULL,
    [PlanningForm_ID] BIGINT NULL,
    [ContinuedCareplan] INT NULL,
    [Identification] INT NULL,
    [Secrecy] SMALLINT NULL,
    [PassedAway] DATETIME NULL,
    [Category_1] INT NULL,
    [Category_2] INT NULL,
    [AdmittedFromUnit_ID] INT NULL,
    [AdmissionType] INT NULL,
    [DischargedToUnit_ID] INT NULL,
    [DischargeType] INT NULL,
    [DischargeStaff_ID] INT NULL,
    [CareReason] INT NULL,
    [Age] SMALLINT NULL,
    [AdmissionMethod] INT NULL,
    [DischargeMethod] INT NULL,
    [ReadyToDischargeDate] DATETIME NULL,
    [PostMortemRequired] SMALLINT NULL,
    [AdminCategory] INT NULL,
    [Spell_ID] INT NULL,
    [CancellationReason] INT NULL,
    [CancellationDate] DATETIME NULL,
    [Clinic_ID] INT NULL,
    [BackToWaitingList] TINYINT NULL,
    [FutureBookingInfo] VARCHAR(600) NULL,
    [CodeValidator] INT NULL,
    [CodeValidationDate] DATETIME NULL,
    [ReasonNotCoded] INT NULL,
    [JournalNote_ID] BIGINT NULL,
    [JournalNoteStatus] INT NULL,
    [CurrentLocation] INT NULL,
    [FuturePriority] INT NULL,
    [ProcedurePerformed] SMALLINT NULL,
    [ConsultationType] INT NULL,
    [BookedViaMVK] SMALLINT NULL,
    [TargetPeriod] INT NULL,
    [TargetDate] DATETIME NULL,
    [DelayedDischargeReason] INT NULL,
    [EstimatedDischargeDate] DATETIME2 NULL,
    [MedicallyFitDischargeDate] DATETIME2 NULL,
    [RegisterCodesComment] VARCHAR(500) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_JOURNALNOTE
CREATE TABLE [ca].[FV_JOURNALNOTE] (
    [primaryKey] BIGINT NOT NULL,
    [JournalNote_ID] BIGINT NULL,
    [JournalNote_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Group_ID] INT NULL,
    [Type] INT NULL,
    [Status] INT NULL,
    [active] TINYINT NULL,
    [Contact_ID] BIGINT NULL,
    [Commitment_ID] BIGINT NULL,
    [Referral_ID] BIGINT NULL,
    [JournalForm_ID] INT NULL,
    [NoteType] INT NULL,
    [Unit_ID] INT NULL,
    [CareProvider_ID] INT NULL,
    [CareProviderProfession_ID] INT NULL,
    [Journal_Datetime] DATETIME NULL,
    [ObservationNote_ID] BIGINT NULL,
    [MeasureNote_ID] BIGINT NULL,
    [Pregnancy_ID] INT NULL,
    [CoordinatedCareplan_ID] INT NULL,
    [Form_Key] INT NULL,
    [ChildHealthcare_ID] INT NULL,
    [LinkCIP_Version_ID] BIGINT NULL,
    [LinkPsychiatricPlan_ID] BIGINT NULL,
    [LinkDischargePlan_ID] BIGINT NULL,
    [Template_ID] INT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_JOURNALTABLE
CREATE TABLE [ca].[FV_JOURNALTABLE] (
    [primaryKey] BIGINT NOT NULL,
    [JournalTable_ID] INT NULL,
    [JournalTable_Key] INT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [active] TINYINT NULL,
    [JournalTableForm_Key] INT NULL,
    [DeleteReason] VARCHAR(200) NULL,
    [Pregnancy_ID] INT NULL,
    [ChildHealthcare_ID] INT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_MEDICATION
CREATE TABLE [ca].[FV_MEDICATION] (
    [primaryKey] BIGINT NOT NULL,
    [Medication_ID] BIGINT NULL,
    [Medication_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [active] TINYINT NULL,
    [Type] INT NULL,
    [Status] INT NULL,
    [Invalidated] TINYINT NULL,
    [Contact_ID] BIGINT NULL,
    [CreationDate] DATETIME NULL,
    [CreatingUnit_ID] INT NULL,
    [CreatingStaff_ID] INT NULL,
    [ResponsibleUnit_ID] INT NULL,
    [ResponsibleStaff_ID] INT NULL,
    [Medication_Start] DATETIME NULL,
    [Medication_End] DATETIME NULL,
    [Drug_ID] INT NULL,
    [EntranceType] INT NULL,
    [Origin] VARCHAR(100) NULL,
    [Indication] INT NULL,
    [Objective] INT NULL,
    [Purpose] INT NULL,
    [ProReNata] TINYINT NULL,
    [Privacy] TINYINT NULL,
    [SubstitutionNotAllowed] TINYINT NULL,
    [SelfAdministration] TINYINT NULL,
    [Signer_ID] INT NULL,
    [Signer_Timestamp] DATETIME NULL,
    [MigrationStatus] INT NULL,
    [Blanding] VARCHAR(MAX) NULL,
    [Patient_Instruction] VARCHAR(MAX) NULL,
    [Staff_Instruction] VARCHAR(MAX) NULL,
    [DiscontinueReason] VARCHAR(MAX) NULL,
    [Discontinued_By] VARCHAR(MAX) NULL,
    [Coercion] TINYINT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_PRESCRIPTION
CREATE TABLE [ca].[FV_PRESCRIPTION] (
    [primaryKey] BIGINT NOT NULL,
    [Prescription_ID] BIGINT NULL,
    [Prescription_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [active] TINYINT NULL,
    [Contact_ID] BIGINT NULL,
    [Medication_ID] BIGINT NULL,
    [CreationDate] DATETIME NULL,
    [CreatingUnit_ID] INT NULL,
    [CreatingStaff_ID] INT NULL,
    [ResponsibleUnit_ID] INT NULL,
    [Iteration] SMALLINT NULL,
    [IterationInterval] SMALLINT NULL,
    [IntervalToFirstDispense] SMALLINT NULL,
    [IntervalTypeToFirstDispense] VARCHAR(20) NULL,
    [Free] TINYINT NULL,
    [Discount] TINYINT NULL,
    [SpecialForm] TINYINT NULL,
    [SubstitutionNotAllowed] TINYINT NULL,
    [StartPack] TINYINT NULL,
    [Benefit] TINYINT NULL,
    [CalledIn] TINYINT NULL,
    [Printed] TINYINT NULL,
    [ePrescription] TINYINT NULL,
    [ePrescription_ID] VARCHAR(20) NULL,
    [Signer_ID] INT NULL,
    [Signer_Timestamp] DATETIME NULL,
    [Fee] FLOAT NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_REFERRAL
CREATE TABLE [ca].[FV_REFERRAL] (
    [primaryKey] BIGINT NOT NULL,
    [Referral_ID] BIGINT NULL,
    [Referral_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Group_ID] INT NULL,
    [Type] SMALLINT NULL,
    [Status] SMALLINT NULL,
    [HCM_Status] INT NULL,
    [active] TINYINT NULL,
    [Commitment_ID] BIGINT NULL,
    [RID] VARCHAR(100) NULL,
    [RequestingUnit_ID] INT NULL,
    [RequestingStaff_ID] INT NULL,
    [RequestingStaff] VARCHAR(100) NULL,
    [RequestingContact_ID] BIGINT NULL,
    [ReceivingUnit_ID] INT NULL,
    [ReceivingStaff_ID] INT NULL,
    [ReceivingStaff] VARCHAR(100) NULL,
    [RequestingDate] DATETIME NULL,
    [ArrivalDate] DATETIME NULL,
    [ReferralType] INT NULL,
    [Priority] SMALLINT NULL,
    [RegistrationDate] DATETIME NULL,
    [TreatByDate] DATETIME NULL,
    [RequestNote_ID] BIGINT NULL,
    [RequestForm_ID] INT NULL,
    [MotivationTerm] INT NULL,
    [MotivationDate] DATETIME NULL,
    [Category_1] INT NULL,
    [Category_2] INT NULL,
    [Age] SMALLINT NULL,
    [MedicalPriority] INT NULL,
    [AdminCategory] INT NULL,
    [SourceOfReferral] INT NULL,
    [ReferralReferenceType] INT NULL,
    [InternalReferral_ID] BIGINT NULL,
    [ExternalReferral_ID] VARCHAR(50) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Table: FV_WARNING
CREATE TABLE [ca].[FV_WARNING] (
    [primaryKey] BIGINT NOT NULL,
    [Warning_ID] BIGINT NULL,
    [Warning_Key] BIGINT NULL,
    [Creator] VARCHAR(50) NULL,
    [latest] TINYINT NOT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [Created] DATETIME NULL,
    [Patient_ID] INT NULL,
    [Type] INT NULL,
    [active] TINYINT NULL,
    [Signer] VARCHAR(20) NULL,
    [Signer_Timestamp] DATETIME NULL,
    [Header] VARCHAR(MAX) NULL,
    [Comment] VARCHAR(MAX) NULL,
    [CancellationReason] VARCHAR(MAX) NULL,
    [ArchiveDate] DATETIME NOT NULL
);
GO

-- Foreign Key Constraints
GO
