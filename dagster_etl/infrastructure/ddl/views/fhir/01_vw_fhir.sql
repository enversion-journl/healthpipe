-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- FHIR pass-through views (vw_fhir schema)
-- Exposes FHIR flat tables for reporting consumption
-- ============================================================================

CREATE OR ALTER VIEW [vw_fhir].[Patient]
AS
SELECT *
FROM [fhir].[Patient];
GO

-- fhir.Patient_Address has no vw_fhir view by design: it is an internal
-- staging table consumed only by ref.Patient_Address_Borger -> dim.Borger.
-- Reporting consumers access borger address data via vw_dim.Borger.

CREATE OR ALTER VIEW [vw_fhir].[Encounter]
AS
SELECT *
FROM [fhir].[Encounter];
GO

CREATE OR ALTER VIEW [vw_fhir].[Encounter_Location]
AS
SELECT *
FROM [fhir].[Encounter_Location];
GO

CREATE OR ALTER VIEW [vw_fhir].[Observation]
AS
SELECT *
FROM [fhir].[Observation];
GO

CREATE OR ALTER VIEW [vw_fhir].[EpisodeOfCare]
AS
SELECT *
FROM [fhir].[EpisodeOfCare];
GO

CREATE OR ALTER VIEW [vw_fhir].[Location]
AS
SELECT *
FROM [fhir].[Location];
GO

CREATE OR ALTER VIEW [vw_fhir].[Organization]
AS
SELECT *
FROM [fhir].[Organization];
GO

CREATE OR ALTER VIEW [vw_fhir].[CarePlan]
AS
SELECT *
FROM [fhir].[CarePlan];
GO

CREATE OR ALTER VIEW [vw_fhir].[CarePlan_Activity]
AS
SELECT *
FROM [fhir].[CarePlan_Activity];
GO

CREATE OR ALTER VIEW [vw_fhir].[ServiceRequest]
AS
SELECT *
FROM [fhir].[ServiceRequest];
GO

CREATE OR ALTER VIEW [vw_fhir].[Questionnaire]
AS
SELECT *
FROM [fhir].[Questionnaire];
GO

CREATE OR ALTER VIEW [vw_fhir].[Questionnaire_item]
AS
SELECT *
FROM [fhir].[Questionnaire_item];
GO

GO

CREATE OR ALTER VIEW [vw_fhir].[QuestionnaireResponse]
AS
SELECT
    [qr_id],
    [questionnaire],
    CASE
        WHEN CHARINDEX(N'|', [questionnaire]) > 0
        THEN LEFT([questionnaire], CHARINDEX(N'|', [questionnaire]) - 1)
        ELSE [questionnaire]
    END AS [questionnaire_canonical],
    CASE
        WHEN CHARINDEX(N'|', [questionnaire]) > 0
        THEN SUBSTRING([questionnaire], CHARINDEX(N'|', [questionnaire]) + 1, LEN([questionnaire]))
        ELSE NULL
    END AS [questionnaire_version],
    [status],
    [patient_id],
    [encounter_id],
    [organization_id],
    [practitioner_id],
    [practitioner_display],
    [authored],
    [source_aktivitet_id],
    [source_model_name],
    [source_model_version],
    [last_updated],
    [created_at],
    [DW_Timestamp_Insert],
    [DW_Timestamp_Update]
FROM [fhir].[QuestionnaireResponse];
GO

CREATE OR ALTER VIEW [vw_fhir].[QuestionnaireResponse_item]
AS
SELECT *
FROM [fhir].[QuestionnaireResponse_item];
GO

GO

-- Internal build view: UNION ALL of Observation + QR-item SFI rows with CodeSystem enrichment.
-- Consumer-facing fhir.SFI_datapoint reads from the materialized fhir.SFI_datapoint_store table.
CREATE OR ALTER VIEW [fhir].[SFI_datapoint_build]
AS
SELECT
    LOWER(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(4000), CONCAT_WS(N'|',
        N'Observation',
        COALESCE(o.[observation_id], N''),
        N'',
        N'',
        N'',
        COALESCE(o.[element_code], N'')
    ))), 2)) AS datapoint_key,
    o.[observation_id] AS fhir_resource_id,
    o.[observation_id] AS source_resource_id,
    CAST(NULL AS NVARCHAR(255)) AS source_link_id,
    CAST(NULL AS NVARCHAR(100)) AS source_item_key,
    CAST(NULL AS NVARCHAR(10)) AS source_answer_index,
    COALESCE(o.[DW_Timestamp_Update], o.[DW_Timestamp_Insert]) AS source_last_updated,
    o.[ifdb_aktivitet_id],
    o.[ifdb_aktivitet_sfi_id],
    o.[patient_id],
    o.[encounter_id],
    COALESCE(o.[organization_id], e.[service_provider_id]) AS organization_id,
    o.[practitioner_id],
    o.[practitioner_display],
    CAST(N'Observation' AS NVARCHAR(50)) AS source_resource,
    o.[effective_date] AS occurred_at,
    COALESCE(
        op.[source_model_name],
        CASE
            WHEN CHARINDEX(N'.', o.[element_code]) > 1
            THEN LEFT(o.[element_code], CHARINDEX(N'.', o.[element_code]) - 1)
            ELSE NULL
        END
    ) AS source_model_name,
    COALESCE(op.[last_seen_in_version], op.[source_model_version]) AS source_model_version,
    op.[source_element_id],
    op.[source_element_name],
    COALESCE(op.[source_element_label], o.[element_display], sc.[Display]) AS source_element_label,
    op.[sfi_path],
    op.[source_hierarchy_path],
    op.[source_group_path],
    op.[source_repetition_path],
    op.[klynge_niveau],
    op.[repetition_niveau],
    op.[raekke_type],
    op.[klynge_type],
    o.[klynge_valg_ledetekst],
    o.[element_code],
    COALESCE(o.[element_display], sc.[Display]) AS element_display,
    CASE
        WHEN o.[value_int] IS NOT NULL THEN N'integer'
        WHEN o.[value_quantity] IS NOT NULL
            AND NULLIF(LTRIM(RTRIM(o.[value_unit])), N'') IS NULL THEN N'decimal'
        WHEN o.[value_quantity] IS NOT NULL THEN N'quantity'
        WHEN o.[value_dec] IS NOT NULL THEN N'decimal'
        WHEN o.[value_bool] IS NOT NULL THEN N'bool'
        WHEN o.[value_datetime] IS NOT NULL THEN N'datetime'
        WHEN o.[value_coding_code] IS NOT NULL THEN N'coding'
        WHEN o.[value_string] IS NOT NULL THEN N'string'
        ELSE NULL
    END AS value_type,
    CASE
        WHEN o.[value_int] IS NOT NULL
            OR o.[value_dec] IS NOT NULL
            OR NULLIF(LTRIM(RTRIM(o.[value_string])), N'') IS NOT NULL
            OR o.[value_datetime] IS NOT NULL
            OR o.[value_bool] IS NOT NULL
            OR o.[value_quantity] IS NOT NULL
            OR NULLIF(LTRIM(RTRIM(o.[value_coding_code])), N'') IS NOT NULL
            OR NULLIF(LTRIM(RTRIM(o.[value_coding_display])), N'') IS NOT NULL
        THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END AS has_value,
    o.[value_int],
    o.[value_dec],
    o.[value_string] AS value_text,
    o.[value_datetime],
    o.[value_bool],
    o.[value_quantity],
    o.[value_unit],
    o.[value_coding_code],
    o.[value_coding_display],
    o.[element_nummer],
    o.[klynge_id],
    o.[klynge_id_hierarki],
    o.[klynge_parent_id],
    o.[repetition_id],
    o.[element_kode],
    o.[element_original_kode],
    o.[element_administrativ_enhed_id],
    op.[klynge_navne],
    op.[klynge_ledetekster],
    op.[klynge_ledetekst],
    o.[repetition_id_hierarki],
    o.[opdateret_tid],
    o.[xml_status],
    o.[xml_status_text]
FROM [fhir].[Observation] AS o
LEFT JOIN [fhir].[Encounter] AS e
    ON e.[encounter_id] = o.[encounter_id]
LEFT JOIN [codesystem].[SFIConcept] AS sc
    ON sc.[Code] = o.[element_code]
OUTER APPLY (
    SELECT
        MAX(CASE WHEN sp.[PropertyCode] = N'source_model_name' THEN sp.[Value] END) AS source_model_name,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_model_version' THEN sp.[Value] END) AS source_model_version,
        MAX(CASE WHEN sp.[PropertyCode] = N'last_seen_in_version' THEN sp.[Value] END) AS last_seen_in_version,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_element_id' THEN sp.[Value] END) AS source_element_id,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_element_name' THEN sp.[Value] END) AS source_element_name,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_element_label' THEN sp.[Value] END) AS source_element_label,
        MAX(CASE WHEN sp.[PropertyCode] = N'sfi_path' THEN sp.[Value] END) AS sfi_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_hierarchy_path' THEN sp.[Value] END) AS source_hierarchy_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_group_path' THEN sp.[Value] END) AS source_group_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_repetition_path' THEN sp.[Value] END) AS source_repetition_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_niveau' THEN sp.[Value] END) AS klynge_niveau,
        MAX(CASE WHEN sp.[PropertyCode] = N'repetition_niveau' THEN sp.[Value] END) AS repetition_niveau,
        MAX(CASE WHEN sp.[PropertyCode] = N'raekke_type' THEN sp.[Value] END) AS raekke_type,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_type' THEN sp.[Value] END) AS klynge_type,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_navne' THEN sp.[Value] END) AS klynge_navne,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_ledetekster' THEN sp.[Value] END) AS klynge_ledetekster,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_ledetekst' THEN sp.[Value] END) AS klynge_ledetekst
    FROM [codesystem].[SFIProperties] AS sp
    WHERE sp.[Code] = o.[element_code]
) AS op
WHERE o.[element_system] = N'urn:dk:ifdb:sfi'

UNION ALL

SELECT
    LOWER(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(4000), CONCAT_WS(N'|',
        N'QuestionnaireResponse',
        COALESCE(qri.[qr_id], N''),
        COALESCE(qri.[link_id], N''),
        COALESCE(qri.[item_key], N''),
        COALESCE(qri.[answer_index], N''),
        -- element_code == the QR item's own SFI link_id; keeps datapoint_key stable.
        COALESCE(qri.[link_id], N'')
    ))), 2)) AS datapoint_key,
    qr.[qr_id] AS fhir_resource_id,
    qr.[qr_id] AS source_resource_id,
    qri.[link_id] AS source_link_id,
    qri.[item_key] AS source_item_key,
    qri.[answer_index] AS source_answer_index,
    COALESCE(
        qri.[DW_Timestamp_Update],
        qr.[DW_Timestamp_Update],
        qri.[DW_Timestamp_Insert],
        qr.[DW_Timestamp_Insert]
    ) AS source_last_updated,
    qr.[source_aktivitet_id] AS ifdb_aktivitet_id,
    CAST(NULL AS NVARCHAR(255)) AS ifdb_aktivitet_sfi_id,
    qr.[patient_id],
    qr.[encounter_id],
    COALESCE(qr.[organization_id], e.[service_provider_id]) AS organization_id,
    qr.[practitioner_id],
    qr.[practitioner_display],
    CAST(N'QuestionnaireResponse' AS NVARCHAR(50)) AS source_resource,
    qr.[authored] AS occurred_at,
    COALESCE(qp.[source_model_name], qr.[source_model_name], qref.[questionnaire_canonical]) AS source_model_name,
    COALESCE(
        qp.[last_seen_in_version],
        qp.[source_model_version],
        qr.[source_model_version],
        qref.[questionnaire_version]
    ) AS source_model_version,
    qp.[source_element_id],
    qp.[source_element_name],
    COALESCE(qp.[source_element_label], sc.[Display], qri.[text]) AS source_element_label,
    qp.[sfi_path],
    qp.[source_hierarchy_path],
    qp.[source_group_path],
    qp.[source_repetition_path],
    qp.[klynge_niveau],
    qp.[repetition_niveau],
    qp.[raekke_type],
    qp.[klynge_type],
    qri.[klynge_valg_ledetekst],
    qri.[link_id] AS element_code,
    COALESCE(sc.[Display], qri.[text]) AS element_display,
    CASE
        WHEN qri.[value_int] IS NOT NULL THEN N'integer'
        WHEN qri.[value_quantity] IS NOT NULL THEN N'quantity'
        WHEN qri.[value_dec] IS NOT NULL THEN N'decimal'
        WHEN qri.[value_bool] IS NOT NULL THEN N'bool'
        WHEN qri.[value_datetime] IS NOT NULL THEN N'datetime'
        WHEN qri.[value_coding_code] IS NOT NULL THEN N'coding'
        WHEN qri.[value_string] IS NOT NULL THEN N'string'
        ELSE NULL
    END AS value_type,
    CASE
        WHEN qri.[value_int] IS NOT NULL
            OR qri.[value_dec] IS NOT NULL
            OR NULLIF(LTRIM(RTRIM(qri.[value_string])), N'') IS NOT NULL
            OR qri.[value_datetime] IS NOT NULL
            OR qri.[value_bool] IS NOT NULL
            OR qri.[value_quantity] IS NOT NULL
            OR NULLIF(LTRIM(RTRIM(qri.[value_coding_code])), N'') IS NOT NULL
            OR NULLIF(LTRIM(RTRIM(qri.[value_coding_display])), N'') IS NOT NULL
        THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END AS has_value,
    qri.[value_int],
    qri.[value_dec],
    qri.[value_string] AS value_text,
    qri.[value_datetime],
    qri.[value_bool],
    qri.[value_quantity],
    qri.[value_unit],
    qri.[value_coding_code],
    qri.[value_coding_display],
    qri.[element_nummer],
    qri.[klynge_id],
    qri.[klynge_id_hierarki],
    qri.[klynge_parent_id],
    qri.[repetition_id],
    qri.[element_kode],
    qri.[element_original_kode],
    qri.[element_administrativ_enhed_id],
    qp.[klynge_navne],
    qp.[klynge_ledetekster],
    qp.[klynge_ledetekst],
    qri.[repetition_id_hierarki],
    qri.[opdateret_tid],
    qri.[xml_status],
    qri.[xml_status_text]
FROM [fhir].[QuestionnaireResponse_item] AS qri
INNER JOIN [fhir].[QuestionnaireResponse] AS qr
    ON qr.[qr_id] = qri.[qr_id]
LEFT JOIN [fhir].[Encounter] AS e
    ON e.[encounter_id] = qr.[encounter_id]
OUTER APPLY (
    SELECT
        COALESCE(
            qr.[questionnaire_canonical],
            CASE
                WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                THEN LEFT(qr.[questionnaire], CHARINDEX(N'|', qr.[questionnaire]) - 1)
                ELSE qr.[questionnaire]
            END
        ) AS questionnaire_canonical,
        COALESCE(
            qr.[questionnaire_version],
            CASE
                WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                THEN SUBSTRING(
                    qr.[questionnaire],
                    CHARINDEX(N'|', qr.[questionnaire]) + 1,
                    LEN(qr.[questionnaire])
                )
                ELSE NULL
            END
        ) AS questionnaire_version
) AS qref
-- No INNER JOIN to fhir.Questionnaire_item: that model-level template is
-- assembled by accumulating observed activity items and may be incomplete, so
-- gating here silently drops QR answers (and can fan-out duplicates). The SFI
-- code IS the QR item's own link_id, display falls back to the catalog/text,
-- and every QR here is SFI-sourced -- so enrich via LEFT JOIN on link_id and
-- never drop a clinical answer. Mirrors _inline_qr_build_leg in
-- DSO/load/refresh_sfi_datapoint.py.
LEFT JOIN [codesystem].[SFIConcept] AS sc
    ON sc.[Code] = qri.[link_id]
OUTER APPLY (
    SELECT
        MAX(CASE WHEN sp.[PropertyCode] = N'source_model_name' THEN sp.[Value] END) AS source_model_name,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_model_version' THEN sp.[Value] END) AS source_model_version,
        MAX(CASE WHEN sp.[PropertyCode] = N'last_seen_in_version' THEN sp.[Value] END) AS last_seen_in_version,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_element_id' THEN sp.[Value] END) AS source_element_id,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_element_name' THEN sp.[Value] END) AS source_element_name,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_element_label' THEN sp.[Value] END) AS source_element_label,
        MAX(CASE WHEN sp.[PropertyCode] = N'sfi_path' THEN sp.[Value] END) AS sfi_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_hierarchy_path' THEN sp.[Value] END) AS source_hierarchy_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_group_path' THEN sp.[Value] END) AS source_group_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'source_repetition_path' THEN sp.[Value] END) AS source_repetition_path,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_niveau' THEN sp.[Value] END) AS klynge_niveau,
        MAX(CASE WHEN sp.[PropertyCode] = N'repetition_niveau' THEN sp.[Value] END) AS repetition_niveau,
        MAX(CASE WHEN sp.[PropertyCode] = N'raekke_type' THEN sp.[Value] END) AS raekke_type,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_type' THEN sp.[Value] END) AS klynge_type,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_navne' THEN sp.[Value] END) AS klynge_navne,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_ledetekster' THEN sp.[Value] END) AS klynge_ledetekster,
        MAX(CASE WHEN sp.[PropertyCode] = N'klynge_ledetekst' THEN sp.[Value] END) AS klynge_ledetekst
    FROM [codesystem].[SFIProperties] AS sp
    WHERE sp.[Code] = qri.[link_id]
) AS qp
WHERE qri.[link_id] <> N''
;
GO

-- Consumer-facing view: materialized SFI datapoints refreshed by dso_refresh_sfi_datapoint_*.
CREATE OR ALTER VIEW [fhir].[SFI_datapoint]
AS
SELECT
    [datapoint_key],
    [fhir_resource_id],
    [source_resource_id],
    [source_link_id],
    [source_item_key],
    [source_answer_index],
    [source_last_updated],
    [ifdb_aktivitet_id],
    [ifdb_aktivitet_sfi_id],
    [patient_id],
    [encounter_id],
    [organization_id],
    [practitioner_id],
    [practitioner_display],
    [source_resource],
    [occurred_at],
    [occurred_at_dt],
    [source_model_name],
    [source_model_version],
    [source_element_id],
    [source_element_name],
    [source_element_label],
    [sfi_path],
    [source_hierarchy_path],
    [source_group_path],
    [source_repetition_path],
    [klynge_niveau],
    [repetition_niveau],
    [raekke_type],
    [klynge_type],
    [klynge_valg_ledetekst],
    [element_code],
    [element_display],
    [value_type],
    [has_value],
    [value_int],
    [value_dec],
    [value_text],
    [value_datetime],
    [value_bool],
    [value_quantity],
    [value_unit],
    [value_coding_code],
    [value_coding_display],
    [element_nummer],
    [klynge_id],
    [klynge_id_hierarki],
    [klynge_parent_id],
    [repetition_id],
    [element_kode],
    [element_original_kode],
    [element_administrativ_enhed_id],
    [klynge_navne],
    [klynge_ledetekster],
    [klynge_ledetekst],
    [repetition_id_hierarki],
    [opdateret_tid],
    [xml_status],
    [xml_status_text]
FROM [fhir].[SFI_datapoint_store];
GO

CREATE OR ALTER VIEW [vw_fhir].[SFI_datapoint]
AS
SELECT *
FROM [fhir].[SFI_datapoint];
GO

-- ============================================================================
-- fhir.IFDB_procedure_event
-- Separate event view for AKTIVITETSRESULTAT-derived Procedure resources.
-- Procedures are kept out of fhir.SFI_datapoint because they are events, not
-- atomic answer/measurement datapoints.
-- ============================================================================
CREATE OR ALTER VIEW [fhir].[IFDB_procedure_event]
AS
SELECT
    p.[procedure_id] AS procedure_id,
    CONCAT(N'Procedure/', p.[procedure_id]) AS event_id,
    p.[patient_id],
    p.[encounter_id],
    COALESCE(p.[performed_datetime], p.[performed_period_start]) AS occurred_at,
    p.[code_system],
    p.[code],
    p.[code_display],
    p.[source_system],
    p.[ifdb_aktivitet_id],
    LOWER(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(4000), CONCAT_WS(N'|',
        N'Procedure',
        COALESCE(p.[ifdb_aktivitet_id], N''),
        COALESCE(p.[procedure_id], N''),
        COALESCE(p.[code_system], N''),
        COALESCE(p.[code], N'')
    ))), 2)) AS clinical_event_id,
    CAST('Procedure' AS NVARCHAR(32)) AS routing_target,
    CAST('procedure_code' AS NVARCHAR(50)) AS routing_source,
    CAST('high' AS NVARCHAR(32)) AS routing_confidence,
    COALESCE(
        rd.[routing_reason],
        CAST('AKTIVITETTYPE_KODE matched IFDB SKS procedure-code classifier.' AS NVARCHAR(MAX))
    ) AS routing_reason,
    p.[last_updated]
FROM [fhir].[Procedure] AS p
LEFT JOIN [meta].[ifdb_aktivitetsresultat_routing_decision] AS rd
    ON rd.[ifdb_aktivitet_id] = p.[ifdb_aktivitet_id]
WHERE p.[source_system] = N'urn:source:ifdb:aktivitetsresultat_procedure'
   OR p.[ifdb_aktivitet_id] IS NOT NULL;
GO

CREATE OR ALTER VIEW [vw_fhir].[IFDB_procedure_event]
AS
SELECT *
FROM [fhir].[IFDB_procedure_event];
GO
