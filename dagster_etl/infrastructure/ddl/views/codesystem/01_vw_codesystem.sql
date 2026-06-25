-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- codesystem views (DSO)
-- Curated lookup views on top of codesystem.* tables.
-- ============================================================================

CREATE OR ALTER VIEW [codesystem].[vw_sfi_alias_member]
AS
SELECT
    a.[AliasCode],
    a.[AliasDisplay],
    a.[AliasType],
    a.[Description],
    a.[ActiveFrom],
    a.[ActiveTo],
    a.[Owner],
    m.[SfiCode],
    m.[MemberRole],
    m.[SortOrder],
    c.[Display] AS [SfiDisplay]
FROM [codesystem].[SFIAlias] AS a
INNER JOIN [codesystem].[SFIAliasMember] AS m
    ON m.[AliasCode] = a.[AliasCode]
LEFT JOIN [codesystem].[SFIConcept] AS c
    ON c.[Code] = m.[SfiCode]
WHERE a.[ActiveFrom] <= CAST(GETDATE() AS DATE)
  AND (a.[ActiveTo] IS NULL OR a.[ActiveTo] >= CAST(GETDATE() AS DATE));
GO

CREATE OR ALTER VIEW [codesystem].[vw_sfi_search_document]
AS
SELECT
    [element_code],
    [element_display],
    [source_model_name],
    [source_element_label],
    [sfi_path],
    [alias_text],
    [search_text],
    [is_active],
    [DW_Timestamp_Update]
FROM [codesystem].[SFISearchDocument]
WHERE [is_active] = 1;
GO
