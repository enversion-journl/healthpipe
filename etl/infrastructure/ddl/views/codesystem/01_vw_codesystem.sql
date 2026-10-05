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
    a.[alias_code],
    a.[alias_display],
    a.[alias_type],
    a.[description],
    a.[active_from],
    a.[active_to],
    a.[owner],
    m.[sfi_code],
    m.[member_role],
    m.[sort_order],
    c.[display] AS [SfiDisplay]
FROM [codesystem].[SFIAlias] AS a
INNER JOIN [codesystem].[SFIAliasMember] AS m
    ON m.[alias_code] = a.[alias_code]
LEFT JOIN [codesystem].[SFIConcept] AS c
    ON c.[code] = m.[sfi_code]
WHERE a.[active_from] <= CAST(GETDATE() AS DATE)
  AND (a.[active_to] IS NULL OR a.[active_to] >= CAST(GETDATE() AS DATE));
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
    [dw_timestamp_update]
FROM [codesystem].[SFISearchDocument]
WHERE [is_active] = 1;
GO
