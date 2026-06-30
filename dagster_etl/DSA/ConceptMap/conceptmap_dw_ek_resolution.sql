-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

/*
 * DW_EK resolution: populate surrogate keys on conceptmap.*_to_sor after MERGE.
 *
 * Consumed by DSA.ConceptMap.utils (not executed as standalone DDL).
 * Each section starts with: -- conceptmap_table: <table_name_without_conceptmap_schema>
 */

-- conceptmap_table: RekvirentToSOR
UPDATE cm
SET cm.[DW_EK_Rekvirent] = m.[DW_EK_Rekvirent],
    cm.[DW_EK_Organisation_Target] = org.[DW_EK_Organisation]
FROM [conceptmap].[RekvirentToSOR] AS cm
LEFT JOIN [map].[Rekvirent] AS m ON m.[bk_Code] = cm.[SourceCode]
LEFT JOIN [ref].[Organisation] AS org ON org.[sor_code] = cm.[TargetCode] AND org.[DW_IsDeleted] = 0

-- conceptmap_table: LagerToSOR
UPDATE cm
SET cm.[DW_EK_Lager] = m.[DW_EK_Lager],
    cm.[DW_EK_Organisation_Target] = org.[DW_EK_Organisation]
FROM [conceptmap].[LagerToSOR] AS cm
LEFT JOIN [map].[Lager] AS m ON m.[bk_Code] = cm.[SourceCode]
LEFT JOIN [ref].[Organisation] AS org ON org.[sor_code] = cm.[TargetCode] AND org.[DW_IsDeleted] = 0

-- conceptmap_table: EANToSOR
UPDATE cm
SET cm.[DW_EK_EAN] = m.[DW_EK_EAN],
    cm.[DW_EK_Organisation_Target] = org.[DW_EK_Organisation]
FROM [conceptmap].[EANToSOR] AS cm
LEFT JOIN [map].[EAN] AS m ON m.[CleanCode_EAN] = cm.[SourceCode]
LEFT JOIN [ref].[Organisation] AS org ON org.[sor_code] = cm.[TargetCode] AND org.[DW_IsDeleted] = 0

-- conceptmap_table: OkostedToSOR
UPDATE cm
SET cm.[DW_EK_Okosted] = oko.[DW_EK_Okosted],
    cm.[DW_EK_Organisation_Target] = org_tgt.[DW_EK_Organisation]
FROM [conceptmap].[OkostedToSOR] AS cm
LEFT JOIN [map].[Okosted] AS oko
    ON oko.[CleanCode_Okosted] = cm.[SourceCode]
LEFT JOIN [ref].[Organisation] AS org_tgt
    ON org_tgt.[sor_code] = cm.[TargetCode] AND org_tgt.[DW_IsDeleted] = 0
