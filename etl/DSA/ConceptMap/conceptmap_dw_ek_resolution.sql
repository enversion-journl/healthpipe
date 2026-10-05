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
SET cm.[dw_ek_Rekvirent] = m.[dw_ek_Rekvirent],
    cm.[dw_ek_Organisation_target] = org.[dw_ek_Organisation]
FROM [conceptmap].[RekvirentToSOR] AS cm
LEFT JOIN [map].[Rekvirent] AS m ON m.[bk_code] = cm.[source_code]
LEFT JOIN [ref].[Organisation] AS org ON org.[sor_code] = cm.[target_code] AND org.[dw_is_deleted] = 0

-- conceptmap_table: LagerToSOR
UPDATE cm
SET cm.[dw_ek_Lager] = m.[dw_ek_Lager],
    cm.[dw_ek_Organisation_target] = org.[dw_ek_Organisation]
FROM [conceptmap].[LagerToSOR] AS cm
LEFT JOIN [map].[Lager] AS m ON m.[bk_code] = cm.[source_code]
LEFT JOIN [ref].[Organisation] AS org ON org.[sor_code] = cm.[target_code] AND org.[dw_is_deleted] = 0

-- conceptmap_table: EANToSOR
UPDATE cm
SET cm.[dw_ek_EAN] = m.[dw_ek_EAN],
    cm.[dw_ek_Organisation_target] = org.[dw_ek_Organisation]
FROM [conceptmap].[EANToSOR] AS cm
LEFT JOIN [map].[ean] AS m ON m.[clean_code_EAN] = cm.[source_code]
LEFT JOIN [ref].[Organisation] AS org ON org.[sor_code] = cm.[target_code] AND org.[dw_is_deleted] = 0

-- conceptmap_table: OkostedToSOR
UPDATE cm
SET cm.[dw_ek_Okosted] = oko.[dw_ek_Okosted],
    cm.[dw_ek_Organisation_target] = org_tgt.[dw_ek_Organisation]
FROM [conceptmap].[OkostedToSOR] AS cm
LEFT JOIN [map].[Okosted] AS oko
    ON oko.[clean_code_Okosted] = cm.[source_code]
LEFT JOIN [ref].[Organisation] AS org_tgt
    ON org_tgt.[sor_code] = cm.[target_code] AND org_tgt.[dw_is_deleted] = 0

-- conceptmap_table: SHAKToSOR
UPDATE cm
SET cm.[dw_ek_SHAK] = shak.[dw_ek_SHAK],
    cm.[dw_ek_Organisation_target] = org_tgt.[dw_ek_Organisation]
FROM [conceptmap].[SHAKToSOR] AS cm
LEFT JOIN [map].[SHAK] AS shak
    ON shak.[clean_code_SHAK] = cm.[source_code]
LEFT JOIN [ref].[Organisation] AS org_tgt
    ON org_tgt.[sor_code] = cm.[target_code] AND org_tgt.[dw_is_deleted] = 0
