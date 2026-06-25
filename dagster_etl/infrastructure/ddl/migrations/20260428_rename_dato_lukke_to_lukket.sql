-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename DW_EK_Dato_Lukke -> DW_EK_Dato_Lukket on fct.Postering
-- (Danish grammar fix: "Lukke" -> "Lukket" to match past tense convention)
--
-- Applies to: fct.Postering
-- Safe to re-run: IF EXISTS guard on old column name.
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Lukke')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Dato_Lukke]', 'DW_EK_Dato_Lukket', 'COLUMN';
    PRINT 'Renamed DW_EK_Dato_Lukke -> DW_EK_Dato_Lukket on fct.Postering';
END
GO
