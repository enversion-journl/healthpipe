-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Add category_code to fhir.Condition
-- WHY: DK Core condition-category binding; filter handle for referral-indication vs
-- encounter-diagnosis. Requires re-post of Condition assets.

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Condition]', 'category_code') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [category_code] NVARCHAR(100) NULL;
END
GO
