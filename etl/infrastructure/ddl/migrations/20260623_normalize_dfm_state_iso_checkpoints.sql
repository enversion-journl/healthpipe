-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: normalize non-ISO datetime checkpoints in meta.dfm_state.
--
-- The ILS sysdato and SydAX archive checkpoint queries previously used
-- CAST(MAX([datetime col]) AS NVARCHAR(50)). Because those columns are
-- `datetime`, that CAST produces SQL Server's style-0 string (e.g.
-- "Jun 23 2026 12:00AM"), which utils/sql_safety.validate_checkpoint_value
-- rejects, breaking the downstream ext.*_dfm delta extracts. The queries now
-- use CONVERT(..., 121) to emit ISO; this migration repairs the values already
-- persisted by earlier runs for the affected arc.* checkpoint owners only.
--
-- Idempotent: scoped to the known affected (schema_name, table_name) rows, and
-- further guarded so only values containing a letter (the month name in the
-- style-0 format) that also parse as a datetime are rewritten. After conversion
-- the ISO value has no letters, so re-running is a no-op.

IF OBJECT_ID(N'[meta].[dfm_state]', 'U') IS NOT NULL
BEGIN
    UPDATE [meta].[dfm_state]
    SET [checkpoint_value] = CONVERT(NVARCHAR(50), TRY_CONVERT(datetime, [checkpoint_value]), 121),
        [updated_at]       = CAST(SYSDATETIME() AS datetime2(0))
    WHERE [schema_name] = 'arc'
      AND [table_name] IN (
            -- SydAX archives (17) — checkpoint on COALESCE([Modified], [CreatedOn])
            'afdeling_sydax',
            'finanskonto_sydax',
            'finanskonto_omkostningssted_sydax',
            'hovedkonto_sydax',
            'omkostningssted_hierarki_sydax',
            'gruppering_sydax',
            'gruppering_autoriseret_sydax',
            'art_sydax',
            'funktion_sydax',
            'dranst_sydax',
            'kreditor_sydax',
            'finanspostering_sydax',
            'kreditortransaktion_sydax',
            'finanspostering_kreditortrans_relation_sydax',
            'finanskladde_sydax',
            'finanskladdelinje_sydax',
            'finanstransaktionstype_sydax',
            -- ILS sysdato archives (7)
            'laftdpris_ils',
            'laftdstam_ils',
            'laftd_ils',
            'udlev_ils',
            'lgrvare_ils',
            'pmdm_ils',
            'indkfak_ils',
            -- ILS sysdato precedents (2)
            'vare_ils',
            'unspsc_ils'
        )
      AND [checkpoint_value] LIKE '%[A-Za-z]%'
      AND TRY_CONVERT(datetime, [checkpoint_value]) IS NOT NULL;
END
GO
