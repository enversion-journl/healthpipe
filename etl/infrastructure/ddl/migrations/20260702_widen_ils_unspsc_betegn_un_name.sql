-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Widen arc.ils_unspsc betegn/un_name from NVARCHAR(80) to NVARCHAR(255).
-- ILS dbo.unspsc declares these as varchar(80), but real UNSPSC descriptions
-- exceed 80 chars (e.g. "Aske som indeholder metaller eller metalforbindelser
-- bortset fra ædelmetalforbindelser ..."), causing the archive UPSERT to fail
-- with "String or binary data would be truncated". dim.UNSPSC already stores
-- Beskrivelse_ILS / Beskrivelse_engelsk_ILS as NVARCHAR(255), so 255 aligns the
-- archive with the downstream dimension. Both columns are NULLable and neither
-- participates in the PK (prrowid) or the (unspsc)/(sysdato) indexes, so a plain
-- ALTER COLUMN is sufficient — no constraint or index rebuild required.
-- NVARCHAR(255) => max_length 510 bytes; NVARCHAR(80) => 160.

IF EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'arc' AND t.name = 'ils_unspsc'
      AND c.name = 'betegn' AND c.max_length < 510
)
BEGIN
    ALTER TABLE [arc].[ils_unspsc] ALTER COLUMN [betegn] NVARCHAR(255) NULL;
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'arc' AND t.name = 'ils_unspsc'
      AND c.name = 'un_name' AND c.max_length < 510
)
BEGIN
    ALTER TABLE [arc].[ils_unspsc] ALTER COLUMN [un_name] NVARCHAR(255) NULL;
END
GO
