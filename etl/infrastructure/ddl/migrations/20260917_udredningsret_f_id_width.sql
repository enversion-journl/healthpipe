-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Preserve the archive's full F_ID in the transaction dimension, on both DSA
-- and DSO. Staging tables are rebuilt with the same width by the publish job.
-- No keys or values are changed. Safe to rerun and safe before first install.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[UdredningsretEgenskaber]')
      AND name = 'f_id' AND system_type_id = TYPE_ID('nvarchar')
      AND max_length > 0 AND max_length < 1000
)
BEGIN
    EXEC(N'ALTER TABLE [dim].[UdredningsretEgenskaber] ALTER COLUMN [f_id] NVARCHAR(500) NULL');
END
GO
