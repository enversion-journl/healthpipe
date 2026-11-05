-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- DSO presentation view; staging deliberately exposes no new view.
IF DB_NAME() LIKE N'%dso%'
BEGIN
    IF OBJECT_ID(N'[dim].[BorgerDemografi]', N'U') IS NULL
        THROW 51000, 'Create BorgerDemografi before deploying its view', 1;
    EXEC(N'CREATE OR ALTER VIEW [vw_dim].[BorgerDemografi]
AS
SELECT
    dw_ek_BorgerDemografi,
    region_id, regionnavn, kommune_id, kommunenavn,
    postnr, postdistrikt, sogn_id, sognenavn, koen_navn,
    dw_timestamp_insert, dw_timestamp_update
FROM [dim].[BorgerDemografi]');
END
GO
