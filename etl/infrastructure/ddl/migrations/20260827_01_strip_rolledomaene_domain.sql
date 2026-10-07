-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Strip the domain prefix from sec.RolleDomaene.bruger so the deployed rows
-- match the DPA functions' new lookup form (bare sAMAccountName).
--
-- Runs before tables/ DDL, so the rows are stripped before the new functions
-- are created. Otherwise the lookup matches no rows and the DPA views CROSS
-- JOIN to 0 rows for every user until the next nightly load.
--
-- PK is (bruger, Sikkerhedsdomaene), so stripping can collide. We mirror the
-- loader's duplicate rule: K wins over U. The rows are fully reloaded on the
-- next run anyway; this only bridges the gap.
--
-- Idempotent: every step is filtered on [bruger] LIKE N'%\%'. Runs on both
-- staging and DSO.
-- ============================================================================

IF EXISTS (SELECT 1 FROM [sec].[RolleDomaene] WHERE [bruger] LIKE N'%\%')
BEGIN
    -- 1a. If the bare row already exists it must inherit 'K' from a prefixed
    --     duplicate before that duplicate is deleted (K over U must not be lost).
    UPDATE [bare]
    SET [Rolle] = 'K'
    FROM [sec].[RolleDomaene] AS [bare]
    WHERE [bare].[bruger] NOT LIKE N'%\%'
      AND [bare].[Rolle] <> 'K'
      AND EXISTS (
          SELECT 1
          FROM [sec].[RolleDomaene] AS [pref]
          WHERE [pref].[bruger] LIKE N'%\%'
            AND [pref].[Rolle] = 'K'
            AND [pref].[Sikkerhedsdomaene] = [bare].[Sikkerhedsdomaene]
            AND RIGHT([pref].[bruger],
                      CHARINDEX(N'\', REVERSE([pref].[bruger]) + N'\') - 1) = [bare].[bruger]
      );

    -- 1b. Delete prefixed rows that would collide with an existing bare row.
    DELETE [pref]
    FROM [sec].[RolleDomaene] AS [pref]
    INNER JOIN [sec].[RolleDomaene] AS [bare]
        ON  [bare].[bruger] NOT LIKE N'%\%'
        AND [bare].[Sikkerhedsdomaene] = [pref].[Sikkerhedsdomaene]
        AND [bare].[bruger] = RIGHT([pref].[bruger],
                                    CHARINDEX(N'\', REVERSE([pref].[bruger]) + N'\') - 1)
    WHERE [pref].[bruger] LIKE N'%\%';

    -- 2. Several prefixed rows can strip to the same key (e.g. two domains with
    --    the same sAMAccountName). Keep one, 'K' before 'U'.
    ;WITH [ranked] AS (
        SELECT [bruger], [Sikkerhedsdomaene], [Rolle],
               ROW_NUMBER() OVER (
                   PARTITION BY RIGHT([bruger],
                                      CHARINDEX(N'\', REVERSE([bruger]) + N'\') - 1),
                                [Sikkerhedsdomaene]
                   ORDER BY CASE WHEN [Rolle] = 'K' THEN 0 ELSE 1 END, [bruger]
               ) AS [rn]
        FROM [sec].[RolleDomaene]
        WHERE [bruger] LIKE N'%\%'
    )
    DELETE FROM [ranked] WHERE [rn] > 1;

    -- 3. Strip the domain from the remaining rows.
    UPDATE [sec].[RolleDomaene]
    SET [bruger] = RIGHT([bruger], CHARINDEX(N'\', REVERSE([bruger]) + N'\') - 1),
        [dw_timestamp_update] = SYSDATETIME()
    WHERE [bruger] LIKE N'%\%';

    PRINT 'Stripped domain prefix from sec.RolleDomaene.bruger (K over U on collisions)';
END
GO
