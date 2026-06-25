-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Rekey ref.ConceptMap_* tables: PK on DW_EK join column (one row per dimension member).
-- Staging ref views now guarantee uniqueness via Relationship filter + ROW_NUMBER.
-- Old composite PK (SourceCode, TargetCode) replaced by DW_EK-based clustered PK.

-- ---- ref.ConceptMap_Rekvirent_To_SOR ----
IF EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ConceptMap_Rekvirent_To_SOR]') AND type = 'U'
)
AND EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[ref].[ConceptMap_Rekvirent_To_SOR]')
      AND name = N'PK_ref_ConceptMap_Rekvirent_To_SOR'
      AND type = 'PK'
)
BEGIN
    -- Remove duplicates: keep best Relationship per DW_EK_Rekvirent
    ;WITH dups AS (
        SELECT ROW_NUMBER() OVER (
            PARTITION BY [DW_EK_Rekvirent]
            ORDER BY CASE [Relationship]
                        WHEN 'equal'      THEN 1
                        WHEN 'equivalent' THEN 2
                        WHEN 'narrower'   THEN 3
                        WHEN 'inexact'    THEN 4
                        ELSE 5
                     END,
                     [DW_Timestamp_Update] DESC
        ) AS _rn
        FROM [ref].[ConceptMap_Rekvirent_To_SOR]
    )
    DELETE FROM dups WHERE _rn > 1;

    ALTER TABLE [ref].[ConceptMap_Rekvirent_To_SOR]
        DROP CONSTRAINT [PK_ref_ConceptMap_Rekvirent_To_SOR];

    ALTER TABLE [ref].[ConceptMap_Rekvirent_To_SOR]
        ADD CONSTRAINT [PK_ref_ConceptMap_Rekvirent_To_SOR]
        PRIMARY KEY CLUSTERED ([DW_EK_Rekvirent]);

    PRINT 'Rekeyed ref.ConceptMap_Rekvirent_To_SOR to PK(DW_EK_Rekvirent)';
END
GO

-- ---- ref.ConceptMap_Lager_To_SOR ----
IF EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ConceptMap_Lager_To_SOR]') AND type = 'U'
)
AND EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[ref].[ConceptMap_Lager_To_SOR]')
      AND name = N'PK_ref_ConceptMap_Lager_To_SOR'
      AND type = 'PK'
)
BEGIN
    ;WITH dups AS (
        SELECT ROW_NUMBER() OVER (
            PARTITION BY [DW_EK_Lager]
            ORDER BY CASE [Relationship]
                        WHEN 'equal'      THEN 1
                        WHEN 'equivalent' THEN 2
                        WHEN 'narrower'   THEN 3
                        WHEN 'inexact'    THEN 4
                        ELSE 5
                     END,
                     [DW_Timestamp_Update] DESC
        ) AS _rn
        FROM [ref].[ConceptMap_Lager_To_SOR]
    )
    DELETE FROM dups WHERE _rn > 1;

    ALTER TABLE [ref].[ConceptMap_Lager_To_SOR]
        DROP CONSTRAINT [PK_ref_ConceptMap_Lager_To_SOR];

    ALTER TABLE [ref].[ConceptMap_Lager_To_SOR]
        ADD CONSTRAINT [PK_ref_ConceptMap_Lager_To_SOR]
        PRIMARY KEY CLUSTERED ([DW_EK_Lager]);

    PRINT 'Rekeyed ref.ConceptMap_Lager_To_SOR to PK(DW_EK_Lager)';
END
GO

-- ---- ref.ConceptMap_EAN_To_SOR ----
IF EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ConceptMap_EAN_To_SOR]') AND type = 'U'
)
AND EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[ref].[ConceptMap_EAN_To_SOR]')
      AND name = N'PK_ref_ConceptMap_EAN_To_SOR'
      AND type = 'PK'
)
BEGIN
    ;WITH dups AS (
        SELECT ROW_NUMBER() OVER (
            PARTITION BY [DW_EK_EAN]
            ORDER BY CASE [Relationship]
                        WHEN 'equal'      THEN 1
                        WHEN 'equivalent' THEN 2
                        WHEN 'narrower'   THEN 3
                        WHEN 'inexact'    THEN 4
                        ELSE 5
                     END,
                     [DW_Timestamp_Update] DESC
        ) AS _rn
        FROM [ref].[ConceptMap_EAN_To_SOR]
    )
    DELETE FROM dups WHERE _rn > 1;

    ALTER TABLE [ref].[ConceptMap_EAN_To_SOR]
        DROP CONSTRAINT [PK_ref_ConceptMap_EAN_To_SOR];

    ALTER TABLE [ref].[ConceptMap_EAN_To_SOR]
        ADD CONSTRAINT [PK_ref_ConceptMap_EAN_To_SOR]
        PRIMARY KEY CLUSTERED ([DW_EK_EAN]);

    PRINT 'Rekeyed ref.ConceptMap_EAN_To_SOR to PK(DW_EK_EAN)';
END
GO

-- ---- ref.ConceptMap_Okosted_To_SOR ----
-- Guard on the join column itself, not just the table+PK: on databases where the
-- later 20260529 already renamed DW_EK_Organisation_Source -> DW_EK_Okosted this
-- migration can still be pending (it has an earlier date-prefix). The rekey is
-- moot on such databases anyway — 20260604 drops this table.
--
-- The rekey body runs via dynamic SQL because a static reference to
-- [DW_EK_Organisation_Source] is bound at batch-compile time (before the IF is
-- evaluated), so it would still fail with "Invalid column name" when the column
-- is absent. Deferring compilation to EXEC time — reached only when the guards
-- pass — keeps it safe (same approach as 20260604).
IF EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]') AND type = 'U'
)
AND EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]')
      AND name = N'PK_ref_ConceptMap_Okosted_To_SOR'
      AND type = 'PK'
)
AND EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]')
      AND name = N'DW_EK_Organisation_Source'
)
BEGIN
    EXEC (N'
        ;WITH dups AS (
            SELECT ROW_NUMBER() OVER (
                PARTITION BY [DW_EK_Organisation_Source]
                ORDER BY CASE [Relationship]
                            WHEN ''equal''      THEN 1
                            WHEN ''equivalent'' THEN 2
                            WHEN ''narrower''   THEN 3
                            WHEN ''inexact''    THEN 4
                            ELSE 5
                         END,
                         [DW_Timestamp_Update] DESC
            ) AS _rn
            FROM [ref].[ConceptMap_Okosted_To_SOR]
        )
        DELETE FROM dups WHERE _rn > 1;

        ALTER TABLE [ref].[ConceptMap_Okosted_To_SOR]
            DROP CONSTRAINT [PK_ref_ConceptMap_Okosted_To_SOR];

        ALTER TABLE [ref].[ConceptMap_Okosted_To_SOR]
            ADD CONSTRAINT [PK_ref_ConceptMap_Okosted_To_SOR]
            PRIMARY KEY CLUSTERED ([DW_EK_Organisation_Source]);
    ');

    PRINT 'Rekeyed ref.ConceptMap_Okosted_To_SOR to PK(DW_EK_Organisation_Source)';
END
GO
