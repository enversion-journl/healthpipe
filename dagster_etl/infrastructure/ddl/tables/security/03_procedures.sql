-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Security Procedures
-- PopulateBrugerOrganisation - Expand user affiliations to all child orgs
-- ============================================================================

CREATE OR ALTER PROCEDURE [sec].[PopulateBrugerOrganisation]
    @Bruger NVARCHAR(128) = NULL,
    @TruncateFirst BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();

    -- Optional: Clean start
    IF @TruncateFirst = 1
    BEGIN
        TRUNCATE TABLE [sec].[BrugerOrganisation];
        PRINT 'Truncated sec.BrugerOrganisation';
    END
    ELSE IF @Bruger IS NOT NULL
    BEGIN
        -- Delete existing rows for specific user only
        DELETE FROM [sec].[BrugerOrganisation] WHERE [Bruger] = @Bruger;
        PRINT 'Deleted existing rows for user: ' + @Bruger;
    END

    -- ========================================================================
    -- PART 1: Direct affiliations (always included)
    -- ========================================================================
    ;WITH DirectAffiliations AS (
        SELECT
            po.PersonaleOrgId,
            po.Bruger,
            po.CodeSOR,
            po.GivAdgangTilUnderorganisationer,
            org.DW_EK_Organisation,
            org.Code,
            org.Name AS OrgName
        FROM [sec].[PersonaleOrg] po
        INNER JOIN [dim].[Organisation] org
            ON org.Code = po.CodeSOR
        WHERE (@Bruger IS NULL OR po.Bruger = @Bruger)
            AND (po.GyldigFra IS NULL OR po.GyldigFra <= GETDATE())
            AND (po.GyldigTil IS NULL OR po.GyldigTil >= GETDATE())
    )
    -- Insert direct affiliations (these are always added)
    INSERT INTO [sec].[BrugerOrganisation] (
        [Bruger],
        [DW_EK_Organisation],
        [PersonaleOrgId],
        [IsDirectAffiliation],
        [HierarchyLevel],
        [CreatedDate]
    )
    SELECT DISTINCT
        Bruger,
        DW_EK_Organisation,
        PersonaleOrgId,
        1,  -- IsDirectAffiliation = true
        0,  -- HierarchyLevel = 0 (direct)
        GETDATE()
    FROM DirectAffiliations
    WHERE NOT EXISTS (
        SELECT 1 FROM [sec].[BrugerOrganisation] bo
        WHERE bo.Bruger = DirectAffiliations.Bruger
        AND bo.DW_EK_Organisation = DirectAffiliations.DW_EK_Organisation
    );

    SET @RowsInserted = @@ROWCOUNT;
    PRINT 'Inserted ' + CAST(@RowsInserted AS VARCHAR(10)) + ' direct affiliations';

    -- ========================================================================
    -- PART 2: Child organizations (only for GivAdgangTilUnderorganisationer = 1)
    -- ========================================================================
    ;WITH
    -- Get direct affiliations that should include children
    DirectWithChildren AS (
        SELECT
            po.PersonaleOrgId,
            po.Bruger,
            org.DW_EK_Organisation
        FROM [sec].[PersonaleOrg] po
        INNER JOIN [dim].[Organisation] org
            ON org.Code = po.CodeSOR
        WHERE (@Bruger IS NULL OR po.Bruger = @Bruger)
            AND po.GivAdgangTilUnderorganisationer = 1  -- Only expand if flag is set
            AND (po.GyldigFra IS NULL OR po.GyldigFra <= GETDATE())
            AND (po.GyldigTil IS NULL OR po.GyldigTil >= GETDATE())
    ),

    -- Recursively find all child organizations
    OrgHierarchy AS (
        -- Anchor: Start from direct affiliations (level 0)
        SELECT
            dwc.PersonaleOrgId,
            dwc.Bruger,
            dwc.DW_EK_Organisation AS ParentDW_EK,
            child.DW_EK_Organisation,
            1 AS HierarchyLevel
        FROM DirectWithChildren dwc
        INNER JOIN [dim].[Organisation] child
            ON child.DW_EK_Org_Parent = dwc.DW_EK_Organisation

        UNION ALL

        -- Recursive: Find children of children
        SELECT
            oh.PersonaleOrgId,
            oh.Bruger,
            oh.DW_EK_Organisation AS ParentDW_EK,
            grandchild.DW_EK_Organisation,
            oh.HierarchyLevel + 1
        FROM OrgHierarchy oh
        INNER JOIN [dim].[Organisation] grandchild
            ON grandchild.DW_EK_Org_Parent = oh.DW_EK_Organisation
        WHERE oh.HierarchyLevel < 10  -- Safety limit to prevent infinite recursion
    )

    -- Insert child organizations
    INSERT INTO [sec].[BrugerOrganisation] (
        [Bruger],
        [DW_EK_Organisation],
        [PersonaleOrgId],
        [IsDirectAffiliation],
        [HierarchyLevel],
        [CreatedDate]
    )
    SELECT DISTINCT
        Bruger,
        DW_EK_Organisation,
        PersonaleOrgId,
        0,  -- IsDirectAffiliation = false (inherited)
        MIN(HierarchyLevel),
        GETDATE()
    FROM OrgHierarchy
    WHERE NOT EXISTS (
        SELECT 1 FROM [sec].[BrugerOrganisation] bo
        WHERE bo.Bruger = OrgHierarchy.Bruger
        AND bo.DW_EK_Organisation = OrgHierarchy.DW_EK_Organisation
    )
    GROUP BY Bruger, DW_EK_Organisation, PersonaleOrgId
    OPTION (MAXRECURSION 10);

    DECLARE @ChildRowsInserted INT = @@ROWCOUNT;
    SET @RowsInserted = @RowsInserted + @ChildRowsInserted;
    PRINT 'Inserted ' + CAST(@ChildRowsInserted AS VARCHAR(10)) + ' child organisation affiliations';

    -- Log results
    DECLARE @Duration INT = DATEDIFF(MILLISECOND, @StartTime, GETDATE());
    PRINT '';
    PRINT 'PopulateBrugerOrganisation completed:';
    PRINT '  Total rows inserted: ' + CAST(@RowsInserted AS VARCHAR(10));
    PRINT '  Duration: ' + CAST(@Duration AS VARCHAR(10)) + ' ms';

    -- Return summary
    SELECT
        @RowsInserted AS RowsInserted,
        @Duration AS DurationMs,
        (SELECT COUNT(DISTINCT Bruger) FROM sec.BrugerOrganisation) AS UniqueUsers,
        (SELECT COUNT(DISTINCT DW_EK_Organisation) FROM sec.BrugerOrganisation) AS UniqueOrganisations;
END
GO
