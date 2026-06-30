MERGE [arc].[sydax_finanspostering_kreditortrans_relation] AS tgt
USING [ext].[sydax_finanspostering_kreditortrans_relation_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_LedgertransId] = src.[ref_LedgertransId] AND tgt.[KreditortransaktionId] = src.[KreditortransaktionId]
WHEN MATCHED THEN UPDATE SET
        tgt.[FinansposteringKreditortransRelationId] = src.[FinansposteringKreditortransRelationId],
        tgt.[FinansposteringId] = src.[FinansposteringId],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_LedgertransId], [KreditortransaktionId], [FinansposteringKreditortransRelationId], [FinansposteringId], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_LedgertransId], src.[KreditortransaktionId], src.[FinansposteringKreditortransRelationId], src.[FinansposteringId], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
