MERGE [arc].[sydax_finanskladde] AS tgt
USING [ext].[sydax_finanskladde_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[FinanskladdeId] = src.[FinanskladdeId] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[Kladdenummer] = src.[Kladdenummer],
        tgt.[Kladdetekst] = src.[Kladdetekst],
        tgt.[Kladdenavn] = src.[Kladdenavn],
        tgt.[ErLukket] = src.[ErLukket],
        tgt.[ref_LedgerjournaltableId] = src.[ref_LedgerjournaltableId],
        tgt.[RunId] = src.[RunId],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[DokumentId] = src.[DokumentId],
        tgt.[DataAreaId] = src.[DataAreaId],
        tgt.[Maskinnummer] = src.[Maskinnummer],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [FinanskladdeId], [StartDateTime], [Kladdenummer], [Kladdetekst], [Kladdenavn], [ErLukket], [ref_LedgerjournaltableId], [RunId], [EndDateTime], [RdwTs], [DokumentId], [DataAreaId], [Maskinnummer], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[FinanskladdeId], src.[StartDateTime], src.[Kladdenummer], src.[Kladdetekst], src.[Kladdenavn], src.[ErLukket], src.[ref_LedgerjournaltableId], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[DokumentId], src.[DataAreaId], src.[Maskinnummer], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
