MERGE [arc].[finanskonto_omkostningssted_sydax] AS tgt
USING [ext].[finanskonto_omkostningssted_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[FinanskontoId] = src.[FinanskontoId] AND tgt.[Omkostningsstedid] = src.[Omkostningsstedid]
WHEN MATCHED THEN UPDATE SET
        tgt.[FinanskontoOmkostningsstedid] = src.[FinanskontoOmkostningsstedid],
        tgt.[ref_LedgerTableId] = src.[ref_LedgerTableId],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [FinanskontoId], [Omkostningsstedid], [FinanskontoOmkostningsstedid], [ref_LedgerTableId], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[FinanskontoId], src.[Omkostningsstedid], src.[FinanskontoOmkostningsstedid], src.[ref_LedgerTableId], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
