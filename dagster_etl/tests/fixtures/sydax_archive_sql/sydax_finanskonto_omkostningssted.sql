MERGE [arc].[sydax_finanskonto_omkostningssted] AS tgt
USING [ext].[sydax_finanskonto_omkostningssted_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[FinanskontoOmkostningsstedid] = src.[FinanskontoOmkostningsstedid] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[FinanskontoId] = src.[FinanskontoId],
        tgt.[Omkostningsstedid] = src.[Omkostningsstedid],
        tgt.[ref_LedgerTableId] = src.[ref_LedgerTableId],
        tgt.[RunId] = src.[RunId],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [FinanskontoOmkostningsstedid], [StartDateTime], [FinanskontoId], [Omkostningsstedid], [ref_LedgerTableId], [RunId], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[FinanskontoOmkostningsstedid], src.[StartDateTime], src.[FinanskontoId], src.[Omkostningsstedid], src.[ref_LedgerTableId], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
